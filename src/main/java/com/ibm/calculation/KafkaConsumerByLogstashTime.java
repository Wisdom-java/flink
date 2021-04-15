package com.ibm.calculation;

import com.ibm.utils.Constants;
import com.ibm.utils.DateUtil;
import com.ibm.vo.AccessLogBack;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.json.JSONObject;
import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaConsumerByLogstashTime {
    //cm bucket map
    private static final Map<String,String> cuBucketMap = new HashMap<>();

    //init cmBucketMap
    static {
        cuBucketMap.put("cu-cn-north00001-compute","cu-cn-north00001-compute");
        cuBucketMap.put("cu-cn-north00001-devci","cu-cn-north00001-devci");
        cuBucketMap.put("cu-cn-north00001-ic1","cu-cn-north00001-ic1");
        cuBucketMap.put("cu-cn-north00001-ic2","cu-cn-north00001-ic2");
        cuBucketMap.put("cu-cn-north00001-ic3","cu-cn-north00001-ic3");
        cuBucketMap.put("cu-cn-north00001-ic4","cu-cn-north00001-ic4");
        cuBucketMap.put("cu-cn-north00001","cu-cn-north00001");
        cuBucketMap.put("cu-cn-north00001-report","cu-cn-north00001-report");
        cuBucketMap.put("cu-cn-north00001-compliance","cu-cn-north00001-compliance");
        cuBucketMap.put("cu-cn-north00002","cu-cn-north00002");
        cuBucketMap.put("cu-cn-north00002-devci","cu-cn-north00002-devci");
    }

    // setting the kafka start consumer start_time
    private static final String startTime = "2020-12-27 18:00:00";

    // access_log version
    private static final String accessLogVersion = "v4_";

    // access_log site
    private static final String getAccessLogSite = "lf_";

    //access_log suffix
    private static final String suffix = ".w3c.log";

    // date format
    private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) {
        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.enableCheckpointing(5000); // checkpoint every 5000 msecs
        Properties properties = new Properties();
        //kafka  setting
        properties.setProperty("bootstrap.servers", Constants.KafkaAddress.CU_LF_KAFKA_PROD.getValue());
        //properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("group.id", "repair_data");

        String accessLogTitle = Constants.AccessLogField.vault_name + "," + Constants.AccessLogField.server_name + "," +
                Constants.AccessLogField.client_id + "," + Constants.AccessLogField.user_id + "," +
                Constants.AccessLogField.remote_address + "," + Constants.AccessLogField.request_method + "," +
                Constants.AccessLogField.protocol + "," + Constants.AccessLogField.object_length + "," +
                Constants.AccessLogField.object_name + "," + Constants.AccessLogField.time_start + "," +
                Constants.AccessLogField.time_finish + "," + Constants.AccessLogField.user_agent + "," +
                Constants.AccessLogField.status + "," + Constants.AccessLogField.midstream_error + "," +
                Constants.AccessLogField.error_code + "," + Constants.AccessLogField.request_latency + "," +
                Constants.AccessLogField.response_length + "," + Constants.AccessLogField.turn_around_time + "," +
                Constants.AccessLogField.error_message ;

        SinkFunction<AccessLogBack> sinkFunction = new SinkFunction<AccessLogBack>(){

            @Override
            public void invoke(AccessLogBack accessLogBack, Context context) throws Exception {

                // folder name
                String folderName = accessLogBack.getFolderName();
                String dateName = accessLogBack.getDataName();
                String vault_name = accessLogBack.getVault_name();
                if (!cuBucketMap.containsKey(vault_name)){
                    vault_name = "ibm";
                }
                String filePath = "/data1/flink/"+folderName + "/" +
                        accessLogVersion + getAccessLogSite + vault_name + "_" + dateName + suffix;

                // if file not exist, touch file and set title
                File file = new File(filePath);
                File parentFile = file.getParentFile();
                if (!parentFile.exists() || !parentFile.isDirectory()){
                    parentFile.mkdirs();
                }
                if (!file.exists()){
                    FileWriter fileWriter = new FileWriter(filePath,true);
                    fileWriter.write(accessLogTitle + "\n");
                    fileWriter.close();
                }
                // writer to file
                FileWriter fileWriter = new FileWriter(filePath,true);
                fileWriter.write(accessLogBack.toString() + "\n");
                fileWriter.close();
            }
        };
        try {
            FlinkKafkaConsumer<String> stringFlinkKafkaConsumer = new FlinkKafkaConsumer<>(Constants.cmAccessLogTopic.cu_flink_lf_ca_access.getTopic(),
                    new SimpleStringSchema(), properties);
            //FlinkKafkaConsumer<String> stringFlinkKafkaConsumer = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties);
            //long startTimeMi = simpleDateFormat.parse(startTime).getTime();
            //stringFlinkKafkaConsumer.setStartFromTimestamp(startTimeMi);
            SingleOutputStreamOperator<AccessLogBack> accessLogBackSingleOutputStreamOperator = env.
                    addSource(stringFlinkKafkaConsumer).flatMap(new FlatMapFunction<String, AccessLogBack>() {
                @Override
                public void flatMap(String s, Collector<AccessLogBack> collector) throws Exception {
                    //2021-01-25T15:08:02.707Z
                    JSONObject jsonObject = new JSONObject(s);
                    String timestamp = jsonObject.getString("@timestamp");
                    AccessLogBack accessLogBack = new AccessLogBack();
                    String dataName = DateUtil.dataName(timestamp);
                    accessLogBack.setDataName(dataName);
                    accessLogBack.setFolderName(dataName.substring(0,10));

                    if (jsonObject.has(Constants.AccessLogField.vault_name.getField())) {
                        accessLogBack.setVault_name(jsonObject.getString(Constants.AccessLogField.vault_name.getField()));
                    }

                    if (jsonObject.has(Constants.AccessLogField.server_name.getField())) {
                        accessLogBack.setServer_name(jsonObject.getString(Constants.AccessLogField.server_name.getField()));
                    } else {
                        accessLogBack.setServer_name(Constants.AccessLogField.None.getField());
                    }

                    if (jsonObject.has(Constants.AccessLogField.client_id.getField())) {
                        accessLogBack.setClient_id(jsonObject.getString(Constants.AccessLogField.client_id.getField()));
                    } else {
                        accessLogBack.setClient_id(Constants.AccessLogField.None.getField());
                    }

                    if (jsonObject.has(Constants.AccessLogField.user_id.getField())) {
                        accessLogBack.setUser_id(jsonObject.getString(Constants.AccessLogField.user_id.getField()));
                    } else {
                        accessLogBack.setUser_id(Constants.AccessLogField.None.getField());
                    }

                    if (jsonObject.has(Constants.AccessLogField.remote_address.getField())) {
                        accessLogBack.setRemote_address(jsonObject.getString(Constants.AccessLogField.remote_address.getField()));
                    } else {
                        accessLogBack.setUser_id(Constants.AccessLogField.None.getField());
                    }

                    if (jsonObject.has(Constants.AccessLogField.request_method.getField())) {
                        accessLogBack.setRequest_method(jsonObject.getString(Constants.AccessLogField.request_method.getField()));
                    } else {
                        accessLogBack.setRequest_method(Constants.AccessLogField.None.getField());
                    }

                    if (jsonObject.has(Constants.AccessLogField.protocol.getField())) {
                        accessLogBack.setProtocol(jsonObject.getString(Constants.AccessLogField.protocol.getField()));
                    } else {
                        accessLogBack.setProtocol(Constants.AccessLogField.None.getField());
                    }

                    if (jsonObject.has(Constants.AccessLogField.object_length.getField())) {
                        accessLogBack.setObject_length(jsonObject.getString(Constants.AccessLogField.object_length.getField()));
                    } else {
                        accessLogBack.setObject_length(Constants.AccessLogField.None.getField());
                    }

                    if (jsonObject.has(Constants.AccessLogField.object_name.getField())) {
                        accessLogBack.setObject_name(jsonObject.getString(Constants.AccessLogField.object_name.getField()));
                    } else {
                        accessLogBack.setObject_name(Constants.AccessLogField.None.getField());
                    }

                    if (jsonObject.has(Constants.AccessLogField.time_start.getField())) {
                        accessLogBack.setTime_start(jsonObject.getString(Constants.AccessLogField.time_start.getField()));
                    } else {
                        accessLogBack.setTime_start(Constants.AccessLogField.None.getField());
                    }

                    if (jsonObject.has(Constants.AccessLogField.time_finish.getField())) {
                        accessLogBack.setTime_finish(jsonObject.getString(Constants.AccessLogField.time_finish.getField()));
                    } else {
                        accessLogBack.setTime_finish(Constants.AccessLogField.None.getField());
                    }

                    if (jsonObject.has(Constants.AccessLogField.user_agent.getField())) {
                        accessLogBack.setUser_agent(jsonObject.getString(Constants.AccessLogField.user_agent.getField()));
                    } else {
                        accessLogBack.setUser_agent(Constants.AccessLogField.None.getField());
                    }

                    if (jsonObject.has(Constants.AccessLogField.status.getField())) {
                        accessLogBack.setStatus(jsonObject.getString(Constants.AccessLogField.status.getField()));
                    } else {
                        accessLogBack.setStatus(Constants.AccessLogField.None.getField());
                    }

                    if (jsonObject.has(Constants.AccessLogField.midstream_error.getField())) {
                        accessLogBack.setMidstream_error(jsonObject.getString(Constants.AccessLogField.midstream_error.getField()));
                    } else {
                        accessLogBack.setMidstream_error(Constants.AccessLogField.None.getField());
                    }

                    if (jsonObject.has(Constants.AccessLogField.error_code.getField())) {
                        accessLogBack.setError_code(jsonObject.getString(Constants.AccessLogField.error_code.getField()));
                    } else {
                        accessLogBack.setError_code(Constants.AccessLogField.None.getField());
                    }

                    if (jsonObject.has(Constants.AccessLogField.request_latency.getField())) {
                        accessLogBack.setRequest_latency(jsonObject.getString(Constants.AccessLogField.request_latency.getField()));
                    } else {
                        accessLogBack.setRequest_latency(Constants.AccessLogField.None.getField());
                    }

                    if (jsonObject.has(Constants.AccessLogField.response_length.getField())) {
                        accessLogBack.setResponse_length(jsonObject.getString(Constants.AccessLogField.response_length.getField()));
                    } else {
                        accessLogBack.setResponse_length(Constants.AccessLogField.None.getField());
                    }

                    if (jsonObject.has(Constants.AccessLogField.turn_around_time.getField())) {
                        accessLogBack.setTurn_around_time(jsonObject.getString(Constants.AccessLogField.object_length.getField()));
                    } else {
                        accessLogBack.setTurn_around_time(Constants.AccessLogField.None.getField());
                    }

                    if (jsonObject.has(Constants.AccessLogField.error_message.getField())) {
                        accessLogBack.setError_code(jsonObject.getString(Constants.AccessLogField.error_message.getField()));
                    } else {
                        accessLogBack.setError_code(Constants.AccessLogField.None.getField());
                    }
                    collector.collect(accessLogBack);
                }
            }).setParallelism(4);
            accessLogBackSingleOutputStreamOperator.addSink(sinkFunction).setParallelism(1);
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            env.execute("Kafka_access_log");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
