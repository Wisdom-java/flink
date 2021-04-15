package com.ibm.calculation;
import com.ibm.utils.Constants;
import com.ibm.utils.DateUtil;
import com.ibm.vo.AccessLog;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.json.JSONObject;
import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.*;

public class CMKafkaConsumerByTimeBack {

    //cm bucket map
    private static final Map<String,String> cmBucketMap = new HashMap<>();
    //init cmBucketMap
    static {
        cmBucketMap.put("cm-cn-central-00001-compute","cm-cn-central-00001-compute");
        cmBucketMap.put("cm-cn-central-00001-devci","cm-cn-central-00001-devci");
        cmBucketMap.put("cm-cn-central-00001-ic1","cm-cn-central-00001-ic1");
        cmBucketMap.put("cm-cn-central-00001-ic2","cm-cn-central-00001-ic2");
        cmBucketMap.put("cm-cn-central-00001-ic3","cm-cn-central-00001-ic3");
        cmBucketMap.put("cm-cn-central-00001-ic4","cm-cn-central-00001-ic4");
        cmBucketMap.put("cm-cn-central-00001","cm-cn-central-00001");
    }

    // setting the kafka start consumer start_time
    private static final String startTime = "2021-01-20 12:00:00";

    // access_log version
    private static final String accessLogVersion = "v3_";

    // access_log site
    private static final String getAccessLogSite = "tj_";

    //access_log suffix
    private static final String suffix = ".w3c.log";

    // date format
    private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * hearttime 10s hearttimeout 50s
     * @param args
     */
    public static void main(String[] args) {
        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.enableCheckpointing(5000); // checkpoint every 5000 msecs
        //event time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //set auto update watermark time 100ms ,if not set is 200ms
        //env.getConfig().setAutoWatermarkInterval(100);
        Properties properties = new Properties();
        //kafka  setting
        //properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("bootstrap.servers", Constants.KafkaAddress.CM_TJ_KAFKA_PROD.getValue());
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

        SinkFunction<List<AccessLog>> sinkFunction = new SinkFunction<List<AccessLog>>(){
            @Override
            public void invoke(List<AccessLog> value, Context context) throws Exception {

                for (AccessLog accessLog : value){
                    long timestamp_finish = accessLog.getTimestamp_finish();
                    String formatDate = simpleDateFormat.format(timestamp_finish);
                    String dateName = DateUtil.dataName(timestamp_finish);
                    // folder name
                    String folderName = formatDate.substring(0,10);
                    String vault_name = accessLog.getVault_name();
                    if (!cmBucketMap.containsKey(vault_name)){
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
                    fileWriter.write(accessLog.toString() + "\n");
                    fileWriter.close();
                }
            }
        };
        try {
            // flink-tj-ca_access
/*            FlinkKafkaConsumer<ObjectNode> objectNodeFlinkKafkaConsumer = new FlinkKafkaConsumer<>(Constants.cmAccessLogTopic.cm_flink_tj_ca_access.getTopic(),
                    new JSONKeyValueDeserializationSchema(false), properties);*/
            FlinkKafkaConsumer<String> stringFlinkKafkaConsumer = new FlinkKafkaConsumer<>(Constants.cmAccessLogTopic.cm_flink_tj_ca_access.getTopic(),
                    new SimpleStringSchema(), properties);
             // setting start consumer time
            //long startTimeMi = simpleDateFormat.parse(startTime).getTime();
            //stringFlinkKafkaConsumer.setStartFromTimestamp(startTimeMi);
            DataStreamSource<String> stringDataStreamSource = env.addSource(stringFlinkKafkaConsumer);
            SingleOutputStreamOperator<AccessLog> singleOutputStreamOperator = stringDataStreamSource.flatMap(new FlatMapFunction<String, AccessLog>() {
                @Override
                public void flatMap(String s, Collector<AccessLog> collector) throws Exception {
                    JSONObject jsonObject = new JSONObject(s);
                    AccessLog accessLog = new AccessLog();
                    if (jsonObject.has(Constants.AccessLogField.vault_name.getField())) {
                        accessLog.setVault_name(jsonObject.getString(Constants.AccessLogField.vault_name.getField()));
                    }

                    if (jsonObject.has(Constants.AccessLogField.server_name.getField())) {
                        accessLog.setServer_name(jsonObject.getString(Constants.AccessLogField.server_name.getField()));
                    } else {
                        accessLog.setServer_name(Constants.AccessLogField.None.getField());
                    }

                    if (jsonObject.has(Constants.AccessLogField.client_id.getField())) {
                        accessLog.setClient_id(jsonObject.getString(Constants.AccessLogField.client_id.getField()));
                    } else {
                        accessLog.setClient_id(Constants.AccessLogField.None.getField());
                    }

                    if (jsonObject.has(Constants.AccessLogField.user_id.getField())) {
                        accessLog.setUser_id(jsonObject.getString(Constants.AccessLogField.user_id.getField()));
                    } else {
                        accessLog.setUser_id(Constants.AccessLogField.None.getField());
                    }

                    if (jsonObject.has(Constants.AccessLogField.remote_address.getField())) {
                        accessLog.setRemote_address(jsonObject.getString(Constants.AccessLogField.remote_address.getField()));
                    } else {
                        accessLog.setUser_id(Constants.AccessLogField.None.getField());
                    }

                    if (jsonObject.has(Constants.AccessLogField.request_method.getField())) {
                        accessLog.setRequest_method(jsonObject.getString(Constants.AccessLogField.request_method.getField()));
                    } else {
                        accessLog.setRequest_method(Constants.AccessLogField.None.getField());
                    }

                    if (jsonObject.has(Constants.AccessLogField.protocol.getField())) {
                        accessLog.setProtocol(jsonObject.getString(Constants.AccessLogField.protocol.getField()));
                    } else {
                        accessLog.setProtocol(Constants.AccessLogField.None.getField());
                    }

                    if (jsonObject.has(Constants.AccessLogField.object_length.getField())) {
                        accessLog.setObject_length(jsonObject.getString(Constants.AccessLogField.object_length.getField()));
                    } else {
                        accessLog.setObject_length(Constants.AccessLogField.None.getField());
                    }

                    if (jsonObject.has(Constants.AccessLogField.object_name.getField())) {
                        accessLog.setObject_name(jsonObject.getString(Constants.AccessLogField.object_name.getField()));
                    } else {
                        accessLog.setObject_name(Constants.AccessLogField.None.getField());
                    }

                    if (jsonObject.has(Constants.AccessLogField.time_start.getField())) {
                        accessLog.setTime_start(jsonObject.getString(Constants.AccessLogField.time_start.getField()));
                    } else {
                        accessLog.setTime_start(Constants.AccessLogField.None.getField());
                    }

                    if (jsonObject.has(Constants.AccessLogField.time_finish.getField())) {
                        accessLog.setTime_finish(jsonObject.getString(Constants.AccessLogField.time_finish.getField()));
                    } else {
                        accessLog.setTime_finish(Constants.AccessLogField.None.getField());
                    }

                    if (jsonObject.has(Constants.AccessLogField.user_agent.getField())) {
                        accessLog.setUser_agent(jsonObject.getString(Constants.AccessLogField.user_agent.getField()));
                    } else {
                        accessLog.setUser_agent(Constants.AccessLogField.None.getField());
                    }

                    if (jsonObject.has(Constants.AccessLogField.status.getField())) {
                        accessLog.setStatus(jsonObject.getString(Constants.AccessLogField.status.getField()));
                    } else {
                        accessLog.setStatus(Constants.AccessLogField.None.getField());
                    }

                    if (jsonObject.has(Constants.AccessLogField.midstream_error.getField())) {
                        accessLog.setMidstream_error(jsonObject.getString(Constants.AccessLogField.midstream_error.getField()));
                    } else {
                        accessLog.setMidstream_error(Constants.AccessLogField.None.getField());
                    }

                    if (jsonObject.has(Constants.AccessLogField.error_code.getField())) {
                        accessLog.setError_code(jsonObject.getString(Constants.AccessLogField.error_code.getField()));
                    } else {
                        accessLog.setError_code(Constants.AccessLogField.None.getField());
                    }

                    if (jsonObject.has(Constants.AccessLogField.request_latency.getField())) {
                        accessLog.setRequest_latency(jsonObject.getString(Constants.AccessLogField.request_latency.getField()));
                    } else {
                        accessLog.setRequest_latency(Constants.AccessLogField.None.getField());
                    }

                    if (jsonObject.has(Constants.AccessLogField.response_length.getField())) {
                        accessLog.setResponse_length(jsonObject.getString(Constants.AccessLogField.response_length.getField()));
                    } else {
                        accessLog.setResponse_length(Constants.AccessLogField.None.getField());
                    }

                    if (jsonObject.has(Constants.AccessLogField.turn_around_time.getField())) {
                        accessLog.setTurn_around_time(jsonObject.getString(Constants.AccessLogField.object_length.getField()));
                    } else {
                        accessLog.setTurn_around_time(Constants.AccessLogField.None.getField());
                    }

                    if (jsonObject.has(Constants.AccessLogField.error_message.getField())) {
                        accessLog.setError_code(jsonObject.getString(Constants.AccessLogField.error_message.getField()));
                    } else {
                        accessLog.setError_code(Constants.AccessLogField.None.getField());
                    }
                    accessLog.setTimestamp_finish(jsonObject.getLong(Constants.AccessLogField.timestamp_finish.getField()));
                    collector.collect(accessLog);
                }
            });
            SingleOutputStreamOperator<List<AccessLog>> listSingleOutputStreamOperator = singleOutputStreamOperator
                    .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<AccessLog>(Time.seconds(900)) {
                @Override
                public long extractTimestamp(AccessLog accessLog) {
                    return accessLog.getTimestamp_finish();
                }
            })
                    .keyBy("vault_name")
                    .timeWindow(Time.milliseconds(50))
                    .apply(new WindowFunction<AccessLog, List<AccessLog>, Tuple, TimeWindow>() {
                        @Override
                        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<AccessLog> iterable, Collector<List<AccessLog>> collector) throws Exception {
                            List<AccessLog> list = IteratorUtils.toList(iterable.iterator());
                            Collections.sort(list, new Comparator<AccessLog>() {
                                @Override
                                public int compare(AccessLog o1, AccessLog o2) {
                                    if (o1.getTimestamp_finish() > o2.getTimestamp_finish()) {
                                        return 1;
                                    } else if (o1.getTimestamp_finish() == o2.getTimestamp_finish()) {
                                        return 0;
                                    }
                                    return -1;
                                }
                            });
                            collector.collect(list);
                        }
                    });
            //apply.print();
            listSingleOutputStreamOperator.addSink(sinkFunction);
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