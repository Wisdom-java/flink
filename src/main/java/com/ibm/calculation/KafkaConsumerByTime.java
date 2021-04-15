package com.ibm.calculation;

import com.ibm.utils.Constants;
import com.ibm.utils.DateUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.json.JSONObject;
import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaConsumerByTime {
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
    private static final String accessLogVersion = "v3_";

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
        properties.setProperty("group.id", "repair_data");

        String accessLogTitle = Constants.AccessLogField.vault_name + "," + Constants.AccessLogField.server_name + "," +
                Constants.AccessLogField.remote_address + "," + Constants.AccessLogField.request_method + "," +
                Constants.AccessLogField.protocol + "," + Constants.AccessLogField.object_length + "," +
                Constants.AccessLogField.object_name + "," + Constants.AccessLogField.time_start + "," +
                Constants.AccessLogField.time_finish + "," + Constants.AccessLogField.user_agent + "," +
                Constants.AccessLogField.status + "," + Constants.AccessLogField.midstream_error + "," +
                Constants.AccessLogField.error_code + "," + Constants.AccessLogField.request_latency + "," +
                Constants.AccessLogField.response_length + "," + Constants.AccessLogField.turn_around_time + "," +
                Constants.AccessLogField.error_message ;

        SinkFunction<String> sinkFunction = new SinkFunction<String>(){

            @Override
            public void invoke(String value, Context context) throws Exception {
                JSONObject jsonObject = new JSONObject(value);
                long timestamp_finish = jsonObject.getLong(Constants.AccessLogField.timestamp_finish.getField());
                String formatDate = simpleDateFormat.format(timestamp_finish);
                String dateName = DateUtil.dataName(timestamp_finish);
                // folder name
                String folderName = formatDate.substring(0,10);

                StringBuffer accessLogSb = new StringBuffer();

                String vault_name = "ibm";

                if (jsonObject.has(Constants.AccessLogField.vault_name.getField())){
                    accessLogSb.append(jsonObject.getString(Constants.AccessLogField.vault_name.getField()) + ",");
                    String vault_name_key = jsonObject.getString(Constants.AccessLogField.vault_name.getField());
                    if (cuBucketMap.containsKey(vault_name_key)){
                        vault_name = cuBucketMap.get(vault_name_key);
                    }
                }else {
                    accessLogSb.append(Constants.AccessLogField.None + ",");
                }

                if (jsonObject.has(Constants.AccessLogField.server_name.getField())){
                    accessLogSb.append(jsonObject.getString(Constants.AccessLogField.server_name.getField()) + ",");
                }else {
                    accessLogSb.append(Constants.AccessLogField.None + ",");
                }

                if (jsonObject.has(Constants.AccessLogField.remote_address.getField())){
                    accessLogSb.append(jsonObject.getString(Constants.AccessLogField.remote_address.getField()) + ",");
                }else {
                    accessLogSb.append(Constants.AccessLogField.None + ",");
                }

                if (jsonObject.has(Constants.AccessLogField.request_method.getField())){
                    accessLogSb.append(jsonObject.getString(Constants.AccessLogField.request_method.getField()) + ",");
                }else {
                    accessLogSb.append(Constants.AccessLogField.None + ",");
                }

                if (jsonObject.has(Constants.AccessLogField.protocol.getField())){
                    accessLogSb.append(jsonObject.getString(Constants.AccessLogField.protocol.getField()) + ",");
                }else {
                    accessLogSb.append(Constants.AccessLogField.None + ",");
                }

                if (jsonObject.has(Constants.AccessLogField.object_length.getField())){
                    accessLogSb.append(jsonObject.getString(Constants.AccessLogField.object_length.getField()) + ",");
                }else {
                    accessLogSb.append(Constants.AccessLogField.None + ",");
                }

                if (jsonObject.has(Constants.AccessLogField.object_name.getField())){
                    accessLogSb.append(jsonObject.getString(Constants.AccessLogField.object_name.getField()) + ",");
                }else {
                    accessLogSb.append(Constants.AccessLogField.None + ",");
                }

                if (jsonObject.has(Constants.AccessLogField.time_start.getField())){
                    accessLogSb.append(jsonObject.getString(Constants.AccessLogField.time_start.getField()) + ",");
                }else {
                    accessLogSb.append(Constants.AccessLogField.None + ",");
                }

                if (jsonObject.has(Constants.AccessLogField.time_finish.getField())){
                    accessLogSb.append(jsonObject.getString(Constants.AccessLogField.time_finish.getField()) + ",");
                }else {
                    accessLogSb.append(Constants.AccessLogField.None+ ",");
                }

                if (jsonObject.has(Constants.AccessLogField.user_agent.getField())){
                    accessLogSb.append(jsonObject.getString(Constants.AccessLogField.user_agent.getField()) + ",");
                }else {
                    accessLogSb.append(Constants.AccessLogField.None + ",");
                }

                if (jsonObject.has(Constants.AccessLogField.status.getField())){
                    accessLogSb.append(jsonObject.getString(Constants.AccessLogField.status.getField()) + ",");
                }else {
                    accessLogSb.append(Constants.AccessLogField.None + ",");
                }

                if (jsonObject.has(Constants.AccessLogField.midstream_error.getField())){
                    accessLogSb.append(jsonObject.getString(Constants.AccessLogField.midstream_error.getField()) + ",");
                }else {
                    accessLogSb.append(Constants.AccessLogField.None + ",");
                }

                if (jsonObject.has(Constants.AccessLogField.error_code.getField())){
                    accessLogSb.append(jsonObject.getString(Constants.AccessLogField.error_code.getField()) + ",");
                }else {
                    accessLogSb.append(Constants.AccessLogField.None + ",");
                }

                if (jsonObject.has(Constants.AccessLogField.request_latency.getField())){
                    accessLogSb.append(jsonObject.getString(Constants.AccessLogField.request_latency.getField()) + ",");
                }else {
                    accessLogSb.append(Constants.AccessLogField.None + ",");
                }

                if (jsonObject.has(Constants.AccessLogField.response_length.getField())){
                    accessLogSb.append(jsonObject.getString(Constants.AccessLogField.response_length.getField()) + ",");
                }else {
                    accessLogSb.append(Constants.AccessLogField.None + ",");
                }

                if (jsonObject.has(Constants.AccessLogField.turn_around_time.getField())){
                    accessLogSb.append(jsonObject.getString(Constants.AccessLogField.object_length.getField()) + ",");
                }else {
                    accessLogSb.append(Constants.AccessLogField.None + ",");
                }

                if (jsonObject.has(Constants.AccessLogField.error_message.getField())){
                    accessLogSb.append(jsonObject.getString(Constants.AccessLogField.error_message.getField()));
                }else {
                    accessLogSb.append(Constants.AccessLogField.None);
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
                fileWriter.write(accessLogSb.toString() + "\n");
                fileWriter.close();
            }
        };
        try {
            FlinkKafkaConsumer<String> stringFlinkKafkaConsumer = new FlinkKafkaConsumer<>(Constants.cmAccessLogTopic.cu_flink_lf_ca_access.getTopic(), new SimpleStringSchema(), properties);
            //long startTimeMi = simpleDateFormat.parse(startTime).getTime();
            //stringFlinkKafkaConsumer.setStartFromTimestamp(startTimeMi);
            DataStreamSource<String> dataStreamSource = env.
                    addSource(stringFlinkKafkaConsumer);
            dataStreamSource.addSink(sinkFunction);
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
