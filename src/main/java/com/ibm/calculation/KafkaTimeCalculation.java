package com.ibm.calculation;


import com.ibm.utils.DateUtil;
import com.ibm.utils.StringUtil;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.json.JSONObject;

import java.io.*;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Properties;

public class KafkaTimeCalculation {

    //文件时间间隔 单位分钟
    private static final int timeGap = 5;

    //开始时间
    private static final String startTime = "2020-08-28 10:00:00";

    //时间格式
    private static final String timeFormat = "yyyy-MM-dd HH:mm:ss";

    //S3路径
    private static final String s3SinkPath = "s3://cm_ac_log_ibm_backup/";

    public static void main(String[] args) {

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.enableCheckpointing(5000); // checkpoint every 5000 msecs
        Properties properties = new Properties();
        //kafka  配置
        //properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("bootstrap.servers",
                "172.18.140.3:9092," +
                "172.18.140.4:9092," +
                "172.18.140.5:9092," +
                "172.18.140.12:9092," +
                "172.18.140.13:9092," +
                "172.18.140.54:9092," +
                "172.18.140.62:9092," +
                "172.18.140.63:9092," +
                "172.18.140.72:9092");
        // only required for Kafka 0.8
        //zookeeper 配置
        //properties.setProperty("zookeeper.connect", "localhost:2181");
        //topic 设置
        properties.setProperty("group.id", "flink-kafka-demo");

        SinkFunction<String> ts = new SinkFunction<String>(){
            boolean flag = true;
            long[] time = null; //开始时间，结束时间数组
            String fileName = ""; //文件名
            @Override
            public void invoke(String value, Context context) throws Exception {
                JSONObject jsonObject = new JSONObject(value);
                //时间格式
                //当前文件时间（时间戳）
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat(timeFormat);
                BigDecimal bdt=new BigDecimal((Double) (jsonObject.get("timestamp_start")));
                long currentFileTime = bdt.longValue();

                if (flag){
                    time = DateUtil.getTime(startTime, timeGap);
                    fileName = startTime;
                    flag = false;
                }
                if (currentFileTime < time[0]){
                    return;
                }
                while(currentFileTime > time[1]){
                    if (currentFileTime > time[1]){ //创建新文件存放数据
                        if ((currentFileTime - time[1]) > timeGap * 60 * 1000 ){
                            fileName = simpleDateFormat.format(currentFileTime).substring(0,17)+"00";
                            time = DateUtil.getTime(fileName, timeGap);
                        }else {
                            fileName = simpleDateFormat.format(time[1]).substring(0,17)+"00";
                            time = DateUtil.getTime(fileName, timeGap);
                        }
                    }
                }

                //文件存放路径
                //FileWriter writer = new FileWriter("/Users/wisdom/Desktop/flink/"+fileName, true);
                //FileWriter writer = new FileWriter("/data1/flink/"+fileName+jsonObject.getString("vault_name"), true);
                FileWriter writer = new FileWriter("s3a://cm_ac_log_ibm_backup/"+fileName+jsonObject.getString("vault_name"), true);
                String remote_adress;
                String server_name;
                String client_id;
                String request_method;
                String protocol;
                String object_size;
                String object_name;
                String time_start;
                String time_finish;
                String user_agent;
                String access_status;
                String vault_name;
                String midstream_error;
                String error_code;
                String request_latency;
                String response_length;
                remote_adress = StringUtil.hasString(jsonObject, "remote_address");
                server_name = StringUtil.hasString(jsonObject, "server_name");
                client_id = StringUtil.hasString(jsonObject, "client_id");
                request_method = StringUtil.hasString(jsonObject, "request_method");
                protocol = StringUtil.hasString(jsonObject, "protocol");
                object_size = StringUtil.hasString(jsonObject, "object_size");
                object_name = StringUtil.hasString(jsonObject, "object_name");
                time_start = StringUtil.hasString(jsonObject, "time_start");
                time_finish = StringUtil.hasString(jsonObject, "time_finish");
                user_agent = StringUtil.hasString(jsonObject, "user_agent");
                access_status = StringUtil.hasString(jsonObject, "access_status");
                vault_name = StringUtil.hasString(jsonObject, "vault_name");
                midstream_error = StringUtil.hasString(jsonObject, "midstream_error");
                error_code = StringUtil.hasString(jsonObject, "error_code");
                if (jsonObject.has("request_latency")){
                    request_latency = String.valueOf(jsonObject.get("request_latency")) + ",";
                }else {
                    request_latency = "None,";
                }
                if (jsonObject.has("response_length")){
                    response_length = String.valueOf(jsonObject.get("response_length"));
                }else {
                    response_length = "None";
                }

                String contentStr = server_name +remote_adress+ client_id+ request_method +protocol
                        + object_size +object_name +time_start +time_finish +user_agent+access_status +vault_name
                        +midstream_error + error_code+request_latency +response_length;
                writer.write(contentStr + "\n");
                writer.close();
            }
        };

        env.
                addSource(new FlinkKafkaConsumer<>("cm-cn-central-00001-ic1", new SimpleStringSchema(), properties))
        //.print();
        //        .writeAsText(s3SinkPath);
        .addSink(createS3SinkFromStaticConfig());
        //.addSink(ts);

                //env.addSource(new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), properties)).addSink(ts);

/*         env.
                addSource(new FlinkKafkaConsumer<>("cm-cn-central-00001", new SimpleStringSchema(), properties)).addSink(ts);
         env.
                addSource(new FlinkKafkaConsumer<>("cm-cn-central-00001-devci-log", new SimpleStringSchema(), properties)).addSink(ts);
         env.
                addSource(new FlinkKafkaConsumer<>("cm-cn-central-00001-ic1", new SimpleStringSchema(), properties)).addSink(ts);
         env.
                addSource(new FlinkKafkaConsumer<>("cm-cn-central-00001-ic1-log", new SimpleStringSchema(), properties)).addSink(ts);
         env.
                addSource(new FlinkKafkaConsumer<>("cm-cn-central-00001-ic2", new SimpleStringSchema(), properties)).addSink(ts);
         env.
                addSource(new FlinkKafkaConsumer<>("cm-cn-central-00001-ic2-log", new SimpleStringSchema(), properties)).addSink(ts);
         env.
                addSource(new FlinkKafkaConsumer<>("cm-cn-central-00001-ic3", new SimpleStringSchema(), properties)).addSink(ts);
         env.
                addSource(new FlinkKafkaConsumer<>("cm-cn-central-00001-ic3-log", new SimpleStringSchema(), properties)).addSink(ts);
         env.
                addSource(new FlinkKafkaConsumer<>("cm-cn-central-00001-ic4", new SimpleStringSchema(), properties)).addSink(ts);*/;
        try {
            env.execute("KafkaTimeCalculation");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

/*    public static SinkFunction toSinkFunction(){



        return ts;
    }*/
    

    private static StreamingFileSink<String> createS3SinkFromStaticConfig() {
        final StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path(s3SinkPath), new SimpleStringEncoder<String>("UTF-8"))
                .build();
        return sink;
    }

}
