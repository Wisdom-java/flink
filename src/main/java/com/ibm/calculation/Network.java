package com.ibm.calculation;

import com.ibm.utils.Constants;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.json.JSONObject;

import java.io.*;
import java.util.*;

public class Network {
    //数据字典
    private static final Map<String,String> dictionaryCuSsInfoMap = new HashMap<>();
    //初始化数据字典
    static {
        try {
            String record;
            // 设定UTF-8字符集，使用带缓冲区的字符输入流BufferedReader读取文件内容
            Network network = new Network();
            //BufferedReader file = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), "UTF-8"));
            BufferedReader file = network.readCuSsInfoFile();
            // file.readLine(); //跳过表头所在的行
            // 遍历数据行并存储在名为records的ArrayList中，每一行records中存储的对象为一个String数组
            while ((record = file.readLine()) != null) {
                String fields[] = record.split(",");
                dictionaryCuSsInfoMap.put(fields[0],fields[1]);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public BufferedReader readCuSsInfoFile(){
        //联通为culist
        //移动为cmlist
        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("cu_ss_info.csv");
        // 设定UTF-8字符集，使用带缓冲区的字符输入流BufferedReader读取文件内容
        BufferedReader file = null;
        try {
            file = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return file;
    }
    public static void main(String[] args) {
        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000); // checkpoint every 5000 msecs
        Properties properties = new Properties();
        //kafka  setting
        //properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("bootstrap.servers", Constants.KafkaAddress.CU_LF_KAFKA_PROD.getValue());
        properties.setProperty("group.id", "network_acc_test");

        Properties producerProperties = new Properties();
        producerProperties.setProperty("bootstrap.servers", Constants.KafkaAddress.CU_LF_KAFKA_PROD.getValue());
        producerProperties.setProperty("group.id", "network_send_test");

        if (null == dictionaryCuSsInfoMap){
            try {
                throw new Exception("dictionaryCuSsInfoMap 为空");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        FlinkKafkaProducer network = new FlinkKafkaProducer<String>("network_test", new SimpleStringSchema(), producerProperties, Optional.ofNullable(null));
        //nj-ca_report 移动南京
        //hz-ca_report 移动杭州
        //tj-ca_report 移动天津
        //lf-ca_report 联通廊坊
        //tj-ca_report 联通天津
        //ts-ca_report 联通唐山
        DataStreamSource<String> dataStreamSource = env.
                addSource(new FlinkKafkaConsumer<>("lf-ca_report", new SimpleStringSchema(), properties));
         dataStreamSource.map(messageOld -> {
            JSONObject jsonObject = new JSONObject(messageOld);
            String timestamp = jsonObject.getString("@timestamp");
            String report_event_type = "";
            if (jsonObject.has("report_event_type")) {
                report_event_type = jsonObject.getString("report_event_type");
            }
            Object beat = jsonObject.get("beat");
            String input_type = jsonObject.getString("input_type");
            Object offsetTemp = jsonObject.get("offset");
            String offset = String.valueOf(offsetTemp);
            String source = jsonObject.getString("source");
            String type = jsonObject.getString("type");
            String message = jsonObject.getString("message");
            JSONObject jsonObjectMessage = new JSONObject(message);
            JSONObject jsonObjectNetWorkQueue = new JSONObject();
            if (jsonObjectMessage.has("network_queue")) {
                jsonObjectNetWorkQueue = new JSONObject(String.valueOf(jsonObjectMessage.get("network_queue")));
            }
            Iterator<String> keys = jsonObjectNetWorkQueue.keys();
            StringBuffer stringBuffer = new StringBuffer();
            while (keys.hasNext()) {
                String key = keys.next();
                JSONObject jsonObjectNew = new JSONObject();
                jsonObjectNew.put("@timestamp", timestamp);
                if (!report_event_type.equals("")) {
                    jsonObjectNew.put("report_event_type", report_event_type);
                }
                jsonObjectNew.put("beat", beat);
                jsonObjectNew.put("input_type", input_type);
                jsonObjectNew.put("ip", key);
                String[] splitKey = key.split(":");
                if (dictionaryCuSsInfoMap.containsKey(splitKey[0])){
                    String setStripe = dictionaryCuSsInfoMap.get(splitKey[0]);
                    String[] setStripeArray = setStripe.split("-");
                    jsonObjectNew.put("stripe",setStripeArray[0]+setStripeArray[1].substring(6,setStripeArray[1].length()));
                }
                Object stringIP = jsonObjectNetWorkQueue.get(key);
                JSONObject jsonObjectIp = new JSONObject(String.valueOf(stringIP));
                Iterator<String> keys1 = jsonObjectIp.keys();
                while (keys1.hasNext()) {
                    String key1 = keys1.next();
                    jsonObjectNew.put(key1, jsonObjectIp.get(key1));
                }
                jsonObjectNew.put("offset", offset);
                jsonObjectNew.put("source", source);
                jsonObjectNew.put("type", type);
                stringBuffer.append(jsonObjectNew.toString() + "|");
            }
            return stringBuffer.toString();
        }).flatMap(new FlatMapFunction<String, Object>() {
             @Override
             public void flatMap(String s, Collector<Object> collector) throws Exception {
                 String[] split = s.split("\\|");
                 for (int i = 0; i < split.length; i++){
                     collector.collect(split[i]);
                 }
             }
         }).addSink(network);
        try {
            env.execute("network-queue");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
