package com.ibm.calculation;

import com.ibm.utils.Constants;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.json.JSONObject;
import java.io.*;
import java.util.*;

public class DiskBusy {
    //csv文件路径 移动list.csv 联通 CUDisk.csv
    //private static final String filePath = "/data1/flink/list.csv";
    //数据字典
    private static final Map<String,String> dictionaryMap = new HashMap<>();
    //初始化数据字典
    static {
        try {
            String record;
            // 设定UTF-8字符集，使用带缓冲区的字符输入流BufferedReader读取文件内容
            DiskBusy diskBusy = new DiskBusy();
            //BufferedReader file = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), "UTF-8"));
            BufferedReader file = diskBusy.readDiskFile();
            // file.readLine(); //跳过表头所在的行
            // 遍历数据行并存储在名为records的ArrayList中，每一行records中存储的对象为一个String数组
            while ((record = file.readLine()) != null) {
                String fields[] = record.split(",");
                dictionaryMap.put(fields[3]+fields[4],fields[1]+","+fields[2]);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public BufferedReader readDiskFile(){
        //联通为culist
        //移动为cmlist
        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("culist.csv");
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

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        //设置检查点
        env.enableCheckpointing(5000);
        //properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("bootstrap.servers", Constants.KafkaAddress.CU_LF_KAFKA_PROD.getValue());
        //properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "disk_busy_test");
        Properties producerProperties = new Properties();
        //producerProperties.setProperty("bootstrap.servers", "localhost:9092");
        producerProperties.setProperty("bootstrap.servers", Constants.KafkaAddress.CU_LF_KAFKA_PROD.getValue());
        producerProperties.setProperty("group.id", "disk_busy_send_test");
        FlinkKafkaProducer diskBusy = new FlinkKafkaProducer<>("disk_busy_test", new SimpleStringSchema(), producerProperties,Optional.ofNullable(null));
        //nj-cs-metricbeat  移动南京
        //hz-cs-metricbeat  移动杭州
        //tj-cs-metricbeat  移动天津
        //lf-cs-metricbeat  联通廊坊
        //tj-cs-metricbeat  联通天津
        //ts-cs-metricbeat  联通天津
        DataStreamSource<String> dataStreamSource = env.
                addSource(new FlinkKafkaConsumer<>(Constants.DiskBusyTopic.cu_lf_cs_metricbeat.getTopic(), new SimpleStringSchema(), properties));
        dataStreamSource.map(message -> {

            JSONObject jsonObject = new JSONObject(message);
            if (null == dictionaryMap){
                throw new Exception("dictionaryMap 为空");
            }
            if (!jsonObject.has("beat")){
                return message;
            }
            Object beat = jsonObject.get("beat");
            JSONObject jsonObjectBeat = new JSONObject(String.valueOf(beat));
            if (!jsonObjectBeat.has("hostname")){
                return message;
            }
            if (!jsonObject.has("system")){
                return message;
            }
            Object system = jsonObject.get("system");
            JSONObject jsonObjectSystem = new JSONObject(String.valueOf(system));
            if (!jsonObjectSystem.has("diskio")){
                return message;
            }
            Object diskio = jsonObjectSystem.get("diskio");
            JSONObject jsonObjectDiskIo = new JSONObject(String.valueOf(diskio));
            if (!jsonObjectDiskIo.has("name")){
                return message;
            }

            //hostname 使用
            String hostname = jsonObjectBeat.getString("hostname");
            //System.out.println("hostname:"+hostname);
            //硬盘名称
            String name = jsonObjectDiskIo.getString("name");
            //System.out.println("name:"+name);
            //拼接key
            String key = hostname + name;
            //得到对应的值
            String value = dictionaryMap.get(key);
            if (null == value){
                return message;
            }
            String[] split = value.split(",");
            jsonObject.put("stripe",split[0]);
            String order = split[1];
            if (Integer.valueOf(split[1]) >= 1 && Integer.valueOf(split[1]) <= 9){
                order = "0" + split[1];
            }
            jsonObject.put("disk_order",split[0]+"_"+order);
            return jsonObject.toString();
        }).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String message) throws Exception {
/*                FileWriter fileWriter = new FileWriter("/data1/flink/disk.txt",true);
                fileWriter.write(message);
                fileWriter.flush();
                fileWriter.close();
                return true;*/
                if (message.contains("stripe")){
                    return true;
                }else {
                    return false;
                }
            }
        })
        .addSink(diskBusy);
        //.print();
        try {
            env.execute("disk_busy_job");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
