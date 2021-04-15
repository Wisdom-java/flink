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
import java.text.DecimalFormat;
import java.util.*;

public class OnOrOffHeap {

    //数据字典
    private static final Map<String,String> dictionaryCuAcInfoMap = new HashMap<>();
    //初始化数据字典
    static {
        try {
            String record;
            // 设定UTF-8字符集，使用带缓冲区的字符输入流BufferedReader读取文件内容
            OnOrOffHeap onOrOffHeapV1 = new OnOrOffHeap();
            //BufferedReader file = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), "UTF-8"));
            BufferedReader file = onOrOffHeapV1.readCuAcInfoFile();
            // file.readLine(); //跳过表头所在的行
            // 遍历数据行并存储在名为records的ArrayList中，每一行records中存储的对象为一个String数组
            while ((record = file.readLine()) != null) {
                String fields[] = record.split(",");
                dictionaryCuAcInfoMap.put(fields[0],fields[1]);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public BufferedReader readCuAcInfoFile(){
        //联通为culist
        //移动为cmlist
        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("cu_ac_info.csv");
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
        //设置检查点
        env.enableCheckpointing(5000);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", Constants.KafkaAddress.CU_LF_KAFKA_PROD.getValue());
        properties.setProperty("group.id", "on_off_heap_receive");
        Properties producerProperties = new Properties();
        producerProperties.setProperty("bootstrap.servers", Constants.KafkaAddress.CU_LF_KAFKA_PROD.getValue());
        producerProperties.setProperty("group.id", "on_off_heap_send");
        // Double format
        DecimalFormat df = new DecimalFormat("0.0000");
        if (null == dictionaryCuAcInfoMap){
            try {
                throw new Exception("dictionaryCuAcInfoMap 为空");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        //lf-on_off_heap 联通廊坊
        FlinkKafkaProducer on_off_heap = new FlinkKafkaProducer<String>("lf-on_off_heap", new SimpleStringSchema(), producerProperties, Optional.ofNullable(null));
        //nj-ca_report 移动南京
        //hz-ca_report 移动杭州
        //tj-ca_report 移动天津
        //lf-ca_report 联通廊坊
        //tj-ca_report 联通天津
        //ts-ca_report 联通唐山
        DataStreamSource<String> dataStreamSource = env.
                addSource(new FlinkKafkaConsumer<>(Constants.NetWorkTopic.cu_lf_ca_report.getTopic(), new SimpleStringSchema(), properties));
        dataStreamSource.map(messageOld -> {
            JSONObject jsonObject = new JSONObject(messageOld);
            String timestamp = jsonObject.getString("@timestamp");
            String report_event_type = "";
            if (jsonObject.has("report_event_type")) {
                report_event_type = jsonObject.getString("report_event_type");
            }

            Object beat = jsonObject.get("beat");
            JSONObject beatJsonObject = new JSONObject(String.valueOf(beat));
            String hostname = beatJsonObject.getString("hostname");
            String input_type = jsonObject.getString("input_type");
            Object offsetTemp = jsonObject.get("offset");
            String offset = String.valueOf(offsetTemp);
            String source = jsonObject.getString("source");
            String type = jsonObject.getString("type");
            String message = jsonObject.getString("message");
            JSONObject jsonObjectMessage = new JSONObject(message);
            JSONObject jsonObjectResourceManager = new JSONObject();
            if (jsonObjectMessage.has("resource_manager")) {
                jsonObjectResourceManager = new JSONObject(String.valueOf(jsonObjectMessage.get("resource_manager")));
            }else {
                return message;
            }
            Object on_heap = jsonObjectResourceManager.get("on_heap");
            Object off_heap = jsonObjectResourceManager.get("off_heap");
            JSONObject jsonObjectOnHeap = new JSONObject(String.valueOf(on_heap));
            Iterator<String> onHeapKeys = jsonObjectOnHeap.keys();
            JSONObject jsonObjectNew = new JSONObject();
            jsonObjectNew.put("@timestamp",timestamp);
            jsonObjectNew.put("report_event_type",report_event_type);
            jsonObjectNew.put("beat",beat);
            jsonObjectNew.put("input_type",input_type);
            jsonObjectNew.put("offset",offset);
            jsonObjectNew.put("source",source);
            jsonObjectNew.put("type",type);
            if (dictionaryCuAcInfoMap.get(hostname) != null){
                jsonObjectNew.put("vip",dictionaryCuAcInfoMap.get(hostname));
            }
            jsonObjectNew.put("on_heap",on_heap);
            jsonObjectNew.put("off_heap",off_heap);
            Double onUsedBytes = null;
            Double onTotalBytes = null;
            while (onHeapKeys.hasNext()){
                String key = onHeapKeys.next();
                if (key.equals("used_bytes")){
                    onUsedBytes = jsonObjectOnHeap.getDouble(key);
                }
                if (key.equals("total_bytes")){
                    onTotalBytes = jsonObjectOnHeap.getDouble(key);
                }
                jsonObjectNew.put("on_"+key,jsonObjectOnHeap.get(key));
            }
            if (null != onUsedBytes && null != onTotalBytes){
                String onRate = df.format(onUsedBytes / onTotalBytes);
                jsonObjectNew.put("on_rate",onRate);

            }
            JSONObject jsonObjectOffHeap = new JSONObject(String.valueOf(off_heap));
            Iterator<String> offHeapKeys = jsonObjectOffHeap.keys();
            Double offUsedBytes = null;
            Double offTotalBytes = null;
            while (offHeapKeys.hasNext()){
                String key = offHeapKeys.next();
                if (key.equals("used_bytes")){
                    offUsedBytes = jsonObjectOnHeap.getDouble(key);
                }
                if (key.equals("total_bytes")){
                    offTotalBytes = jsonObjectOnHeap.getDouble(key);
                }
                jsonObjectNew.put("off_"+key,jsonObjectOffHeap.get(key));
            }
            if (null != offUsedBytes && null != offTotalBytes){
                String offRate = df.format(offUsedBytes / offTotalBytes);
                jsonObjectNew.put("off_rate",offRate);
            }
            return jsonObjectNew.toString();
        }).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String message) throws Exception {
                if (message.contains("on_heap")){
                    return true;
                }else {
                    return false;
                }
            }
        }).addSink(on_off_heap);
        try {
            env.execute("cu_lf_on_off_heap_job");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
