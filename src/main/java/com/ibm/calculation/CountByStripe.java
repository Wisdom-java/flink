package com.ibm.calculation;

import com.ibm.utils.Constants;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.json.JSONObject;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class CountByStripe {
    private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    //数据字典
    private static final Map<String,String> dictionaryCuSsSetStripeMap = new HashMap<>();
    //初始化数据字典
    static {
        try {
            String record;
            // 设定UTF-8字符集，使用带缓冲区的字符输入流BufferedReader读取文件内容
            CountByStripe countByStripe = new CountByStripe();
            BufferedReader file = countByStripe.readCuSsInfoFile();
            //跳过表头所在的行
            file.readLine();
            // 遍历数据行并存储在名为records的ArrayList中，每一行records中存储的对象为一个String数组
            while ((record = file.readLine()) != null) {
                String fields[] = record.split(",");
                String set = fields[2];
                String stripe = fields[3];
                int stripeInt = Integer.valueOf(stripe);
                if (stripeInt < 10){
                    stripe = "0" + stripe;
                }
                dictionaryCuSsSetStripeMap.put(fields[0],"set" + set + stripe);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public BufferedReader readCuSsInfoFile(){
        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("cu_ss_set_stripe.csv");
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
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", Constants.KafkaAddress.CU_TS_KAFKA_PROD.getValue());
        properties.setProperty("group.id", "repair_data");
        FlinkKafkaConsumer<String> stringFlinkKafkaConsumer = new FlinkKafkaConsumer<>(Constants.countByStripe.cu_ts_cs_report.getTopic(),
                new SimpleStringSchema(), properties);
        Properties producerProperties = new Properties();
        producerProperties.setProperty("bootstrap.servers", Constants.KafkaAddress.CU_TS_KAFKA_PROD.getValue());
        producerProperties.setProperty("group.id", "count_by_stripe");

        FlinkKafkaProducer count_by_stripe_producer = new FlinkKafkaProducer<String>("flink_count_by_stripe", new SimpleStringSchema(), producerProperties, Optional.ofNullable(null));

        SingleOutputStreamOperator<com.ibm.vo.CountByStripe> stringSingleOutputStreamOperator = env.addSource(stringFlinkKafkaConsumer).flatMap(new FlatMapFunction<String, com.ibm.vo.CountByStripe>() {
            @Override
            public void flatMap(String s, Collector<com.ibm.vo.CountByStripe> collector) throws Exception {
                JSONObject jsonObject = new JSONObject(s);
                com.ibm.vo.CountByStripe countByStripe = new com.ibm.vo.CountByStripe();
                if (jsonObject.has("message") && jsonObject.has("beat")){
                    //使用@timestamp作为计算时间
                    //String timestamp = jsonObject.getString("@timestamp");
                    //Date timeStampDate = DateUtil.parseUTCText(timestamp);
                    String beat = String.valueOf(jsonObject.get("beat"));
                    String message = jsonObject.getString("message");
                    JSONObject jsonObjectMessage = new JSONObject(message);
                    JSONObject jsonObjectBeat = new JSONObject(beat);
                    long time = jsonObjectMessage.getLong("time");
                    //countByStripe.setTimeStr(simpleDateFormat.format(timeStampDate));
                    countByStripe.setTimeStr(simpleDateFormat.format(time));
                    countByStripe.setHostname(jsonObjectBeat.getString("hostname"));
                    //set timestampe
                    //countByStripe.setTimestamp(simpleDateFormat.parse(simpleDateFormat.format(timeStampDate)).getTime());
                    countByStripe.setTimestamp(time);
                    String hostName = jsonObjectBeat.getString("hostname");
                    //set stripe_num
                    countByStripe.setStripeNum(dictionaryCuSsSetStripeMap.get(hostName));
                    if (jsonObjectMessage.has("rebuilder_undofinalize_stats")){
                        String rebuilderUndofinalizeStatsStr = String.valueOf(jsonObjectMessage.get("rebuilder_undofinalize_stats"));
                        JSONObject jsonObjectRUS = new JSONObject(rebuilderUndofinalizeStatsStr);
                        countByStripe.setRebuild_sources_success(jsonObjectRUS.getInt("rebuild_sources_success"));
                        countByStripe.setRebuild_sources_failed(jsonObjectRUS.getInt("rebuild_sources_failed"));
                        countByStripe.setUndo_sources_failed(jsonObjectRUS.getInt("rebuild_sources_failed"));
                        countByStripe.setUndo_sources_success(jsonObjectRUS.getInt("undo_sources_success"));
                        countByStripe.setFinalize_sources_failed(jsonObjectRUS.getInt("finalize_sources_failed"));
                        countByStripe.setFinalize_sources_success(jsonObjectRUS.getInt("finalize_sources_success"));
                        countByStripe.setNum_rebuild_permits_available(jsonObjectRUS.getInt("num_rebuild_permits_available"));
                        countByStripe.setNum_rebuild_permits_used(jsonObjectRUS.getInt("num_rebuild_permits_used"));
                        countByStripe.setLocal_rebuild_queue_drop_count(jsonObjectRUS.getInt("local_rebuild_queue_drop_count"));
                        countByStripe.setLocal_rebuild_queue_length(jsonObjectRUS.getInt("local_rebuild_queue_length"));
                        countByStripe.setTarget_rebuild_rate(jsonObjectRUS.getInt("target_rebuild_rate"));
                        countByStripe.setAverage_rebuild_duration(jsonObjectRUS.getInt("average_rebuild_duration"));
                        countByStripe.setAverage_rebuild_length(jsonObjectRUS.getInt("average_rebuild_length"));
                        countByStripe.setAverage_rebuild_rate(jsonObjectRUS.getInt("average_rebuild_rate"));
                        countByStripe.setTotal_rebuild_queue_size(jsonObjectRUS.getInt("total_rebuild_queue_size"));
                        countByStripe.setGlobal_rebuild_priority(jsonObjectRUS.getInt("global_rebuild_priority"));
                        countByStripe.setHigh_priority_source_missing_slices(jsonObjectRUS.getInt("high_priority_source_missing_slices"));
                        countByStripe.setUndo_finalize_queue_drop_count(jsonObjectRUS.getInt("undo_finalize_queue_drop_count"));
                        countByStripe.setUndo_finalize_queue_length(jsonObjectRUS.getInt("undo_finalize_queue_length"));
                        collector.collect(countByStripe);
                    }
                }
            }
        });
        stringSingleOutputStreamOperator
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<com.ibm.vo.CountByStripe>(Time.seconds(120)) {
                    @Override
                    public long extractTimestamp(com.ibm.vo.CountByStripe countByStripe) {
                        return countByStripe.getTimestamp();
                    }
                })
                .keyBy("stripeNum")
                .timeWindow(Time.seconds(60))
                .apply(new WindowFunction<com.ibm.vo.CountByStripe, String, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<com.ibm.vo.CountByStripe> iterable, Collector<String> collector) throws Exception {
                        //List collectorAll = new ArrayList();
                        List<com.ibm.vo.CountByStripe> list = IteratorUtils.toList(iterable.iterator());
                        int count = list.size();
                        long rebuild_sources_success_total = 0;
                        long rebuild_sources_failed_total = 0;
                        long undo_sources_success_total = 0;
                        long undo_sources_failed_total = 0;
                        long finalize_sources_success_total = 0;
                        long finalize_sources_failed_total = 0;
                        long num_rebuild_permits_available_total = 0;
                        long num_rebuild_permits_used_total = 0;
                        long local_rebuild_queue_length_total = 0;
                        long local_rebuild_queue_drop_count_total = 0;
                        long target_rebuild_rate_total = 0;
                        long average_rebuild_rate_total = 0;
                        long average_rebuild_length_total = 0;
                        long average_rebuild_duration_total = 0;
                        long total_rebuild_queue_size_total = 0;
                        long global_rebuild_priority_total = 0;
                        long high_priority_source_missing_slices_total = 0;
                        long undo_finalize_queue_length_total = 0;
                        long undo_finalize_queue_drop_count_total = 0;
                        for (int i =0 ; i < count; i++){
                            com.ibm.vo.CountByStripe countByStripe = list.get(i);
                            rebuild_sources_success_total += countByStripe.getRebuild_sources_success();
                            rebuild_sources_failed_total += countByStripe.getRebuild_sources_failed();
                            undo_sources_success_total += countByStripe.getUndo_sources_success();
                            undo_sources_failed_total += countByStripe.getUndo_sources_failed();
                            finalize_sources_success_total += countByStripe.getFinalize_sources_success();
                            finalize_sources_failed_total += countByStripe.getFinalize_sources_failed();
                            num_rebuild_permits_available_total += countByStripe.getNum_rebuild_permits_available();
                            num_rebuild_permits_used_total += countByStripe.getNum_rebuild_permits_used();
                            local_rebuild_queue_length_total += countByStripe.getLocal_rebuild_queue_length();
                            local_rebuild_queue_drop_count_total += countByStripe.getLocal_rebuild_queue_drop_count();
                            target_rebuild_rate_total += countByStripe.getTarget_rebuild_rate();
                            average_rebuild_rate_total += countByStripe.getAverage_rebuild_rate();
                            average_rebuild_length_total += countByStripe.getAverage_rebuild_length();
                            average_rebuild_duration_total += countByStripe.getAverage_rebuild_duration();
                            total_rebuild_queue_size_total += countByStripe.getTotal_rebuild_queue_size();
                            global_rebuild_priority_total += countByStripe.getGlobal_rebuild_priority();
                            high_priority_source_missing_slices_total += countByStripe.getHigh_priority_source_missing_slices();
                            undo_finalize_queue_length_total += countByStripe.getUndo_finalize_queue_length();
                            undo_finalize_queue_drop_count_total += countByStripe.getUndo_finalize_queue_drop_count();
                        }
                        JSONObject jsonObject = new JSONObject();
                        jsonObject.put("rebuild_sources_success_total",rebuild_sources_success_total);
                        jsonObject.put("rebuild_sources_success_avg",rebuild_sources_success_total / count);
                        jsonObject.put("rebuild_sources_failed_total",rebuild_sources_failed_total);
                        jsonObject.put("rebuild_sources_failed_avg",rebuild_sources_failed_total / count);
                        jsonObject.put("undo_sources_success_total",undo_sources_success_total);
                        jsonObject.put("undo_sources_success_avg",undo_sources_success_total / count);
                        jsonObject.put("undo_sources_failed_total",undo_sources_failed_total);
                        jsonObject.put("undo_sources_failed_avg",undo_sources_failed_total / count);
                        jsonObject.put("finalize_sources_failed_total",finalize_sources_failed_total);
                        jsonObject.put("finalize_sources_failed_avg",finalize_sources_failed_total / count);
                        jsonObject.put("finalize_sources_success_total",finalize_sources_success_total);
                        jsonObject.put("finalize_sources_success_avg",finalize_sources_success_total / count);
                        jsonObject.put("num_rebuild_permits_available_total",num_rebuild_permits_available_total);
                        jsonObject.put("num_rebuild_permits_available_avg",num_rebuild_permits_available_total / count);
                        jsonObject.put("num_rebuild_permits_used_total",num_rebuild_permits_used_total);
                        jsonObject.put("num_rebuild_permits_used_avg",num_rebuild_permits_used_total / count);
                        jsonObject.put("local_rebuild_queue_length_total",local_rebuild_queue_length_total);
                        jsonObject.put("local_rebuild_queue_length_avg",local_rebuild_queue_length_total / count);
                        jsonObject.put("local_rebuild_queue_drop_count_total",local_rebuild_queue_drop_count_total);
                        jsonObject.put("local_rebuild_queue_drop_count_avg",local_rebuild_queue_drop_count_total / count);
                        jsonObject.put("target_rebuild_rate_total",target_rebuild_rate_total);
                        jsonObject.put("target_rebuild_rate_avg",target_rebuild_rate_total / count);
                        jsonObject.put("average_rebuild_rate_total",average_rebuild_rate_total);
                        jsonObject.put("average_rebuild_rate_avg",average_rebuild_rate_total / count);
                        jsonObject.put("average_rebuild_length_total",average_rebuild_length_total);
                        jsonObject.put("average_rebuild_length_avg",average_rebuild_length_total / count);
                        jsonObject.put("average_rebuild_duration_total",average_rebuild_duration_total);
                        jsonObject.put("average_rebuild_duration_avg",average_rebuild_duration_total / count);
                        jsonObject.put("total_rebuild_queue_size_total",total_rebuild_queue_size_total);
                        jsonObject.put("total_rebuild_queue_size_avg",total_rebuild_queue_size_total / count);
                        jsonObject.put("global_rebuild_priority_total",global_rebuild_priority_total);
                        jsonObject.put("global_rebuild_priority_avg",global_rebuild_priority_total / count);
                        jsonObject.put("high_priority_source_missing_slices_total",high_priority_source_missing_slices_total);
                        jsonObject.put("high_priority_source_missing_slices_avg",high_priority_source_missing_slices_total / count);
                        jsonObject.put("undo_finalize_queue_drop_count_total",undo_finalize_queue_drop_count_total);
                        jsonObject.put("undo_finalize_queue_drop_count_avg",undo_finalize_queue_drop_count_total / count);
                        jsonObject.put("undo_finalize_queue_length_total",undo_finalize_queue_length_total);
                        jsonObject.put("undo_finalize_queue_length_avg",undo_finalize_queue_length_total / count);
                        jsonObject.put("count",count);
                        jsonObject.put("stripe",list.get(0).getStripeNum());
                        collector.collect(jsonObject.toString());
                    }
                }).addSink(count_by_stripe_producer);
        try {
            env.execute("count_by_stripe");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
