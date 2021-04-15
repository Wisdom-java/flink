package com.ibm.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class Producer {
    public static void main(String[] args) throws Exception{
        // 创建生产者
        Properties kafkaProps = new Properties();
        // 指定broker（这里指定了2个，1个备用），如果你是集群更改主机名即可，如果不是只写运行的主机名
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        //设置主题
        //kafkaProps.put("group.id", "test");
        // 设置序列化（自带的StringSerializer，如果消息的值为对象，就需要使用其他序列化方式，如Avro ）
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 实例化出producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProps);

        while (true){
            // 创建ProducerRecord对象
            // 创建ProducerRecord对象

/*            ProducerRecord<String, String> recordTest = new ProducerRecord<>("nj-ca_report", "Hello", "{\n" +
                    "\t\"@timestamp\": \"2020-09-02T00:51:46.046Z\",\n" +
                    "\t\"beat\": {\n" +
                    "\t\t\"hostname\": \"njscac4m1804.albatross.10086.cn\",\n" +
                    "\t\t\"name\": \"njscac4m1804.albatross.10086.cn\",\n" +
                    "\t\t\"version\": \"5.2.1\"\n" +
                    "\t},\n" +
                    "\t\"input_type\": \"log\",\n" +
                    "\t\"offset\": \"offset\",\n" +
                    "\t\"source\": \"source\",\n" +
                    "\t\"type\": \"type\",\n" +
                    "\t\"message\": \"{\\\"time_hr\\\":\\\"2020-09-02 00:51:45.539\\\",\\\"time\\\":1599007905539,\\\"network_queue\\\":{\\\"172.16.82.29:5000\\\":{\\\"pending_requests\\\":6,\\\"pending_bytes\\\":\\\"0\\\",\\\"pending_writes\\\":0,\\\"high_priority\\\":0,\\\"low_priority\\\":0,\\\"is_writable_report\\\":{\\\"percent_blocked_by_netty\\\":0,\\\"percent_blocked_by_max_out_standing_bytes\\\":0,\\\"percent_total_blocked\\\":0,\\\"num_times_transitioned_to_blocked\\\":0,\\\"average_queue_time\\\":\\\"0\\\",\\\"max_queue_time\\\":\\\"1\\\"},\\\"cancellation_report\\\":{\\\"cancelled\\\":\\\"0\\\",\\\"sent\\\":\\\"0\\\"}},\\\"172.20.80.18:5000\\\":{\\\"pending_requests\\\":2,\\\"pending_bytes\\\":\\\"20592\\\",\\\"pending_writes\\\":0,\\\"high_priority\\\":0,\\\"low_priority\\\":0,\\\"is_writable_report\\\":{\\\"percent_blocked_by_netty\\\":0,\\\"percent_blocked_by_max_out_standing_bytes\\\":0,\\\"percent_total_blocked\\\":0,\\\"num_times_transitioned_to_blocked\\\":0,\\\"average_queue_time\\\":\\\"0\\\",\\\"max_queue_time\\\":\\\"3\\\"},\\\"cancellation_report\\\":{\\\"cancelled\\\":\\\"0\\\",\\\"sent\\\":\\\"0\\\"}}}}\"\n" +
                    "}");*/
            ProducerRecord<String, String> recordTest = new ProducerRecord<String, String>("test", "Hello", "{\"@timestamp\":\"2021-02-27T06:19:11.220Z\",\"beat\":{\"hostname\":\"tjscssc0802\",\"name\":\"tjscssc0802\",\"version\":\"5.2.1\"},\"input_type\":\"log\",\"message\":\"{\\\"time_hr\\\":\\\"2021-02-27 06:19:10.425\\\",\\\"time\\\":" +
                    System.currentTimeMillis() +
                    ",\\\"rebuilder_undofinalize_stats\\\":{\\\"rebuild_sources_success\\\":\\\"0\\\",\\\"rebuild_sources_failed\\\":\\\"0\\\",\\\"undo_sources_success\\\":\\\"9\\\",\\\"undo_sources_failed\\\":\\\"0\\\",\\\"finalize_sources_success\\\":\\\"0\\\",\\\"finalize_sources_failed\\\":\\\"0\\\",\\\"num_rebuild_permits_available\\\":\\\"10\\\",\\\"num_rebuild_permits_used\\\":\\\"1\\\",\\\"local_rebuild_queue_length\\\":\\\"0\\\",\\\"local_rebuild_queue_drop_count\\\":\\\"3013312\\\",\\\"target_rebuild_rate\\\":\\\"20971520\\\",\\\"average_rebuild_rate\\\":\\\"1793987\\\",\\\"average_rebuild_length\\\":\\\"291269\\\",\\\"average_rebuild_duration\\\":\\\"162\\\",\\\"total_rebuild_queue_size\\\":\\\"0\\\",\\\"global_rebuild_priority\\\":\\\"0\\\",\\\"high_priority_source_missing_slices\\\":\\\"0\\\",\\\"undo_finalize_queue_length\\\":\\\"0\\\",\\\"undo_finalize_queue_drop_count\\\":\\\"0\\\"}}\",\"offset\":51005777,\"source\":\"/var/log/dsnet-core/report.log\",\"type\":\"tj-cs_report\"}");
            Future<RecordMetadata> send = producer.send(recordTest);
            Thread.sleep(1000);
        }
    }
}
