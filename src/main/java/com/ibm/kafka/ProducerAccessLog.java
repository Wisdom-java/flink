package com.ibm.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class ProducerAccessLog {
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

            ProducerRecord<String, String> recordTest = new ProducerRecord<String, String>("test", "Hello", "{\n" +
                    "\t\"@timestamp\": \"2021-01-25T15:08:02.707Z\",\n" +
                    "\t\"VIP\": \"TJVIP02\",\n" +
                    "\t\"beat\": {\n" +
                    "\t\t\"hostname\": \"tjscaca0201.albatross.10086.cn\",\n" +
                    "\t\t\"name\": \"tjscaca0201.albatross.10086.cn\",\n" +
                    "\t\t\"version\": \"5.2.1\"\n" +
                    "\t},\n" +
                    "\t\"input_type\": \"log\",\n" +
                    "\t\"internal_ip\": \"172.16.10.25\",\n" +
                    "\t\"message\": \"{\\\"server_name\\\":\\\"cm-cn-central-00001.albatross.10086.cn\\\",\\\"remote_address\\\":\\\"183.195.90.161\\\",\\\"remote_user\\\":\\\"pMkD2WkNF5ip7UfsRUrT\\\",\\\"timestamp_start\\\":\\\"1611587232236\\\",\\\"timestamp_finish\\\":\\\"1611587281741\\\",\\\"time_start\\\":\\\"25/Jan/2021:15:07:12 +0000\\\",\\\"time_finish\\\":\\\"25/Jan/2021:15:08:01 +0000\\\",\\\"request_method\\\":\\\"PUT\\\",\\\"request_uri\\\":\\\"/s1vqJq8BdzoVUn4Aso0A?X-Amz-Date=20210125T150651Z\\u0026X-Amz-Algorithm=AWS4-HMAC-SHA256\\u0026X-Amz-Signature=c617aa58b2df96e8276db8ffb12bdc2c34d3829cc88c784630a72af8ffedb809\\u0026x-client-request-id=7C196358-F24A-48CF-B624-C354581ED390--1530564789\\u0026bin=1002000001\\u0026X-Amz-SignedHeaders=content-length%3Bcontent-type%3Bhost\\u0026X-Amz-Credential=pMkD2WkNF5ip7UfsRUrT%2F20210125%2F%2Fs3%2Faws4_request\\u0026X-Amz-Expires=3360\\u0026region=china\\\",\\\"protocol\\\":\\\"HTTP/1.1\\\",\\\"status\\\":200,\\\"response_length\\\":\\\"0\\\",\\\"request_length\\\":\\\"20795205\\\",\\\"user_agent\\\":\\\"cloudd/962 CFNetwork/1209 Darwin/20.2.0\\\",\\\"request_latency\\\":\\\"49505\\\",\\\"request_id\\\":\\\"10e92585-96ec-4326-9cc8-5e9dbac04b68\\\",\\\"request_type\\\":\\\"REST.PUT.OBJECT\\\",\\\"interface_type\\\":\\\"s3\\\",\\\"stat\\\":{\\\"client_wait\\\":49256.92,\\\"storage_wait\\\":9.367,\\\"digest\\\":47.385,\\\"commit\\\":189.035,\\\"turn_around_time\\\":192.228,\\\"total_transfer\\\":49503.839,\\\"pre_transfer\\\":0.932,\\\"post_transfer\\\":0.052},\\\"object_length\\\":\\\"20795205\\\",\\\"version_name\\\":\\\"28709b62-f202-4cd0-abc8-feef7cc93bab\\\",\\\"version_transient\\\":true,\\\"delete_marker\\\":false,\\\"last_modified\\\":\\\"2021-01-25T15:08:01.549Z\\\",\\\"last_changed\\\":\\\"2021-01-25T15:08:01.549Z\\\",\\\"object_name\\\":\\\"s1vqJq8BdzoVUn4Aso0A\\\",\\\"vault_name\\\":\\\"cm-cn-central-00001\\\",\\\"is_secure\\\":true,\\\"https\\\":{\\\"protocol\\\":\\\"TLSv1.2\\\",\\\"cipher_suite\\\":\\\"TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384\\\"},\\\"principals\\\":{\\\"identity\\\":\\\"9d9872ee-6974-706d-103b-726199884250@00000000-0000-0000-0000-000000000000\\\",\\\"aws\\\":\\\"pMkD2WkNF5ip7UfsRUrT\\\"},\\\"type\\\":\\\"http\\\",\\\"format\\\":1}\",\n" +
                    "\t\"offset\": 324627401,\n" +
                    "\t\"source\": \"/var/log/dsnet-core/access.log\",\n" +
                    "\t\"type\": \"tj-ca_access\"\n" +
                    "}");
            Future<RecordMetadata> send = producer.send(recordTest);
            Thread.sleep(1000);
        }
    }
}
