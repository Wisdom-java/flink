package com.ibm.utils;

public class Constants {

    /**
     * kafka地址枚举类
     */
    public static enum KafkaAddress {

        CM_NJ_KAFKA_PROD("172.18.140.3:9092," +
                "172.18.140.4:9092," +
                "172.18.140.5:9092," +
                "172.18.140.12:9092," +
                "172.18.140.13:9092," +
                "172.18.140.54:9092," +
                "172.18.140.62:9092," +
                "172.18.140.63:9092," +
                "172.18.140.72:9092"),
        CM_TJ_KAFKA_PROD("172.16.140.3:9092," +
                "172.16.140.4:9092," +
                "172.16.140.5:9092," +
                "172.16.140.12:9092," +
                "172.16.140.13:9092," +
                "172.16.140.54:9092," +
                "172.16.140.62:9092," +
                "172.16.140.63:9092," +
                "172.16.140.72:9092"),
        CM_HZ_KAFKA_PROD("172.20.140.3:9092," +
                "172.20.140.4:9092," +
                "172.20.140.5:9092," +
                "172.20.140.12:9092," +
                "172.20.140.13:9092," +
                "172.20.140.54:9092," +
                "172.20.140.62:9092," +
                "172.20.140.63:9092," +
                "172.20.140.72:9092"),
        CU_LF_KAFKA_PROD("172.16.130.12:9092," +
                "172.16.130.13:9092," +
                "172.16.130.18:9092," +
                "172.16.130.30:9092," +
                "172.16.130.31:9092"),
        CU_TJ_KAFKA_PROD("172.18.130.12:9092," +
                "172.18.130.13:9092," +
                "172.18.130.18:9092," +
                "172.18.130.30:9092," +
                "172.18.130.31:9092"),
        CU_TS_KAFKA_PROD("172.20.130.12:9092," +
                "172.20.130.13:9092," +
                "172.20.130.18:9092," +
                "172.20.130.30:9092," +
                "172.20.130.31:9092");

        private String address;

        KafkaAddress(String address) {
            this.address = address;
        }

        public String getValue() {
            return address;
        }

    }

    public static enum NetWorkTopic{
        cm_nj_ca_report("nj-ca_report"), //移动南京ca_report
        cm_tj_ca_report("tj-ca_report"), //移动天京ca_report
        cm_hz_ca_report("hz-ca_report"), //移动杭州ca_report
        cu_lf_ca_report("lf-ca_report"), //联通廊坊ca_report
        cu_tj_ca_report("tj-ca_report"), //联通天津ca_report
        cu_ts_ca_report("ts-ca_report"), //联通唐山ca_report
        ;

        private String topic;
        NetWorkTopic(String topic){
            this.topic = topic;
        }

        public String getTopic() {
            return topic;
        }
    }
    //lf-cs_report
    public static enum DiskBusyTopic{
        cm_nj_cs_metricbeat("nj-cs-metricbeat"), //移动南京metricbeat
        cm_tj_cs_metricbeat("tj-cs-metricbeat"), //移动天津metricbeat
        cm_hz_cs_metricbeat("hz-cs-metricbeat"), //移动杭州metricbeat
        cu_lf_cs_metricbeat("lf-cs-metricbeat"), //联通廊坊metricbeat
        cu_tj_cs_metricbeat("tj-cs-metricbeat"), //联通天津metricbeat
        cu_ts_cs_metricbeat("ts-cs-metricbeat"), //联通唐山metricbeat
        ;

        private String topic;
        DiskBusyTopic(String topic){
            this.topic = topic;
        }

        public String getTopic() {
            return topic;
        }
    }

    // access_log

    public static enum AccessLogField{

        timestamp_finish("timestamp_finish"),
        vault_name("vault_name"),
        client_id("client_id"),
        user_id("user_id"),
        server_name("server_name"),
        remote_address("remote_address"),
        request_method("request_method"),
        protocol("protocol"),
        object_length("object_length"),
        object_name("object_name"),
        time_start("time_start"),
        time_finish("time_finish"),
        user_agent("user_agent"),
        status("status"),
        midstream_error("midstream_error"),
        error_code("error_code"),
        request_latency("request_latency"),
        response_length("response_length"),
        turn_around_time("turn_around_time"),
        error_message("error_message"),
        None("None"),
        ;

        private String field;
        AccessLogField(String field){ this.field = field; }
        public String getField(){ return field; }
    }

    // access_log topic
    public static enum cmAccessLogTopic{
        cm_flink_tj_ca_access("flink-tj-ca_access"),
        cm_flink_nj_ca_access("flink-nj-ca_access"),
        cm_flink_hz_ca_access("flink-hz-ca_access"),
        cu_flink_lf_ca_access("flink-lf-ca_access"),
        cu_flink_tj_ca_access("flink-tj-ca_access"),
        cu_flink_ts_ca_access("flink-ts-ca_access")
        ;
        private String topic;
        cmAccessLogTopic(String topic){this.topic = topic; }
        public String getTopic(){ return topic; }
    }

    // countStripe
    public static enum countByStripe{
        cm_tj_cs_report("tj-cs_report"),
        cm_nj_cs_report("nj-cs_report"),
        cm_hz_cs_report("hz-cs_report"),
        cu_lf_cs_report("lf-cs_report"),
        cu_tj_cs_report("tj-cs_report"),
        cu_ts_cs_report("ts-cs_report")
        ;
        private String topic;
        countByStripe(String topic){this.topic = topic; }
        public String getTopic(){return topic;}
    }
}