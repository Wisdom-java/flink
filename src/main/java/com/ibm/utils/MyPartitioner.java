package com.ibm.utils;

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.util.Preconditions;

/**
 * kafka 自定义kafka数据分区策略
 */
public class MyPartitioner extends FlinkKafkaPartitioner {

    @Override
    public int partition(Object o, byte[] bytes, byte[] bytes1, String s, int[] partitions) {
        int length = partitions.length;
        return 0;
    }

}
