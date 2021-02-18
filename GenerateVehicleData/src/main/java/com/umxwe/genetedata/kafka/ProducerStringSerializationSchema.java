package com.umxwe.genetedata.kafka;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;

/**
 * @ClassName ProducerStringSerializationSchema
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/18
 */
public class ProducerStringSerializationSchema implements KafkaSerializationSchema<String> {
    private static final long serialVersionUID = 5556700683110420508L;
    private String topic;

    public ProducerStringSerializationSchema(String topic) {
        super();
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(String element, Long timestamp) {
        return new ProducerRecord<byte[], byte[]>(topic, element.getBytes(StandardCharsets.UTF_8));
    }
}
