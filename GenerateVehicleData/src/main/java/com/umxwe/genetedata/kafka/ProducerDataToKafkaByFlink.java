package com.umxwe.genetedata.kafka;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.umxwe.genetedata.utils.KafkaUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

/**
 * @ClassName ProducerDataToKafkaByFlink
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/18
 */
public class ProducerDataToKafkaByFlink {
    public static void main(String[] args) throws Exception {
        String servers="itserver23:6667,itserver22:6667,itserver21:6667";
        String topic = "vehicleentity";

        // create kafka topic if not exists.
        AdminClient kafkAdminClient = AdminClient.create(KafkaUtils.producerProps(servers));
        NewTopic newTopic = new NewTopic(topic, 6, (short) 1);
        newTopic.configs(new ImmutableMap.Builder<String, String>().put("cleanup.policy", "delete").
                put("retention.ms", Long.toString(86400000L)).
                put("retention.bytes", "-1").
                build());
        if (!new ArrayList<String>(kafkAdminClient.listTopics().names().get()).contains(topic))
            kafkAdminClient.createTopics(new ImmutableList.Builder<NewTopic>().add(newTopic).build());
        kafkAdminClient.close();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> producer =
                env.addSource(new EventSourceGenerator(100));
        producer
                .addSink(new FlinkKafkaProducer<String>(topic, new ProducerStringSerializationSchema(topic),
                        KafkaUtils.producerProps(servers), FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
        env.execute("flink-produce-data");

    }
}
