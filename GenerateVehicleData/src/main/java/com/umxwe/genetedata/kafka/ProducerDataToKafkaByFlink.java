package com.umxwe.genetedata.kafka;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.umxwe.genetedata.utils.KafkaUtils;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.ArrayList;

/**
 * @ClassName ProducerDataToKafkaByFlink
 * @Description Todo flin-on-yarn-session: ./bin/flink run   -ys 4 -yid
 * application_1618190618576_0001 /mnt/data1/GenerateVehicleData-1.0-SNAPSHOT-jar-with-dependencies.jar
 * --server itserver23:6667,itserver22:6667,itserver21:6667 --topic vehicleentity --numPartitions 6
 * --replicationFactor 1 --dpv 1000
 * @Author owen(umxwe)
 * @Date 2021/2/18
 */
public class ProducerDataToKafkaByFlink {
    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromArgs(args);
        String servers = parameter.get("servers");
        if ("".equals(servers) || null == servers) {
            servers = "itserver23:6667,itserver22:6667,itserver21:6667";
        }
        String topic = parameter.get("topic");
        if ("".equals(topic) || null == topic) {
            topic = "vehicleentity";
        }
        String numPartitionsStr = parameter.get("numPartitions");
        int numPartitions;
        if ("".equals(numPartitionsStr) || null == numPartitionsStr) {
            numPartitions = 6;
        } else {
            numPartitions = Integer.parseInt(numPartitionsStr);
        }
        String replicationFactorStr = parameter.get("replicationFactor");
        short replicationFactor;
        if ("".equals(replicationFactorStr) || null == replicationFactorStr) {
            replicationFactor = 1;
        } else {
            replicationFactor = Short.parseShort(replicationFactorStr);
        }

        String dpvStr = parameter.get("dpv");
        long dpv;
        if ("".equals(dpvStr) || null == dpvStr) {
            dpv = 10;
        } else {
            dpv = Long.parseLong(dpvStr);
        }

        // create kafka topic if not exists.
        AdminClient kafkAdminClient = AdminClient.create(KafkaUtils.producerProps(servers));
        NewTopic newTopic = new NewTopic(topic, numPartitions, replicationFactor);
        newTopic.configs(new ImmutableMap.Builder<String, String>().put("cleanup.policy", "delete").
                put("retention.ms", Long.toString(86400000L)).
                put("retention.bytes", "-1").
                build());
        if (!new ArrayList<String>(kafkAdminClient.listTopics().names().get()).contains(topic))
            kafkAdminClient.createTopics(new ImmutableList.Builder<NewTopic>().add(newTopic).build());
        kafkAdminClient.close();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> producer =
                env.addSource(new EventSourceGenerator(dpv));
        producer
                .addSink(new FlinkKafkaProducer<String>(topic, new ProducerStringSerializationSchema(topic),
                        KafkaUtils.producerProps(servers), FlinkKafkaProducer.Semantic.EXACTLY_ONCE));

        env.execute("flink-produce-data");

    }
}
