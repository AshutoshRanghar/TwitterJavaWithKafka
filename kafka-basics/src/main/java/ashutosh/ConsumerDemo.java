package ashutosh;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";
        String groupId="my-fourth-application";
        String topic="turbine";
    Logger logger=LoggerFactory.getLogger(ConsumerDemo.class.getName());
    Properties properties=new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        KafkaConsumer<String,String> consumer=new KafkaConsumer<String,String>(properties);
       //Subscribe for only single topic thats why singleton used.
        consumer.subscribe(Arrays.asList(topic));

        while (true)
        {
         ConsumerRecords<String,String> records= consumer.poll(Duration.ofMillis(100));//poll is changed instead of direct duration we need to pass Duration object.
            for (ConsumerRecord<String,String> record:records)
            {
            logger.info("Key :"+record.key()+"Value is "+record.value());
            logger.info("Partition "+record.partition()+"Offset"+record.offset());

            }
        }
    }
    }
