package ashutosh;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer_Assign_and_Seek {
    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";
        String topic="turbine";
        Logger logger=LoggerFactory.getLogger(ConsumerDemo.class.getName());
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        KafkaConsumer<String,String> consumer=new KafkaConsumer<String,String>(properties);
        TopicPartition partition_To_ReadFrom=new TopicPartition(topic,0);
        long offset_To_Read=15L;
        //assign
        consumer.assign(Arrays.asList(partition_To_ReadFrom));

        //seek
        consumer.seek(partition_To_ReadFrom,offset_To_Read);
        int numberOfMessagesToRead=5;
        boolean keepOnReading=true;
        int numOfMessagesReadSoFar=0;

        while (keepOnReading)
        {
            ConsumerRecords<String,String> records= consumer.poll(Duration.ofMillis(100));//poll is changed instead of direct duration we need to pass Duration object.
            for (ConsumerRecord<String,String> record:records)
            {
                logger.info("Key : "+record.key()+"  Value is : "+record.value());
                logger.info("Partition : "+record.partition()+"  Offset Value : "+record.offset());
                logger.info("The current offset val"+numOfMessagesReadSoFar);
                numOfMessagesReadSoFar=numOfMessagesReadSoFar+1;
                if(numOfMessagesReadSoFar>=numberOfMessagesToRead)
                {
                    keepOnReading=false;//to exit
                    break; //exit the for loop
                }
            }
        }
        logger.info("Exiting the applicaiton");
    }
}


//From this code we can read from a range of offset of a Kafka Consumer.