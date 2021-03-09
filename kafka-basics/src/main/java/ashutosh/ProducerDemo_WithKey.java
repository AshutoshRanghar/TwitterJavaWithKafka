package ashutosh;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemo_WithKey {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String bootstrapServers="127.0.0.1:9092";
        final Logger logger= LoggerFactory.getLogger(ProducerDemo_WithKey.class);
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //Key and Value<String,String>
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);
        //Create a producer record.
      for(int i=200;i<=210;i++) {
          String topic="turbine";
          String key="id_"+Integer.toString(i);
         String value="Data_"+Integer.toString(i);
          logger.info("Key:"+key);
        //Same Key records go into same partitions .
          ProducerRecord<String, String> record = new ProducerRecord<String, String>("turbine",key,value);
          producer.send(record, new Callback() {
              public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                  if (e == null) {
                      logger.info("Received new Metadata. \n" + "Topic Name " + recordMetadata.topic()
                              + "\nPartition :" + recordMetadata.partition() +
                              "\nOffset:" + recordMetadata.offset() +
                              "\nTimestamp: " + recordMetadata.timestamp());

                  } else
                      logger.info("Error while producing " + e);
              }
          }).get();
      }
        producer.flush();
    }
}

//Foot note
//Even with the multiple run we will be able to see that for same keys the data will be going to the same topic.Also do check first whether your topic has multiple partition or not.