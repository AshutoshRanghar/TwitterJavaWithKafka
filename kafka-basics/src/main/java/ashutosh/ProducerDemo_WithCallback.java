package ashutosh;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo_WithCallback {
    public static void main(String[] args) {
        String bootstrapServers="127.0.0.1:9092";
        final Logger logger= LoggerFactory.getLogger(ProducerDemo_WithCallback.class);
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //Key and Value<String,String>
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);
        //Create a producer record.
      for(int i=200;i<=210;i++) {
         // String topic="turbine";
         // String key="id_"+Integer.toString(i);
          //String value="Data_"+Integer.toString(i);
          ProducerRecord<String, String> record = new ProducerRecord<String, String>("turbine",Integer.toString(i));
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
          });
      }
        producer.flush();
    }
}
