package elasticSearch_Kafka;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchBulkConsumer {
    public static RestHighLevelClient createClient() {
        //replaced with own credenitals
        String hostname = "kafka-course-8273425835.us-east-1.bonsaisearch.net";
        String username = "88lq5l3z5";
        String password = "yk9owykcdd";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client=new RestHighLevelClient(builder);
        return  client;
        //Return client which will help to insert data to elastic search

    }

    public static KafkaConsumer<String,String> createConsumer(String topic)
    {
        String bootstrapServers = "127.0.0.1:9092";
        String groupId="kafka-demo-elastic-search";
        // String topic="twitter_tweets";
        Logger logger=LoggerFactory.getLogger(ElasticSearch_Consumer.class.getName());
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //Consumer Commit Strategy

        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        //MAX_POLL_RECORDS_CONFIG 10 records batch
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"10");//maximum number of bulk/batch records recieved from 1 single request


        KafkaConsumer<String,String> consumer=new KafkaConsumer<String,String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return  consumer;
    }


    public  static void  main(String[] args) throws IOException {
        Logger logger= LoggerFactory.getLogger(ElasticSearch_Consumer.class.getName());

        RestHighLevelClient client=createClient();
        //Use this command to reset the topic offsets
        //kafka-consumer-groups --bootstrap-server 127.0.0.1:9092 --group kafka-demo-elastic-search --reset-offsets --to-earliest --execute --topic twitter_tweets
        KafkaConsumer<String,String> consumer=createConsumer("twitter_tweets");
        while (true)
        {
            ConsumerRecords<String,String> records= consumer.poll(Duration.ofMillis(100));//poll is changed instead of direct duration we need to pass Duration object.
             Integer records_count=records.count();
            logger.info("Received"+records.count()+ "records");
            //We are creating Bulk request Class of Elastic Search
            BulkRequest bulkRequest=new BulkRequest();
            for (ConsumerRecord<String,String> record:records) {   //Imp point as we are assigning jsonString the value of the Consumer Value
              try {
                  String jsonString = record.value();
                  String main_id = record.topic() + "_" + record.partition() + "_" + record.offset();
                  IndexRequest indexRequest = new IndexRequest(
                          "twitter",
                          "tweets",
                          main_id//This is used to maintaining idempotence in the Elastic Search so that a single ID
                  ).source(jsonString, XContentType.JSON);
                  //Adding request to bulk parameter
                  bulkRequest.add(indexRequest);
              }
              catch (NullPointerException e)
              {
                  logger.warn("Skipping bad data "+record.value());
              }
              }

            if(records_count>0) {
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);

                try {
                    Thread.sleep(5000);
                    logger.info("Committing the Offset after record count"+records_count);
                    consumer.commitSync();
                    logger.info("Offsets has been comitted");

                    //This is used to show how it is inserted
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    //client.close();
}
