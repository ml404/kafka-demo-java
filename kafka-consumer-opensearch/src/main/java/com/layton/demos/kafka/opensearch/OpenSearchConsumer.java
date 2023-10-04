package com.layton.demos.kafka.opensearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {

    public static final String TOPIC = "wikimedia.recentchange";

    public static void main(String[] args) throws IOException {
        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class);

        //create an opensource client
        RestHighLevelClient openSearchClient = createOpenSearchClient();
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
            consumer.wakeup();

            //join the main thread to allow the execution of the code in the main thread
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try (openSearchClient; consumer) {
            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);
            if (!indexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("The Wikimedia Index has been created!");
            } else {
                log.info("The Wikimedia Index already exists");
            }
            consumer.subscribe(Collections.singleton(TOPIC));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

                BulkRequest bulkRequest = new BulkRequest();

                int recordCount = records.count();

                log.info(String.format("Received %s records", recordCount));

                for (ConsumerRecord<String, String> record : records) {
                    //if you have a unique id when sending the request, you have an idempotent consumer
                    String id = extractId(record.value());
                    IndexRequest request = new IndexRequest("wikimedia").source(record.value(), XContentType.JSON).id(id);
                    bulkRequest.add(request);
                }

                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info(String.format("Inserted '%s' record(s).", bulkResponse.getItems().length));
                    consumer.commitSync();
                    log.info("Offsets have been committed!");
                }


            }
        }
        catch (WakeupException e) {
            log.info("Consumer is starting to shut down");
        } catch (Exception e) {
            log.info("Unexpected exception in the consumer", e);
        } finally {
            consumer.close();
            openSearchClient.close();
            log.info("Consumer has been taken out back");
        }


        // main code logic

    }

    private static String extractId(String json) {
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {

        String groupId = "consumer-opensearch-demo";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // create consumer
        return new KafkaConsumer<>(properties);

    }


    public static RestHighLevelClient createOpenSearchClient() {
        String connectionString = "http://localhost:9200";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connectionURI = URI.create(connectionString);
        // extract login information if it exists
        String userInfo = connectionURI.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connectionURI.getHost(), connectionURI.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connectionURI.getHost(), connectionURI.getPort(), connectionURI.getScheme())).setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp).setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }

}
