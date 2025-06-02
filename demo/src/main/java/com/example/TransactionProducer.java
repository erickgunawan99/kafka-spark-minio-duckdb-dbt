package com.example;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class TransactionProducer {

    private static final Logger logger = LoggerFactory.getLogger(TransactionProducer.class);
    private static final String TOPIC = "order";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final int NUM_THREADS = 2;
    private static final int NUM_PARTITIONS = 4;
    private static final short REPLICATION_FACTOR = 1;

    private static void createTopicIfNotExist() {

        Properties prop = new Properties();
        prop.put("bootstrap.servers", BOOTSTRAP_SERVERS);

        try (AdminClient admin = AdminClient.create(prop)) {
            boolean exist = admin.listTopics().names().get().contains(TOPIC);
            if (!exist) {
                logger.info("topic not exist, creating topic {}", TOPIC);
                NewTopic newTopic = new NewTopic(TOPIC, NUM_PARTITIONS, REPLICATION_FACTOR);
                admin.createTopics(Collections.singletonList(newTopic)).all().get();
                logger.info("succesfully created topic", TOPIC);
            }
            else {
                logger.info("topic {} exists", TOPIC);
            }
        }
        catch (Exception ex) {
            logger.error("error creating topic", ex);
        }
    }  

  
    

    public static void main(String[] args) {
       createTopicIfNotExist();
        
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.put(ProducerConfig.BATCH_SIZE_CONFIG,1310);
        prop.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        prop.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        prop.put(ProducerConfig.ACKS_CONFIG, "1");

        ExecutorService exec = Executors.newFixedThreadPool(NUM_THREADS);
        ObjectMapper obj = new ObjectMapper();

        for (int i = 0; i < NUM_THREADS; i++) {
            exec.submit((Runnable) () -> {
                long startTime = System.currentTimeMillis();
                long recordsSent = 0;

                try (KafkaProducer<String, String> producer = new KafkaProducer<>(prop)) {
                    while (true) {
                        Transaction randomTrans = Transaction.randomTrans();
                        String transJson = obj.writeValueAsString(randomTrans);
                        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, randomTrans.getTransactionId(), transJson);
                        producer.send(record, (RecordMetadata metadata, Exception exception) -> {
                            if (exception != null ) {
                                logger.error("failed send key {} and due to {}", record.key(), exception.getMessage());
                            } else {
                                logger.info("record w key {} sent to partition {} w offset{}", record.key(), metadata.partition(), metadata.offset());
                            }
                        });
                        recordsSent++;
                        long elapsedTime = System.currentTimeMillis() - startTime;
                        if (elapsedTime >= 1000) {
                            double thru = recordsSent / (elapsedTime/1000);
                            logger.info("throughput {} rec/sec", thru);
                            recordsSent = 0;
                            startTime = System.currentTimeMillis();
                        }
                        
                    }} catch (Exception ex) {
                        logger.error("error", ex);
                }});
            }
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                exec.shutdownNow();
                logger.info("producer shutdown");
            }));

    }
}