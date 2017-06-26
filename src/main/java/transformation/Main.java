package transformation;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * Created by amaingli on 6/26/17.
 */
public class Main {
    public static void main(String[] args){
        Properties consumerProps = new Properties();

        consumerProps.put("bootstrap.servers", "localhost:9092");
        consumerProps.put("group.id", "test-consumer-group");
        // offsets committed automatically with a frequency controlled by the auto.commit.interval.ms
        consumerProps.put("enable.auto.commit", "true");
        consumerProps.put("auto.commit.interval.ms", "1000");
        consumerProps.put("session.timeout.ms", "30000");  // heartbeat, regarded dead
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  // Strings to strings
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("auto.offset.reset", "earliest");

        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:9092");
        producerProps.put("acks", "all");
        producerProps.put("retries", 0);
        producerProps.put("batch.size", 16384);
        producerProps.put("linger.ms", 1);
        producerProps.put("buffer.memorty", 33554432);
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProps);
        Collection collection = new ArrayList<TopicPartition>();
        //specify the topic partitions
        TopicPartition partition0 = new TopicPartition("transformation-consumer", 0);
        collection.add(partition0);
        //assignment is essential
        consumer.assign(Arrays.asList(partition0));
        //reset the offset to the beginning
        consumer.seekToBeginning(collection);

        System.out.println(consumer.assignment());
        System.out.println(consumer.beginningOffsets(collection));


        Producer<String, String> producer = new KafkaProducer<String, String>(producerProps);

        int count = 0;
        Date start = new Date();
        System.out.println(start);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                count++;
                //System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
                //System.out.println();
                //System.out.println("count: " + count);
                producer.send(new ProducerRecord<String, String>("transformation-producer", record.key(), record.value()));


                Date end = new Date();
                long interval = end.getTime()-start.getTime();
                System.out.println(interval);


            }
        }

    }
}
