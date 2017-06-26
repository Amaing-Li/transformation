package transformation;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by amaingli on 6/23/17.
 */
public class ConsumerTrial {
    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("goup.id", "transformation-consumer");
        //1 props.put("enable.auto.commit", "true");  // offsets committed automatically with a frequency controlled by the auto.commit.interval.ms
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");  // heartbeat, regarded dead
        //props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  // bytes to strings
        //props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        KafkaConsumer<Byte, Byte> consumer = new KafkaConsumer<Byte, Byte>(props);
        consumer.subscribe(Arrays.asList("transformation-test"));  // topic
/*1       while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
            }
        }*/
        while (true) {
            ConsumerRecords<Byte, Byte> records = consumer.poll(100);
            for (ConsumerRecord<Byte, Byte> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());

            }
        }

/*        final int minBatchSize = 200;
        //List<ConsumerRecord<String, String>> buffer = new ArrayList<ConsumerRecord<String, String>>();
        List<ConsumerRecord<Byte, Byte>> buffer = new ArrayList<ConsumerRecord<Byte, Byte>>();
        while (true) {
            //ConsumerRecords<String, String> records = consumer.poll(100);
            ConsumerRecords<Byte, Byte> records = consumer.poll(100);
            //for (ConsumerRecord<Byte, String> record : records) {
            for (ConsumerRecord<Byte, Byte> record : records) {
                buffer.add(record);
            }
            if (buffer.size() >= minBatchSize) {  //?
                //insertIntoDb(buffer);
                consumer.commitSync(); //to mark all received messaes as commited
                buffer.clear();
            }
        }*/
    }


}
