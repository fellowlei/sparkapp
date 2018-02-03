package kafka.api;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProduerDemo {
    public static void main(String[] args) {

        Producer<String, String> producer = getProducer();
        for (int i = 0; i < 100; i++){
            System.out.println(i);
            producer.send(new ProducerRecord<String, String>("mylog", Integer.toString(i), Integer.toString(i)));
        }


        producer.close();
    }

    public static void send(String topic,String head,String msg){
        Producer<String, String> producer = getProducer();
        producer.send(new ProducerRecord<String, String>(topic, head, msg));
    }

    public static Producer<String,String> getProducer(){
        Properties props = getPropertie();
        Producer<String, String> producer = new KafkaProducer<>(props);
        return producer;
    }


    public static Properties getPropertie(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
}
