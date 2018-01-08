package kafka.demo;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by lulei on 2018/1/8.
 */
public class IpGen {

    private static ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
    private static Random random =new Random();
    private static String  pre= "192.168.1.";
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.28.5.2:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        final Producer<String, String> producer = new KafkaProducer<String, String>(props);


        String topic = "ip_collect";
        executorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                for(int i=1; i<11; i++){
                    for(int j=1; j<=i; j++){
                        String ip = genStaticIp(j);
                        System.out.println(ip);
                        producer.send(new ProducerRecord<String, String>(topic, ip, ip));
                    }
                }

            }
        },1,1, TimeUnit.SECONDS);

//        producer.close();
//        System.out.println("end");

    }

    public static String genIp(){
        int last = random.nextInt(100);
        String ip = pre + last;
        return ip;
    }

    public static String genStaticIp(int last){
        String ip = pre + last;
        return ip;
    }
}
