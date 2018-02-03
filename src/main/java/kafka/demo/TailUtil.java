package kafka.demo;

import kafka.api.KafkaProduerDemo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class TailUtil {
    public static boolean stop = false;
    public static void main(String[] args) throws IOException {
            tail("d:/app/access.log");
    }

    public static void tail(String filePath) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(new File(filePath)));
        String line = null;
        while (!stop) {
            while ((line = br.readLine()) != null) {
                System.out.println("line" + line);
                KafkaProduerDemo.send("mylog","log",line);
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        System.out.println("end");
        br.close();
    }
}
