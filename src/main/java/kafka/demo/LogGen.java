package kafka.demo;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LogGen {
    private static ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);


    public static void main(String[] args) throws FileNotFoundException {
        File file = new File("d:/app/access.log");
        PrintWriter writer = new PrintWriter(file);
        executorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                for(int i=0; i<10;i++){
                    String msg = "id=" + i + ",name=name"+ i +"\n";
//                    String msg2 = "001  000001 0.6";
                    writer.append(msg);
                    writer.flush();
                }
            }
        },1,1000, TimeUnit.MILLISECONDS);
    }
}
