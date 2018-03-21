package socket;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by lulei on 2018/3/21.
 */
public class SocketServer {
    public static void main(String[] args) throws IOException, InterruptedException {
        int port = 8888;
        ServerSocket server = new ServerSocket(port);
        System.out.println("start server at port " + port);

        Socket socket = server.accept();
        OutputStream outputStream = socket.getOutputStream();
        PrintWriter printWriter = new PrintWriter(outputStream);
        for(int i=0; i<100; i++){
            if(i % 5 == 0){
                Thread.sleep(100);
            }
            printWriter.println(i);
        }
        socket.close();
        server.close();

    }
    public static void readFromSocket(Socket socket) throws IOException {
        InputStream inputStream = socket.getInputStream();
        byte[] bytes = new byte[1024];
        int len;
        StringBuilder sb = new StringBuilder();
        while ((len = inputStream.read(bytes)) != -1) {
            // 注意指定编码格式，发送方和接收方一定要统一，建议使用UTF-8
            sb.append(new String(bytes, 0, len,"UTF-8"));
        }
        System.out.println("get message from client: " + sb);
        inputStream.close();
        socket.close();
    }
}
