package socket;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

/**
 * Created by lulei on 2018/3/21.
 */
public class SocketClient {
    public static void main(String[] args) throws IOException {
        String host = "localhost";
        int port = 8888;
        Socket socket = new Socket(host, port);
        OutputStream outputStream = socket.getOutputStream();
        String message="output demo";
        socket.getOutputStream().write(message.getBytes("UTF-8"));
        outputStream.close();
        socket.close();
    }
}
