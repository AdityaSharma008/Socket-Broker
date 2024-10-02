import java.net.*;

public class KafkaBroker {
    public static void main(String[] args) {
        try{
            ServerSocket serverSocket = new ServerSocket(9092);
            Socket client = serverSocket.accept();
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
