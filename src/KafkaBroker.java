import java.net.*;

public class KafkaBroker {
    private static final int PORT = 9092;
    public static void main(String[] args) {
        try{
            ServerSocket serverSocket = new ServerSocket(PORT);
            Socket client = serverSocket.accept();
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
