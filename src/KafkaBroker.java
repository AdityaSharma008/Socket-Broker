import java.io.*;
import java.net.*;

public class KafkaBroker {
    private static final int PORT = 9092;

    public static void main(String[] args) {
        try{
            ServerSocket serverSocket = new ServerSocket(PORT);
            while (true) {
                Socket client = serverSocket.accept();
                System.out.println("Client Connected");
                handleClient(client);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void handleClient(Socket client){
        try {
            DataInputStream in = new DataInputStream(client.getInputStream());
            DataOutputStream out = new DataOutputStream(client.getOutputStream());

            out.write(createMessage(7));

            in.close();
            out.close();
            client.close();

        } catch (IOException e){
            e.printStackTrace();
        }
    }

    private static byte[] createMessage(int id){
        byte[] idBytes = intToByteArray(id);
        byte[] lenBytes = intToByteArray(idBytes.length);

        byte[] both = new byte[lenBytes.length + idBytes.length];
        System.arraycopy(lenBytes, 0, both, 0, lenBytes.length);
        System.arraycopy(idBytes, 0, both, lenBytes.length, idBytes.length);

        return both;
    }

    private static byte[] intToByteArray(int n){
        return new byte[] {
                (byte)(n >> 24),
                (byte)(n >> 16),
                (byte)(n >> 8),
                (byte)n
        };
    }
}
