import java.io.*;
import java.net.*;
import java.util.Arrays;

public class KafkaBroker {
    private static final int PORT = 9092;

    public static void main(String[] args) {
        try {
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

    private static void handleClient(Socket client) {
        try {
            DataInputStream in = new DataInputStream(client.getInputStream());
            DataOutputStream out = new DataOutputStream(client.getOutputStream());

            byte[] messageLengthBytes = new byte[4];
            in.readFully(messageLengthBytes);
            int messageLength = byteToInt(messageLengthBytes, 0);

            byte[] inputBytes = new byte[messageLength];
            in.readFully(inputBytes);

            int coRelationID = byteToInt(inputBytes, 4);
            System.out.println(coRelationID);
            out.write(createMessage(coRelationID));

            in.close();
            out.close();
            client.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static byte[] createMessage(int id) {
        byte[] idBytes = intToByteArray(id);
        byte[] lenBytes = intToByteArray(idBytes.length);

        byte[] both = new byte[lenBytes.length + idBytes.length];
        System.arraycopy(lenBytes, 0, both, 0, lenBytes.length);
        System.arraycopy(idBytes, 0, both, lenBytes.length, idBytes.length);
        return both;
    }

    private static byte[] intToByteArray(int n) {
        return new byte[] {
                (byte) (n >> 24),
                (byte) (n >> 16),
                (byte) (n >> 8),
                (byte) n
        };
    }

    private static int byteToInt(byte[] arr, int start) {
        return (arr[start] & 0xFF) << 24 |
                (arr[start + 1] & 0xFF) << 16 |
                (arr[start + 2] & 0xFF) << 8 |
                (arr[start + 3] & 0xFF);
    }
}
