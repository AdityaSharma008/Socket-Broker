import java.io.*;
import java.net.*;

public class Broker {
    private static final int PORT = 9092;

    public static void main(String[] args) {
        try (ServerSocket serverSocket = createServerSocket(PORT)) {
            while (true) {
                handleIncomingConnection(serverSocket);
            }
        } catch (IOException e) {
            System.err.println("Error in the server socket: " + e.getMessage());
            e.printStackTrace();
        }
    }

    static ServerSocket createServerSocket(int port) throws IOException {
        return new ServerSocket(port);
    }

    static void handleIncomingConnection(ServerSocket serverSocket) {
        try (Socket clientSocket = serverSocket.accept()) {
            System.out.println("Client Connected");
            handleClient(clientSocket);
        } catch (IOException e) {
            System.err.println("Error handling client connection: " + e.getMessage());
            e.printStackTrace();
        }
    }

    //Request format: 4bytes -> messageLength, 4bytes, 4bytes -> correlationId
    static void handleClient(Socket client) {
        try (DataInputStream in = new DataInputStream(client.getInputStream());
             DataOutputStream out = new DataOutputStream(client.getOutputStream())) {

            byte[] inputBytes = readClientMessage(in);
            int apiVersion = twoByteIntToInt(inputBytes, 2);
            int correlationID = byteToInt(inputBytes, 4);

            int errorCode = -1;
            if (apiVersion < 0 || apiVersion > 4) {
                errorCode = 35;
            }

            byte[] response = createMessage(correlationID, errorCode);
            out.write(response);

        } catch (IOException e) {
            System.err.println("Error in communication with client: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static byte[] readClientMessage(DataInputStream in) throws IOException {
        byte[] messageLengthBytes = new byte[4];
        in.readFully(messageLengthBytes);
        int messageLength = byteToInt(messageLengthBytes, 0);

        byte[] inputBytes = new byte[messageLength];
        in.readFully(inputBytes);
        return inputBytes;
    }

    static byte[] createMessage(int id, int errorCode) {
        byte[] idBytes = intToByteArray(id);
        byte[] errorBytes = new byte[0];

        if (errorCode != -1) {
            errorBytes = intToTwoByteArray(errorCode);
        }

        byte[] lenBytes = intToByteArray(idBytes.length + errorBytes.length);
        byte[] message = new byte[lenBytes.length + idBytes.length + errorBytes.length];

        System.arraycopy(lenBytes, 0, message, 0, lenBytes.length);
        System.arraycopy(idBytes, 0, message, lenBytes.length, idBytes.length);
        System.arraycopy(errorBytes, 0, message, lenBytes.length + idBytes.length, errorBytes.length);

        return message;
    }

    static byte[] intToByteArray(int n) {
        return new byte[]{
                (byte) (n >> 24),
                (byte) (n >> 16),
                (byte) (n >> 8),
                (byte) n
        };
    }

    static byte[] intToTwoByteArray(int n) {
        return new byte[]{
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

    private static int twoByteIntToInt(byte[] arr, int start) {
        return ((arr[start] & 0xFF) << 8) |
                (arr[start + 1] & 0xFF);
    }
}
