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

    // Create a server socket bound to the given port
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

    // Handles a client request by reading the message, processing it, and responding
    static void handleClient(Socket client) {
        try (DataInputStream in = new DataInputStream(client.getInputStream());
             DataOutputStream out = new DataOutputStream(client.getOutputStream())) {

            byte[] inputBytes = readClientMessage(in);

            int apiVersion = twoByteIntToInt(inputBytes, 2);  // Assuming apiVersion is at index 2
            int correlationID = byteToInt(inputBytes, 4);     // Assuming correlationId is at index 4

            // Determine error code based on apiVersion
            int errorCode = 0;
            if (apiVersion < 0 || apiVersion > 4) {
                errorCode = 35;  // Set error code if apiVersion is invalid
            }

            // Create response message and send to client
            byte[] response = createMessage(correlationID, errorCode);
            out.write(response);

        } catch (IOException e) {
            System.err.println("Error in communication with client: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // Reads the client's message length and the message content
    private static byte[] readClientMessage(DataInputStream in) throws IOException {
        byte[] messageLengthBytes = new byte[4];  // First 4 bytes are the message length

        in.readFully(messageLengthBytes);
        int messageLength = byteToInt(messageLengthBytes, 0);

        byte[] inputBytes = new byte[messageLength];  // Array for msg
        in.readFully(inputBytes);
        return inputBytes;
    }

    // Create message that includes correlationId and optional errorCode
    static byte[] createMessage(int correlationId, int errorCode) {
        byte[] idBytes = intToByteArray(correlationId);
        byte[] errorBytes = new byte[0];  // Default empty errorBytes

        errorBytes = intToTwoByteArray(errorCode);


        // Calculate total message length (idBytes + errorBytes) and convert to 4-byte array
        byte[] lenBytes = intToByteArray(idBytes.length + errorBytes.length);
        byte[] message = new byte[lenBytes.length + idBytes.length + errorBytes.length];

        // Copy length, id, and errorCode bytes into the final message
        System.arraycopy(lenBytes, 0, message, 0, lenBytes.length);
        System.arraycopy(idBytes, 0, message, lenBytes.length, idBytes.length);
        System.arraycopy(errorBytes, 0, message, lenBytes.length + idBytes.length, errorBytes.length);

        return message;
    }

    // Converts a 32-bit integer to a 4-byte array (big-endian)
    static byte[] intToByteArray(int n) {
        return new byte[]{
                (byte) (n >> 24),
                (byte) (n >> 16),
                (byte) (n >> 8),
                (byte) n
        };
    }

    // Converts a 16-bit integer to a 2-byte array (big-endian)
    static byte[] intToTwoByteArray(int n) {
        return new byte[]{
                (byte) (n >> 8),
                (byte) n
        };
    }

    // Converts 4 bytes from the array to a 32-bit integer
    private static int byteToInt(byte[] arr, int start) {
        return (arr[start] & 0xFF) << 24 |
                (arr[start + 1] & 0xFF) << 16 |
                (arr[start + 2] & 0xFF) << 8 |
                (arr[start + 3] & 0xFF);
    }

    // Converts 2 bytes from the array to a 16-bit integer
    private static int twoByteIntToInt(byte[] arr, int start) {
        return ((arr[start] & 0xFF) << 8) |
                (arr[start + 1] & 0xFF);
    }
}
