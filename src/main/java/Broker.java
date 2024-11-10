import java.io.*;
import java.net.*;
import java.util.Arrays;

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

            int apiKey = twoByteIntToInt(inputBytes, 0);
            int apiVersion = twoByteIntToInt(inputBytes, 2);  // Assuming apiVersion is at index 2
            int correlationID = byteToInt(inputBytes, 4);     // Assuming correlationId is at index 4

            // Determine error code based on apiVersion
            int errorCode = 0;
            if (apiVersion < 0 || apiVersion > 4) {
                errorCode = 35;  // Set error code if apiVersion is invalid
            }

            // Create response message and send to client
            byte[] response = createMessage(correlationID, errorCode, apiKey);
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
    static byte[] createMessage(int correlationId, int errorCode, int apiKey) {
        int minVersion = 0, maxVersion = 4;
        int throttle_time_ms = 0;
        byte[] tagBuffer = {0x00};

        byte[] idBytes = intToByteArray(correlationId, 4);
        byte[] errorBytes = intToByteArray(errorCode, 2);
        byte[] apiBytes = intToByteArray(apiKey, 2);

        byte[] message = concatenate(idBytes, errorBytes, intToByteArray(2, 1), apiBytes,
                intToByteArray(minVersion, 2), intToByteArray(maxVersion, 2), tagBuffer, intToByteArray(throttle_time_ms, 4), tagBuffer);

        System.out.println(Arrays.toString(message));

        return concatenate(intToByteArray(message.length, 4), message);
    }

    // Converts a 32-bit integer to a 4-byte array (big-endian)
    private static byte[] concatenate(byte[]... arrays) {
        int totalLength = 0;
        for (byte[] array : arrays) {
            totalLength += array.length;
        }
        byte[] result = new byte[totalLength];
        int offset = 0;
        for (byte[] array : arrays) {
            System.arraycopy(array, 0, result, offset, array.length);
            offset += array.length;
        }
        return result;
    }

    private static byte[] intToByteArray(int n, int numBytes){
        if(numBytes == 1){
            return new byte[]{
                    (byte) n
            };
        }
        else if(numBytes == 2) {
            return new byte[]{
                    (byte) (n >> 8),
                    (byte) n
            };
        }
        else {
            return new byte[]{
                    (byte) (n >> 24),
                    (byte) (n >> 16),
                    (byte) (n >> 8),
                    (byte) n
            };
        }
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
