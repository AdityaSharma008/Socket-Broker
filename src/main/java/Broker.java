import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;

public class Broker {
    private static final int PORT = 9092;

    public static void main(String[] args) {
        try (ServerSocket serverSocket = createServerSocket(PORT)) {
            serverSocket.setReuseAddress(true);
            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    System.out.println("Client Connected");
                    ClientHandler client = new ClientHandler(clientSocket);
                    new Thread(client).start();
                } catch (IOException e) {
                    System.err.println("Error handling client connection: " + e.getMessage());
                    e.printStackTrace();
                }
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

    // Handles a client request by reading the message, processing it, and responding
    protected static class ClientHandler implements Runnable{
        private final Socket clientSocket;

        public ClientHandler(Socket socket){
            this.clientSocket = socket;
        }

        public void run(){
            handleClient(clientSocket);
        }

        private void handleClient(Socket client) {
            try (DataInputStream in = new DataInputStream(client.getInputStream());
                 DataOutputStream out = new DataOutputStream(client.getOutputStream())) {

                byte[] inputBytes = readClientMessage(in);

                int apiKey = byteToInt(inputBytes, 0, 2);
                int apiVersion = byteToInt(inputBytes, 2, 2);  // Assuming apiVersion is at index 2
                int correlationID = byteToInt(inputBytes, 4, 4);     // Assuming correlationId is at index 4

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
        private byte[] readClientMessage(DataInputStream in) throws IOException {
            byte[] messageLengthBytes = new byte[4];  // First 4 bytes are the message length

            in.readFully(messageLengthBytes);
            int messageLength = byteToInt(messageLengthBytes, 0, 4);

            byte[] inputBytes = new byte[messageLength];  // Array for msg
            in.readFully(inputBytes);
            return inputBytes;
        }

        // Create message that includes correlationId and optional errorCode
        private byte[] createMessage(int correlationId, int errorCode, int apiKey) {
            int minVersion = 0, maxVersion = 4;
            int throttle_time_ms = 0;
            byte[] tagBuffer = {0x00};

            byte[] idBytes = intToByteArray(correlationId, 4);
            byte[] errorBytes = intToByteArray(errorCode, 2);
            byte[] apiBytes = intToByteArray(apiKey, 2);

            byte[] message = concatenate(idBytes, errorBytes, intToByteArray(2, 1), apiBytes,
                    intToByteArray(minVersion, 2), intToByteArray(maxVersion, 2), tagBuffer, intToByteArray(throttle_time_ms, 4), tagBuffer);

            return concatenate(intToByteArray(message.length, 4), message);
        }

        // Converts a 32-bit integer to a 4-byte array (big-endian)
        private byte[] concatenate(byte[]... arrays) {
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

        private byte[] intToByteArray(int value, int size) {
            if (size < 1 || size > 4) {
                throw new IllegalArgumentException("Size must be between 1 and 4 bytes for an int.");
            }
            ByteBuffer buffer = ByteBuffer.allocate(4);
            buffer.putInt(value);

            byte[] fullArray = buffer.array();
            byte[] result = new byte[size];

            System.arraycopy(fullArray, 4 - size, result, 0, size);

            return result;
        }

        // Converts 4 bytes from the array to a 32-bit integer
        private int byteToInt(byte[] arr, int start, int byteCount) {
            int result = 0;
            for (int i = 0; i < byteCount; i++) {
                result |= (arr[start + i] & 0xFF) << ((byteCount - i - 1) * 8);
            }
            return result;
        }
    }
}
