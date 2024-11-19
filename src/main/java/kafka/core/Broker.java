package kafka.core;

import kafka.utils.ByteUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

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
    public static ServerSocket createServerSocket(int port) throws IOException {
        return new ServerSocket(port);
    }

    // Handles a client request by reading the message, processing it, and responding
    public static class ClientHandler implements Runnable{
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

                int apiKey = ByteUtils.byteToInt(inputBytes, 0, 2);
                int apiVersion = ByteUtils.byteToInt(inputBytes, 2, 2);  // Assuming apiVersion is at index 2
                int correlationID = ByteUtils.byteToInt(inputBytes, 4, 4);     // Assuming correlationId is at index 4

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
            int messageLength = ByteUtils.byteToInt(messageLengthBytes, 0, 4);

            byte[] inputBytes = new byte[messageLength];  // Array for msg
            in.readFully(inputBytes);
            return inputBytes;
        }

        // Create message that includes correlationId and optional errorCode
        private byte[] createMessage(int correlationId, int errorCode, int apiKey) {
            int minVersion = 0, maxVersion = 4;
            int throttle_time_ms = 0;
            byte[] tagBuffer = {0x00};

            byte[] idBytes = ByteUtils.intToByteArray(correlationId, 4);
            byte[] errorBytes = ByteUtils.intToByteArray(errorCode, 2);
            byte[] apiBytes = ByteUtils.intToByteArray(apiKey, 2);

            byte[] message = ByteUtils.concatenate(idBytes, errorBytes, ByteUtils.intToByteArray(2, 1), apiBytes,
                    ByteUtils.intToByteArray(minVersion, 2), ByteUtils.intToByteArray(maxVersion, 2), tagBuffer, ByteUtils.intToByteArray(throttle_time_ms, 4), tagBuffer);

            return ByteUtils.concatenate(ByteUtils.intToByteArray(message.length, 4), message);
        }
    }
}
