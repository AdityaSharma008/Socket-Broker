//package main.java;

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

    private static ServerSocket createServerSocket(int port) throws IOException {
        return new ServerSocket(port);
    }

    private static void handleIncomingConnection(ServerSocket serverSocket) {
        try (Socket clientSocket = serverSocket.accept()) {
            System.out.println("Client Connected");
            handleClient(clientSocket);
        } catch (IOException e) {
            System.err.println("Error handling client connection: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void handleClient(Socket client) {
        try (DataInputStream in = new DataInputStream(client.getInputStream());
             DataOutputStream out = new DataOutputStream(client.getOutputStream())) {

            byte[] inputBytes = readClientMessage(in);
            int correlationID = byteToInt(inputBytes, 4);

            System.out.println("Correlation ID: " + correlationID);
            byte[] response = createMessage(correlationID);
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

    private static byte[] createMessage(int id) {
        byte[] idBytes = intToByteArray(id);
        byte[] lenBytes = intToByteArray(idBytes.length);

        byte[] message = new byte[lenBytes.length + idBytes.length];
        System.arraycopy(lenBytes, 0, message, 0, lenBytes.length);
        System.arraycopy(idBytes, 0, message, lenBytes.length, idBytes.length);
        return message;
    }

    private static byte[] intToByteArray(int n) {
        return new byte[]{
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
