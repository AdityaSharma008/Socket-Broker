package kafka.core;

import kafka.protocols.Message;
import kafka.requestHandlers.APIVersionsV4RequestHandler;
import kafka.utils.ByteUtils;
import kafka.protocols.Request;

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
            try (DataOutputStream out = new DataOutputStream(client.getOutputStream())) {

                Request request = Request.readFrom(clientSocket);

                APIVersionsV4RequestHandler message = new APIVersionsV4RequestHandler(request);
                Message msg = message.handle();

                // Create response message and send to client
//                byte[] response = createMessage(correlationID, errorCode, apiKey);
                byte[] response = msg.toBytes();
                out.write(response);

            } catch (IOException e) {
                System.err.println("Error in communication with client: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
}
