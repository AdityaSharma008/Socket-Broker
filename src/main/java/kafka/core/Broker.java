package kafka.core;

import kafka.protocols.Message;
import kafka.requestHandlers.APIVersionsV4RequestHandler;
import kafka.protocols.Request;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Broker {
    private static final int PORT = 9092;
    private static final int THREAD_POOL_SIZE = 10;
    private static final ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

    public static void main(String[] args) {
        new Broker().startServer(PORT);
    }

    public void startServer(int port) {
        try (ServerSocket serverSocket = createServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            acceptClients(serverSocket);
        } catch (IOException e) {
            System.err.println("Error in the server socket: " + e.getMessage());
            e.printStackTrace();
        } finally {
            threadPool.shutdown();
        }
    }

    private ServerSocket createServerSocket(int port) throws IOException {
        return new ServerSocket(port);
    }

    private void acceptClients(ServerSocket serverSocket) {
        while (true) {
            try {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Client Connected");
                handleClient(clientSocket);
            } catch (IOException e) {
                System.err.println("Error handling client connection: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    private void handleClient(Socket clientSocket) {
        ClientHandler client = new ClientHandler(clientSocket);
        threadPool.execute(client);
    }

    // Handles a client request by reading the message, processing it, and responding
    public static class ClientHandler implements Runnable {
        private final Socket clientSocket;

        public ClientHandler(Socket socket) {
            this.clientSocket = socket;
        }

        public void run() {
            handleClient(clientSocket);
        }

        private void handleClient(Socket client) {
            try (DataOutputStream out = new DataOutputStream(client.getOutputStream())) {

                Request request = Request.readFrom(clientSocket);

                APIVersionsV4RequestHandler message = new APIVersionsV4RequestHandler(request);
                Message msg = message.handle();

                byte[] response = msg.toBytes();
                out.write(response);

            } catch (IOException e) {
                System.err.println("Error in communication with client: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
}
