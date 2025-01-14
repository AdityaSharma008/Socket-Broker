package kafka.protocols;

import kafka.utils.ByteUtils;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;

public class Request {
    private final short apiKey;
    private final short apiVersion;
    private final int correlationID;

    Request(short apiKey, short apiVersion, int correlationID) {
        this.apiKey = apiKey;
        this.apiVersion = apiVersion;
        this.correlationID = correlationID;
    }

    public static Request readFrom(Socket clientSocket) {
        try {
            DataInputStream in = new DataInputStream(clientSocket.getInputStream());

            byte[] inputBytes = readClientMessage(in);

            short apiKey = (short)ByteUtils.byteToInt(inputBytes, 0, 2);
            short apiVersion = (short)ByteUtils.byteToInt(inputBytes, 2, 2);  // Assuming apiVersion is at index 2
            int correlationID = ByteUtils.byteToInt(inputBytes, 4, 4);     // Assuming correlationId is at index 4

            return new Request(apiKey, apiVersion, correlationID);
        } catch (IOException e) {
            System.err.println("Error in communication with client: " + e.getMessage());
            e.printStackTrace();

            return null;
        }
    }

    private static byte[] readClientMessage(DataInputStream in) throws IOException {
        byte[] messageLengthBytes = new byte[4];  // First 4 bytes are the message length

        in.readFully(messageLengthBytes);
        int messageLength = ByteUtils.byteToInt(messageLengthBytes, 0, 4);

        byte[] inputBytes = new byte[messageLength];  // Array for msg
        in.readFully(inputBytes);
        return inputBytes;
    }

    public short getApiKey() {
        return apiKey;
    }

    public short getApiVersion() {
        return apiVersion;
    }

    public int getCorrelationID() {
        return correlationID;
    }
}
