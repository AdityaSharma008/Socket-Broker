import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;

import kafka.core.Broker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;


public class BrokerTest {
    ServerSocket mockServerSocket;
    Socket mockSocket;
    Broker.ClientHandler clientHandler;
    ByteArrayOutputStream outputStream;
    int correlationID, apiKey, apiVersion;

    @BeforeEach
    public void setup() throws IOException {
        correlationID = 12345;
        apiKey = 0;
        apiVersion = 2;
        // Mock ServerSocket and Socket objects
        mockServerSocket = Mockito.mock(ServerSocket.class);
        mockSocket = Mockito.mock(Socket.class);
        clientHandler = new Broker.ClientHandler(mockSocket);
        outputStream = new ByteArrayOutputStream();
        when(mockSocket.getOutputStream()).thenReturn(outputStream);

        when(mockServerSocket.accept()).thenReturn(mockSocket);
    }

    @Test
    public void testServerSocketCreation() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(9092)) {
            assertNotNull(serverSocket, "ServerSocket should not be null");
            assertEquals(9092, serverSocket.getLocalPort(), "ServerSocket should bind to port 9092");
        }
    }

    @Test
    public void testClientAccept() throws IOException {
        mockServerSocket.accept();
        verify(mockServerSocket).accept(); // Verifying if accept method was called
    }

    @Test
    public void testHandleClient() throws IOException {
        // Simulate input/output streams for a client connection
        byte[] inputData = createTestInput(apiKey, apiVersion, correlationID);
        InputStream inputStream = new ByteArrayInputStream(inputData);

        // Setting up streams in the client socket
        when(mockSocket.getInputStream()).thenReturn(inputStream);
        clientHandler.run();

        // Expectation: no error, so errorCode should be 0
        byte[] expectedResponse = createMessage(correlationID, 0, apiKey);
        byte[] actualResponse = outputStream.toByteArray();

        assertArrayEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testWrongAPIVersion() throws IOException {
        // Test scenario where API version is invalid
        correlationID = 12345;
        apiKey = 0;
        apiVersion = 10; // Invalid API version
        byte[] inputData = createTestInput(apiKey, apiVersion, correlationID);
        InputStream inputStream = new ByteArrayInputStream(inputData);

        when(mockSocket.getInputStream()).thenReturn(inputStream);

        clientHandler.run();

        // Expectation: invalid API version, so errorCode should be 35
        byte[] expectedResponse = createMessage(correlationID, 35, apiKey);
        byte[] actualResponse = outputStream.toByteArray();

        assertArrayEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testSequentialRequests() throws IOException {
        for (int i = 0; i < 3; i++) {
            correlationID += i;
            apiVersion += 2 * i;
            int errorCode = apiVersion > 4 ? 35 : 0;
            byte[] inputData = createTestInput(apiKey, apiVersion, correlationID);
            InputStream inputStream = new ByteArrayInputStream(inputData);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            // Setting up streams in the client socket
            when(mockSocket.getInputStream()).thenReturn(inputStream);
            when(mockSocket.getOutputStream()).thenReturn(outputStream);

            Broker.ClientHandler clientHandler = new Broker.ClientHandler(mockSocket);
            clientHandler.run();

            byte[] expectedResponse = createMessage(correlationID, errorCode, apiKey);
            byte[] actualResponse = outputStream.toByteArray();

            assertArrayEquals(expectedResponse, actualResponse);
        }
    }

    // Helper method to create a mock client message
    private byte[] createTestInput(int apiKey, int apiVersion, int correlationID) {
        // Convert various fields to byte arrays
        byte[] correlationIdBytes = intToByteArray(correlationID, 4);
        byte[] apiKeyBytes = intToByteArray(apiKey, 2);
        byte[] apiVerBytes = intToByteArray(apiVersion, 2);

        // Calculate message length and construct final input byte array
        byte[] messageLengthBytes = intToByteArray(correlationIdBytes.length + apiKeyBytes.length + apiVerBytes.length, 4);

        return concatenate(messageLengthBytes, apiKeyBytes, apiVerBytes, correlationIdBytes);
    }

    //correlationid(4) + errorcode(2) + 2(1) + apikey(2) + minVersion(2) + maxversion(2) + tag_buffer + throttle(4) + tag_buffer
    byte[] createMessage(int correlationId, int errorCode, int apiKey) {
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
}
