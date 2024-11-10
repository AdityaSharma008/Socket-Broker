import java.io.*;
import java.net.*;
import java.util.Arrays;

import org.junit.*;
import org.mockito.*;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;


public class BrokerTest {
    ServerSocket mockServerSocket;
    Socket mockSocket;

    @Before
    public void setup() throws IOException {
        // Mock ServerSocket and Socket objects
        mockServerSocket = Mockito.mock(ServerSocket.class);
        mockSocket = Mockito.mock(Socket.class);

        when(mockServerSocket.accept()).thenReturn(mockSocket);
    }

    @Test
    public void testServerSocketCreation() throws IOException {
        assertNotNull(Broker.createServerSocket(9092));
    }

    @Test
    public void testClientAccept() throws IOException {
        mockServerSocket.accept();
        verify(mockServerSocket).accept(); // Verifying if accept method was called
    }

    @Test
    public void testHandleClient() throws IOException {
        // Simulate input/output streams for a client connection
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        int correlationID = 12345, apiKey = 0, apiVersion = 0;
        byte[] inputData = createTestInput(apiKey, apiVersion, correlationID);
        InputStream inputStream = new ByteArrayInputStream(inputData);

        // Setting up mocked streams in the client socket
        when(mockSocket.getInputStream()).thenReturn(inputStream);
        when(mockSocket.getOutputStream()).thenReturn(outputStream);

        Broker.handleClient(mockSocket);

        // Expectation: no error, so errorCode should be 0
        byte[] expectedResponse = createMessage(correlationID, 0, apiKey);
        byte[] actualResponse = outputStream.toByteArray();

        assertArrayEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testWrongAPIVersion() throws IOException {
        // Test scenario where API version is invalid
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        int correlationID = 12345, apiKey = 0, apiVersion = 10; // Invalid API version
        byte[] inputData = createTestInput(apiKey, apiVersion, correlationID);
        InputStream inputStream = new ByteArrayInputStream(inputData);

        when(mockSocket.getInputStream()).thenReturn(inputStream);
        when(mockSocket.getOutputStream()).thenReturn(outputStream);

        Broker.handleClient(mockSocket);

        // Expectation: invalid API version, so errorCode should be 35
        byte[] expectedResponse = createMessage(correlationID, 35, apiKey);
        byte[] actualResponse = outputStream.toByteArray();

        assertArrayEquals(expectedResponse, actualResponse);
    }

    // Helper method to create a mock client message
    private byte[] createTestInput(int apiKey, int apiVersion, int correlationID) {
        // Convert various fields to byte arrays
        byte[] correlationIdBytes = intToByteArray(correlationID, 4);
        byte[] apiKeyBytes = intToByteArray(apiKey, 2);
        byte[] apiVerBytes = intToByteArray(apiVersion, 2);

        // Calculate message length and construct final input byte array
        byte[] messageLengthBytes = intToByteArray(correlationIdBytes.length + apiKeyBytes.length + apiVerBytes.length, 4);
        byte[] input = new byte[messageLengthBytes.length + correlationIdBytes.length + apiKeyBytes.length + apiVerBytes.length];

        // Copying bytes to input array
        System.arraycopy(messageLengthBytes, 0, input, 0, messageLengthBytes.length);
        System.arraycopy(apiKeyBytes, 0, input, messageLengthBytes.length, apiKeyBytes.length);
        System.arraycopy(apiVerBytes, 0, input, messageLengthBytes.length + apiKeyBytes.length, apiVerBytes.length);
        System.arraycopy(correlationIdBytes, 0, input, messageLengthBytes.length + apiKeyBytes.length + apiVerBytes.length, correlationIdBytes.length);

        System.out.println(Arrays.toString(input));
        return input;
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

        System.out.println(Arrays.toString(message));

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

    private byte[] intToByteArray(int n, int numBytes){
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
}
