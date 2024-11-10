import java.io.*;
import java.net.*;

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

        // Expectation: no error, so errorCode should be -1
        byte[] expectedResponse = Broker.createMessage(correlationID, 0);
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
        byte[] expectedResponse = Broker.createMessage(correlationID, 35);
        byte[] actualResponse = outputStream.toByteArray();

        assertArrayEquals(expectedResponse, actualResponse);
    }

    // Helper method to create a mock client message
    private byte[] createTestInput(int apiKey, int apiVersion, int correlationID) {
        // Convert various fields to byte arrays
        byte[] correlationIdBytes = Broker.intToByteArray(correlationID);
        byte[] apiKeyBytes = Broker.intToTwoByteArray(apiKey);
        byte[] apiVerBytes = Broker.intToTwoByteArray(apiVersion);

        // Calculate message length and construct final input byte array
        byte[] messageLengthBytes = Broker.intToByteArray(correlationIdBytes.length + apiKeyBytes.length + apiVerBytes.length);
        byte[] input = new byte[messageLengthBytes.length + correlationIdBytes.length + apiKeyBytes.length + apiVerBytes.length];

        // Copying bytes to input array
        System.arraycopy(messageLengthBytes, 0, input, 0, messageLengthBytes.length);
        System.arraycopy(apiKeyBytes, 0, input, messageLengthBytes.length, apiKeyBytes.length);
        System.arraycopy(apiVerBytes, 0, input, messageLengthBytes.length + apiKeyBytes.length, apiVerBytes.length);
        System.arraycopy(correlationIdBytes, 0, input, messageLengthBytes.length + apiKeyBytes.length + apiVerBytes.length, correlationIdBytes.length);

        return input;
    }
}
