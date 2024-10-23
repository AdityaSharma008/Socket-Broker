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
        verify(mockServerSocket).accept();
    }

    //test handle client
    @Test
    public void testHandleClient() throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        int correlationID = 12345, apiKey = 0, apiVersion = 0;
        byte[] inputData = createTestInput(apiKey, apiVersion, correlationID);
        InputStream inputStream = new ByteArrayInputStream(inputData);

        when(mockSocket.getInputStream()).thenReturn(inputStream);
        when(mockSocket.getOutputStream()).thenReturn(outputStream);

        Broker.handleClient(mockSocket);

        byte[] expectedResponse = Broker.createMessage(correlationID);

        byte[] actualResponse = outputStream.toByteArray();
        assertArrayEquals(expectedResponse, actualResponse);
    }

    private byte[] createTestInput(int apiKey, int apiVersion, int correlationID) {
        byte[] correlationIdBytes = Broker.intToByteArray(correlationID);
        byte[] apiKeyBytes = intToTwoByteArray(apiKey);
        byte[] apiVerBytes = intToTwoByteArray(apiVersion);

        byte[] messageLengthBytes = Broker.intToByteArray(correlationIdBytes.length + apiKeyBytes.length + apiVerBytes.length);

        byte[] input = new byte[messageLengthBytes.length + correlationIdBytes.length + apiKeyBytes.length + apiVerBytes.length];

        System.arraycopy(messageLengthBytes, 0, input, 0, messageLengthBytes.length);
        System.arraycopy(apiKeyBytes, 0, input, messageLengthBytes.length, apiKeyBytes.length);
        System.arraycopy(apiVerBytes, 0, input, messageLengthBytes.length + apiKeyBytes.length, apiVerBytes.length);
        System.arraycopy(correlationIdBytes, 0, input, messageLengthBytes.length + apiKeyBytes.length + apiVerBytes.length, correlationIdBytes.length);

        return input;
    }

    private byte[] intToTwoByteArray(int n){
        return new byte[]{
                (byte) (n >> 8),
                (byte) n
        };
    }
}
