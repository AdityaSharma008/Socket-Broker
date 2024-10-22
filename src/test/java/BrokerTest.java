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

        byte[] inputData = createTestInput();
        InputStream inputStream = new ByteArrayInputStream(inputData);

        when(mockSocket.getInputStream()).thenReturn(inputStream);
        when(mockSocket.getOutputStream()).thenReturn(outputStream);

        Broker.handleClient(mockSocket);

        int expectedCorrelationId = 12345;
        byte[] expectedResponse = Broker.createMessage(expectedCorrelationId);

        byte[] actualResponse = outputStream.toByteArray();
        assertArrayEquals(expectedResponse, actualResponse);
    }

    private byte[] createTestInput() {
        byte[] correlationIdBytes = Broker.intToByteArray(12345);
        //todo: change test api bytes to actual api bytes
        byte[] apiBytes = new byte[]{0, 0, 0, 0};
        byte[] messageLengthBytes = Broker.intToByteArray(correlationIdBytes.length + apiBytes.length);

        byte[] input = new byte[messageLengthBytes.length + correlationIdBytes.length + apiBytes.length];
        System.arraycopy(messageLengthBytes, 0, input, 0, messageLengthBytes.length);
        System.arraycopy(apiBytes, 0, input, messageLengthBytes.length, apiBytes.length);
        System.arraycopy(correlationIdBytes, 0, input, messageLengthBytes.length + apiBytes.length, correlationIdBytes.length);

        return input;
    }
}
