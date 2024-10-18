import java.io.*;
import java.net.*;

import org.junit.*;
import org.mockito.*;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;


public class BrokerTest {

    ServerSocket mockServerSocket;
    Socket mockSocket;
    DataInputStream mockInputStream;
    DataOutputStream mockOutputStream;

    @Before
    public void setup() throws IOException {
        mockServerSocket = Mockito.mock(ServerSocket.class);
        mockSocket = Mockito.mock(Socket.class);
        mockInputStream = Mockito.mock(DataInputStream.class);
        mockOutputStream = Mockito.mock(DataOutputStream.class);

        when(mockServerSocket.accept()).thenReturn(mockSocket);

        when(mockSocket.getInputStream()).thenReturn(mockInputStream);
        when(mockSocket.getOutputStream()).thenReturn(mockOutputStream);
    }

    @Test
    public void testServerSocketCreation() throws IOException {
        assertNotNull(Broker.createServerSocket(9092));
    }

    @Test
    public void clientConnectionHandling() throws IOException{
        mockServerSocket.accept();
        verify(mockServerSocket).accept();
    }
}
