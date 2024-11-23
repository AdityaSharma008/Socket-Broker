import java.io.*;
import java.net.*;
import java.util.Arrays;
import java.util.Map;

import kafka.core.Broker;
import kafka.protocols.APIVersions;
import kafka.utils.ByteUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;


public class APIVersionsResponseTest {
    ServerSocket mockServerSocket;
    Socket mockSocket;
    Broker.ClientHandler clientHandler;
    ByteArrayOutputStream outputStream;
    Helper helper = new Helper();
    int correlationID, apiKey, apiVersion;

    @BeforeEach
    public void setup() throws IOException {
        correlationID = 12345;
        apiKey = 18;
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
        byte[] inputData = helper.createTestInput(apiKey, apiVersion, correlationID);
        InputStream inputStream = new ByteArrayInputStream(inputData);

        // Setting up streams in the client socket
        when(mockSocket.getInputStream()).thenReturn(inputStream);
        clientHandler.run();

        // Expectation: no error, so errorCode should be 0
        byte[] expectedResponse = createMessage(correlationID, 0);
        byte[] actualResponse = outputStream.toByteArray();

        assertArrayEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testWrongAPIVersion() throws IOException {
        // Test scenario where API version is invalid
        correlationID = 12345;
        apiKey = 18;
        apiVersion = 10; // Invalid API version
        byte[] inputData = helper.createTestInput(apiKey, apiVersion, correlationID);
        InputStream inputStream = new ByteArrayInputStream(inputData);

        when(mockSocket.getInputStream()).thenReturn(inputStream);

        clientHandler.run();

        // Expectation: invalid API version, so errorCode should be 35
        byte[] expectedResponse = createMessage(correlationID, 35);
        byte[] actualResponse = outputStream.toByteArray();

        System.out.println(Arrays.toString(expectedResponse));
        System.out.println(Arrays.toString(actualResponse));

        assertArrayEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testSequentialRequests() throws IOException {
        for (int i = 0; i < 3; i++) {
            correlationID += i;
            apiVersion += 2 * i;
            int errorCode = apiVersion > 4 ? 35 : 0;
            byte[] inputData = helper.createTestInput(apiKey, apiVersion, correlationID);
            InputStream inputStream = new ByteArrayInputStream(inputData);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            // Setting up streams in the client socket
            when(mockSocket.getInputStream()).thenReturn(inputStream);
            when(mockSocket.getOutputStream()).thenReturn(outputStream);

            Broker.ClientHandler clientHandler = new Broker.ClientHandler(mockSocket);
            clientHandler.run();

            byte[] expectedResponse = createMessage(correlationID, errorCode);
            byte[] actualResponse = outputStream.toByteArray();

            assertArrayEquals(expectedResponse, actualResponse);
        }
    }

    //correlationid(4) + errorcode(2) + numOfFieldsAfter(1) + Available APIs(apikey(2) + minVersion(2) + maxversion(2) + tag_buffer) + throttle(4) + tag_buffer
    byte[] createMessage(int correlationId, int errorCode) {
        int throttle_time_ms = 0;
        byte[] tagBuffer = {0x00};

        Map<Integer, APIVersions> apiVersionsMap = helper.getAPIVersionsMap();
        int numOfApis = apiVersionsMap.size();

        byte[] idBytes = ByteUtils.intToByteArray(correlationId, 4);
        byte[] errorBytes = ByteUtils.intToByteArray(errorCode, 2);
        byte[] apiBytes = apiBytesHelper(apiVersionsMap);

        byte[] message = ByteUtils.concatenate(idBytes, errorBytes, ByteUtils.intToByteArray(numOfApis + 1, 1), apiBytes,
                ByteUtils.intToByteArray(throttle_time_ms, 4), tagBuffer);

        return ByteUtils.concatenate(ByteUtils.intToByteArray(message.length, 4), message);
    }

    private byte[] apiBytesHelper(Map<Integer, APIVersions> apiVersionsMap){
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        for (APIVersions apiVersions : apiVersionsMap.values()) {
            baos.write(ByteUtils.intToByteArray(apiVersions.getApiKey(), 2), 0, 2);
            baos.write(ByteUtils.intToByteArray(apiVersions.getMinVersion(), 2), 0, 2);
            baos.write(ByteUtils.intToByteArray(apiVersions.getMaxVersion(), 2), 0, 2);
            baos.write(new byte[]{0x00}, 0, 1);
        }

        return baos.toByteArray();
    }
}
