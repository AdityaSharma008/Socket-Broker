import kafka.Main;
import kafka.core.Broker;
import kafka.protocols.ResponseHeaders;
import kafka.utils.ByteUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.mockito.Mockito.when;

public class FetchResponseTest {
    ServerSocket mockServerSocket;
    Socket mockSocket;
    Broker broker;
    Broker.ClientHandler clientHandler;
    ByteArrayOutputStream outputStream;
    Helper helper = new Helper();
    int correlationID, apiKey, apiVersion, minVersion, maxVersion;


    @BeforeEach
    public void setup() throws IOException {
        correlationID = 12345;
        apiKey = 1;
        apiVersion = 2;
        minVersion = 0;
        maxVersion = 16;
        // Mock ServerSocket and Socket objects
        mockServerSocket = Mockito.mock(ServerSocket.class);
        mockSocket = Mockito.mock(Socket.class);
        broker = new Broker(new Main().getApiVersionsMap());
        clientHandler = broker.new ClientHandler(mockSocket);
        outputStream = new ByteArrayOutputStream();
        when(mockSocket.getOutputStream()).thenReturn(outputStream);

        when(mockServerSocket.accept()).thenReturn(mockSocket);
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
        byte[] expectedResponse = createMessage(correlationID, 0, apiKey);
        byte[] actualResponse = outputStream.toByteArray();

        assertArrayEquals(expectedResponse, actualResponse);
    }

    @Test
    public void testWrongAPIVersion() throws IOException {
        // Test scenario where API version is invalid
        correlationID = 12345;
        apiVersion = 100; // Invalid API version
        byte[] inputData = helper.createTestInput(apiKey, apiVersion, correlationID);
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
            int errorCode = apiVersion > maxVersion || apiVersion < minVersion? 35 : 0;
            byte[] inputData = helper.createTestInput(apiKey, apiVersion, correlationID);
            InputStream inputStream = new ByteArrayInputStream(inputData);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            // Setting up streams in the client socket
            when(mockSocket.getInputStream()).thenReturn(inputStream);
            when(mockSocket.getOutputStream()).thenReturn(outputStream);

            Broker.ClientHandler clientHandler = broker.new ClientHandler(mockSocket);
            clientHandler.run();

            byte[] expectedResponse = createMessage(correlationID, errorCode, apiKey);
            byte[] actualResponse = outputStream.toByteArray();

            assertArrayEquals(expectedResponse, actualResponse);
        }
    }

    //responseHeader + throttle time + error code + session id + responses + tagbuffer
    byte[] createMessage(int correlationId, int errorCode, int apiKey) {
        int throttle_time_ms = 0;
        byte[] tagBuffer = {0x00};

        ResponseHeaders responseHeaders = new ResponseHeaders.ResponseHeaderV1(correlationId);
        byte[] idBytes = responseHeaders.toBytes();
        byte[] errorBytes = ByteUtils.intToByteArray(errorCode, 2);

        byte[] message = ByteUtils.concatenate(idBytes, ByteUtils.intToByteArray(throttle_time_ms, 4), errorBytes,
                                                ByteUtils.intToByteArray(0, 4), ByteUtils.intToByteArray(1, 1), tagBuffer);

        return ByteUtils.concatenate(ByteUtils.intToByteArray(message.length, 4), message);
    }
}
