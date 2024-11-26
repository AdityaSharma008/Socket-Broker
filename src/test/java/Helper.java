import kafka.protocols.APIVersions;
import kafka.utils.ByteUtils;

import java.util.LinkedHashMap;
import java.util.Map;


public class Helper {
    private final Map<Integer, APIVersions> apiVersionsMap;
    public Helper(){
        apiVersionsMap = new LinkedHashMap<>();
        apiVersionsMap.put(1, new APIVersions((short)1, (short)0, (short)16));
        apiVersionsMap.put(18, new APIVersions((short)18, (short)0, (short)4));
    }

    // Helper method to create a mock client message
    public byte[] createTestInput(int apiKey, int apiVersion, int correlationID) {
        // Convert various fields to byte arrays
        byte[] correlationIdBytes = ByteUtils.intToByteArray(correlationID, 4);
        byte[] apiKeyBytes = ByteUtils.intToByteArray(apiKey, 2);
        byte[] apiVerBytes = ByteUtils.intToByteArray(apiVersion, 2);

        // Calculate message length and construct final input byte array
        byte[] messageLengthBytes = ByteUtils.intToByteArray(correlationIdBytes.length + apiKeyBytes.length + apiVerBytes.length, 4);

        return ByteUtils.concatenate(messageLengthBytes, apiKeyBytes, apiVerBytes, correlationIdBytes);
    }

    public Map<Integer, APIVersions> getAPIVersionsMap(){
        return apiVersionsMap;
    }
}
