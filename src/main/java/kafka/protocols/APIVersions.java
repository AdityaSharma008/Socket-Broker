package kafka.protocols;

public class APIVersions {
    private final short apiKey;
    private final short minVersion;
    private final short maxVersion;

    public APIVersions(short apiKey, short minVersion, short maxVersion) {
        this.apiKey = apiKey;
        this.minVersion = minVersion;
        this.maxVersion = maxVersion;
    }

    public int getApiKey() {
        return apiKey;
    }

    public short getMinVersion() {
        return minVersion;
    }

    public short getMaxVersion() {
        return maxVersion;
    }
}
