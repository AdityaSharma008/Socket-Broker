package kafka.protocols;

public class APIVersions {
    private final int apiKey;
    private final int minVersion;
    private final int maxVersion;

    public APIVersions(int apiKey, int minVersion, int maxVersion) {
        this.apiKey = apiKey;
        this.minVersion = minVersion;
        this.maxVersion = maxVersion;
    }

    public int getApiKey() {
        return apiKey;
    }

    public int getMinVersion() {
        return minVersion;
    }

    public int getMaxVersion() {
        return maxVersion;
    }
}
