package kafka;

import kafka.core.Broker;
import kafka.protocols.APIVersions;

import java.util.LinkedHashMap;
import java.util.Map;

public class Main {
    private static final int PORT = 9092;
    private static Map<Short, APIVersions> apiVersionsMap;
    public Main(){
        apiVersionsMap = new LinkedHashMap<>();
    }

    public static void main(String[] args) {
        Main mainClass = new Main();
        createAPIMap();

        Broker broker = new Broker(apiVersionsMap);
        broker.startServer(PORT);
    }

    private static void createAPIMap(){
        apiVersionsMap.put((short)1, new APIVersions((short)1, (short)0, (short)16));
        apiVersionsMap.put((short)18, new APIVersions((short)18, (short)0, (short)4));
    }

    public Map<Short, APIVersions> getApiVersionsMap() {
        if(apiVersionsMap.isEmpty()) {
            createAPIMap();
        }
        return apiVersionsMap;
    }
}
