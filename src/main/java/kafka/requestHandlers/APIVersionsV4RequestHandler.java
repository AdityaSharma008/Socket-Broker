package kafka.requestHandlers;

import kafka.Main;
import kafka.protocols.APIVersions;
import kafka.protocols.Message;
import kafka.protocols.Request;
import kafka.protocols.ResponseHeaders;

import java.util.Map;

/*
*
*
* */

public class APIVersionsV4RequestHandler {
    Request request;
    final short minVersion, maxVersion;
    short errorCode;
    int throttleTime;
    Map<Short, APIVersions> apiVersionsMap;

    public APIVersionsV4RequestHandler(Request request) {
        this.request = request;
        this.apiVersionsMap = new Main().getApiVersionsMap();
        this.minVersion = (short)apiVersionsMap.get(request.getApiKey()).getMinVersion();
        this.maxVersion = (short)apiVersionsMap.get(request.getApiKey()).getMaxVersion();
        this.throttleTime = 0;
        this.errorCode = (request.getApiVersion() > this.maxVersion || request.getApiVersion() < this.minVersion) ? (short) 35 : 0;
    }

    public Message handle() {
        ResponseHeaders header = new ResponseHeaders.ResponseHeaderV0(request.getCorrelationID());
        Message message = new Message(header);

        message.addField(errorCode);
        message.addField((byte) (apiVersionsMap.size() + 1)); //Indicates num of fields to follow

        for(APIVersions apiVersions: apiVersionsMap.values()) {
            message.addField((short) apiVersions.getApiKey());
            message.addField(apiVersions.getMinVersion());
            message.addField(apiVersions.getMaxVersion());
            message.addTagBuffer();
        }

        message.addField(throttleTime);
        message.addTagBuffer();

        return message;
    }
}
