package kafka.requestHandlers;

import kafka.protocols.Message;
import kafka.protocols.Request;
import kafka.protocols.ResponseHeaders;

public class APIVersionsV4RequestHandler {
    Request request;
    final short minVersion, maxVersion;
    short errorCode;
    int throttleTime;

    public APIVersionsV4RequestHandler(Request request) {
        this.request = request;
        this.minVersion = 0;
        this.maxVersion = 4;
        this.throttleTime = 0;
        this.errorCode = (request.getApiVersion() > this.maxVersion || request.getApiVersion() < this.minVersion) ? (short) 35 : 0;
    }

    public Message handle() {
        ResponseHeaders header = new ResponseHeaders.ResponseHeaderV0(request.getCorrelationID());
        Message message = new Message(header);

        message.addField(errorCode);
        message.addField((byte) 2);
        message.addField((short) request.getApiKey());
        message.addField(minVersion);
        message.addField(maxVersion);
        message.addTagBuffer();
        message.addField(throttleTime);
        message.addTagBuffer();

        return message;
    }
}
