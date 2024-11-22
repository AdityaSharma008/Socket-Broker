package kafka.requestHandlers;

import kafka.protocols.Message;
import kafka.protocols.Request;
import kafka.protocols.ResponseHeaders;
import kafka.utils.ByteUtils;

//todo: improve error code logic and message
public class APIVersionsV4RequestHandler {
    Request request;
    final int minVersion, maxVersion;
    int errorCode;

    public APIVersionsV4RequestHandler(Request request) {
        this.request = request;
        this.minVersion = 0;
        this.maxVersion = 4;
        if (request.getApiVersion() > this.maxVersion || request.getApiVersion() < this.minVersion) {
            this.errorCode = 35;
        } else {
            this.errorCode = 0;
        }
    }

    public Message handle() {
        ResponseHeaders header = new ResponseHeaders.ResponseHeaderV0(request.getCorrelationID());
        Message message = new Message(header);

        message.addErrorCode(errorCode);
        message.addField(ByteUtils.intToByteArray(2, 1));
        message.addField(ByteUtils.intToByteArray(request.getApiKey(), 2));
        message.addField(ByteUtils.intToByteArray(minVersion, 2));
        message.addField(ByteUtils.intToByteArray(maxVersion, 2));
        message.addTagBuffer();
        message.addThrottleTime(0);
        message.addTagBuffer();

        return message;
    }
}
