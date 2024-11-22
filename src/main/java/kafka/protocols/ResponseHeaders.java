package kafka.protocols;

import kafka.utils.ByteUtils;


//todo: explore factory pattern
public class ResponseHeaders {
    int correlationID;

    public ResponseHeaders(int correlationID) {
        this.correlationID = correlationID;
    }

    public byte[] toBytes() {
        return ByteUtils.intToByteArray(this.correlationID, 4);
    }

    public static class ResponseHeaderV0 extends ResponseHeaders {
        public ResponseHeaderV0(int correlationID) {
            super(correlationID);
        }

        @Override
        public byte[] toBytes() {
            return ByteUtils.intToByteArray(this.correlationID, 4);
        }
    }

    public static class ResponseHeaderV1 extends ResponseHeaders {
        int correlationID;

        public ResponseHeaderV1(int correlationID) {
            super(correlationID);
        }

        @Override
        public byte[] toBytes() {
            return ByteUtils.concatenate(ByteUtils.intToByteArray(this.correlationID, 4), new byte[]{0x00});
        }
    }

}