package kafka.protocols;

import kafka.utils.ByteUtils;

interface ResponseHeaders {
    byte[] toBytes();
}

class ResponseHeaderV0 implements ResponseHeaders{
    int correlationID;
    ResponseHeaderV0(int correlationID){
        this.correlationID = correlationID;
    }

    public byte[] toBytes(){
        return ByteUtils.intToByteArray(this.correlationID, 4);
    }
}

class ResponseHeaderV1 implements ResponseHeaders{
    int correlationID;
    ResponseHeaderV1(int correlationID){
        this.correlationID = correlationID;
    }

    public byte[] toBytes(){
        return ByteUtils.concatenate(ByteUtils.intToByteArray(this.correlationID, 4), new byte[]{0x00});
    }
}
