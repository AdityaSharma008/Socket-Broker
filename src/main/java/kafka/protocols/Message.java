package kafka.protocols;

import kafka.utils.ByteUtils;

import java.util.ArrayList;
import java.util.List;

public class Message {
     final int correlationID;
     final byte[] tagBuffer;
     List<byte[]> fields;
     ResponseHeaders header;

     Message(int correlationID, ResponseHeaders header){
         this.correlationID = correlationID;
         tagBuffer = new byte[]{0x00};
         fields = new ArrayList<>();
         this.header = header;
     }

     public void addErrorCode(int code){
         fields.add(ByteUtils.intToByteArray(code, 2));
     }

    public void addThrottleTime(int timeInMS){
        fields.add(ByteUtils.intToByteArray(timeInMS, 4));
    }

    public void addTagBuffer(){
         fields.add(tagBuffer);
    }

    public byte[] toBytes(){
         int n = fields.size();

         byte[][] fieldArray = new byte[n + 1][];
         fieldArray[0] = header.toBytes();

         for(int i = 0; i < n; i++){
             fieldArray[i + 1] = fields.get(i);
         }

         return ByteUtils.concatenate(fieldArray);
    }
}
