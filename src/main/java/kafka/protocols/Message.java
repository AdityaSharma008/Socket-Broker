package kafka.protocols;

import kafka.utils.ByteUtils;

import java.util.ArrayList;
import java.util.List;

//todo: improve messageLen logic and addField

public class Message {
     final byte[] tagBuffer;
     List<byte[]> fields;
     ResponseHeaders header;
     int messageLength;

     public Message(ResponseHeaders header){
         tagBuffer = new byte[]{0x00};
         fields = new ArrayList<>();
         this.header = header;
         this.messageLength = 0;
     }

     public void addErrorCode(int code){
         fields.add(ByteUtils.intToByteArray(code, 2));
         messageLength += 2;
     }

    public void addThrottleTime(int timeInMS){
        fields.add(ByteUtils.intToByteArray(timeInMS, 4));
        messageLength += 4;
    }

    public void addTagBuffer(){
         fields.add(tagBuffer);
         messageLength += 1;
    }

    public void addField(byte[] field){
         fields.add(field);
         messageLength += field.length;
    }

    public byte[] toBytes(){
         int n = fields.size();

         byte[][] fieldArray = new byte[n + 2][];
         fieldArray[0] = ByteUtils.intToByteArray(messageLength + 4, 4);
         fieldArray[1] = header.toBytes();

         for(int i = 0; i < n; i++){
             fieldArray[i + 2] = fields.get(i);
         }

         return ByteUtils.concatenate(fieldArray);
    }
}
