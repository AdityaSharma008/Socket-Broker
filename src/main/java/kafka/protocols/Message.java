package kafka.protocols;

import kafka.utils.ByteUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class Message {
    final byte[] tagBuffer;
    private final List<byte[]> fields;
    ResponseHeaders header;

    public Message(ResponseHeaders header) {
        tagBuffer = new byte[]{0x00};
        fields = new ArrayList<>();
        this.header = header;
    }

    public void addTagBuffer() {
        fields.add(tagBuffer);
    }

    public void addField(short field) {
        fields.add(ByteUtils.intToByteArray(field, 2));
    }

    public void addField(int field) {
        fields.add(ByteUtils.intToByteArray(field, 4));
    }

    public void addField(byte field) {
        fields.add(new byte[]{field});
    }

    public void addField(byte[] field) {
        fields.add(field);
    }

    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int totalLength = calculateMessageLength();

        baos.write(ByteUtils.intToByteArray(totalLength, 4));
        baos.write(header.toBytes());

        for (byte[] field : fields) {
            baos.write(field);
        }

        return baos.toByteArray();
    }

    private int calculateMessageLength() {
        int length = 0;
        for (byte[] field : fields) {
            length += field.length;
        }
        return length + header.toBytes().length; // +4 for message length field
    }
}
