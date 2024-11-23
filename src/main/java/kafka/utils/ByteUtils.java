package kafka.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ByteUtils {

    public static byte[] concatenate(byte[]... arrays) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        for (byte[] field : arrays) {
            try {
                baos.write(field);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return baos.toByteArray();
    }

    // Converts a 32-bit integer to a 4-byte array (big-endian)
    public static byte[] intToByteArray(int value, int size) {
        if (size < 1 || size > 4) {
            throw new IllegalArgumentException("Size must be between 1 and 4 bytes for an int.");
        }
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(value);

        byte[] fullArray = buffer.array();
        byte[] result = new byte[size];

        System.arraycopy(fullArray, 4 - size, result, 0, size);

        return result;
    }

    // Converts 4 bytes from the array to a 32-bit integer
    public static int byteToInt(byte[] arr, int start, int byteCount) {
        int result = 0;
        for (int i = 0; i < byteCount; i++) {
            result |= (arr[start + i] & 0xFF) << ((byteCount - i - 1) * 8);
        }
        return result;
    }
}
