package org.apache.iotdb.tsfile.encoding.decoder;

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.io.IOException;
import java.nio.ByteBuffer;

public class OrderSensitiveValueDecoder extends Decoder {
    boolean isFirst = true;
    int nowNum = 0;
    int totalNum = 0;
    int controlBitsOffset = 0;
    int index = 0;
    byte[] vals = new byte[4];
    long[] pow = new long[] {1, 256, 65536, 16777216, 4294967296L, (long) Math.pow(256, 5), (long) Math.pow(256, 6), (long) Math.pow(256, 7)};
    int[] map; // map index to length
    boolean isLong = true;  // Determine the range of the data values

    public OrderSensitiveValueDecoder() {
        super(TSEncoding.ORDER_SENSITIVE_VALUE);
    }

    @Override
    public boolean hasNext(ByteBuffer buffer) throws IOException {
        if (isFirst) {
            if(isLong) {
                map = new int[]{8, 1, 2, 4};
            }
            else{
                map = new int[]{4, 0, 1, 2};
            }
            totalNum = readValueSize(buffer);
            controlBitsOffset = buffer.limit()-4-(totalNum+3)/4;
            isFirst = false;
            return true;
        }
        if (nowNum < totalNum){
            return true;
        }
        return false;
    }

    @Override
    public long readLong(ByteBuffer buffer) {
        int valueLen = readValueLen(buffer);
        long value = readForwardValue(valueLen, buffer);
        nowNum++;
        return value;
    }

    @Override
    public void reset() {
        isFirst = true;
        nowNum = 0;
        totalNum = 0;
        controlBitsOffset = 0;
    }

    private int readValueSize(ByteBuffer buffer) {
        int size = 0;
        int offset = buffer.limit() - 4;
        for(int i=0; i<4; i++){
            long temp = buffer.get(offset+i);
            if(temp<0) temp = temp+256;
            size += temp*pow[i];
        }
        return size;
    }

    public int readValueLen(ByteBuffer buffer){
        if(index == 0){
            byte temp = buffer.get(controlBitsOffset +nowNum/4);
            for(int i=0; i<4; i++) {
                vals[3-i] = (byte) (temp & 0x03);
                temp >>= 2;
            }
        }
        byte temp = vals[index];
        index++;
        if(index == 4) index = 0;
        return map[temp];
    }

    public long readForwardValue(int byteNum, ByteBuffer buffer) {
        long val = 0;
        for(int i=0; i<byteNum; i++){
            long temp = buffer.get();
            if(temp<0) temp = temp+256;
            val += temp*pow[i];
        }
        if(val>Integer.MAX_VALUE && byteNum<=4){
            val = val-Integer.MAX_VALUE-Integer.MAX_VALUE-2;
        }
        if(val %2 == 0)
            return val/2;
        return -(val-1)/2;
    }
}

