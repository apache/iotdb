package org.apache.iotdb.tsfile.encoding.encoder;

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class OrderSensitiveTimeEncoder extends Encoder{
    byte[] lens;
    byte[] vals;
    int valNum = 0;
    boolean isFirst = true;
    long lastValue;
    int index = 0;
    boolean isLong = false; // Determine the range of the data values
    byte[] map;
    public OrderSensitiveTimeEncoder() {
        super(TSEncoding.ORDER_SENSITIVE_TIME);
    }

    @Override
    public void encode(long value, ByteArrayOutputStream out){
        if (isFirst) {
            if(isLong) {
                map = new byte[]{-1,1,2,-1,3,-1,-1,-1,0};
            } else {
                map = new byte[]{1, 2, 3, -1, 0};
            }
            lens = new byte[1000];
            vals = new byte[1000];
            writeBits(value, out);
            lastValue = value;
            isFirst = false;
        } else {
            long delta = value-lastValue;
            lastValue = value;
            int byteNum = mapDeltaToLen(delta);
            writeBits(delta, byteNum, out);
        }
        valNum++;
    }

    @Override
    public void flush(ByteArrayOutputStream out) throws IOException {
        // flush control bits
        out.write(vals, 0, index);
        out.write(lens,0, (valNum+3)/4);
        // flush valNum
        int byteNum = 4;
        while(byteNum>0) {
            out.write(valNum);
            valNum = valNum>>8;
            byteNum--;
        }
    }

    public int mapDeltaToLen(long delta) {  //Provide options for lengths of 1, 2, 4, and 8
        int newLen;
        if(isLong) {
            if (delta < 0) {
                newLen = 8;
            } else if (delta < 256) {
                newLen = 1;
            } else if (delta < 65536) {
                newLen = 2;
            } else if (delta < (long)Integer.MAX_VALUE*2) {
                newLen = 4;
            } else {
                newLen = 8;
            }
        } else {
            if (delta < 0) {
                newLen = 4;
            } else if (delta == 0) {
                newLen = 0;
            } else if (delta < 256) {
                newLen = 1;
            } else if (delta < 65536) {
                newLen = 2;
            } else {
                newLen = 4;
            }
        }
        return newLen;
    }


    private void writeBits(long value, int byteNum, ByteArrayOutputStream out){
        // update control bits
        if(valNum/4 >= lens.length-1){
            byte[] expanded = new byte[lens.length*2];
            System.arraycopy(lens, 0, expanded, 0, lens.length);
            this.lens = expanded;
        }
        byte temp = map[byteNum];
//        if(byteNum == 1) temp= 1;
//        if(byteNum == 2) temp = 2;
//        if(byteNum == 4) temp = 3;
        lens[valNum/4] = (byte) (lens[valNum/4]|(temp<<(2*(3-valNum%4))));
        // update data bits
        while(byteNum>0) {
            vals[index] = (byte) (value & 0xFFL);
            value = value>>8;
            byteNum--;
            index++;
            if(index == 1000) {
                out.write(vals, 0, vals.length);
                index = 0;
            }
        }
    }

    private void writeBits(long value, ByteArrayOutputStream out) {
        int byteNum = 8;
        while(byteNum>0) {
            out.write((int)value);
            value = value>>8;
            byteNum--;
        }
    }
}
