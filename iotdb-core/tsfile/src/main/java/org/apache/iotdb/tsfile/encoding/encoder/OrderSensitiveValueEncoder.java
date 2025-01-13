package org.apache.iotdb.tsfile.encoding.encoder;

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class OrderSensitiveValueEncoder extends Encoder {
    byte[] lens;
    byte[] vals;
    int valNum = 0;
    int index = 0;
    boolean isFirst = true;
    boolean isLong = true;
    byte[] map;

    public OrderSensitiveValueEncoder() {
        super(TSEncoding.ORDER_SENSITIVE_VALUE);
    }

    public void encode(long value, ByteArrayOutputStream out){
        if (isFirst) {
            lens = new byte[1000];
            vals = new byte[1000];
            if(isLong) {
                map = new byte[]{-1,1,2,-1,3,-1,-1,-1,0};
            } else {
                map = new byte[]{1, 2, 3, -1, 0};
            }
            if (value <= 0) value = -2 * value + 1;
            else value = 2 * value;
            writeBits(value, out);
            isFirst = false;
        }
        else {
            int byteNum = mapDataToLen(value);
            if (value <= 0) value = -2 * value + 1;
            else value = 2 * value;
            writeBits(value, byteNum, out);
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

    public int mapDataToLen(long data) {
        // zigzag mapping data to length
        // Provide options for lengths of 1, 2, 4, and 8
        if (data <= 0){
            data = data-1;
            data = -data;
        }
        if(data<128) return 1;
        if(data<32768) return 2;
        if(data-1<Integer.MAX_VALUE) return 4;
        return 8;
    }

    private void writeBits(long value, int byteNum, ByteArrayOutputStream out){
        // update control bits
        if(valNum/4 >= lens.length-1){
            byte[] expanded = new byte[lens.length*2];
            System.arraycopy(lens, 0, expanded, 0, lens.length);
            this.lens = expanded;
        }
        byte temp = map[byteNum];
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
        if(!isLong) byteNum =4;
        while(byteNum>0) {
            out.write((int)value);
            value = value>>8;
            byteNum--;
        }
    }
}
