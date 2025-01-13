package org.apache.iotdb.tsfile.encoding.encoder.compressedsorter.decodeoperator;

import org.apache.iotdb.tsfile.encoding.encoder.compressedsorter.datastructure.CompressedData;

public class OrderSensitiveValueOperator {
    int valsLen = 0;  // Record the length of valid data in the vals array (in bytes)
    int valNum = 0;  // Record how many data items have been compressed so far
    boolean isFirst = true;
    public long nowValue = 0;
    public int nowNum = 0;
    public int nowPos = 0;

    long[] pow = new long[] {1, 256, 65536, 16777216, 4294967296L, (long) Math.pow(256, 5), (long) Math.pow(256, 6), (long) Math.pow(256, 7)};

    public OrderSensitiveValueOperator(long nowValue, int nowNum, int nowPos){
        this.nowValue = nowValue;
        this.nowNum = nowNum;
        this.nowPos = nowPos;
        if(nowNum!=0){
            isFirst = false;
        } else {
            isFirst = true;
        }
    }

    public OrderSensitiveValueOperator(boolean isFirst, long nowValue, int nowNum, int nowPos){
        this.nowValue = nowValue;
        this.nowNum = nowNum;
        this.nowPos = nowPos;
        this.isFirst = isFirst;
    }

    public void reset(OrderSensitiveValueOperator decoderTemp) {
        this.nowValue = decoderTemp.nowValue;
        this.nowNum = decoderTemp.nowNum;
        this.nowPos = decoderTemp.nowPos;
        this.isFirst = decoderTemp.isFirst;
    }

    public void reset(long nowValue, int nowNum, int nowPos) {
        this.nowValue = nowValue;
        this.nowNum = nowNum;
        this.nowPos = nowPos;
        if(nowNum!=0){
            isFirst = false;
        } else {
            isFirst = true;
        }
    }

    public long forwardDecode(byte[] deltas, byte[] lens) {
        if(isFirst){
            nowValue = readForwardValue(8, deltas);
            isFirst = false;
        } else {
            int valueLen = readValueLen(lens);
            nowValue = readForwardValue(valueLen, deltas);
        }
        nowNum++;
        return nowValue;
    }

    public long forwardDecode(CompressedData data) {
        if(isFirst){
            nowValue = readForwardValue(8, data.vals);
            isFirst = false;
        } else {
            int valueLen = readValueLen(data.lens);
            nowValue = readForwardValue(valueLen, data.vals);
        }
        nowNum++;
        return nowValue;
    }

    public long backwardDecode(CompressedData data) {
        nowNum--;
        int valueLen = readValueLen(data.lens);
        nowValue = readBackwardValueDelta(valueLen, data.vals);
        return nowValue;
    }

    public long backwardDecode(byte[] deltas, byte[] lens) {
        nowNum--;
        int valueLen = readValueLen(lens);
        nowValue = readBackwardValueDelta(valueLen, deltas);
        return nowValue;
    }

    public long backwardDecode(long value, int num, int pos, byte[] deltas, byte[] lens) {
        this.nowValue = value;
        this.nowNum = num;
        this.nowPos = pos;
        nowNum--;
        int valueLen = readValueLen(lens);
        nowValue -= readForwardValue(valueLen, deltas);
        return nowValue;
    }


    public int readValueLen(byte[] lens){
        byte temp = lens[nowNum/4];
        temp = (byte) (temp>>(2*(3-nowNum%4)));
        temp = (byte) (temp & 0x03);
        if(temp == 0) return 8;
        if(temp == 1) return 1;
        if(temp == 2) return 2;
        return 4;
    }

    public long readForwardValue(int byteNum, byte[] deltas) {
        // change the pointer with the read position
        long val = 0;
        long temp;
        for(int i=0; i<byteNum; i++){
            temp = deltas[nowPos];
            if(temp<0) temp = temp+256;
            val += temp*pow[i];
            nowPos++;
        }
        if(val %2 == 0)
            return val/2;
        return -(val-1)/2;
    }

    public long readBackwardValueDelta(int byteNum, byte[] deltas) {
        // Change the pointer first, and then change the pointer again after reading is complete
        nowPos = nowPos-byteNum;
        long val = readForwardValue(byteNum, deltas);
        nowPos = nowPos-byteNum;
        return val;
    }

    public void encode(long value, CompressedData compressedData){
        if (isFirst) {
            if (value <= 0) value = -2 * value + 1;
            else value = 2 * value;
            writeBits(value, compressedData);
            isFirst = false;
        }
        else {
            int byteNum = mapDataToLen(value);
            if (value <= 0) value = -2 * value + 1;
            else value = 2 * value;
            writeBits(value, byteNum, compressedData);
        }
        valNum++;
    }

    public int mapDataToLen(long data) {
        // Map 0 to -1, and decrement negative numbers by 1 sequentially
        if (data <= 0){
            data = data-1;
            data = -data;
        }
        if(data<128) return 1;
        if(data<32768) return 2;
        if(data-1<Integer.MAX_VALUE) return 4;
        return 8;
    }

    private void writeBits(long value, int byteNum, CompressedData compressedData){
        // write control bits
        compressedData.checkExpand(valsLen, valNum, byteNum);
        byte temp = 0;
        if(byteNum == 1) temp= 1;
        if(byteNum == 2) temp = 2;
        if(byteNum == 4) temp = 3;
        compressedData.lens[valNum/4] = (byte) (compressedData.lens[valNum/4]|(temp<<(2*(3-valNum%4))));
        // write data bits
        while(byteNum>0) {
            compressedData.vals[valsLen] = (byte) (value & 0xFFL);
            value = value>>8;
            byteNum--;
            valsLen++;
        }
    }

    private void writeBits(long value, CompressedData compressedData) {
        int byteNum = 8;
        while(byteNum>0) {
            compressedData.vals[valsLen] = (byte) (value & 0xFFL);
            value = value>>8;
            byteNum--;
            valsLen++;
        }
    }

    public int getValsLen() {
        return this.valsLen;
    }

    public int getValsNum() {
        return this.valNum;
    }
}
