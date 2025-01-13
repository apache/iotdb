package org.apache.iotdb.tsfile.encoding.encoder.compressedsorter.decodeoperator;

import org.apache.iotdb.tsfile.encoding.encoder.compressedsorter.datastructure.CompressedSeriesData;

import java.util.LinkedList;

public class OrderSensitiveValueMergeOperator {
    int nowNum = 0;   // Used to confirm the lens array
    int nowIndex = 0;
    int nowPageCount = 0;
    int nowValueIndex = 0;   // Used to confirm the vals array
    int nowValuePos = 0;
    LinkedList<CompressedSeriesData> data;
    byte[] lens;
    byte[] varInts;
    long[] pow = new long[] {1, 256, 65536, 16777216, 4294967296L, (long) Math.pow(256, 5), (long) Math.pow(256, 6), (long) Math.pow(256, 7)};

    public OrderSensitiveValueMergeOperator(int nowIndex, LinkedList<CompressedSeriesData> sortedPageList){
        // Set the read position to the element at the nowIndex position in the sortedPageList,
        // and point the pointer to the first element
        CompressedSeriesData nowPage = sortedPageList.get(nowIndex);
        this.nowNum = 0;
        this.nowIndex = nowIndex;
        this.varInts = nowPage.getValueData().vals;
        this.lens = nowPage.getValueData().lens;
        this.nowPageCount = nowPage.getCount();
        this.nowValuePos = 0;
        this.nowValueIndex = nowIndex;
        this.data = sortedPageList;
    }


    public long forwardDecode() {
        int valueLen = readValueLen();
        long nowValue = readForwardValueDelta(valueLen);
        nowNum++;
        if(nowNum >= this.nowPageCount){
            this.nowIndex++;
            this.nowNum=0;
            if(this.nowIndex<data.size()){
                this.lens = data.get(nowIndex).getValueData().lens;
                this.nowPageCount = data.get(nowIndex).getCount();
            }
        }
        return nowValue;
    }

    public long backwardDecode() {
        nowNum--;
        if(nowNum < 0) {
            this.nowIndex--;
            this.lens = data.get(nowIndex).getValueData().lens;
            this.nowPageCount = data.get(nowIndex).getCount();
            this.nowNum = this.nowPageCount-1;
        }
        int valueLen = readValueLen();
        return readBackwardValueDelta(valueLen);
    }

    public int readValueLen(){
        byte temp = lens[nowNum/4];
        temp = (byte) (temp>>(2*(3-nowNum%4)));
        temp = (byte) (temp & 0x03);
        if(temp == 0) return 8;
        if(temp == 3) return 4;
        return temp;
    }

    public long readForwardValueDelta(int byteNum) {
        long delta = 0;
        for(int i=0; i<byteNum; i++){
            long temp = varInts[nowValuePos];
            if(temp<0) temp = temp+256;
            delta += temp*pow[i];
            nowValuePos++;
            if(nowValuePos==varInts.length){
                nowValueIndex++;
                nowValuePos = 0;
                if(nowValueIndex < data.size()){
                    this.varInts = data.get(nowValueIndex).getValueData().vals;
                }
            }
        }
        if(delta %2 == 0)
            return delta/2;
        return -(delta-1)/2;
    }

    public long readBackwardValueDelta(int byteNum) {
        nowValuePos = nowValuePos-byteNum;
        if (nowValuePos<0){
            nowValueIndex--;
            this.varInts = data.get(nowValueIndex).getValueData().vals;
            nowValuePos += data.get(nowValueIndex).getPageValueLen();
        }
        long delta = readForwardValueDelta(byteNum);
        nowValuePos = nowValuePos-byteNum;
        if (nowValuePos<0){
            nowValueIndex--;
            this.varInts = data.get(nowValueIndex).getValueData().vals;
            nowValuePos += data.get(nowValueIndex).getPageValueLen();
        }
        return delta;
    }


    public int getNowNum() {
        return nowNum;
    }

    public int getNowIndex() {
        return nowIndex;
    }

    public int getNowValuePos() {
        return nowValuePos;
    }

    public int getNowValueIndex() {
        return nowValueIndex;
    }

    public boolean hasForwardNext() {
        return this.nowIndex < data.size();
    }
}
