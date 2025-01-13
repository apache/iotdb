package org.apache.iotdb.tsfile.encoding.encoder.compressedsorter.decodeoperator;

import org.apache.iotdb.tsfile.encoding.encoder.compressedsorter.datastructure.CompressedSeriesData;

import java.util.LinkedList;

public class OrderSensitiveTimeMergeOperator {
    long nowValue = 0;
    int nowNum = 0;   // 用来确认len数组内部的偏移
    int nowIndex = 0;   // 用来确定当前的pagedata在pagelist中的位置
    int nowPageCount = 0;
    int nowValueIndex = 0;   // 用来确认val数组在pagelist中的位置
    int nowValuePos = 0;    // 用来确认当前读指针在vals中的偏移
    LinkedList<CompressedSeriesData> data;
    byte[] lens;
    byte[] deltas;
    long[] pow = new long[] {1, 256, 65536, 16777216, 4294967296L, (long) Math.pow(256, 5), (long) Math.pow(256, 6), (long) Math.pow(256, 7)};

    public OrderSensitiveTimeMergeOperator(int nowIndex,long nowValue, LinkedList<CompressedSeriesData> sortedPageList){
        // 设置读的位置为sortedPageList里面第nowIndex位置的元素，并且指针指向第一个元素
        CompressedSeriesData nowPage = sortedPageList.get(nowIndex);
        this.nowNum = 0;
        this.nowIndex = nowIndex;
        this.nowValue = nowValue;
        this.deltas = nowPage.getTimeData().vals;
        this.lens = nowPage.getTimeData().lens;
        this.nowPageCount = nowPage.getCount();
        this.nowValuePos = 0;
        this.nowValueIndex = nowIndex;
        this.data = sortedPageList;
    }

    public OrderSensitiveTimeMergeOperator(LinkedList<CompressedSeriesData> sortedPageList) {
        this.data = sortedPageList;
    }

    public OrderSensitiveTimeMergeOperator(int nowIndex, LinkedList<CompressedSeriesData> sortedPageList){
        // 设置读的位置为sortedPageList里面第nowIndex位置的元素，并且指针指向第一个元素
        CompressedSeriesData nowPage = sortedPageList.get(nowIndex);
        this.nowNum = 0;
        this.nowIndex = nowIndex;
        this.nowValue = nowPage.getMinTime();
        this.deltas = nowPage.getTimeData().vals;
        this.lens = nowPage.getTimeData().lens;
        this.nowPageCount = nowPage.getCount();
        this.nowValuePos = 0;
        this.nowValueIndex = nowIndex;
        this.data = sortedPageList;
    }

    public long forwardDecode() {
        // 正向解码，从前往后解码
        int valueLen = readValueLen();
        nowValue += readForwardValueDelta(valueLen);
        nowNum++;
        if(nowNum >= this.nowPageCount){
            this.nowIndex++;
            this.nowNum=0;
            if(this.nowIndex<data.size()){
                this.lens = data.get(nowIndex).getTimeData().lens;
                this.nowPageCount = data.get(nowIndex).getCount();
            }
        }
        return nowValue;
    }

    public long backwardDecode() {
        // 逆向解码，从后往前解码
        nowNum--;
        if(nowNum < 0) {
            this.nowIndex--;
            this.lens = data.get(nowIndex).getTimeData().lens;
            this.nowPageCount = data.get(nowIndex).getCount();
            this.nowNum = this.nowPageCount-1;
        }
        int valueLen = readValueLen();
        nowValue -= readBackwardValueDelta(valueLen);
        return nowValue;
    }

    public int readValueLen(){
        byte temp = lens[nowNum/4];
        temp = (byte) (temp>>(2*(3-nowNum%4)));
        temp = (byte) (temp & 0x03);
        if(temp == 0) return 8;
        if(temp == 3) return 4;
        return temp;
    }

    public int readNowValueLen(){
        byte temp = 0;
        if(nowNum == 0) {
            nowNum = data.get(nowIndex-1).getCount();
            temp = lens[(nowNum-1)/4];
            temp = (byte) (temp>>(2*(3-(nowNum-1)%4)));
            temp = (byte) (temp & 0x03);
            nowNum = 0;
        } else {
            temp = lens[(nowNum - 1) / 4];
            temp = (byte) (temp >> (2 * (3 - (nowNum - 1) % 4)));
            temp = (byte) (temp & 0x03);
        }
        if(temp == 0) return 8;
        if(temp == 3) return 4;
        return temp;
    }

    public long readForwardValueDelta(int byteNum) {
        // 指针位置随着读的变化而变化
        long delta = 0;
        for(int i=0; i<byteNum; i++){
            long temp = deltas[nowValuePos];
            if(temp<0) temp = temp+256;
            delta += temp*pow[i];
            nowValuePos++;
            if(nowValuePos==deltas.length){
                nowValueIndex++;
                nowValuePos = 0;
                if(nowValueIndex < data.size()){
                    this.deltas = data.get(nowValueIndex).getTimeData().vals;
                }
            }
        }
        if(delta>Integer.MAX_VALUE && byteNum<=4){
            delta = delta-Integer.MAX_VALUE-Integer.MAX_VALUE-2;
        }
        return delta;
    }

    public long readBackwardValueDelta(int byteNum) {
        //先变指针，读完之后还得变指针
        nowValuePos = nowValuePos-byteNum;
        if (nowValuePos<0){
            nowValueIndex--;
            this.deltas = data.get(nowValueIndex).getTimeData().vals;
            nowValuePos += data.get(nowValueIndex).getPageTimeLen();
        }
        long delta = readForwardValueDelta(byteNum);
        nowValuePos = nowValuePos-byteNum;
        if (nowValuePos<0){
            nowValueIndex--;
            this.deltas = data.get(nowValueIndex).getTimeData().vals;
            nowValuePos += data.get(nowValueIndex).getPageTimeLen();
        }
        return delta;
    }

    public long getNowValue() {
        return this.nowValue;
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