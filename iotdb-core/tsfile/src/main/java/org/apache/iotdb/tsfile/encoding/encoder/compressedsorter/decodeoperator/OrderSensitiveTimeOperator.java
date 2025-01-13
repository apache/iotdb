package org.apache.iotdb.tsfile.encoding.encoder.compressedsorter.decodeoperator;

import org.apache.iotdb.tsfile.encoding.encoder.compressedsorter.datastructure.CompressedData;
import org.apache.iotdb.tsfile.encoding.encoder.compressedsorter.datastructure.CompressedSeriesData;

import java.util.LinkedList;

public class OrderSensitiveTimeOperator {
    long lastValue = 0;
    int valsLen = 0;  // Record the length of valid data in the vals array (in bytes)
    int valNum = 0;  // Record how many data items have been compressed so far
    public long nowValue = 0;
    public int nowNum = 0;
    public int nowPos = 0;
    boolean isFirst = true;
    long[] pow = new long[] {1, 256, 65536, 16777216, 4294967296L, (long) Math.pow(256, 5), (long) Math.pow(256, 6), (long) Math.pow(256, 7)};
    public int getNowNum() {
        return nowNum;
    }

    public int getNowPos() {
        return nowPos;
    }

    public long getNowValue() {
        return nowValue;
    }
    public OrderSensitiveTimeOperator(long nowValue, int nowNum, int nowPos){
        this.nowValue = nowValue;
        this.nowNum = nowNum;
        this.nowPos = nowPos;
        if(nowNum!=0){
            isFirst = false;
        } else {
            isFirst = true;
        }
    }

    public OrderSensitiveTimeOperator(boolean isFirst, long nowValue, int nowNum, int nowPos){
        this.nowValue = nowValue;
        this.nowNum = nowNum;
        this.nowPos = nowPos;
        this.isFirst = isFirst;
    }

    public void reset(OrderSensitiveTimeOperator decoderTemp) {
        this.nowValue = decoderTemp.getNowValue();
        this.nowNum = decoderTemp.getNowNum();
        this.nowPos = decoderTemp.getNowPos();
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
            nowValue = readForwardValueDelta(8, deltas);
            isFirst = false;
        } else {
            int valueLen = readValueLen(lens);
            nowValue += readForwardValueDelta(valueLen, deltas);
        }
        nowNum++;
        return nowValue;
    }
    public long forwardDecode(CompressedData data) {
        if(isFirst){
            nowValue = readForwardValueDelta(8, data.vals);
            isFirst = false;
        } else {
            int valueLen = readValueLen(data.lens);
            nowValue += readForwardValueDelta(valueLen, data.vals);
        }
        nowNum++;
        return nowValue;
    }
    public long backwardDecode(byte[] deltas, byte[] lens) {
        nowNum--;
        int valueLen = readValueLen(lens);
        nowValue -= readBackwardValueDelta(valueLen, deltas);
        return nowValue;
    }

    public long backwardDecode(CompressedData data) {
        nowNum--;
        int valueLen = readValueLen(data.lens);
        nowValue -= readBackwardValueDelta(valueLen, data.vals);
        return nowValue;
    }

    public long backwardDecode(long value, int num, int pos, byte[] deltas, byte[] lens) {
        this.nowValue = value;
        this.nowNum = num;
        this.nowPos = pos;
        nowNum--;
        int valueLen = readValueLen(lens);
        nowValue -= readForwardValueDelta(valueLen, deltas);
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

    public long readForwardValueDelta(int byteNum, byte[] deltas) {
        long delta = 0;
        for(int i=0; i<byteNum; i++){
            long temp = deltas[nowPos];
            if(temp<0) temp = temp+256;
            delta += temp*pow[i];
            nowPos++;
        }
        if(delta>Integer.MAX_VALUE && byteNum<=4){
            delta = delta-Integer.MAX_VALUE-Integer.MAX_VALUE-2;
        }
        return delta;
    }

    public long readBackwardValueDelta(int byteNum, byte[] deltas) {
        nowPos = nowPos-byteNum;
        long delta = readForwardValueDelta(byteNum, deltas);
        nowPos = nowPos-byteNum;
        return delta;
    }

    public int changeEncodeWithFixedLenByInd(int index, int begPos, long newDelta, int lenInd, CompressedData compressedData, int endPos) {
        // Modify the value of the element at the specified position, with the overall data movement not exceeding the endPos position (excluding endPos)
        // The determination of the new length is based on the length of an element at a certain original position
        int newLen = getLen(lenInd, compressedData);
        int originalLen = getLen(index,compressedData);
        writeLen(index, newLen, compressedData);
        if(newLen > originalLen) {
            compressedData.rightMoveVals(newLen-originalLen, begPos+originalLen, endPos-(newLen-originalLen));
        }
        if(newLen < originalLen) {
            compressedData.leftMoveVals(originalLen-newLen, begPos+originalLen, endPos);
        }
        writeBits(newDelta, newLen, begPos, compressedData);
        return newLen - originalLen;
    }

    public int changeEncodeWithFixedLen(int index, int begPos, long newDelta, int newLen, CompressedData compressedData, int endPos) {
        // Modify the value of the element at the specified position. The overall data movement does not exceed the endPos position (endPos is not included).
        // The new length is determined by the length of an element at a certain original position.
        int originalLen = getLen(index,compressedData);
        writeLen(index, newLen, compressedData);
        if(newLen > originalLen) {
            compressedData.rightMoveVals(newLen-originalLen, begPos+originalLen, endPos-(newLen-originalLen));
        }
        if(newLen < originalLen) {
            compressedData.leftMoveVals(originalLen-newLen, begPos+originalLen, endPos);
        }
        writeBits(newDelta, newLen, begPos, compressedData);
        return newLen - originalLen;
    }

    public int changeEncodeWithFixedLen(boolean isTime, LinkedList<CompressedSeriesData> sortedPageList, int lensIndex, int lensNum, int valsIndex, int valsPos, long newDelta, int newLen, int endValsIndex, int endValsPos) {
        // Modify the value of the element at the specified position. The overall data movement does not exceed the endPos position (endPos is not included).
        // The new length is determined by the length of an element at a certain original position.
        CompressedData lenData;
        if(isTime) lenData = sortedPageList.get(lensIndex).getTimeData();
        else lenData = sortedPageList.get(lensIndex).getValueData();
        int originalLen = getLen(lensNum, lenData);
        writeLen(lensNum, newLen, lenData);
        if(newLen > originalLen) {
            rightMoveVals(isTime, sortedPageList, valsIndex, valsPos, endValsIndex, endValsPos, newLen-originalLen);
        }
        if(newLen < originalLen) {
            leftMoveVals(isTime, sortedPageList, valsIndex, valsPos, endValsIndex, endValsPos, originalLen-newLen);
        }
        CompressedData valData;
        if(isTime) valData = sortedPageList.get(valsIndex).getTimeData();
        else valData = sortedPageList.get(valsIndex).getValueData();
        writeBits(isTime, sortedPageList, valsIndex, newDelta, newLen, valsPos, valData);
        return newLen - originalLen;
    }

    public void rightMoveVals(boolean isTime, LinkedList<CompressedSeriesData> sortedPageList, int begIndex, int begPos, int endIndex, int endPos, int len) {
        byte[] data = new byte[0];
        if(endIndex<sortedPageList.size()) {
            if(isTime) data = sortedPageList.get(endIndex).getTimeData().vals;
            else data = sortedPageList.get(endIndex).getValueData().vals;
        }
        int rightSidePos = endPos-len;
        int rightSideIndex = endIndex;
        byte[] rightSideData = data;
        while(rightSidePos < 0){
            rightSideIndex--;
            if(isTime) rightSideData = sortedPageList.get(rightSideIndex).getTimeData().vals;
            else rightSideData = sortedPageList.get(rightSideIndex).getValueData().vals;
            rightSidePos += rightSideData.length;
        }

        while(true){
            rightSidePos--;
            endPos--;
            if(rightSidePos<0) {
                rightSideIndex--;
                if(isTime) rightSideData = sortedPageList.get(rightSideIndex).getTimeData().vals;
                else rightSideData = sortedPageList.get(rightSideIndex).getValueData().vals;
                rightSidePos = rightSideData.length-1;
            }
            if(endPos<0) {
                endIndex--;
                if(isTime) data = sortedPageList.get(endIndex).getTimeData().vals;
                else data = sortedPageList.get(endIndex).getValueData().vals;
                endPos = data.length-1;
            }
            data[endPos] = rightSideData[rightSidePos];
            if(rightSideIndex==begIndex && rightSidePos == begPos){
                break;
            }
        }
    }

    public void leftMoveVals(boolean isTime, LinkedList<CompressedSeriesData> sortedPageList, int leftSideIndex, int leftSidePos, int endIndex, int endPos, int len) {
        // The given parameter is the position of the left boundary affected by the move
        // not the starting position of the move
        byte[] leftSideData;
        if(isTime) leftSideData = sortedPageList.get(leftSideIndex).getTimeData().vals;
        else leftSideData = sortedPageList.get(leftSideIndex).getValueData().vals;
        int begPos = leftSidePos+len;
        int begIndex = leftSideIndex;
        while (begPos>=leftSideData.length) {
            if(isTime) begPos -= sortedPageList.get(leftSideIndex).getTimeData().vals.length;
            else begPos -= sortedPageList.get(leftSideIndex).getValueData().vals.length;
            begIndex++;
        }
        byte[] data;
        if(isTime) data = sortedPageList.get(begIndex).getTimeData().vals;
        else data = sortedPageList.get(begIndex).getValueData().vals;
        while(true){
            leftSideData[leftSidePos] = data[begPos];
            leftSidePos++;
            begPos++;
            if(begPos==data.length){
                begIndex++;
                begPos=0;
                if(begIndex<sortedPageList.size()){
                    if(isTime) data = sortedPageList.get(begIndex).getTimeData().vals;
                    else data = sortedPageList.get(begIndex).getValueData().vals;
                }
            }
            if(leftSidePos == leftSideData.length){
                leftSideIndex++;
                leftSidePos = 0;
                if(leftSideIndex<sortedPageList.size()) {
                    if(isTime) leftSideData = sortedPageList.get(leftSideIndex).getTimeData().vals;
                    else leftSideData = sortedPageList.get(leftSideIndex).getValueData().vals;
                }
            }
            if(leftSidePos==endPos && leftSideIndex == endIndex){
                break;
            }
        }
    }

    public void writeLen(int index, int len, CompressedData compressedData) {
        // Change the length of the data at the specified position in the compressed data
        byte temp = 0;
        if(len == 1) temp= 1;
        if(len == 2) temp = 2;
        if(len == 4) temp = 3;
        byte unmask = (byte) (0x3 << (2*(3-index%4)));
        compressedData.lens[index/4] = (byte) (compressedData.lens[index/4]&(~unmask));
        compressedData.lens[index/4] = (byte) (compressedData.lens[index/4]|(temp<<(2*(3-index%4))));
    }

    public int getLen(int index, CompressedData compressedData) {
        // Get the length of the data at the specified position in the compressed data
        byte unmask = (byte) (0x3 << (2*(3-index%4)));
        byte val = (byte) ((compressedData.lens[index/4]&unmask)>>(2*(3-index%4)));
        val = (byte) (val & 0x3);
        if(val==0) return 8;
        if(val == 3 || val == -1) return 4;
        return val;
    }

    private void writeBits(long value, int byteNum, int beg, CompressedData compressedData){
        while(byteNum>0) {
            compressedData.vals[beg] = (byte) (value & 0xFFL);
            value = value>>8;
            byteNum--;
            beg++;
        }
    }

    private void writeBits(boolean isTime, LinkedList<CompressedSeriesData> sortedPageList, int valsIndex, long value, int byteNum, int beg, CompressedData compressedData){
        while(byteNum>0) {
            compressedData.vals[beg] = (byte) (value & 0xFFL);
            value = value>>8;
            byteNum--;
            beg++;
            if (beg == compressedData.getLen()) {
                beg = 0;
                valsIndex++;
                if(isTime) compressedData = sortedPageList.get(valsIndex).getTimeData();
                else compressedData = sortedPageList.get(valsIndex).getValueData();
            }
        }
    }

    public void reset() {
        this.isFirst = true;
    }

    public void encode(long value, CompressedData compressedData){
        if (isFirst) {
            writeBits(value, compressedData);
            lastValue = value;
            isFirst = false;
        } else {
            long valuetemp = value-lastValue;
            lastValue = value;
            if(valuetemp<0) {
                if(valuetemp==-1){
                    valuetemp = -1;
                }
                writeBits(valuetemp, 8, compressedData);
            } else{
                if (valuetemp < 256) {
                    writeBits(valuetemp, 1, compressedData);
                } else if (valuetemp < 65536) {
                    writeBits(valuetemp, 2, compressedData);
                } else {
                    writeBits(valuetemp, 4, compressedData);
                }
            }
        }
        valNum++;
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
