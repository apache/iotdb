package org.apache.iotdb.tsfile.encoding.encoder.compressedsorter;

import org.apache.iotdb.tsfile.encoding.encoder.compressedsorter.datastructure.CompressedData;
import org.apache.iotdb.tsfile.encoding.encoder.compressedsorter.decodeoperator.OrderSensitiveTimeOperator;
import org.apache.iotdb.tsfile.encoding.encoder.compressedsorter.decodeoperator.OrderSensitiveValueOperator;

public class CompressedBubbleSorter {
    CompressedData compressedTimeData;
    CompressedData compressedValueData;
    private OrderSensitiveTimeOperator timeDecoderTemp;
    private OrderSensitiveValueOperator valueDecoderTemp;
    private OrderSensitiveTimeOperator timeBackwardDecoder;
    private OrderSensitiveValueOperator valueBackwardDecoder;
    OrderSensitiveTimeOperator encoder;

    public CompressedBubbleSorter(CompressedData timeData, CompressedData valueData) {
        this.compressedTimeData = timeData;
        this.compressedValueData = valueData;
        this.timeBackwardDecoder = new OrderSensitiveTimeOperator(0,0,0);
        this.valueBackwardDecoder = new OrderSensitiveValueOperator(0,0,0);
        this.timeDecoderTemp = new OrderSensitiveTimeOperator(0,0,0);
        this.valueDecoderTemp = new OrderSensitiveValueOperator(0,0,0);
        this.encoder = new OrderSensitiveTimeOperator(0,0,0);
    }

    public int blockSort(int begIndex, int endIndex, int timeBegPos, int timeEndPos, int valueBegPos, int valueEndPos) {
        // Within the basic blocks being sorted,
        // use the continuous compression sorting algorithm for sorting
        OrderSensitiveTimeOperator timeDecoder = new OrderSensitiveTimeOperator(true,0, 0, 0);
        OrderSensitiveValueOperator valueDecoder = new OrderSensitiveValueOperator(true,0, 0, 0);
        long nowVal = 0;
        int temValPos;
        int temValValPos;
        long temVal;
        int nowInd = begIndex;
        while (true) {
            temValPos = timeDecoder.nowPos;
            temValValPos = valueDecoder.nowPos;
            if(temValPos >= timeEndPos) {
                break;
            }
            temVal = timeDecoder.forwardDecode(compressedTimeData);
            valueDecoder.forwardDecode(compressedValueData);
            if(temVal >= nowVal || nowInd==begIndex){
                nowVal = temVal;
                nowInd++;
                if(nowInd == endIndex+1){
                    return timeEndPos;
                }
            } else{
                // find an out-of-order point
                timeEndPos += persistentSortOpeHoldLen(nowVal, temVal, temValPos, temValValPos, timeDecoder, valueDecoder, begIndex, timeEndPos);  // change!修改基本模块的运行逻辑
                timeDecoder = timeDecoderTemp;
                valueDecoder = valueDecoderTemp;
                nowVal = timeDecoder.nowValue;
                nowInd = timeDecoder.nowNum;
            }
        }
        return timeEndPos;
    }

    public int persistentSortOpeHoldLen(long nowValue, long valueTemp, int valueTempPos, int valueValueTempPos, OrderSensitiveTimeOperator timeForwardDecoder, OrderSensitiveValueOperator valueForwardDecoder, int blockBegInd, int blockEndPos) {
        // compressed bubble sort basic operation
        long upBound = nowValue;
        this.timeBackwardDecoder.reset(timeForwardDecoder.nowValue, timeForwardDecoder.nowNum, timeForwardDecoder.nowPos);
        this.valueBackwardDecoder.reset(valueForwardDecoder.nowValue, valueForwardDecoder.nowNum, valueForwardDecoder.nowPos);
        while (valueBackwardDecoder.backwardDecode(this.compressedValueData) > Long.MIN_VALUE && timeBackwardDecoder.backwardDecode(this.compressedTimeData)>valueTemp) {
            upBound = timeBackwardDecoder.nowValue;
            if(timeBackwardDecoder.nowNum == blockBegInd+1) {
                timeBackwardDecoder.backwardDecode(this.compressedTimeData);
                valueBackwardDecoder.backwardDecode(this.compressedValueData);
                break;
            }
        }
        int upBoundPos = timeBackwardDecoder.nowPos;
        int valueUpBoundPos = valueBackwardDecoder.nowPos;
        int valueTempInd = timeForwardDecoder.nowNum-1;
        long tailVal = valueTemp;
        int newHeadPos = timeForwardDecoder.nowPos;
        int valueNewHeadPos = valueForwardDecoder.nowPos;
        while(timeForwardDecoder.nowValue<upBound) {
            tailVal = timeForwardDecoder.nowValue;
            newHeadPos = timeForwardDecoder.nowPos;
            valueNewHeadPos = valueForwardDecoder.nowPos;
            if(newHeadPos >= blockEndPos) {
                break;
            }
            timeForwardDecoder.forwardDecode(this.compressedTimeData);
            valueForwardDecoder.forwardDecode(this.compressedValueData);

        }
        // change three delta
        this.encoder.reset();
        int valueTempVar = encoder.changeEncodeWithFixedLenByInd(valueTempInd, valueTempPos, valueTemp-timeBackwardDecoder.nowValue, timeBackwardDecoder.nowNum, compressedTimeData, timeForwardDecoder.nowPos);
        int upBoundVar;
        if(newHeadPos < blockEndPos) {
            upBoundVar = encoder.changeEncodeWithFixedLenByInd(timeBackwardDecoder.nowNum, upBoundPos, upBound - tailVal, timeForwardDecoder.nowNum - 1, compressedTimeData, timeForwardDecoder.nowPos);
        } else {
            upBoundVar = encoder.changeEncodeWithFixedLen(timeBackwardDecoder.nowNum, upBoundPos, upBound - tailVal, 8, compressedTimeData, timeForwardDecoder.nowPos);
        }
        int newHeadVar = 0;
        valueTempPos += upBoundVar;
        if(newHeadPos < blockEndPos) {
            newHeadPos += upBoundVar+valueTempVar;
            newHeadVar = encoder.changeEncodeWithFixedLen(timeForwardDecoder.nowNum-1, newHeadPos, timeForwardDecoder.nowValue-nowValue, 8, compressedTimeData, timeForwardDecoder.nowPos);
        } else {
            newHeadPos += upBoundVar+valueTempVar;
        }
        // compressed data move
        dataMove(compressedTimeData.vals, upBoundPos, valueTempPos, newHeadPos);
        dataMove(compressedValueData.vals, valueUpBoundPos, valueValueTempPos, valueNewHeadPos);
        lenDataMove(compressedTimeData.lens, timeBackwardDecoder.nowNum, valueTempInd, timeForwardDecoder.nowNum-1);
        lenDataMove(compressedValueData.lens, valueBackwardDecoder.nowNum, valueTempInd, valueForwardDecoder.nowNum-1);
        timeDecoderTemp.reset(timeBackwardDecoder);
        valueDecoderTemp.reset(valueBackwardDecoder);

        return valueTempVar+upBoundVar+newHeadVar;
    }

    public void dataExchange(byte[] data, int beg, int end, int len) {
        for (int i=0; i<len; i++) {
            data[beg+i] = (byte) (data[beg+i] ^ data[end-len+i]);
            data[end-len+i] = (byte) (data[beg+i] ^ data[end-len+i]);
            data[beg+i] = (byte) (data[beg+i] ^ data[end-len+i]);
        }
    }

    public void dataMove(byte[] data, int beg, int mid, int end) {
        if(mid == end || mid == beg) return;
        while(mid>beg && end>mid) {
            if(end-mid <= mid-beg) {
                dataExchange(data, beg, end, end-mid);
                beg = beg+end-mid;
            } else{
                dataExchange(data, beg, 2*mid-beg, mid-beg);
                int t = beg;
                beg = mid;
                mid = 2*mid-t;
            }
        }
    }

    public void lenDataExchange(byte[] lens, int pos1, int pos2) {
        byte unmask1 = (byte) (0x3 << (2*(3-pos1%4)));
        byte unmask2 = (byte) (0x3 << (2*(3-pos2%4)));
        byte val1 = (byte) ((lens[pos1/4]&unmask1)>>(2*(3-pos1%4)));
        byte val2 = (byte) ((lens[pos2/4]&unmask2)>>(2*(3-pos2%4)));
        val1 = (byte) (val1 & 0x3);
        val2 = (byte) (val2 & 0x3);
        lens[pos1/4] = (byte) ((lens[pos1/4]&(~unmask1))|(val2<<(2*(3-pos1%4))));
        lens[pos2/4] = (byte) ((lens[pos2/4]&(~unmask2))|(val1<<(2*(3-pos2%4))));
    }

    public void lenDataExchange(byte[] lens, int beg, int end, int len) {
        for (int i=0; i<len; i++) {
            lenDataExchange(lens, beg+i, end-len+i);
        }
    }


    public void lenDataMove(byte[] lens, int begInd, int midInd, int endInd) {
        if(midInd == endInd || midInd == begInd) return;
        while(midInd>begInd && endInd>midInd) {
            if(endInd-midInd <= midInd-begInd) {
                lenDataExchange(lens, begInd, endInd, endInd-midInd);
                begInd = begInd+endInd-midInd;
            } else{
                lenDataExchange(lens, begInd, 2*midInd-begInd, midInd-begInd);
                int t = begInd;
                begInd = midInd;
                midInd = 2*midInd-t;
            }
        }
    }
}

