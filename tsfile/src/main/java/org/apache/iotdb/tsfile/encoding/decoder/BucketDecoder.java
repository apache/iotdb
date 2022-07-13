package org.apache.iotdb.tsfile.encoding.decoder;

import org.apache.iotdb.tsfile.encoding.HuffmanTree.HuffmanCode;
import org.apache.iotdb.tsfile.encoding.HuffmanTree.HuffmanTree;
import org.apache.iotdb.tsfile.encoding.encoder.IntRleEncoder;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class BucketDecoder extends Decoder {
    private int recordNum = -1;
    private int minNum;
    private List<Integer> level;
    private List<Integer> rec;
    private int curPos;
    private int segmentLength;
    public BucketDecoder() {
        super(TSEncoding.BUCKET);
        recordNum = -1;
        level = new ArrayList<>();
        rec = new ArrayList<>();
        curPos = 0;
    }

    @Override
    public void reset() {
        recordNum = -1;
        level.clear();
        rec.clear();
        curPos = 0;
    }

    public int readInt(ByteBuffer buffer) {
        if(recordNum == -1) {
            reset();
            minNum = buffer.getInt();
            segmentLength = buffer.getInt();
            HuffmanDecoderV2 decoder = new HuffmanDecoderV2();
            int tmp = decoder.readInt(buffer);
            level.add(tmp);
            recordNum = decoder.recordnum;
            for(int i = 0; i < recordNum-1; i++) {
                tmp = decoder.readInt(buffer);
                level.add(tmp);
            }
            int bitwidth = 32-Integer.numberOfLeadingZeros(segmentLength) - 1;
            for(int i = 0; i < recordNum; i++) {
                tmp = 0;
                for(int j = bitwidth-1; j >=0; j--) {
                    int b = readbit(buffer);
                    tmp |= (b<<j);
                }
                int lv = level.get(i);
                rec.add(tmp + minNum + lv * segmentLength);
            }
        }
        int ret = rec.get(curPos++);
        if(curPos == recordNum) {
            reset();
            clearBuffer(buffer);
        }
        return ret;
    }

    @Override
    public boolean hasNext(ByteBuffer buffer) {
        if(recordNum == -1) {
            if (buffer.hasRemaining())
                return true;
            else
                return false;
        }
        else if(curPos < recordNum)
            return true;
        else
            return false;
    }

    private int numberLeftInBuffer = 0;
    private int byteBuffer = 0;


    private int readbit(ByteBuffer buffer) {
        if (numberLeftInBuffer == 0) {
            loadBuffer(buffer);
            numberLeftInBuffer = 8;
        }
        int top = ((byteBuffer >> 7) & 1);
        byteBuffer <<= 1;
        numberLeftInBuffer--;
        return top;
    }

    private void loadBuffer(ByteBuffer buffer) {
        byteBuffer = buffer.get();
    }

    private void clearBuffer(ByteBuffer buffer) {
        while (numberLeftInBuffer > 0) {
            readbit(buffer);
        }
    }
}
