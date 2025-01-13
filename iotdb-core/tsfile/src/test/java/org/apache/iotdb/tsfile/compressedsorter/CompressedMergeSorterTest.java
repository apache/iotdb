package org.apache.iotdb.tsfile.compressedsorter;

import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.encoding.decoder.DeltaBinaryDecoder;
import org.apache.iotdb.tsfile.encoding.decoder.LongChimpDecoder;
import org.apache.iotdb.tsfile.encoding.decoder.LongSprintzDecoder;
import org.apache.iotdb.tsfile.encoding.encoder.DeltaBinaryEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.encoding.encoder.LongChimpEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.LongSprintzEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.compressedsorter.CompressedMergeSorter;
import org.apache.iotdb.tsfile.encoding.encoder.compressedsorter.datastructure.CompressedData;
import org.apache.iotdb.tsfile.encoding.encoder.compressedsorter.datastructure.CompressedSeriesData;
import org.apache.iotdb.tsfile.encoding.encoder.compressedsorter.decodeoperator.OrderSensitiveTimeMergeOperator;
import org.apache.iotdb.tsfile.encoding.encoder.compressedsorter.decodeoperator.OrderSensitiveTimeOperator;
import org.apache.iotdb.tsfile.encoding.encoder.compressedsorter.decodeoperator.OrderSensitiveValueMergeOperator;
import org.apache.iotdb.tsfile.encoding.encoder.compressedsorter.decodeoperator.OrderSensitiveValueOperator;
import org.junit.Test;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.LinkedList;

import static org.junit.Assert.assertEquals;

public class CompressedMergeSorterTest {
    private String filePath = "D:\\senior\\DQ\\research\\compressed_sort\\test\\compaction_time.txt";
    private static int ROW_NUM = 1000200;
    private static int PAGE_SIZE =500000;
    private static int ENCODE_TYPE = 0;   //0->TS_2DIFF    1->Chimp   2->Sprintz
    private static int STEP_SIZE = 1;
    private static int REAPEAT_NUM = 1;
    private static int STARTI_INDEX =0; //700000;60000

    @Test
    public void testCorrect() {
        // 一次测试 依次按照page的size将page按size两两合并
        long[] allTimes = new long[ROW_NUM];
        long[] allValues = new long[ROW_NUM];
        long[] timesPage1 = new long[PAGE_SIZE];
        long[] valuesPage1 = new long[PAGE_SIZE];
        long[] timesPage2 = new long[PAGE_SIZE];
        long[] valuesPage2 = new long[PAGE_SIZE];
        long[] timeTwoPage = new long[PAGE_SIZE*2];
        long[] valueTwoPage = new long[PAGE_SIZE*2];
        CompressedSeriesData pageData1;
        CompressedSeriesData pageData2;
        prepareData(allTimes, allValues);
        int pageNum = ROW_NUM/PAGE_SIZE;
        int startIndex = 0;
        while(startIndex+2*PAGE_SIZE <= ROW_NUM){
            for(int j=0; j<PAGE_SIZE; j++) {
                timesPage1[j] = allTimes[startIndex+j];
                valuesPage1[j] = allValues[startIndex+j];
                timesPage2[j] = allTimes[startIndex+PAGE_SIZE+j];
                valuesPage2[j] = allValues[startIndex+PAGE_SIZE+j];
            }
            for(int j=0; j<2*PAGE_SIZE; j++) {
                timeTwoPage[j] = allTimes[startIndex+j];
                valueTwoPage[j] = allValues[startIndex+j];
            }
            startIndex += STEP_SIZE;
            sortTimeWithValue(timesPage1, valuesPage1);
            sortTimeWithValue(timesPage2, valuesPage2);
            sortTimeWithValue(timeTwoPage, valueTwoPage);
            if(timesPage2[0] > timesPage1[PAGE_SIZE-1]) {
                continue;  // If there is no overlap in the time column, skip directly
            }
            if(timesPage1[0] < timesPage2[0]) {
                pageData1 = getPageData(PAGE_SIZE, timesPage1, valuesPage1);
                pageData2 = getPageData(PAGE_SIZE, timesPage2, valuesPage2);
            } else{
                pageData2 = getPageData(PAGE_SIZE, timesPage1, valuesPage1);
                pageData1 = getPageData(PAGE_SIZE, timesPage2, valuesPage2);
            }
            LinkedList<CompressedSeriesData> data = new LinkedList<>();
            data.add(pageData1);
            data.add(pageData2);
            CompressedMergeSorter sorter = new CompressedMergeSorter(data);
            sorter.sortPage(pageData2);
            OrderSensitiveTimeMergeOperator timeDecoder = new OrderSensitiveTimeMergeOperator(0,0, data);
            OrderSensitiveValueMergeOperator valueDecoder = new OrderSensitiveValueMergeOperator(0, data);
            for(int j=0; j<2*PAGE_SIZE; j++) {
                assertEquals(timeTwoPage[j], timeDecoder.forwardDecode());
                assertEquals(valueTwoPage[j], valueDecoder.forwardDecode());
            }
        }
    }

    @Test
    public void testMergeTime() throws IOException {
        // Test the time of the compaction method
        long[] allTimes = new long[ROW_NUM];
        long[] allValues = new long[ROW_NUM];
        long[] timesPage1 = new long[PAGE_SIZE];
        long[] valuesPage1 = new long[PAGE_SIZE];
        long[] timesPage2 = new long[PAGE_SIZE];
        long[] valuesPage2 = new long[PAGE_SIZE];
        long[] timeTwoPage = new long[PAGE_SIZE*2];
        long[] valueTwoPage = new long[PAGE_SIZE*2];

        long[] timePageTemp;
        long[] valuePageTemp;
        CompressedSeriesData pageData1;
        CompressedSeriesData pageData2;
        prepareData(allTimes, allValues);
        long totalTimeNew = 0;
        long totalTimeTS_2DIFF = 0;
        long totalTimeChimp = 0;
        long totalTimeSprintz = 0;
        long startTime = 0;
        long compactimeNum = 0;
        for(int r=0; r<REAPEAT_NUM; r++){
            int startIndex = 0;
            while(startIndex+2*PAGE_SIZE <= ROW_NUM){
                for(int j=0; j<PAGE_SIZE; j++) {
                    timesPage1[j] = allTimes[startIndex+j];
                    valuesPage1[j] = allValues[startIndex+j];
                    timesPage2[j] = allTimes[startIndex+PAGE_SIZE+j];
                    valuesPage2[j] = allValues[startIndex+PAGE_SIZE+j];
                }
                for(int j=0; j<2*PAGE_SIZE; j++) {
                    timeTwoPage[j] = allTimes[startIndex+j];
                    valueTwoPage[j] = allValues[startIndex+j];
                }
                startIndex += STEP_SIZE;
                sortTimeWithValue(timesPage1, valuesPage1);
                sortTimeWithValue(timesPage2, valuesPage2);
                sortTimeWithValue(timeTwoPage, valueTwoPage);
                if(timesPage2[0] > timesPage1[PAGE_SIZE-1]) {
                    continue;  // If there is no overlap in the time column, skip directly
                }
                if(timesPage1[0] > timesPage2[0]) {
                    timePageTemp = timesPage1;
                    timesPage1 = timesPage2;
                    timesPage2 = timePageTemp;
                    valuePageTemp = valuesPage1;
                    valuesPage1 = valuesPage2;
                    valuesPage2 = valuePageTemp;
                }
                pageData1 = getPageData(PAGE_SIZE, timesPage1, valuesPage1);
                pageData2 = getPageData(PAGE_SIZE, timesPage2, valuesPage2);
                ByteBuffer timeBuffer1TS_2DIFF = getByteBuffer(PAGE_SIZE, timesPage1, 0);
                ByteBuffer timeBuffer2TS_2DIFF = getByteBuffer(PAGE_SIZE, timesPage2, 0);
                ByteBuffer valueBuffer1TS_2DIFF = getByteBuffer(PAGE_SIZE, valuesPage1, 0);
                ByteBuffer valueBuffer2TS_2DIFF = getByteBuffer(PAGE_SIZE, valuesPage2, 0);

                ByteBuffer timeBuffer1Chimp = getByteBuffer(PAGE_SIZE, timesPage1, 1);
                ByteBuffer timeBuffer2Chimp = getByteBuffer(PAGE_SIZE, timesPage2, 1);
                ByteBuffer valueBuffer1Chimp = getByteBuffer(PAGE_SIZE, valuesPage1, 1);
                ByteBuffer valueBuffer2Chimp = getByteBuffer(PAGE_SIZE, valuesPage2, 1);

                ByteBuffer timeBuffer1Sprintz = getByteBuffer(PAGE_SIZE, timesPage1, 2);
                ByteBuffer timeBuffer2Sprintz = getByteBuffer(PAGE_SIZE, timesPage2, 2);
                ByteBuffer valueBuffer1Sprintz = getByteBuffer(PAGE_SIZE, valuesPage1, 2);
                ByteBuffer valueBuffer2Sprintz = getByteBuffer(PAGE_SIZE, valuesPage2, 2);

                LinkedList<CompressedSeriesData> data = new LinkedList<>();
                data.add(pageData1);
                data.add(pageData2);
                CompressedMergeSorter sorter = new CompressedMergeSorter(data);
                startTime = System.nanoTime();
                sorter.sortPage(pageData2);
                totalTimeNew += System.nanoTime()-startTime;
                compactimeNum++;

                startTime = System.nanoTime();
                CompactionPage(timeBuffer1TS_2DIFF, valueBuffer1TS_2DIFF, timeBuffer2TS_2DIFF, valueBuffer2TS_2DIFF, 0);
                totalTimeTS_2DIFF += System.nanoTime()-startTime;

                startTime = System.nanoTime();
                CompactionPage(timeBuffer1Chimp, valueBuffer1Chimp, timeBuffer2Chimp, valueBuffer2Chimp, 1);
                totalTimeChimp += System.nanoTime()-startTime;

                startTime = System.nanoTime();
                CompactionPage(timeBuffer1Sprintz, valueBuffer1Sprintz, timeBuffer2Sprintz, valueBuffer2Sprintz, 2);
                totalTimeSprintz += System.nanoTime()-startTime;
            }
        }
        writeDataToTXT(new long[]{compactimeNum, totalTimeNew/compactimeNum, totalTimeTS_2DIFF/compactimeNum, totalTimeChimp/compactimeNum , totalTimeSprintz/compactimeNum});
    }

    @Test
    public void testNewMemoryPrepare() throws Exception {
        long[] allTimes = new long[ROW_NUM];
        long[] allValues = new long[ROW_NUM];
        long[] timesPage1 = new long[PAGE_SIZE];
        long[] valuesPage1 = new long[PAGE_SIZE];
        long[] timesPage2 = new long[PAGE_SIZE];
        long[] valuesPage2 = new long[PAGE_SIZE];

        long[] timePageTemp;
        long[] valuePageTemp;
        CompressedSeriesData pageData1;
        CompressedSeriesData pageData2;
        if(STARTI_INDEX+2*PAGE_SIZE > ROW_NUM) {
            return;
        }
        prepareData(allTimes, allValues);
        for(int i=0; i<ROW_NUM-2*PAGE_SIZE; i++) {
            for (int j = 0; j < PAGE_SIZE; j++) {
                timesPage1[j] = allTimes[i + j];
                valuesPage1[j] = allValues[i + j];
                timesPage2[j] = allTimes[i + PAGE_SIZE + j];
                valuesPage2[j] = allValues[i + PAGE_SIZE + j];
            }
            sortTimeWithValue(timesPage1, valuesPage1);
            sortTimeWithValue(timesPage2, valuesPage2);
            if (timesPage2[0] > timesPage1[PAGE_SIZE - 1]) {
                continue;  // If there is no overlap in the time column, skip directly
            }
            if (timesPage1[0] > timesPage2[0]) {
                timePageTemp = timesPage1;
                timesPage1 = timesPage2;
                timesPage2 = timePageTemp;
                valuePageTemp = valuesPage1;
                valuesPage1 = valuesPage2;
                valuesPage2 = valuePageTemp;
            }
            pageData1 = getPageData(PAGE_SIZE, timesPage1, valuesPage1);
            pageData2 = getPageData(PAGE_SIZE, timesPage2, valuesPage2);

            try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream("D:\\senior\\DQ\\research\\compressed_sort\\test\\pagedata1"))) {
                out.writeObject(pageData1);
                System.out.println("Object serialized successfully.");
            } catch (IOException e) {
                e.printStackTrace();
            }

            try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream("D:\\senior\\DQ\\research\\compressed_sort\\test\\pagedata2"))) {
                out.writeObject(pageData2);
                System.out.println("Object serialized successfully.");
            } catch (IOException e) {
                e.printStackTrace();
            }
            return;
        }
    }

    @Test
    public void testNewMemory() throws Exception {
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        CompressedSeriesData pageData1 = null;
        CompressedSeriesData pageData2 = null;
        try (ObjectInputStream in = new ObjectInputStream(new FileInputStream("D:\\senior\\DQ\\research\\compressed_sort\\test\\pagedata1"))) {
            pageData1 = (CompressedSeriesData)in.readObject();
            in.close();
            System.gc();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return;
        }

        try (ObjectInputStream in = new ObjectInputStream(new FileInputStream("D:\\senior\\DQ\\research\\compressed_sort\\test\\pagedata2"))) {
            pageData2 = (CompressedSeriesData) in.readObject();
            in.close();
            System.gc();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return;
        }

        System.gc();
        synchronized (this) {
            try {
                wait(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.gc();
        recordMemory(memoryMXBean);

        LinkedList<CompressedSeriesData> data = new LinkedList<>();
        data.add(pageData1);
        data.add(pageData2);
        CompressedMergeSorter sorter = new CompressedMergeSorter(data);
        sorter.sortPage(pageData2);

        recordMemory(memoryMXBean);
        return;
    }

    @Test
    public void testOldMemoryPrepare() throws Exception {
        long[] allTimes = new long[ROW_NUM];
        long[] allValues = new long[ROW_NUM];
        long[] timesPage1 = new long[PAGE_SIZE];
        long[] valuesPage1 = new long[PAGE_SIZE];
        long[] timesPage2 = new long[PAGE_SIZE];
        long[] valuesPage2 = new long[PAGE_SIZE];

        long[] timePageTemp;
        long[] valuePageTemp;
        CompressedSeriesData pageData1;
        CompressedSeriesData pageData2;
        if(STARTI_INDEX+2*PAGE_SIZE > ROW_NUM) {
            return;
        }
        prepareData(allTimes, allValues);
        for(int i=0; i<ROW_NUM-2*PAGE_SIZE; i++) {
            for (int j = 0; j < PAGE_SIZE; j++) {
                timesPage1[j] = allTimes[i + j];
                valuesPage1[j] = allValues[i + j];
                timesPage2[j] = allTimes[i + PAGE_SIZE + j];
                valuesPage2[j] = allValues[i + PAGE_SIZE + j];
            }
            if(getMinMaxValue(timesPage2, true) > getMinMaxValue(timesPage1, false)) {
                continue;
            }
            sortTimeWithValue(timesPage1, valuesPage1);
            sortTimeWithValue(timesPage2, valuesPage2);
            if (timesPage2[0] > timesPage1[PAGE_SIZE - 1]) {
                return;  // 如果时间列没有overlap，则直接跳过
            }
            //recordMemory(memoryMXBean);
            if (timesPage1[0] > timesPage2[0]) {
                timePageTemp = timesPage1;
                timesPage1 = timesPage2;
                timesPage2 = timePageTemp;
                valuePageTemp = valuesPage1;
                valuesPage1 = valuesPage2;
                valuesPage2 = valuePageTemp;
            }
            ByteBuffer timeBuffer1TS_2DIFF = getByteBuffer("D:\\senior\\DQ\\research\\compressed_sort\\test\\timebuffer1.bat", PAGE_SIZE, timesPage1, ENCODE_TYPE);
            ByteBuffer timeBuffer2TS_2DIFF = getByteBuffer("D:\\senior\\DQ\\research\\compressed_sort\\test\\timebuffer2.bat", PAGE_SIZE, timesPage2, ENCODE_TYPE);
            ByteBuffer valueBuffer1TS_2DIFF = getByteBuffer("D:\\senior\\DQ\\research\\compressed_sort\\test\\valuebuffer1.bat", PAGE_SIZE, valuesPage1, ENCODE_TYPE);
            ByteBuffer valueBuffer2TS_2DIFF = getByteBuffer("D:\\senior\\DQ\\research\\compressed_sort\\test\\valuebuffer2.bat",PAGE_SIZE, valuesPage2, ENCODE_TYPE);
            return;
        }
    }
    @Test
    public void testOldMemory() throws Exception {
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        ByteBuffer timeBuffer1TS_2DIFF = getByteBuffer("D:\\senior\\DQ\\research\\compressed_sort\\test\\timebuffer1.bat");
        ByteBuffer timeBuffer2TS_2DIFF = getByteBuffer("D:\\senior\\DQ\\research\\compressed_sort\\test\\timebuffer2.bat");
        ByteBuffer valueBuffer1TS_2DIFF = getByteBuffer("D:\\senior\\DQ\\research\\compressed_sort\\test\\valuebuffer1.bat");
        ByteBuffer valueBuffer2TS_2DIFF = getByteBuffer("D:\\senior\\DQ\\research\\compressed_sort\\test\\valuebuffer2.bat");
        System.gc();
        recordMemory(memoryMXBean);
        CompactionPage(memoryMXBean, timeBuffer1TS_2DIFF, valueBuffer1TS_2DIFF, timeBuffer2TS_2DIFF, valueBuffer2TS_2DIFF, ENCODE_TYPE);
        //recordMemory(memoryMXBean);
    }

    public ByteBuffer getByteBuffer(String bufferName) {
        try (FileInputStream fis = new FileInputStream(bufferName);
             FileChannel channel = fis.getChannel()) {
            long fileSize = channel.size();
            ByteBuffer buffer = ByteBuffer.allocate((int) fileSize);
            channel.read(buffer); // 将数据读入 ByteBuffer
            channel.close();
            buffer.position(0);
            return buffer;
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public ByteBuffer getByteBuffer(int rowNum, long[] data, int encoderType) throws IOException {
        Encoder timeEncoder = null;
        if(encoderType == 0) {
            timeEncoder = new DeltaBinaryEncoder.LongDeltaEncoder();
        }
        if(encoderType == 1) {
            timeEncoder = new LongChimpEncoder();
        }
        if(encoderType == 2) {
            timeEncoder = new LongSprintzEncoder();
        }
        ByteArrayOutputStream dataOutputStream = new ByteArrayOutputStream();
        for(int i=0; i<rowNum; i++) {
            timeEncoder.encode(data[i], dataOutputStream);
        }
        timeEncoder.flush(dataOutputStream);
        ByteBuffer temp = ByteBuffer.wrap(dataOutputStream.toByteArray());
        return temp;
    }

    public ByteBuffer getByteBuffer(String bufferName ,int rowNum, long[] data, int encoderType) throws IOException {
        Encoder timeEncoder = null;
        if(encoderType == 0) {
            timeEncoder = new DeltaBinaryEncoder.LongDeltaEncoder();
        }
        if(encoderType == 1) {
            timeEncoder = new LongChimpEncoder();
        }
        if(encoderType == 2) {
            timeEncoder = new LongSprintzEncoder();
        }
        ByteArrayOutputStream dataOutputStream = new ByteArrayOutputStream();
        for(int i=0; i<rowNum; i++) {
            timeEncoder.encode(data[i], dataOutputStream);
        }
        timeEncoder.flush(dataOutputStream);
        ByteBuffer temp = ByteBuffer.wrap(dataOutputStream.toByteArray());
        try (FileOutputStream fos = new FileOutputStream(bufferName);
             FileChannel channel = fos.getChannel()) {
            channel.write(temp);
        }
        return temp;
    }

    public void CompactionPage(ByteBuffer timeBuffer1, ByteBuffer valueBuffer1, ByteBuffer timeBuffer2, ByteBuffer valueBuffer2, int encoderType) throws IOException {
        //encoderType= 0 -> ts_2diff    encoderType= 1 -> Chimp   encoderTye = 2 -> Sprintz
        Encoder timeEncoder = null;
        Encoder valueEncoder = null;
        Decoder timeDecoder = null;
        Decoder valueDecoder = null;
        long[] time1 = new long[PAGE_SIZE];
        long[] time2 = new long[PAGE_SIZE];
        long[] value1 = new long[PAGE_SIZE];
        long[] value2 = new long[PAGE_SIZE];
        if(encoderType == 0) {
            timeEncoder = new DeltaBinaryEncoder.LongDeltaEncoder();
            timeDecoder = new DeltaBinaryDecoder.LongDeltaDecoder();
            valueEncoder = new DeltaBinaryEncoder.LongDeltaEncoder();
            valueDecoder = new DeltaBinaryDecoder.LongDeltaDecoder();
        }
        if(encoderType == 1) {
            timeEncoder = new LongChimpEncoder();
            valueEncoder = new LongChimpEncoder();
            valueDecoder = new LongChimpDecoder();
            timeDecoder = new LongChimpDecoder();
        }
        if(encoderType == 2) {
            timeEncoder = new LongSprintzEncoder();
            valueEncoder = new LongSprintzEncoder();
            valueDecoder = new LongSprintzDecoder();
            timeDecoder = new LongSprintzDecoder();
        }
        for(int i=0; i<PAGE_SIZE; i++) {
            time1[i] = timeDecoder.readLong(timeBuffer1);
            value1[i] = valueDecoder.readLong(valueBuffer1);
        }
        if(encoderType == 0) {
            timeDecoder = new DeltaBinaryDecoder.LongDeltaDecoder();
            valueDecoder = new DeltaBinaryDecoder.LongDeltaDecoder();
        }
        if(encoderType == 1) {
            valueDecoder = new LongChimpDecoder();
            timeDecoder = new LongChimpDecoder();
        }
        if(encoderType == 2) {
            timeDecoder = new LongSprintzDecoder();
            valueDecoder = new LongSprintzDecoder();
        }
        for(int i=0; i<PAGE_SIZE; i++) {
            time2[i] = timeDecoder.readLong(timeBuffer2);
            value2[i] = valueDecoder.readLong(valueBuffer2);
        }
        int begIndex = 0;
        int endIndex = 0;
        ByteArrayOutputStream timeOutputStream = new ByteArrayOutputStream();
        ByteArrayOutputStream valueOutputStream = new ByteArrayOutputStream();
        while(begIndex<PAGE_SIZE || endIndex<PAGE_SIZE) {
            if(begIndex<PAGE_SIZE && endIndex<PAGE_SIZE){
                if(time2[begIndex]<time1[endIndex]) {
                    timeEncoder.encode(time2[begIndex], timeOutputStream);
                    valueEncoder.encode(value2[begIndex], valueOutputStream);
                    begIndex++;
                }
                else {
                    timeEncoder.encode(time1[endIndex], timeOutputStream);
                    valueEncoder.encode(value1[endIndex], valueOutputStream);
                    endIndex++;
                }
            } else{
                if(endIndex<PAGE_SIZE) {
                    timeEncoder.encode(time1[endIndex], timeOutputStream);
                    valueEncoder.encode(value1[endIndex], valueOutputStream);
                    endIndex++;
                } else {
                    timeEncoder.encode(time2[begIndex], timeOutputStream);
                    valueEncoder.encode(value2[begIndex], valueOutputStream);
                    begIndex++;
                }
            }
        }
        timeEncoder.flush(timeOutputStream);
        valueEncoder.flush(valueOutputStream);
    }

    public void CompactionPage(MemoryMXBean memoryMXBean, ByteBuffer timeBuffer1, ByteBuffer valueBuffer1, ByteBuffer timeBuffer2, ByteBuffer valueBuffer2, int encoderType) throws IOException {
        //encoderType= 0 -> ts_2diff    encoderType= 1 -> Chimp   encoderTye = 2 -> Sprintz
        Encoder timeEncoder = null;
        Encoder valueEncoder = null;
        Decoder timeDecoder = null;
        Decoder valueDecoder = null;
        long[] time1 = new long[PAGE_SIZE];
        long[] time2 = new long[PAGE_SIZE];
        long[] value1 = new long[PAGE_SIZE];
        long[] value2 = new long[PAGE_SIZE];
        //recordMemory(memoryMXBean);
        if(encoderType == 0) {
            timeEncoder = new DeltaBinaryEncoder.LongDeltaEncoder();
            timeDecoder = new DeltaBinaryDecoder.LongDeltaDecoder();
            valueEncoder = new DeltaBinaryEncoder.LongDeltaEncoder();
            valueDecoder = new DeltaBinaryDecoder.LongDeltaDecoder();
        }
        if(encoderType == 1) {
            timeEncoder = new LongChimpEncoder();
            valueEncoder = new LongChimpEncoder();
            valueDecoder = new LongChimpDecoder();
            timeDecoder = new LongChimpDecoder();
        }
        if(encoderType == 2) {
            timeEncoder = new LongSprintzEncoder();
            valueEncoder = new LongSprintzEncoder();
            valueDecoder = new LongSprintzDecoder();
            timeDecoder = new LongSprintzDecoder();
        }
        for(int i=0; i<PAGE_SIZE; i++) {
            time1[i] = timeDecoder.readLong(timeBuffer1);
            value1[i] = valueDecoder.readLong(valueBuffer1);
        }
        if(encoderType == 0) {
            timeDecoder = new DeltaBinaryDecoder.LongDeltaDecoder();
            valueDecoder = new DeltaBinaryDecoder.LongDeltaDecoder();
        }
        if(encoderType == 1) {
            valueDecoder = new LongChimpDecoder();
            timeDecoder = new LongChimpDecoder();
        }
        if(encoderType == 2) {
            timeDecoder = new LongSprintzDecoder();
            valueDecoder = new LongSprintzDecoder();
        }
        for(int i=0; i<PAGE_SIZE; i++) {
            time2[i] = timeDecoder.readLong(timeBuffer2);
            value2[i] = valueDecoder.readLong(valueBuffer2);
        }
        int begIndex = 0;
        int endIndex = 0;
        ByteArrayOutputStream timeOutputStream = new ByteArrayOutputStream();
        ByteArrayOutputStream valueOutputStream = new ByteArrayOutputStream();
        //System.gc();
        //recordMemory(memoryMXBean);
        while(begIndex<PAGE_SIZE || endIndex<PAGE_SIZE) {
            if(begIndex<PAGE_SIZE && endIndex<PAGE_SIZE){
                if(time2[begIndex]<time1[endIndex]) {
                    timeEncoder.encode(time2[begIndex], timeOutputStream);
                    valueEncoder.encode(value2[begIndex], valueOutputStream);
                    begIndex++;
                }
                else {
                    timeEncoder.encode(time1[endIndex], timeOutputStream);
                    valueEncoder.encode(value1[endIndex], valueOutputStream);
                    endIndex++;
                }
            } else{
                if(endIndex<PAGE_SIZE) {
                    timeEncoder.encode(time1[endIndex], timeOutputStream);
                    valueEncoder.encode(value1[endIndex], valueOutputStream);
                    endIndex++;
                } else {
                    timeEncoder.encode(time2[begIndex], timeOutputStream);
                    valueEncoder.encode(value2[begIndex], valueOutputStream);
                    begIndex++;
                }
            }
        }
        System.gc();
        timeEncoder.flush(timeOutputStream);
        valueEncoder.flush(valueOutputStream);
        time1[0] = 0;
        time2[0] = 0;
        value1[0] = 0;
        value2[0] = 0;
        timeBuffer1.position(0);
        timeBuffer2.position(0);
        valueBuffer1.position(0);
        valueBuffer2.position(0);
        timeDecoder.readLong(timeBuffer2);
        valueDecoder.readLong(valueBuffer2);
        timeDecoder.readLong(timeBuffer1);
        valueDecoder.readLong(valueBuffer1);
//        synchronized (this) {
//            try {
//                wait(5000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
        recordMemory(memoryMXBean);
//        timeEncoder=null;
//        valueEncoder=null;
    }

    public long getMinMaxValue(long[] values, boolean isMin) {
        if(isMin) {
            long min = Long.MAX_VALUE;
            for(int i=0; i<PAGE_SIZE; i++) {
                if(values[i] < min) {
                    min = values[i];
                }
            }
            return min;
        }
        else {
            long max = Long.MIN_VALUE;
            for(int i=0; i<PAGE_SIZE; i++) {
                if (values[i] > max) {
                    max = values[i];
                }
            }
            return max;
        }
    }

    public void sortTimeWithValue(long[] times, long[] values) {
        // Sort the data column and the time column based on the size of the time column
        long[][] sorted = new long[times.length][2];
        for(int i=0; i< times.length; i++){
            sorted[i][0] = times[i];
            sorted[i][1] = values[i];
        }
        Arrays.sort(sorted, (a, b) -> Long.compare(a[0], b[0]));
        for(int i=0; i<times.length; i++) {
            times[i] = sorted[i][0];
            values[i] = sorted[i][1];
        }
        sorted = null;
    }

    public CompressedSeriesData getPageData(int rowNum, long[] times, long[] values) {
        OrderSensitiveTimeOperator timeEncoder = new OrderSensitiveTimeOperator(0,0,0);
        CompressedData timeCompressedData = new CompressedData();
        OrderSensitiveValueOperator valueEncoder = new OrderSensitiveValueOperator(0,0,0);
        CompressedData valueCompressedData = new CompressedData();
        for (int i=0; i<rowNum; i++) {
            timeEncoder.encode(times[i], timeCompressedData);
            valueEncoder.encode(values[i], valueCompressedData);
        }
        byte[] temp = new byte[timeEncoder.getValsNum()/4+1];
        System.arraycopy(timeCompressedData.lens, 0, temp, 0, timeEncoder.getValsNum()/4+1);
        timeCompressedData.lens = temp;
        temp = new byte[timeEncoder.getValsNum()/4+1];
        System.arraycopy(valueCompressedData.lens, 0, temp, 0, valueEncoder.getValsNum()/4+1);
        valueCompressedData.lens = temp;
        temp = new byte[timeEncoder.getValsLen()];
        System.arraycopy(timeCompressedData.vals, 0, temp, 0, timeEncoder.getValsLen());
        timeCompressedData.vals = temp;
        temp = new byte[valueEncoder.getValsLen()];
        System.arraycopy(valueCompressedData.vals, 0, temp, 0, valueEncoder.getValsLen());
        valueCompressedData.vals = temp;
        System.gc();
        return new CompressedSeriesData(timeCompressedData, valueCompressedData, PAGE_SIZE, Arrays.stream(times).min().getAsLong(), Arrays.stream(times).max().getAsLong());
    }

    public void prepareData(long[] times, long[] values) {
        //samsung dataset
        readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/samsung/s-10_cleaned_new.csv", 2, ROW_NUM+1, 0, times);
        readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/samsung/s-10_cleaned_new.csv", 2, ROW_NUM+1, 1, values);

        //artificial dataset
        //readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/artificial/exponential/exponential_1_1000_new_new.csv", 2, ROW_NUM+1, 0, times);
        //readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/artificial/exponential/exponential_1_1000_new_new.csv", 2, ROW_NUM+1, 1, values);

        //carnet dataset
        //readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/swzl/swzl_clean.csv", 2, ROW_NUM+1, 0, times);
        //readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/swzl/swzl_clean.csv", 2, ROW_NUM+1, 1, values);

        //shipnet dataset
        //readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/ship/shipNet.csv", 2, ROW_NUM+1, 1, times);
        //readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/ship/shipNet.csv", 2, ROW_NUM+1, 4, values);
    }

    public boolean readCSV(String filePath, int line_begin, int line_end, int col, long[] data) {
        // Read the CSV file from column `col`
        // starting from line `line_begin` to line `line_end` into the `data` array
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(filePath));
            String line;
            int currentLine = 1;
            int index = 0;

            while ((line = reader.readLine()) != null) {
                if (currentLine >= line_begin && currentLine <= line_end) {
                    String[] tokens = line.split(",");
                    data[index] = (long) Double.parseDouble(tokens[col]);
                    index++;
                }
                currentLine++;
            }
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void recordMemory(MemoryMXBean memoryMXBean) {
        MemoryUsage heap = memoryMXBean.getHeapMemoryUsage();
        MemoryUsage nonHeap = memoryMXBean.getNonHeapMemoryUsage();
        writeDataToTXT(new long[]{heap.getUsed(), nonHeap.getUsed()});
    }

    public void writeDataToTXT(long[] data) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))) {
            for (long number : data) {
                writer.write(number + ",");
            }
            writer.write("\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
