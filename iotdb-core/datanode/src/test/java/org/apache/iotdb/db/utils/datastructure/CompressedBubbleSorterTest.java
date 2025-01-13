package org.apache.iotdb.db.utils.datastructure;

import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.encoding.decoder.DeltaBinaryDecoder;
import org.apache.iotdb.tsfile.encoding.decoder.LongChimpDecoder;
import org.apache.iotdb.tsfile.encoding.decoder.LongSprintzDecoder;
import org.apache.iotdb.tsfile.encoding.encoder.*;
import org.apache.iotdb.tsfile.encoding.encoder.compressedsorter.CompressedBubbleSorter;
import org.apache.iotdb.tsfile.encoding.encoder.compressedsorter.datastructure.CompressedData;
import org.apache.iotdb.tsfile.encoding.encoder.compressedsorter.decodeoperator.OrderSensitiveTimeOperator;
import org.apache.iotdb.tsfile.encoding.encoder.compressedsorter.decodeoperator.OrderSensitiveValueOperator;
import org.junit.Test;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.OptionalLong;

import static org.junit.Assert.assertEquals;

public class CompressedBubbleSorterTest {
    int ROW_NUM = 1000000;
    // 0:TS_2DIFF   1:Chimp   2:RLBE   3:Sprintz
    // tvListType = 0 -> timTVList   tvListType = 1 -> backTVList
    int encoderType = 0;
    Boolean tvlistType = true;
    private int REAPEAT_NUM = 1;
    private int STEP_SIZE = 10000;
    private int PAGE_SIZE = 200000;
    @Test
    public void testSortCorrect() throws IOException {
        long[] times = new long[ROW_NUM];
        long[] values = new long[ROW_NUM];
        prepareData(times, values);
        OptionalLong maxOptional = Arrays.stream(times).max();
        long max = maxOptional.orElseThrow(() -> new RuntimeException("数组为空，无法找出最大值"));
        times[ROW_NUM - 1] = max+3000;
        OrderSensitiveTimeOperator timeEncoder = new OrderSensitiveTimeOperator(0, 0, 0);
        OrderSensitiveValueOperator valueEncoder = new OrderSensitiveValueOperator(0, 0, 0);
        CompressedData timeCompressedData = new CompressedData();
        CompressedData valueCompressedData = new CompressedData();
        for (int i=0; i<ROW_NUM; i++) {
            timeEncoder.encode(times[i], timeCompressedData);
            valueEncoder.encode(values[i], valueCompressedData);
        }
        // sort
        CompressedBubbleSorter sorter = new CompressedBubbleSorter(timeCompressedData, valueCompressedData);
        sorter.blockSort(0,ROW_NUM-1, 8, timeEncoder.getValsLen(), 8, valueEncoder.getValsLen());
        // test
        OrderSensitiveValueOperator valueDecoder = new OrderSensitiveValueOperator(0, 0, 0);
        OrderSensitiveTimeOperator timeDecoder = new OrderSensitiveTimeOperator(0, 0, 0);
        sortTimeWithValue(times, values);
        for(int i=0; i<ROW_NUM; i++){
            assertEquals(times[i], timeDecoder.forwardDecode(timeCompressedData));
            assertEquals(values[i], valueDecoder.forwardDecode(valueCompressedData));
        }
    }

    @Test
    public void testAllSortMemory() throws InterruptedException, IOException {
        int[] rowNums = new int[] {10, 200000, 400000, 600000, 800000, 1000000};
        for(int i=0; i<rowNums.length; i++){
            ROW_NUM = rowNums[i];
            //testCompressedBubbleSortMemory();
            //testOldSortMemory();
            System.gc();
//            synchronized (this) {
//            try {
//                wait(5000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
            //}
        }
    }

    @Test
    public void testCompressedBubbleSortMemory() throws InterruptedException {
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        long[] times = new long[ROW_NUM];
        long[] values = new long[ROW_NUM];
        prepareData(times, values);
        OptionalLong maxOptional = Arrays.stream(times).max();
        long max = maxOptional.orElseThrow(() -> new RuntimeException("The array is empty, and it is not possible to find the maximum value."));
        times[ROW_NUM - 1] = max+3000;
        OrderSensitiveTimeOperator timeEncoder = new OrderSensitiveTimeOperator(0,0,0);
        OrderSensitiveValueOperator valueEncoder = new OrderSensitiveValueOperator(0,0,0);
        CompressedData timeCompressedData = new CompressedData();
        CompressedData valueCompressedData = new CompressedData();
        for (int i=0; i<ROW_NUM; i++) {
            timeEncoder.encode(times[i], timeCompressedData);
            valueEncoder.encode(values[i], valueCompressedData);
        }
        times = null;
        values = null;
        int timeLen = timeEncoder.getValsLen();
        int valueLen = valueEncoder.getValsLen();
        timeEncoder = null;
        valueEncoder = null;
        timeCompressedData.reset(ROW_NUM, timeLen);
        valueCompressedData.reset(ROW_NUM, valueLen);
        System.gc();
        synchronized (this) {
            try {
                wait(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        recordMemory(memoryMXBean);
        CompressedBubbleSorter sorter = new CompressedBubbleSorter(timeCompressedData, valueCompressedData);
        sorter.blockSort(0,ROW_NUM-1, 8, timeLen, 8, valueLen);
        recordMemory(memoryMXBean);
    }

    @Test
    public void testOldSortMemory() throws IOException {
        Encoder timeEncoder = null;
        Encoder valueEncoder = null;
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        long[] times = new long[ROW_NUM];
        long[] values = new long[ROW_NUM];
        prepareData(times, values);
        if(encoderType == 0) {
            timeEncoder = new DeltaBinaryEncoder.LongDeltaEncoder();
            valueEncoder = new DeltaBinaryEncoder.LongDeltaEncoder();
        }
        if(encoderType == 1){
            timeEncoder = new LongChimpEncoder();
            valueEncoder = new LongChimpEncoder();
        }
        if(encoderType == 2) {
            timeEncoder = new LongRLBE();
            valueEncoder = new LongRLBE();
        }
        if(encoderType == 3) {
            timeEncoder = new LongSprintzEncoder();
            valueEncoder = new LongSprintzEncoder();
        }

        ByteArrayOutputStream timeStream = new ByteArrayOutputStream();
        ByteArrayOutputStream valueStream = new ByteArrayOutputStream();
        for (int i=0; i<ROW_NUM; i++) {
            timeEncoder.encode(times[i], timeStream);
            valueEncoder.encode(values[i], valueStream);
        }
        times = new long[1];   // To allow system.gc() to perform space reclamation
        values = new long[1];
        timeEncoder.flush(timeStream);
        valueEncoder.flush(valueStream);
        ByteBuffer timeBuffer2 = ByteBuffer.wrap(timeStream.toByteArray());
        ByteBuffer valueBuffer2 = ByteBuffer.wrap(valueStream.toByteArray());
        System.gc();
//        synchronized (this) {
//            try {
//                wait(5000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
        recordMemory(memoryMXBean);
        SortMemory(memoryMXBean, timeBuffer2, valueBuffer2, encoderType, tvlistType);
        //recordMemory(memoryMXBean);
    }

    public void SortMemory(MemoryMXBean memoryMXBean, ByteBuffer timeBuffer, ByteBuffer valueBuffer, int encoderType, boolean tvListType) throws IOException {
        //tvListType = 0 -> timTVList   tvListType = 1 -> backTVList
        //encoderType= 0 -> ts_2diff    encoderType= 1 -> Chimp
        TVList tvList;
        if(!tvListType) {
            tvList = new TimLongTVList();
        } else {
            tvList = new BackLongTVList();
        }
        Encoder timeEncoder = null;
        Encoder valueEncoder = null;
        Decoder timeDecoder = null;
        Decoder valueDecoder = null;
        if(encoderType == 0) {
            timeEncoder = new DeltaBinaryEncoder.LongDeltaEncoder();
            timeDecoder = new DeltaBinaryDecoder.LongDeltaDecoder();
            valueEncoder = new DeltaBinaryEncoder.LongDeltaEncoder();
            valueDecoder = new DeltaBinaryDecoder.LongDeltaDecoder();
        }
        if(encoderType == 1){
            timeEncoder = new LongChimpEncoder();
            valueEncoder = new LongChimpEncoder();
            valueDecoder = new LongChimpDecoder();
            timeDecoder = new LongChimpDecoder();
        }
        if(encoderType == 3) {
            timeEncoder = new LongSprintzEncoder();
            valueEncoder = new LongSprintzEncoder();
            valueDecoder = new LongSprintzDecoder();
            timeDecoder = new LongSprintzDecoder();
        }
        for(int i=0; i<Math.min(PAGE_SIZE, ROW_NUM); i++) {
            tvList.putLong(timeDecoder.readLong(timeBuffer), valueDecoder.readLong(valueBuffer));
        }
        tvList.sort();
        ByteArrayOutputStream timeOutputStream = new ByteArrayOutputStream();
        ByteArrayOutputStream valueOutputStream = new ByteArrayOutputStream();
        for(int i=0; i<Math.min(PAGE_SIZE, ROW_NUM); i++) {
            timeEncoder.encode(tvList.getTime(i), timeOutputStream);
            valueEncoder.encode(tvList.getLong(i), valueOutputStream);
        }
        System.gc();
        recordMemory(memoryMXBean);
        timeEncoder.flush(timeOutputStream);
        valueEncoder.flush(valueOutputStream);
        tvList.getLong(0);
        timeBuffer.position(0);
        valueBuffer.position(0);
        timeDecoder.readLong(timeBuffer);
        valueDecoder.readLong(valueBuffer);
    }

    public void SortMemory(ByteBuffer timeBuffer, ByteBuffer valueBuffer, int encoderType, boolean tvListType) {
        //tvListType = 0 -> timTVList   tvListType = 1 -> backTVList
        //encoderType= 0 -> ts_2diff    encoderType= 1 -> Chimp
        TVList tvList;
        if(!tvListType) {
            tvList = new TimLongTVList();
        } else {
            tvList = new BackLongTVList();
        }
        Encoder timeEncoder = null;
        Encoder valueEncoder = null;
        Decoder timeDecoder = null;
        Decoder valueDecoder = null;
        if(encoderType == 0) {
            timeEncoder = new DeltaBinaryEncoder.LongDeltaEncoder();
            timeDecoder = new DeltaBinaryDecoder.LongDeltaDecoder();
            valueEncoder = new DeltaBinaryEncoder.LongDeltaEncoder();
            valueDecoder = new DeltaBinaryDecoder.LongDeltaDecoder();
        }
        if(encoderType == 1){
            timeEncoder = new LongChimpEncoder();
            valueEncoder = new LongChimpEncoder();
            valueDecoder = new LongChimpDecoder();
            timeDecoder = new LongChimpDecoder();
        }
        if(encoderType == 3) {
            timeEncoder = new LongSprintzEncoder();
            valueEncoder = new LongSprintzEncoder();
            valueDecoder = new LongSprintzDecoder();
            timeDecoder = new LongSprintzDecoder();
        }
        for(int i=0; i<Math.min(PAGE_SIZE, ROW_NUM); i++) {
            tvList.putLong(timeDecoder.readLong(timeBuffer), valueDecoder.readLong(valueBuffer));
        }
        tvList.sort();
        ByteArrayOutputStream timeOutputStream = new ByteArrayOutputStream();
        ByteArrayOutputStream valueOutputStream = new ByteArrayOutputStream();
        for(int i=0; i<Math.min(PAGE_SIZE, ROW_NUM); i++) {
            timeEncoder.encode(tvList.getTime(i), timeOutputStream);
            valueEncoder.encode(tvList.getLong(i), valueOutputStream);
        }
        //System.gc();
    }

    @Test
    public void testSortTimeAll() throws IOException {
        int[] pageNums = {200000, 400000, 600000, 800000, 1000000};
        int[] repeatNums = {1,1,1,1,40};
        int[] stepNums = {10000,10000,10000,10000,10000};
        for(int i=4; i<5; i++) {
            this.PAGE_SIZE = pageNums[i];
            this.REAPEAT_NUM = repeatNums[i];
            this.STEP_SIZE = stepNums[i];
            testSortTime();
        }
    }

    @Test
    public void testSortTime() throws IOException {
        long[] times = new long[ROW_NUM];
        long[] values = new long[ROW_NUM];
        long newTime = 0;
        long TS_2DIFF_TimTime = 0;
        long Chimp_TimTime = 0;
        long RLBE_TimTime = 0;
        long sprintz_TimTime = 0;
        long TS_2DIFF_BackTime= 0;
        long Chimp_BackTime = 0;
        long RLBE_BackTime = 0;
        long sprintz_BackTime = 0;
        long startTime= 0;
        long sortNum = 0;
        prepareData(times, values);
        for(int r=0; r<REAPEAT_NUM; r++) {
            int startIndex = 0;
            while(startIndex+PAGE_SIZE <= ROW_NUM) {
                OrderSensitiveTimeOperator timeEncoder = new OrderSensitiveTimeOperator(0,0,0);
                OrderSensitiveValueOperator valueEncoder = new OrderSensitiveValueOperator(0,0,0);
                CompressedData timeCompressedData = new CompressedData();
                CompressedData valueCompressedData = new CompressedData();

                Encoder TS_2DIFF_TimtimeEncoder = new DeltaBinaryEncoder.LongDeltaEncoder();
                Encoder TS_2DIFF_TimvalueEncoder = new DeltaBinaryEncoder.LongDeltaEncoder();
                ByteArrayOutputStream TS_2DIFF_TimtimeStream = new ByteArrayOutputStream();
                ByteArrayOutputStream TS_2DIFF_TimvalueStream = new ByteArrayOutputStream();

                Encoder Chimp_TimtimeEncoder = new LongChimpEncoder();
                Encoder Chimp_TimvalueEncoder = new LongChimpEncoder();
                ByteArrayOutputStream Chimp_TimtimeStream = new ByteArrayOutputStream();
                ByteArrayOutputStream Chimp_TimvalueStream = new ByteArrayOutputStream();

                Encoder sprintz_TimtimeEncoder = new LongSprintzEncoder();
                Encoder sprintz_TimvalueEncoder = new LongSprintzEncoder();
                ByteArrayOutputStream sprintz_TimtimeStream = new ByteArrayOutputStream();
                ByteArrayOutputStream sprintz_TimvalueStream = new ByteArrayOutputStream();

                Encoder TS_2DIFF_BacktimeEncoder = new DeltaBinaryEncoder.LongDeltaEncoder();
                Encoder TS_2DIFF_BackvalueEncoder = new DeltaBinaryEncoder.LongDeltaEncoder();
                ByteArrayOutputStream TS_2DIFF_BacktimeStream = new ByteArrayOutputStream();
                ByteArrayOutputStream TS_2DIFF_BackvalueStream = new ByteArrayOutputStream();

                Encoder Chimp_BacktimeEncoder = new LongChimpEncoder();
                Encoder Chimp_BackvalueEncoder = new LongChimpEncoder();
                ByteArrayOutputStream Chimp_BacktimeStream = new ByteArrayOutputStream();
                ByteArrayOutputStream Chimp_BackvalueStream = new ByteArrayOutputStream();

                Encoder sprintz_BacktimeEncoder = new LongSprintzEncoder();
                Encoder sprintz_BackvalueEncoder = new LongSprintzEncoder();
                ByteArrayOutputStream sprintz_BacktimeStream = new ByteArrayOutputStream();
                ByteArrayOutputStream sprintz_BackvalueStream = new ByteArrayOutputStream();
                long maxTime = 0;
                for (int i = 0; i < PAGE_SIZE; i++) {
                    if(times[startIndex+i]>maxTime) {
                        maxTime = times[startIndex+i];
                    }
                    timeEncoder.encode(times[startIndex+i], timeCompressedData);
                    valueEncoder.encode(values[startIndex+i], valueCompressedData);

                    TS_2DIFF_TimtimeEncoder.encode(times[startIndex+i], TS_2DIFF_TimtimeStream);
                    TS_2DIFF_TimvalueEncoder.encode(values[startIndex+i], TS_2DIFF_TimvalueStream);
                    Chimp_TimtimeEncoder.encode(times[startIndex+i], Chimp_TimtimeStream);
                    Chimp_TimvalueEncoder.encode(values[startIndex+i], Chimp_TimvalueStream);
                    sprintz_TimtimeEncoder.encode(times[startIndex+i], sprintz_TimtimeStream);
                    sprintz_TimvalueEncoder.encode(values[startIndex+i], sprintz_TimvalueStream);

                    TS_2DIFF_BacktimeEncoder.encode(times[startIndex+i], TS_2DIFF_BacktimeStream);
                    TS_2DIFF_BackvalueEncoder.encode(values[startIndex+i], TS_2DIFF_BackvalueStream);
                    Chimp_BacktimeEncoder.encode(times[startIndex+i], Chimp_BacktimeStream);
                    Chimp_BackvalueEncoder.encode(values[startIndex+i], Chimp_BackvalueStream);
                    sprintz_BacktimeEncoder.encode(times[startIndex+i], sprintz_BacktimeStream);
                    sprintz_BackvalueEncoder.encode(values[startIndex+i], sprintz_BackvalueStream);
                }
                timeEncoder.encode(maxTime, timeCompressedData);
                valueEncoder.encode(values[startIndex], valueCompressedData);
                TS_2DIFF_TimtimeEncoder.flush(TS_2DIFF_TimtimeStream);
                TS_2DIFF_TimvalueEncoder.flush(TS_2DIFF_TimvalueStream);
                Chimp_TimtimeEncoder.flush(Chimp_TimtimeStream);
                Chimp_TimvalueEncoder.flush(Chimp_TimvalueStream);
                sprintz_TimtimeEncoder.flush(sprintz_TimtimeStream);
                sprintz_TimvalueEncoder.flush(sprintz_TimvalueStream);

                TS_2DIFF_BacktimeEncoder.flush(TS_2DIFF_BacktimeStream);
                TS_2DIFF_BackvalueEncoder.flush(TS_2DIFF_BackvalueStream);
                Chimp_BacktimeEncoder.flush(Chimp_BacktimeStream);
                Chimp_BackvalueEncoder.flush(Chimp_BackvalueStream);
                sprintz_BacktimeEncoder.flush(sprintz_BacktimeStream);
                sprintz_BackvalueEncoder.flush(sprintz_BackvalueStream);

                startTime = System.nanoTime();
                CompressedBubbleSorter sorter = new CompressedBubbleSorter(timeCompressedData, valueCompressedData);
                sorter.blockSort(0, PAGE_SIZE, 8, timeEncoder.getValsLen(), 8, valueEncoder.getValsLen());
                newTime += System.nanoTime() - startTime;

                ByteBuffer timeBuffer = ByteBuffer.wrap(TS_2DIFF_TimtimeStream.toByteArray());
                ByteBuffer valueBuffer = ByteBuffer.wrap(TS_2DIFF_TimvalueStream.toByteArray());
                startTime = System.nanoTime();
                SortMemory(timeBuffer, valueBuffer, 0, false);
                TS_2DIFF_TimTime += System.nanoTime()-startTime;

                ByteBuffer timeBuffer2 = ByteBuffer.wrap(Chimp_TimtimeStream.toByteArray());
                ByteBuffer valueBuffer2 = ByteBuffer.wrap(Chimp_TimvalueStream.toByteArray());
                startTime = System.nanoTime();
                SortMemory(timeBuffer2, valueBuffer2, 1, false);
                Chimp_TimTime += System.nanoTime()-startTime;

                ByteBuffer timeBuffer2_3 = ByteBuffer.wrap(sprintz_TimtimeStream.toByteArray());
                ByteBuffer valueBuffer2_3 = ByteBuffer.wrap(sprintz_TimvalueStream.toByteArray());
                startTime = System.nanoTime();
                SortMemory(timeBuffer2_3, valueBuffer2_3, 3, false);
                sprintz_TimTime += System.nanoTime()-startTime;

                ByteBuffer timeBuffer3 = ByteBuffer.wrap(TS_2DIFF_BacktimeStream.toByteArray());
                ByteBuffer valueBuffer3 = ByteBuffer.wrap(TS_2DIFF_BackvalueStream.toByteArray());
                startTime = System.nanoTime();
                SortMemory(timeBuffer3, valueBuffer3, 0, true);
                TS_2DIFF_BackTime += System.nanoTime()-startTime;

                ByteBuffer timeBuffer4 = ByteBuffer.wrap(Chimp_BacktimeStream.toByteArray());
                ByteBuffer valueBuffer4 = ByteBuffer.wrap(Chimp_BackvalueStream.toByteArray());
                startTime = System.nanoTime();
                SortMemory(timeBuffer4, valueBuffer4, 1, true);
                Chimp_BackTime += System.nanoTime()-startTime;

                ByteBuffer timeBuffer4_3 = ByteBuffer.wrap(sprintz_BacktimeStream.toByteArray());
                ByteBuffer valueBuffer4_3 = ByteBuffer.wrap(sprintz_BackvalueStream.toByteArray());
                startTime = System.nanoTime();
                SortMemory(timeBuffer4_3, valueBuffer4_3, 3, true);
                sprintz_BackTime += System.nanoTime()-startTime;

                startIndex += STEP_SIZE;
                sortNum++;
            }
        }
        writeDataToTXT(new long[] {sortNum, newTime/sortNum, TS_2DIFF_TimTime/sortNum, Chimp_TimTime/sortNum, RLBE_TimTime/sortNum, sprintz_TimTime/sortNum, TS_2DIFF_BackTime/sortNum, Chimp_BackTime/sortNum, RLBE_BackTime/sortNum, sprintz_BackTime/sortNum});
    }

    public void sortTimeWithValue(long[] times, long[] values) {
        // Sort the data column and the time column based on the magnitude of the time column
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

    public void recordMemory(MemoryMXBean memoryMXBean) {
        MemoryUsage heap = memoryMXBean.getHeapMemoryUsage();
        MemoryUsage nonHeap = memoryMXBean.getNonHeapMemoryUsage();
        writeDataToTXT(new long[]{heap.getUsed(), nonHeap.getUsed(), nonHeap.getCommitted(), nonHeap.getMax()});
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

    public void writeDataToTXT(long[] data) {
        String filePath = "D:\\senior\\DQ\\research\\compressed_sort\\test\\memtable_sort_memory.txt";
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