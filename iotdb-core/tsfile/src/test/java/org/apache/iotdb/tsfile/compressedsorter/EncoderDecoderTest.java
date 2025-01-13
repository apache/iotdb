package org.apache.iotdb.tsfile.compressedsorter;

import org.apache.iotdb.tsfile.encoding.decoder.*;
import org.apache.iotdb.tsfile.encoding.encoder.*;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class EncoderDecoderTest {
    private static int ROW_NUM = 1000000;
    private static int REAPEAT_NUM = 1000;
    @Test
    public void testTimeEncodeSize() throws IOException {
        OrderSensitiveTimeEncoder timeEncoder1 = new OrderSensitiveTimeEncoder();
        ByteArrayOutputStream out1 = new ByteArrayOutputStream();
        Encoder timeEncoder2 = new DeltaBinaryEncoder.LongDeltaEncoder();
        ByteArrayOutputStream out2 = new ByteArrayOutputStream();
        Encoder timeEncoder3 = new LongChimpEncoder();
        ByteArrayOutputStream out3 = new ByteArrayOutputStream();
        Encoder timeEncoder4 = new LongSprintzEncoder();
        ByteArrayOutputStream out4 = new ByteArrayOutputStream();
        long[] times = new long[ROW_NUM];
        long[] values = new long[ROW_NUM];
        prepareData(times, values);
        //Collections.sort(times);   // test sorted data or not
        for(int i=0; i<ROW_NUM-1; i++) {
            timeEncoder1.encode(times[i], out1);
            timeEncoder2.encode(times[i], out2);
            timeEncoder3.encode(times[i], out3);
            timeEncoder4.encode(times[i], out4);
        }
        timeEncoder1.flush(out1);
        timeEncoder2.flush(out2);
        timeEncoder3.flush(out3);
        timeEncoder4.flush(out4);
    }

    @Test
    public void testValueEncodeSize() throws IOException {
        OrderSensitiveValueEncoder valueEncoder = new OrderSensitiveValueEncoder();
        ByteArrayOutputStream out1 = new ByteArrayOutputStream();
        Encoder timeEncoder2 = new DeltaBinaryEncoder.LongDeltaEncoder();
        ByteArrayOutputStream out2 = new ByteArrayOutputStream();
        Encoder timeEncoder3 = new LongChimpEncoder();
        ByteArrayOutputStream out3 = new ByteArrayOutputStream();
        Encoder valueEncoder4 = new LongSprintzEncoder();
        ByteArrayOutputStream out4 = new ByteArrayOutputStream();
        long[] times = new long[ROW_NUM];
        long[] values = new long[ROW_NUM];
        prepareData(times, values);
        sortTimeWithValue(times, values);   // test sorted data or not
        for(int i=0; i<ROW_NUM-1; i++) {
            valueEncoder.encode(values[i], out1);
            timeEncoder2.encode(values[i], out2);
            timeEncoder3.encode(values[i], out3);
            valueEncoder4.encode(values[i], out4);
        }
        timeEncoder2.flush(out2);
        timeEncoder3.flush(out3);
        valueEncoder4.flush(out4);
        times[1] = 0;
    }

    @Test
    public void testTimeValueEncodeDecodeTime() throws IOException {
        // total是指时间列和空间列一起考虑，但是编码和解码时间仍然是分开统计
        long encodeTime1 = 0;
        long encodeTime2 = 0;
        long encodeTime3 = 0;
        long encodeTime4 = 0;
        long encodeTime5 = 0;

        long decodeTime1 = 0;
        long decodeTime2 = 0;
        long decodeTime3 = 0;
        long decodeTime4 = 0;
        long decodeTime5 = 0;
        OrderSensitiveTimeEncoder timeEncoder1 = new OrderSensitiveTimeEncoder();
        OrderSensitiveValueEncoder valueEncoder1 = new OrderSensitiveValueEncoder();
        ByteArrayOutputStream timeCompressedData1 = new ByteArrayOutputStream();
        ByteArrayOutputStream valueCompressedData1 = new ByteArrayOutputStream();
        OrderSensitiveTimeDecoder timeDecoder1 = new OrderSensitiveTimeDecoder();
        OrderSensitiveValueDecoder valueDecoder1 = new OrderSensitiveValueDecoder();

        Encoder timeEncoder2 = new DeltaBinaryEncoder.LongDeltaEncoder();
        Decoder timeDecoder2 = new DeltaBinaryDecoder.LongDeltaDecoder();
        ByteArrayOutputStream out2 = new ByteArrayOutputStream();
        Encoder valueEncoder2 = new DeltaBinaryEncoder.LongDeltaEncoder();
        Decoder valueDecoder2 = new DeltaBinaryDecoder.LongDeltaDecoder();
        ByteArrayOutputStream valueOut2 = new ByteArrayOutputStream();

        Encoder timeEncoder3 = new LongChimpEncoder();
        Decoder timeDecoder3 = new LongChimpDecoder();
        ByteArrayOutputStream out3 = new ByteArrayOutputStream();
        Encoder valueEncoder3 = new LongChimpEncoder();
        Decoder valueDecoder3 = new LongChimpDecoder();
        ByteArrayOutputStream valueOut3 = new ByteArrayOutputStream();

        Encoder timeEncoder5 = new LongSprintzEncoder();
        Decoder timeDecoder5 = new LongSprintzDecoder();
        ByteArrayOutputStream out5 = new ByteArrayOutputStream();
        Encoder valueEncoder5 = new LongSprintzEncoder();
        Decoder valueDecoder5 = new LongSprintzDecoder();
        ByteArrayOutputStream valueOut5 = new ByteArrayOutputStream();


        long[] times = new long[ROW_NUM];
        long[] values = new long[ROW_NUM];
        prepareData(times, values);
        //sortTimeWithValue(times, values);
        for(int j=0; j<REAPEAT_NUM; j++) {
            // Order-Sensitive encode time
            long startTime = System.currentTimeMillis();
            timeEncoder1 = new OrderSensitiveTimeEncoder();
            timeCompressedData1 = new ByteArrayOutputStream();
            valueEncoder1 = new OrderSensitiveValueEncoder();
            valueCompressedData1 = new ByteArrayOutputStream();
            for(int i=0; i<ROW_NUM-1; i++) {
                timeEncoder1.encode(times[i], timeCompressedData1);
                valueEncoder1.encode(values[i], valueCompressedData1);
            }
            timeEncoder1.flush(timeCompressedData1);
            valueEncoder1.flush(valueCompressedData1);
            encodeTime1 += System.currentTimeMillis()-startTime;
            //decode time
            startTime = System.currentTimeMillis();
            ByteBuffer timeBuffer1 = ByteBuffer.wrap(timeCompressedData1.toByteArray());
            ByteBuffer valueBuffer1 = ByteBuffer.wrap(valueCompressedData1.toByteArray());
            timeDecoder1 = new OrderSensitiveTimeDecoder();
            valueDecoder1 = new OrderSensitiveValueDecoder();
            timeDecoder1.hasNext(timeBuffer1);
            valueDecoder1.hasNext(valueBuffer1);
            for(int i=0; i<ROW_NUM-1; i++) {
                timeDecoder1.readLong(timeBuffer1);
                valueDecoder1.readLong(valueBuffer1);
//                assertEquals(timeDecoder1.readLong(timeBuffer1), times[i]);
//                assertEquals(valueDecoder1.readLong(valueBuffer1), values[i]);
            }
            decodeTime1 += System.currentTimeMillis()-startTime;

            // TS_2DIFF
            startTime = System.currentTimeMillis();
            timeEncoder2 = new DeltaBinaryEncoder.LongDeltaEncoder();
            out2 = new ByteArrayOutputStream();
            valueEncoder2 = new DeltaBinaryEncoder.LongDeltaEncoder();
            valueOut2 = new ByteArrayOutputStream();
            for(int i=0; i<ROW_NUM-1; i++) {
                timeEncoder2.encode(times[i], out2);
                valueEncoder2.encode(values[i], valueOut2);
            }
            timeEncoder2.flush(out2);
            valueEncoder2.flush(valueOut2);
            encodeTime2 += System.currentTimeMillis()-startTime;
            startTime = System.currentTimeMillis();
            ByteBuffer timeBuffer2 = ByteBuffer.wrap(out2.toByteArray());
            ByteBuffer valueBuffer2 = ByteBuffer.wrap(valueOut2.toByteArray());
            timeDecoder2 = new DeltaBinaryDecoder.LongDeltaDecoder();
            valueDecoder2 = new DeltaBinaryDecoder.LongDeltaDecoder();
            for(int i=0; i<ROW_NUM-1; i++) {
                timeDecoder2.readLong(timeBuffer2);
                valueDecoder2.readLong(valueBuffer2);
//                assertEquals(timeDecoder2.readLong(timeBuffer2), times[i]);
//                assertEquals(valueDecoder2.readLong(valueBuffer2), values[i]);
            }
            decodeTime2 += System.currentTimeMillis()-startTime;

            // Chimp
            startTime = System.currentTimeMillis();
            timeEncoder3 = new LongChimpEncoder();
            out3 = new ByteArrayOutputStream();
            valueEncoder3 = new LongChimpEncoder();
            valueOut3 = new ByteArrayOutputStream();
            for(int i=0; i<ROW_NUM-1; i++) {
                timeEncoder3.encode(times[i], out3);
                valueEncoder3.encode(values[i], valueOut3);
            }
            timeEncoder3.flush(out3);
            valueEncoder3.flush(valueOut3);
            encodeTime3 += System.currentTimeMillis()-startTime;
            startTime = System.currentTimeMillis();
            ByteBuffer buffer3 = ByteBuffer.wrap(out3.toByteArray());
            ByteBuffer valueBuffer3 = ByteBuffer.wrap(valueOut3.toByteArray());
            timeDecoder3 = new LongChimpDecoder();
            valueDecoder3 = new LongChimpDecoder();
            for(int i=0; i<ROW_NUM-1; i++) {
                timeDecoder3.readLong(buffer3);
                valueDecoder3.readLong(valueBuffer3);
//                assertEquals(timeDecoder3.readLong(buffer3), times[i]);
//                assertEquals(valueDecoder3.readLong(valueBuffer3), values[i]);
            }
            decodeTime3 += System.currentTimeMillis()-startTime;

            // Sprintz
            startTime = System.currentTimeMillis();
            timeEncoder5 = new LongSprintzEncoder();
            out5 = new ByteArrayOutputStream();
            valueEncoder5 = new LongSprintzEncoder();
            valueOut5 = new ByteArrayOutputStream();
            for(int i=0; i<ROW_NUM-1; i++) {
                timeEncoder5.encode(times[i], out5);
                valueEncoder5.encode(values[i], valueOut5);
            }
            timeEncoder5.flush(out5);
            valueEncoder5.flush(valueOut5);
            encodeTime5 += System.currentTimeMillis()-startTime;
            startTime = System.currentTimeMillis();
            ByteBuffer buffer5 = ByteBuffer.wrap(out5.toByteArray());
            ByteBuffer valueBuffer5 = ByteBuffer.wrap(valueOut5.toByteArray());
            timeDecoder5 = new LongSprintzDecoder();
            valueDecoder5 = new LongSprintzDecoder();
            for(int i=0; i<ROW_NUM-1; i++) {
                timeDecoder5.readLong(buffer5);
                valueDecoder5.readLong(valueBuffer5);
//                assertEquals(timeDecoder5.readLong(buffer5), times[i]);
//                assertEquals(valueDecoder5.readLong(valueBuffer5), values[i]);
            }
            decodeTime5 += System.currentTimeMillis()-startTime;
        }
        // record result
        writeDataToTXT(new long[] {encodeTime1, encodeTime2, encodeTime3, encodeTime4, encodeTime5, decodeTime1, decodeTime2, decodeTime3, decodeTime4, decodeTime5});
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

    public void writeDataToTXT(long[] data) {
        String filePath = "D:\\senior\\DQ\\research\\compressed_sort\\test\\encode_decode_time.txt";
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
