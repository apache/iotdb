package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static java.lang.Math.min;
import static org.junit.Assert.assertEquals;

public class SubcolumnSAPHANATest {

    public static int bitWidth(int value) {
        return 32 - Integer.numberOfLeadingZeros(value);
    }

    public static void intToBytes(int srcNum, byte[] result, int pos, int width) {
        int cnt = pos & 0x07;
        int index = pos >> 3;
        while (width > 0) {
            int m = width + cnt >= 8 ? 8 - cnt : width;
            width -= m;
            int mask = 1 << (8 - cnt);
            cnt += m;
            byte y = (byte) (srcNum >>> width);
            y = (byte) (y << (8 - cnt));
            mask = ~(mask - (1 << (8 - cnt)));
            result[index] = (byte) (result[index] & mask | y);
            srcNum = srcNum & ~(-1 << width);
            if (cnt == 8) {
                index++;
                cnt = 0;
            }
        }
    }

    public static int bytesToInt(byte[] result, int pos, int width) {
        int ret = 0;
        int cnt = pos & 0x07;
        int index = pos >> 3;
        while (width > 0) {
            int m = width + cnt >= 8 ? 8 - cnt : width;
            width -= m;
            ret = ret << m;
            byte y = (byte) (result[index] & (0xff >> cnt));
            y = (byte) ((y & 0xff) >>> (8 - cnt - m));
            ret = ret | (y & 0xff);
            cnt += m;
            if (cnt == 8) {
                cnt = 0;
                index++;
            }
        }
        return ret;
    }

    public static void boolToBytes(boolean value, byte[] result, int pos) {
        int byteIndex = pos >> 3;
        int bitOffset = pos & 0x07;

        if (value) {
            result[byteIndex] |= (1 << (7 - bitOffset));
        } else {
            result[byteIndex] &= ~(1 << (7 - bitOffset));
        }
    }

    public static boolean bytesToBool(byte[] result, int pos) {
        int byteIndex = pos >> 3;
        int bitOffset = pos & 0x07;

        return (result[byteIndex] & (1 << (7 - bitOffset))) != 0;
    }

    public static void pack8Values(int[] values, int offset, int width, int encode_pos,
            byte[] encoded_result) {
        int bufIdx = 0;
        int valueIdx = offset;
        // remaining bits for the current unfinished Integer
        int leftBit = 0;

        while (valueIdx < 8 + offset) {
            // buffer is used for saving 32 bits as a part of result
            int buffer = 0;
            // remaining size of bits in the 'buffer'
            int leftSize = 32;

            // encode the left bits of current Integer to 'buffer'
            if (leftBit > 0) {
                buffer |= (values[valueIdx] << (32 - leftBit));
                leftSize -= leftBit;
                leftBit = 0;
                valueIdx++;
            }

            while (leftSize >= width && valueIdx < 8 + offset) {
                // encode one Integer to the 'buffer'
                buffer |= (values[valueIdx] << (leftSize - width));
                leftSize -= width;
                valueIdx++;
            }
            // If the remaining space of the buffer can not save the bits for one Integer,
            if (leftSize > 0 && valueIdx < 8 + offset) {
                // put the first 'leftSize' bits of the Integer into remaining space of the
                // buffer
                buffer |= (values[valueIdx] >>> (width - leftSize));
                leftBit = width - leftSize;
            }

            // put the buffer into the final result
            for (int j = 0; j < 4; j++) {
                encoded_result[encode_pos] = (byte) ((buffer >>> ((3 - j) * 8)) & 0xFF);
                encode_pos++;
                bufIdx++;
                if (bufIdx >= width) {
                    return;
                }
            }
        }

    }

    public static void unpack8Values(byte[] encoded, int offset, int width, int[] result_list, int result_offset) {
        int byteIdx = offset;
        long buffer = 0;
        // total bits which have read from 'buf' to 'buffer'. i.e.,
        // number of available bits to be decoded.
        int totalBits = 0;
        int valueIdx = 0;

        while (valueIdx < 8) {
            // If current available bits are not enough to decode one Integer,
            // then add next byte from buf to 'buffer' until totalBits >= width
            while (totalBits < width) {
                buffer = (buffer << 8) | (encoded[byteIdx] & 0xFF);
                byteIdx++;
                totalBits += 8;
            }

            // If current available bits are enough to decode one Integer,
            // then decode one Integer one by one until left bits in 'buffer' is
            // not enough to decode one Integer.
            while (totalBits >= width && valueIdx < 8) {
                // result_list.add((int) (buffer >>> (totalBits - width)));
                result_list[result_offset + valueIdx] = (int) (buffer >>> (totalBits - width));
                valueIdx++;
                totalBits -= width;
                buffer = buffer & ((1L << totalBits) - 1);
            }
        }
    }

    public static int bitPacking(int[] numbers, int bit_width, int encode_pos,
            byte[] encoded_result, int num_values) {
        int block_num = num_values / 8;
        int remainder = num_values % 8;

        for (int i = 0; i < block_num; i++) {
            pack8Values(numbers, i * 8, bit_width, encode_pos, encoded_result);
            encode_pos += bit_width;
        }

        encode_pos *= 8;

        for (int i = 0; i < remainder; i++) {
            intToBytes(numbers[block_num * 8 + i], encoded_result, encode_pos, bit_width);
            encode_pos += bit_width;
        }

        return (encode_pos + 7) / 8;
    }

    public static int decodeBitPacking(
            byte[] encoded, int decode_pos, int bit_width, int num_values, int[] result_list) {
        // ArrayList<Integer> result_list = new ArrayList<>();
        // int[] result_list = new int[num_values];
        int block_num = num_values / 8;
        int remainder = num_values % 8;

        for (int i = 0; i < block_num; i++) { // bitpacking
            unpack8Values(encoded, decode_pos, bit_width, result_list, i * 8);
            decode_pos += bit_width;
        }

        decode_pos *= 8;

        for (int i = 0; i < remainder; i++) {
            result_list[block_num * 8 + i] = bytesToInt(encoded, decode_pos, bit_width);
            decode_pos += bit_width;
        }

        return (decode_pos + 7) / 8;
    }

    public static void int2Bytes(int integer, int encode_pos, byte[] cur_byte) {
        cur_byte[encode_pos] = (byte) (integer >> 24);
        cur_byte[encode_pos + 1] = (byte) (integer >> 16);
        cur_byte[encode_pos + 2] = (byte) (integer >> 8);
        cur_byte[encode_pos + 3] = (byte) (integer);
    }

    public static void intByte2Bytes(int integer, int encode_pos, byte[] cur_byte) {
        cur_byte[encode_pos] = (byte) (integer);
    }

    public static void long2intBytes(long integer, int encode_pos, byte[] cur_byte) {
        cur_byte[encode_pos] = (byte) (integer >> 24);
        cur_byte[encode_pos + 1] = (byte) (integer >> 16);
        cur_byte[encode_pos + 2] = (byte) (integer >> 8);
        cur_byte[encode_pos + 3] = (byte) (integer);
    }

    public static int bytes2Integer(byte[] encoded, int start, int num) {
        int value = 0;

        for (int i = 0; i < num; i++) {
            value <<= 8;
            int b = encoded[i + start] & 0xFF;
            value |= b;
        }
        return value;
    }

    public static long bytesLong2Integer(byte[] encoded, int decode_pos) {
        long value = 0;
        for (int i = 0; i < 4; i++) {
            value <<= 8;
            int b = encoded[i + decode_pos] & 0xFF;
            value |= b;
        }
        return value;
    }

    public static int Subcolumn(int[] x, int x_length, int m, int block_size) {

        int betaBest = 1;

        int cMin = Integer.MAX_VALUE;

        // int[] beta_list = {1, 2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31};
        // int[] beta_list = { 1, 2, 3, 5, 7, 11 };
        // int[] beta_list = { 1, 2, 3, 4 };
        int[] beta_list = { 2, 3, 4 };

        int bw = bitWidth(block_size);

        int[] bitWidthListList = new int[m];

        for (int beta : beta_list) {
            if (beta > m) {
                break;
            }
            // System.out.println("beta: " + beta);

            int l = (m + beta - 1) / beta;

            // System.out.println("l: " + l);

            int[][] subcolumnList = new int[l][x_length];

            int cost = 0;

            for (int i = 0; i < l; i++) {
                int maxValuePart = 0;
                for (int j = 0; j < x_length; j++) {
                    subcolumnList[i][j] = (x[j] >> (i * beta)) & ((1 << beta) - 1);
                    if (subcolumnList[i][j] > maxValuePart) {
                        maxValuePart = subcolumnList[i][j];
                    }
                }
                bitWidthListList[i] = bitWidth(maxValuePart);
            }

            for (int i = 0; i < l; i++) {
                int bpCost = bitWidthListList[i] * x_length;
                int rleCost = 0;

                // int count = 1;
                int currentNumber = subcolumnList[i][0];

                int index = 0;

                boolean bpBest = false;

                for (int j = 1; j < x_length; j++) {
                    if (subcolumnList[i][j] != currentNumber) {
                        index++;
                        currentNumber = subcolumnList[i][j];
                    }

                    if (bw * index + bitWidthListList[i] * index >= bpCost) {
                        bpBest = true;
                        break;
                    }
                }

                if (bpBest) {
                    cost += bpCost;
                    continue;
                }

                index++;

                // System.out.println("index: " + index);

                rleCost = bw * index + bitWidthListList[i] * index;

                // System.out.println("bpCost: " + bpCost + " rleCost: " + rleCost);

                if (bpCost <= rleCost) {
                    cost += bpCost;
                } else {
                    cost += rleCost;
                }
            }

            // System.out.println("cost: " + cost);

            if (cost < cMin) {
                cMin = cost;
                betaBest = beta;
            }
        }

        return betaBest;
    }

    public static int SubcolumnEncoder(int[] list, int encode_pos, byte[] encoded_result, int[] beta, int block_size) {
        int list_length = list.length;
        int maxValue = 0;
        for (int i = 0; i < list_length; i++) {
            if (list[i] > maxValue) {
                maxValue = list[i];
            }
        }

        int m = bitWidth(maxValue);

        intByte2Bytes(m, encode_pos, encoded_result);
        encode_pos += 1;

        if (m == 0) {
            return encode_pos;
        }

        // int[] bitWidthList = new int[m];

        // int[][] subcolumnList = new int[m][list_length];

        int l;

        // int betaBest = beta[0];
        // byte betaBest = (byte) beta[0];

        l = (m + beta[0] - 1) / beta[0];

        int[] bitWidthList = new int[l];

        int[][] subcolumnList = new int[l][list_length];

        intByte2Bytes(beta[0], encode_pos, encoded_result);
        encode_pos += 1;

        int bw = bitWidth(block_size);
        int mask = (1 << beta[0]) - 1;

        for (int i = 0; i < l; i++) {
            int maxValuePart = 0;
            int shiftAmount = i * beta[0];
            for (int j = 0; j < list_length; j++) {
                subcolumnList[i][j] = (list[j] >> shiftAmount) & mask;
                if (subcolumnList[i][j] > maxValuePart) {
                    maxValuePart = subcolumnList[i][j];
                }
            }
            bitWidthList[i] = bitWidth(maxValuePart);
        }

        encode_pos = bitPacking(bitWidthList, 8, encode_pos, encoded_result, l);

        int[] encodingType = new int[l];

        // encoded_result 预留大小为 (l + 7) / 8 的大小，存储每个分列的类型
        int preTypePos = encode_pos;
        encode_pos += (l + 7) / 8;

        for (int i = l - 1; i >= 0; i--) {
            // 对于每个分列，计算使用 bit packing 还是 rle
            int bpCost = bitWidthList[i] * list_length;
            int rleCost = 0;

            int previous = subcolumnList[i][0];
            int index = 0;

            for (int j = 1; j < list_length; j++) {
                int currentNumber = subcolumnList[i][j];
                if (currentNumber != previous) {
                    index++;
                    previous = currentNumber;
                }

                if (bw * index + bitWidthList[i] * index >= bpCost) {
                    break;
                }
            }

            index++;

            rleCost = bw * index + bitWidthList[i] * index;

            if (bpCost <= rleCost) {
                encodingType[i] = 0;

                encode_pos = bitPacking(subcolumnList[i], bitWidthList[i], encode_pos, encoded_result, list_length);

            } else {
                encodingType[i] = 1;

                encoded_result[encode_pos] = (byte) (index >> 8);
                encode_pos += 1;
                encoded_result[encode_pos] = (byte) (index & 0xFF);
                encode_pos += 1;

                index = 0;
                int[] run_length = new int[list_length];
                int[] rle_values = new int[list_length];
                previous = subcolumnList[i][0];

                for (int j = 1; j < list_length; j++) {
                    int currentNumber = subcolumnList[i][j];
                    if (currentNumber != previous) {
                        run_length[index] = j;
                        rle_values[index] = previous;
                        index++;
                        previous = currentNumber;
                    }
                }

                run_length[index] = list_length;
                rle_values[index] = previous;
                index++;

                encode_pos = bitPacking(run_length, bw, encode_pos, encoded_result, index);

                encode_pos = bitPacking(rle_values, bitWidthList[i], encode_pos, encoded_result, index);

            }

        }

        preTypePos = bitPacking(encodingType, 1, preTypePos, encoded_result, l);

        return encode_pos;
    }

    public static int SubcolumnDecoder(byte[] encoded_result, int encode_pos, int[] list, int block_size) {
        int list_length = list.length;

        // int m = encoded_result[encode_pos];
        int m = bytes2Integer(encoded_result, encode_pos, 1);
        encode_pos += 1;

        if (m == 0) {
            return encode_pos;
        }

        int bw = bitWidth(block_size);

        int beta = bytes2Integer(encoded_result, encode_pos, 1);
        encode_pos += 1;

        int l = (m + beta - 1) / beta;

        int[] bitWidthList = new int[l];

        encode_pos = decodeBitPacking(encoded_result, encode_pos, 8, l, bitWidthList);

        int[][] subcolumnList = new int[l][list_length];

        int[] encodingType = new int[l];

        encode_pos = decodeBitPacking(encoded_result, encode_pos, 1, l, encodingType);

        for (int i = l - 1; i >= 0; i--) {
            int type = encodingType[i];
            int bitWidth = bitWidthList[i];
            if (type == 0) {
                encode_pos = decodeBitPacking(encoded_result, encode_pos, bitWidth, list_length,
                        subcolumnList[i]);
            } else {
                int index = ((encoded_result[encode_pos] & 0xFF) << 8) | (encoded_result[encode_pos + 1] & 0xFF);

                encode_pos += 2;

                int[] run_length = new int[index];
                int[] rle_values = new int[index];

                encode_pos = decodeBitPacking(encoded_result, encode_pos, bw, index, run_length);
                encode_pos = decodeBitPacking(encoded_result, encode_pos, bitWidth, index, rle_values);

                int currentIndex = 0;
                for (int j = 0; j < index; j++) {
                    int endPos = run_length[j];
                    int value = rle_values[j];
                    while (currentIndex < endPos) {
                        subcolumnList[i][currentIndex] = value;
                        currentIndex++;
                    }
                }
            }
        }

        for (int i = 0; i < l; i++) {
            int shiftAmount = i * beta;
            for (int j = 0; j < list_length; j++) {
                list[j] |= subcolumnList[i][j] << shiftAmount;
            }
        }

        return encode_pos;
    }

    public static int[] getAbsDeltaTsBlock(
            int[] ts_block,
            int i,
            int block_size,
            int remaining,
            int[] min_delta) {
        int[] ts_block_delta = new int[remaining];

        int value_delta_min = Integer.MAX_VALUE;
        int value_delta_max = Integer.MIN_VALUE;
        int base = i * block_size;
        int end = i * block_size + remaining;

        for (int j = base; j < end; j++) {
            int cur = ts_block[j];
            if (cur < value_delta_min) {
                value_delta_min = cur;
            }
            if (cur > value_delta_max) {
                value_delta_max = cur;
            }
        }

        for (int j = base; j < end; j++) {
            ts_block_delta[j - base] = ts_block[j] - value_delta_min;
        }

        min_delta[0] = value_delta_min;

        return ts_block_delta;
    }

    public static int BlockEncoder(int[] data, int block_index, int block_size, int remainder,
            int encode_pos, byte[] encoded_result, int[] beta) {
        int[] min_delta = new int[3];

        int[] data_delta = getAbsDeltaTsBlock(data, block_index, block_size,
                remainder, min_delta);
                
        int2Bytes(min_delta[0], encode_pos, encoded_result);
        encode_pos += 4;

        if (block_index == 0) {
            int maxValue = 0;
            for (int j = 0; j < remainder; j++) {
                if (data_delta[j] > maxValue) {
                    maxValue = data_delta[j];
                }
            }
            int m = bitWidth(maxValue);

            beta[0] = Subcolumn(data_delta, remainder, m, block_size);
        }

        encode_pos = SubcolumnEncoder(data_delta, encode_pos,
                encoded_result, beta, block_size);

        return encode_pos;
    }

    public static int BlockDecoder(byte[] encoded_result, int block_index, int block_size, int remainder,
            int encode_pos, int[] data) {
        int[] min_delta = new int[3];

        min_delta[0] = bytes2Integer(encoded_result, encode_pos, 4);
        encode_pos += 4;

        int[] block_data = new int[remainder];

        encode_pos = SubcolumnDecoder(encoded_result, encode_pos,
                block_data, block_size);

        for (int i = 0; i < remainder; i++) {
            data[block_index * block_size + i] = block_data[i] + min_delta[0];
        }

        return encode_pos;
    }

    public static int Encoder(int[] data, int block_size, byte[] encoded_result) {
        int data_length = data.length;
        int encode_pos = 0;

        int2Bytes(data_length, encode_pos, encoded_result);
        encode_pos += 4;

        int2Bytes(block_size, encode_pos, encoded_result);
        encode_pos += 4;

        int num_blocks = data_length / block_size;

        int remainder = data_length % block_size;

        int[] beta = new int[1];
        beta[0] = 2;

        for (int i = 0; i < num_blocks; i++) {
            encode_pos = BlockEncoder(data, i, block_size, block_size, encode_pos, encoded_result, beta);
        }

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                int value = data[num_blocks * block_size + i];
                int2Bytes(value, encode_pos, encoded_result);
                encode_pos += 4;
            }
        } else {
            encode_pos = BlockEncoder(data, num_blocks, block_size, remainder, encode_pos,
                    encoded_result, beta);
        }

        // System.out.println("beta: " + beta[0]);

        return encode_pos;
    }

    public static int[] Decoder(byte[] encoded_result) {
        int encode_pos = 0;

        int data_length = bytes2Integer(encoded_result, encode_pos, 4);
        encode_pos += 4;

        int block_size = bytes2Integer(encoded_result, encode_pos, 4);
        encode_pos += 4;

        int num_blocks = data_length / block_size;

        int[] data = new int[data_length];

        for (int i = 0; i < num_blocks; i++) {
            encode_pos = BlockDecoder(encoded_result, i, block_size, block_size, encode_pos, data);
        }

        int remainder = data_length % block_size;

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                data[num_blocks * block_size + i] = bytes2Integer(encoded_result, encode_pos, 4);
                encode_pos += 4;
            }
        } else {
            encode_pos = BlockDecoder(encoded_result, num_blocks, block_size, remainder,
                    encode_pos, data);
        }

        return data;
    }

    public static int getDecimalPrecision(String str) {
        // 查找小数点的位置
        int decimalIndex = str.indexOf(".");

        // 如果没有小数点，精度为0
        if (decimalIndex == -1) {
            return 0;
        }

        // 获取小数点后的部分并返回其长度
        return str.substring(decimalIndex + 1).length();
    }

    public static String extractFileName(String path) {
        if (path == null || path.isEmpty()) {
            return "";
        }

        File file = new File(path);
        String fileName = file.getName();

        int dotIndex = fileName.lastIndexOf('.');

        if (dotIndex == -1 || dotIndex == 0) {
            return fileName;
        }

        return fileName.substring(0, dotIndex);
    }


    private static long[] generateTimestampSeries(int totalPoints, double equidistantRatio) {
        Random random = new Random(42); // 固定种子以确保可重复性

        long currentTime = System.currentTimeMillis();
        int equidistantPoints = (int) (totalPoints * equidistantRatio);
        int randomPoints = totalPoints - equidistantPoints;

        // 创建等距部分的时间戳数组
        long[] equidistantArray = new long[equidistantPoints];
        for (int i = 0; i < equidistantPoints; i++) {
            equidistantArray[i] = currentTime;
            currentTime += 1000; // 每秒一个点
        }

        // 创建随机部分的时间戳数组
        long[] randomArray = new long[randomPoints];
        for (int i = 0; i < randomPoints; i++) {
            // 随机间隔，从1毫秒到1小时
            long randomInterval = 1 + random.nextInt(3600000);
            currentTime += randomInterval;
            randomArray[i] = currentTime;
        }

        // 将随机部分的时间戳插入到等距部分中，确保有序
        long[] result = Arrays.copyOf(equidistantArray, totalPoints);

        for (int i = 0; i < randomArray.length; i++) {
            // 找到插入位置
            int insertPos = Arrays.binarySearch(result, 0, equidistantPoints + i, randomArray[i]);
            if (insertPos < 0) {
                insertPos = -insertPos - 1; // 转换为插入位置
            }

            // 移动元素并插入
            System.arraycopy(result, insertPos, result, insertPos + 1, equidistantPoints + i - insertPos);
            result[insertPos] = randomArray[i];
        }

        return result;
    }
    @Test
    public void testSubcolumn() throws IOException {
//        String parent_dir = "D:/github/xjz17/subcolumn/";
//        // String parent_dir = "D:/encoding-subcolumn/";
//
//        String input_parent_dir = parent_dir + "dataset/";
//        // String input_parent_dir = parent_dir + "dataset/CMS9";
//
        String output_parent_dir = "/Users/xiaojinzhao/Documents/GitHub/subcolumn/result/subcolumn_vs_sap_hana/";
        // String output_parent_dir = parent_dir + "result/";

        // String outputPath = output_parent_dir + "subcolumn.csv";
//        String outputPath = output_parent_dir + "sap_hana.csv";

        // int block_size = 512;
        int block_size = 256;

        // int repeatTime = 100;
        int repeatTime = 500;

        // repeatTime = 1;


        int totalPoints = 10000; // 10万个点
        int numTests = 11; // 11个测试用例
        String[] rate_list = {"0.0","0.1","0.2","0.3","0.4","0.5","0.6","0.7","0.8","0.9","1.0"};
        // 测试不同分段占比
        for (int i = 0; i < numTests; i++) {
            double rate = i * 0.1;
            String outputPath = output_parent_dir + "subcolumn_" + rate_list[numTests-i-1] + ".csv";
            System.out.println(outputPath);
            CsvWriter writer = new CsvWriter(outputPath, ',', StandardCharsets.UTF_8);
            writer.setRecordDelimiter('\n');

            String[] head = {
                    "Encoding Algorithm",
                    "Encoding Time",
                    "Decoding Time",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head);


                long encodeTime = 0;
                long decodeTime = 0;
                double ratio = 0;
                double compressed_size = 0;

                int length = 0;
                long[] data2_arr = generateTimestampSeries(totalPoints,rate);
                byte[] encoded_result = new byte[Long.BYTES*totalPoints];
                long s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime; repeat++) {
                    length = SubcolumnLongTest.Encoder(data2_arr, block_size, encoded_result);
                }

                long e = System.nanoTime();
                encodeTime += ((e - s) / repeatTime);
                compressed_size += length;

                double ratioTmp;

                ratioTmp = compressed_size / (double) (data2_arr.length * Long.BYTES);

                ratio += ratioTmp;

                System.out.println("Decode");

                long[] data2_arr_decoded = new long[data2_arr.length];

                s = System.nanoTime();

                for (int repeat = 0; repeat < repeatTime; repeat++) {
                    data2_arr_decoded =SubcolumnLongTest.Decoder(encoded_result);
                }

                e = System.nanoTime();
                decodeTime += ((e - s) / repeatTime);


                String[] record = {
                        "Sub-columns",
                        String.valueOf(encodeTime),
                        String.valueOf(decodeTime),
                        String.valueOf(data2_arr.length),
                        String.valueOf(compressed_size),
                        String.valueOf(ratio)
                };
                writer.writeRecord(record);
                System.out.println(ratio);

            writer.close();
        }
    }

    @Test
    public void testSAPHANA() throws IOException {
//        String parent_dir = "D:/github/xjz17/subcolumn/";
//        // String parent_dir = "D:/encoding-subcolumn/";
//
//        String input_parent_dir = parent_dir + "dataset/";
//        // String input_parent_dir = parent_dir + "dataset/CMS9";
//
        String output_parent_dir = "/Users/xiaojinzhao/Documents/GitHub/subcolumn/result/subcolumn_vs_sap_hana/";
        // String output_parent_dir = parent_dir + "result/";

        // String outputPath = output_parent_dir + "subcolumn.csv";
//        String outputPath = output_parent_dir + "sap_hana.csv";

        // int block_size = 512;
        int block_size = 256;
        String[] rate_list = {"0.0","0.1","0.2","0.3","0.4","0.5","0.6","0.7","0.8","0.9","1.0"};
        // int repeatTime = 100;
        int repeatTime = 100;

        // repeatTime = 1;


        int totalPoints = 10000; // 10万个点
        int numTests = 11; // 11个测试用例

        // 测试不同分段占比
        for (int i = 0; i < numTests; i++) {
            double rate = i * 0.1;
            String outputPath = output_parent_dir + "sap_hana_" + rate_list[numTests-1-i] + ".csv";
            System.out.println(outputPath);
            CsvWriter writer = new CsvWriter(outputPath, ',', StandardCharsets.UTF_8);
            writer.setRecordDelimiter('\n');

            String[] head = {
                    "Encoding Algorithm",
                    "Encoding Time",
                    "Decoding Time",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head);


            long encodeTime = 0;
            long decodeTime = 0;
            double ratio = 0;
            double compressed_size = 0;

            int length = 0;
            long[] data2_arr = generateTimestampSeries(totalPoints,rate);
            byte[] encoded_result = new byte[Long.BYTES*totalPoints];



            CompressedData compressed= compress(data2_arr);
            long s = System.nanoTime();
            for (int repeat = 0; repeat < repeatTime; repeat++) {
                compressed = compress(data2_arr);
                length = 8 + 8 + (compressed.tsx.length * 4) + (compressed.ts0Array.length * 8) + (compressed.ts0Indices.length * 4);
            }

            long e = System.nanoTime();
            encodeTime += ((e - s) / repeatTime);
            compressed_size += length;

            double ratioTmp;

            ratioTmp = compressed_size / (double) (data2_arr.length * Long.BYTES);

            ratio += ratioTmp;

            System.out.println("Decode");

            long[] data2_arr_decoded = new long[data2_arr.length];

            s = System.nanoTime();

            for (int repeat = 0; repeat < repeatTime; repeat++) {
                long[] decompressed = decompress(compressed);
            }

            e = System.nanoTime();
            decodeTime += ((e - s) / repeatTime);


            String[] record = {
                    "SAP HANA",
                    String.valueOf(encodeTime),
                    String.valueOf(decodeTime),
                    String.valueOf(data2_arr.length),
                    String.valueOf(compressed_size),
                    String.valueOf(ratio)
            };
            writer.writeRecord(record);
            System.out.println(ratio);

            writer.close();
        }
    }
//    @Test
//    public void main() throws IOException {
//
//    }

        // 压缩数据结构
        public static class CompressedData {
            public long ts0; // 基准时间戳
            public long tsm; // 单位增量（100纳秒为单位）
            public int[] tsx; // 单位数量数组
            public long[] ts0Array; // 不规则时间戳数组
            public int[] ts0Indices; // 不规则时间戳的索引

            public CompressedData(long ts0, long tsm, int[] tsx, long[] ts0Array, int[] ts0Indices) {
                this.ts0 = ts0;
                this.tsm = tsm;
                this.tsx = tsx;
                this.ts0Array = ts0Array;
                this.ts0Indices = ts0Indices;
            }
        }

        /**
         * 压缩时间戳数组
         * @param timeArr 原始时间戳数组（毫秒）
         * @return 压缩后的数据
         */
        public static CompressedData compress(long[] timeArr) {
            if (timeArr == null || timeArr.length == 0) {
                return null;
            }

            // 1. 计算基准时间戳 (ts.b)
            long ts0 = timeArr[0];

            // 2. 计算最常见的间隔 (转换为100纳秒单位)
            long mostCommonInterval = findMostCommonInterval(timeArr);
            long tsm = mostCommonInterval * 10000; // 毫秒转换为100纳秒单位

            // 3. 识别不规则的时间戳
            List<Long> irregularTimestamps = new ArrayList<>();
            List<Integer> irregularIndices = new ArrayList<>();
            int[] tsx = new int[timeArr.length];

            for (int i = 0; i < timeArr.length; i++) {
                long timestamp = timeArr[i];
                long expectedTime = ts0 + (i * mostCommonInterval);

                if (timestamp == expectedTime) {
                    // 规则时间戳，计算单位数
                    tsx[i] = i;
                } else {
                    // 不规则时间戳，记录到ts0Array
                    irregularTimestamps.add(timestamp);
                    irregularIndices.add(i);
                    tsx[i] = -1; // 标记为不规则
                }
            }

            // 转换为数组
            long[] ts0Array = new long[irregularTimestamps.size()];
            int[] ts0Indices = new int[irregularIndices.size()];

            for (int i = 0; i < irregularTimestamps.size(); i++) {
                ts0Array[i] = irregularTimestamps.get(i);
                ts0Indices[i] = irregularIndices.get(i);
            }

            return new CompressedData(ts0, tsm, tsx, ts0Array, ts0Indices);
        }

        /**
         * 解压时间戳数组
         * @param compressedData 压缩后的数据
         * @return 原始时间戳数组
         */
        public static long[] decompress(CompressedData compressedData) {
            if (compressedData == null) {
                return null;
            }

            int length = compressedData.tsx.length;
            long[] result = new long[length];

            // 处理不规则时间戳
            for (int i = 0; i < compressedData.ts0Indices.length; i++) {
                int index = compressedData.ts0Indices[i];
                result[index] = compressedData.ts0Array[i];
            }

            // 处理规则时间戳
            for (int i = 0; i < length; i++) {
                if (compressedData.tsx[i] != -1) { // 不是不规则时间戳
                    // 计算时间戳: ts0 + (tsm * tsx) / 10000 (转换为毫秒)
                    result[i] = compressedData.ts0 + (compressedData.tsm * compressedData.tsx[i]) / 10000;
                }
            }

            return result;
        }

        /**
         * 查找最常见的时间间隔
         * @param timeArr 时间戳数组
         * @return 最常见的时间间隔（毫秒）
         */
        private static long findMostCommonInterval(long[] timeArr) {
            if (timeArr.length < 2) {
                return 0;
            }

            // 计算所有间隔
            long[] intervals = new long[timeArr.length - 1];
            for (int i = 1; i < timeArr.length; i++) {
                intervals[i - 1] = timeArr[i] - timeArr[i - 1];
            }

            // 找出最常见的间隔
            long mostCommon = intervals[0];
            int maxCount = 1;

            for (int i = 0; i < intervals.length; i++) {
                int count = 0;
                for (int j = 0; j < intervals.length; j++) {
                    if (intervals[i] == intervals[j]) {
                        count++;
                    }
                }

                if (count > maxCount) {
                    maxCount = count;
                    mostCommon = intervals[i];
                }
            }

            return mostCommon;
        }


}
