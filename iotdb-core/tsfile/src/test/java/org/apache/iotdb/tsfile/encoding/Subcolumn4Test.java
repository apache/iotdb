package org.apache.iotdb.tsfile.encoding;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;

import org.junit.Test;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

public class Subcolumn4Test {
    // 只对第一个 block 求最合适的 beta，之后的 block 都用同一个 beta
    // 使用 intToBytes 等操作
    // 对于 RLE 的分列，run_length 数组改为存累计长度
    // 将位宽等数组改为 byte 类型

    public static int bitWidth(int value) {
        return 32 - Integer.numberOfLeadingZeros(value);
    }

    public static byte bitWidthByte(int value) {
        return (byte) (32 - Integer.numberOfLeadingZeros(value));
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

    public static void byteToBytes(byte srcNum, byte[] result, int pos, int width) {
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
            srcNum = (byte) (srcNum & ~(-1 << width));
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

    public static byte bytesToByte(byte[] result, int pos, int width) {
        byte ret = 0;
        int cnt = pos & 0x07;
        int index = pos >> 3;
        while (width > 0) {
            int m = width + cnt >= 8 ? 8 - cnt : width;
            width -= m;
            ret = (byte) (ret << m);
            byte y = (byte) (result[index] & (0xff >> cnt));
            y = (byte) ((y & 0xff) >>> (8 - cnt - m));
            ret = (byte) (ret | (y & 0xff));
            cnt += m;
            if (cnt == 8) {
                cnt = 0;
                index++;
            }
        }
        return ret;
    }

    public static int bytesToIntSigned(byte[] result, int pos, int width) {
        int ret = 0;
        int cnt = pos & 0x07;
        int index = pos >> 3;
        int originalWidth = width;
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

        int shift = 32 - originalWidth;
        ret = (ret << shift) >> shift;

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

    public static void bitPacking(int[] values, byte[] array, int startBitPosition, int bitWidth, int numValues) {
        if (bitWidth == 0) {
            return;
        }
        for (int i = 0; i < numValues; i++) {
            intToBytes(values[i], array, startBitPosition + i * bitWidth, bitWidth);
        }
    }

    public static void bitPacking(byte[] values, byte[] array, int startBitPosition, int bitWidth, int numValues) {
        if (bitWidth == 0) {
            return;
        }
        for (int i = 0; i < numValues; i++) {
            byteToBytes(values[i], array, startBitPosition + i * bitWidth, bitWidth);
        }
    }

    public static int[] bitUnpacking(byte[] array, int startBitPosition, int bitWidth, int numValues) {
        int[] values = new int[numValues];
        if (bitWidth == 0) {
            return values;
        }
        for (int i = 0; i < numValues; i++) {
            values[i] = bytesToInt(array, startBitPosition + i * bitWidth, bitWidth);
        }
        return values;
    }

    public static byte[] bitUnpackingByte(byte[] array, int startBitPosition, int bitWidth, int numValues) {
        byte[] values = new byte[numValues];
        if (bitWidth == 0) {
            return values;
        }
        for (int i = 0; i < numValues; i++) {
            values[i] = bytesToByte(array, startBitPosition + i * bitWidth, bitWidth);
        }
        return values;
    }

    public static int Subcolumn(int[] x, int x_length, int m, int block_size) {

        int betaBest = 1;

        int cMin = Integer.MAX_VALUE;

        // int[] beta_list = {1, 2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31};
        // int[] beta_list = { 1, 2, 3, 5, 7, 11 };
        // int[] beta_list = { 1, 2, 3, 4 };
        int[] beta_list = { 2, 3, 4 };

        byte bw = bitWidthByte(block_size);

        // bitWidthListList[i] 表示 beta 取 i 时的 bitWidthList
        // int[][] bitWidthListList = new int[m + 1][m];
        byte[][] bitWidthListList = new byte[m + 1][m];

        for (int beta : beta_list) {
            if (beta > m) {
                break;
            }
            // System.out.println("beta: " + beta);

            int l = (m + beta - 1) / beta;

            // System.out.println("l: " + l);

            byte[][] subcolumnListList = new byte[l][x_length];

            int cost = 0;

            for (int i = 0; i < l; i++) {
                byte maxValuePart = 0;
                for (int j = 0; j < x_length; j++) {
                    subcolumnListList[i][j] = (byte) ((x[j] >> (i * beta)) & ((1 << beta) - 1));
                    if (subcolumnListList[i][j] > maxValuePart) {
                        maxValuePart = subcolumnListList[i][j];
                    }
                }
                bitWidthListList[beta][i] = bitWidthByte(maxValuePart);
            }

            for (int i = 0; i < l; i++) {
                int bpCost = bitWidthListList[beta][i] * x_length;
                int rleCost = 0;

                // int count = 1;
                byte currentNumber = subcolumnListList[i][0];

                int index = 0;

                boolean bpBest = false;

                for (int j = 1; j < x_length; j++) {
                    if (subcolumnListList[i][j] == currentNumber) {
                        // count++;
                    } else {
                        index++;
                        currentNumber = subcolumnListList[i][j];
                    }

                    if (bw * index + bitWidthListList[beta][i] * index >= bpCost) {
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

                rleCost = bw * index + bitWidthListList[beta][i] * index;

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

    public static int SubcolumnEncoder(int[] list, int startBitPosition, byte[] encoded_result, int[] beta, int block_size) {
        int list_length = list.length;
        int maxValue = 0;
        for (int i = 0; i < list_length; i++) {
            if (list[i] > maxValue) {
                maxValue = list[i];
            }
        }

        byte m = bitWidthByte(maxValue);

        byteToBytes(m, encoded_result, startBitPosition, 6);

        startBitPosition += 6;


        if (m == 0) {
            return startBitPosition;
        }

        byte[] bitWidthList = new byte[m];

        byte[][] subcolumnList = new byte[m][list_length];

        int l;

        // int betaBest = beta[0];
        byte betaBest = (byte) beta[0];

        l = (m + betaBest - 1) / betaBest;

        byteToBytes(betaBest, encoded_result, startBitPosition, 6);

        startBitPosition += 6;

        int bw = bitWidth(block_size);

        for (int i = 0; i < l; i++) {
            byte maxValuePart = 0;
            for (int j = 0; j < list_length; j++) {
                subcolumnList[i][j] = (byte) ((list[j] >> (i * betaBest)) & ((1 << betaBest) - 1));
                if (subcolumnList[i][j] > maxValuePart) {
                    maxValuePart = subcolumnList[i][j];
                }
            }
            bitWidthList[i] = bitWidthByte(maxValuePart);
        }

        bitPacking(bitWidthList, encoded_result, startBitPosition, 8, l);

        startBitPosition += 8 * l;

        for (int i = l - 1; i >= 0; i--) {
            // 对于每个分列，计算使用 bit packing 还是 rle
            int bpCost = bitWidthList[i] * list_length;
            int rleCost = 0;

            // int count = 1;
            byte currentNumber = subcolumnList[i][0];

            int index = 0;

            // run_length 改为存累计长度
            int[] run_length = new int[list_length];
            byte[] rle_values = new byte[list_length];

            for (int j = 1; j < list_length; j++) {
                if (subcolumnList[i][j] == currentNumber) {
                    // count++;
                } else {
                    run_length[index] = j;
                    rle_values[index] = currentNumber;
                    index++;
                    currentNumber = subcolumnList[i][j];
                }
            }

            run_length[index] = list_length;
            rle_values[index] = currentNumber;
            index++;

            rleCost = bw * index + bitWidthList[i] * index;

            if (bpCost <= rleCost) {
                // intToBytes(0, encoded_result, startBitPosition, 1);
                boolToBytes(false, encoded_result, startBitPosition);
                startBitPosition += 1;

                bitPacking(subcolumnList[i], encoded_result, startBitPosition, bitWidthList[i], list_length);
                startBitPosition += bitWidthList[i] * list_length;
            } else {
                // intToBytes(1, encoded_result, startBitPosition, 1);
                boolToBytes(true, encoded_result, startBitPosition);
                startBitPosition += 1;

                intToBytes(index, encoded_result, startBitPosition, 16);
                startBitPosition += 16;

                bitPacking(run_length, encoded_result, startBitPosition, bw, index);
                startBitPosition += bw * index;

                bitPacking(rle_values, encoded_result, startBitPosition, bitWidthList[i], index);
                startBitPosition += bitWidthList[i] * index;
            }

        }

        return startBitPosition;
    }

    public static int SubcolumnDecoder(byte[] encoded_result, int startBitPosition, int[] list, int block_size) {
        int list_length = list.length;

        byte m = bytesToByte(encoded_result, startBitPosition, 6);

        startBitPosition += 6;

        if (m == 0) {
            return startBitPosition;
        }

        byte bw = bitWidthByte(block_size);

        byte beta = bytesToByte(encoded_result, startBitPosition, 6);
        startBitPosition += 6;

        int l = (m + beta - 1) / beta;

        byte[] bitWidthList = bitUnpackingByte(encoded_result, startBitPosition, 8, l);
        startBitPosition += 8 * l;

        byte[][] subcolumnList = new byte[l][list_length];

        for (int i = l - 1; i >= 0; i--) {
            // int type = bytesToInt(encoded_result, startBitPosition, 1);
            boolean type = bytesToBool(encoded_result, startBitPosition);
            startBitPosition += 1;
            // if (type == 0) {
            if (!type) {
                subcolumnList[i] = bitUnpackingByte(encoded_result, startBitPosition, bitWidthList[i], list_length);
                startBitPosition += bitWidthList[i] * list_length;
            } else {
                int index = bytesToInt(encoded_result, startBitPosition, 16);
                startBitPosition += 16;

                int[] run_length = bitUnpacking(encoded_result, startBitPosition, bw, index);
                startBitPosition += bw * index;

                byte[] rle_values = bitUnpackingByte(encoded_result, startBitPosition, bitWidthList[i], index);
                startBitPosition += bitWidthList[i] * index;

                int currentIndex = 0;
                for (int j = 0; j < index; j++) {
                    int endPos = run_length[j];
                    while (currentIndex < endPos) {
                        subcolumnList[i][currentIndex] = rle_values[j];
                        currentIndex++;
                    }
                }

            }
        }

        for (int i = 0; i < list_length; i++) {
            list[i] = 0;
            for (int j = 0; j < l; j++) {
                list[i] |= subcolumnList[j][i] << (j * beta);
            }
        }

        return startBitPosition;
    }

    /**
     * 仅将数据处理为非负数，也就是将 ts_block 中的数据都减去最小值
     */
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
            int startBitPosition, byte[] encoded_result, int[] beta) {
        int[] min_delta = new int[3];

        int[] data_delta = getAbsDeltaTsBlock(data, block_index, block_size,
                remainder, min_delta);

        intToBytes(min_delta[0], encoded_result, startBitPosition, 32);

        startBitPosition += 32;
        
        // System.out.println("min_delta: " + min_delta[0]);

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

        startBitPosition = SubcolumnEncoder(data_delta, startBitPosition,
                encoded_result, beta, block_size);

        return startBitPosition;
    }

    public static int BlockDecoder(byte[] encoded_result, int block_index, int block_size, int remainder,
            int startBitPosition, int[] data) {
        int[] min_delta = new int[3];

        min_delta[0] = bytesToIntSigned(encoded_result, startBitPosition, 32);
        startBitPosition += 32;

        int[] block_data = new int[remainder];

        startBitPosition = SubcolumnDecoder(encoded_result, startBitPosition,
                block_data, block_size);

        for (int i = 0; i < remainder; i++) {
            data[block_index * block_size + i] = block_data[i] + min_delta[0];
        }

        return startBitPosition;
    }

    public static int Encoder(int[] data, int block_size, byte[] encoded_result) {
        int data_length = data.length;
        int startBitPosition = 0;

        intToBytes(data_length, encoded_result, startBitPosition, 32);
        startBitPosition += 32;

        intToBytes(block_size, encoded_result, startBitPosition, 32);
        startBitPosition += 32;

        int num_blocks = data_length / block_size;

        int remainder = data_length % block_size;

        int[] beta = new int[1];
        beta[0] = 2;

        for (int i = 0; i < num_blocks; i++) {
            startBitPosition = BlockEncoder(data, i, block_size, block_size, startBitPosition, encoded_result, beta);
        }

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                intToBytes(data[num_blocks * block_size + i], encoded_result, startBitPosition, 32);
                startBitPosition += 32;
            }
        } else {
            startBitPosition = BlockEncoder(data, num_blocks, block_size, remainder, startBitPosition,
                    encoded_result, beta);
        }

        return startBitPosition;
    }

    public static int[] Decoder(byte[] encoded_result) {
        int startBitPosition = 0;

        int data_length = bytesToInt(encoded_result, startBitPosition, 32);
        startBitPosition += 32;

        int block_size = bytesToInt(encoded_result, startBitPosition, 32);
        startBitPosition += 32;

        int num_blocks = data_length / block_size;

        int[] data = new int[data_length];

        for (int i = 0; i < num_blocks; i++) {
            startBitPosition = BlockDecoder(encoded_result, i, block_size, block_size, startBitPosition, data);
        }

        int remainder = data_length % block_size;

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                data[num_blocks * block_size + i] = bytesToIntSigned(encoded_result, startBitPosition, 32);
                startBitPosition += 32;
            }
        } else {
            startBitPosition = BlockDecoder(encoded_result, num_blocks, block_size, remainder,
                    startBitPosition, data);
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

    @Test
    public void testSubcolumn() throws IOException {
        String parent_dir = "D:/github/xjz17/subcolumn/elf_resources/dataset/";
        // String parent_dir = "D:/compress-subcolumn/dataset/";

        String output_parent_dir = "D:/compress-subcolumn/";

        String outputPath = output_parent_dir + "subcolumn40.csv";

        // int block_size = 1024;
        int block_size = 512;

        int repeatTime = 500;
        // TODO 真正计算时，记得注释掉将下面的内容
        // repeatTime = 1;

        CsvWriter writer = new CsvWriter(outputPath, ',', StandardCharsets.UTF_8);

        String[] head = {
                "Dataset",
                "Encoding Algorithm",
                "Encoding Time",
                "Decoding Time",
                "Points",
                "Compressed Size",
                "Compression Ratio"
        };
        writer.writeRecord(head);

        File directory = new File(parent_dir);
        // File[] csvFiles = directory.listFiles();
        File[] csvFiles = directory.listFiles((dir, name) -> name.endsWith(".csv"));

        for (File file : csvFiles) {
            String datasetName = extractFileName(file.toString());
            System.out.println(datasetName);

            InputStream inputStream = Files.newInputStream(file.toPath());

            CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
            ArrayList<Float> data1 = new ArrayList<>();

            int max_decimal = 0;
            while (loader.readRecord()) {
                String f_str = loader.getValues()[0];
                int cur_decimal = getDecimalPrecision(f_str);
                if (cur_decimal > max_decimal)
                    max_decimal = cur_decimal;
                data1.add(Float.valueOf(f_str));
            }
            inputStream.close();
            int[] data2_arr = new int[data1.size()];
            int max_mul = (int) Math.pow(10, max_decimal);
            for (int i = 0; i < data1.size(); i++) {
                data2_arr[i] = (int) (data1.get(i) * max_mul);
            }

            System.out.println(max_decimal);
            byte[] encoded_result = new byte[data2_arr.length * 4];

            long encodeTime = 0;
            long decodeTime = 0;
            double ratio = 0;
            double compressed_size = 0;

            int length = 0;

            long s = System.nanoTime();
            for (int repeat = 0; repeat < repeatTime; repeat++) {
                length = Encoder(data2_arr, block_size, encoded_result);
            }

            long e = System.nanoTime();
            encodeTime += ((e - s) / repeatTime);
            compressed_size += length / 8;
            double ratioTmp = compressed_size / (double) (data1.size() * Long.BYTES);
            ratio += ratioTmp;

            System.out.println("Decode");

            s = System.nanoTime();

            for (int repeat = 0; repeat < repeatTime; repeat++) {
                int[] data2_arr_decoded = Decoder(encoded_result);
                for (int i = 0; i < data2_arr_decoded.length; i++) {
                    assert data2_arr[i] == data2_arr_decoded[i];
                }
            }

            e = System.nanoTime();
            decodeTime += ((e - s) / repeatTime);

            String[] record = {
                    datasetName,
                    "Subcolumn",
                    String.valueOf(encodeTime),
                    String.valueOf(decodeTime),
                    String.valueOf(data1.size()),
                    String.valueOf(compressed_size),
                    String.valueOf(ratio)
            };
            writer.writeRecord(record);
            System.out.println(ratio);
        }

        writer.close();
    }
}
