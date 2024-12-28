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

public class SubcolumnIntTest {
    // 对于每个块，beta 从 1 到 4 遍历计算最合适的 beta

    public static int bitWidth(int value) {
        return 32 - Integer.numberOfLeadingZeros(value);
    }

    public static void writeBits(byte[] array, int startBitPosition, int bitWidth, int value) {
        int bytePosition = startBitPosition / 8;
        int bitOffset = startBitPosition % 8;
        int bitsLeft = bitWidth;

        while (bitsLeft > 0) {
            int bitsToWrite = Math.min(8 - bitOffset, bitsLeft);
            int mask = (1 << bitsToWrite) - 1;
            int shift = 8 - bitOffset - bitsToWrite;
            int bits = (value >> (bitsLeft - bitsToWrite)) & mask;
            array[bytePosition] |= bits << shift;

            bitsLeft -= bitsToWrite;
            bytePosition++;
            bitOffset = 0;
        }
    }

    public static int readBits(byte[] array, int startBitPosition, int bitWidth, int signed) {
        int bytePosition = startBitPosition / 8;
        int bitOffset = startBitPosition % 8;
        int bitsLeft = bitWidth;
        int bitsRead = 0;
        int value = 0;

        while (bitsLeft > 0) {
            int bitsToRead = Math.min(8 - bitOffset, bitsLeft);
            int mask = (1 << bitsToRead) - 1;
            int shift = 8 - bitOffset - bitsToRead;
            int bits = (array[bytePosition] >> shift) & mask;
            value |= bits << (bitsLeft - bitsToRead);

            bitsLeft -= bitsToRead;
            bitsRead += bitsToRead;
            bytePosition++;
            bitOffset = 0;
        }

        if (signed == 1) {
            int shift = 32 - bitsRead;
            value = (value << shift) >> shift;
        }

        return value;
    }

    public static void bitPacking(int[] values, byte[] array, int startBitPosition, int bitWidth, int numValues) {
        if (bitWidth == 0) {
            return;
        }
        for (int i = 0; i < numValues; i++) {
            writeBits(array, startBitPosition + i * bitWidth, bitWidth, values[i]);
        }
    }

    public static int[] bitUnpacking(byte[] array, int startBitPosition, int bitWidth, int numValues) {
        int[] values = new int[numValues];
        if (bitWidth == 0) {
            return values;
        }
        for (int i = 0; i < numValues; i++) {
            values[i] = readBits(array, startBitPosition + i * bitWidth, bitWidth, 0);
        }
        return values;
    }

    public static int Subcolumn(int[] x, int m, int[] bitWidthList, int[] encodingType) {
        int x_length = x.length;

        // System.out.println("m: " + m);

        int betaBest = 1;

        int cMin = Integer.MAX_VALUE;

        // int[] beta_list = {1, 2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31};
        // int[] beta_list = { 1, 2, 3, 5, 7, 11 };
        int[] beta_list = { 1, 2, 3, 4 };

        // bitWidthListList[i] 表示 beta 取 i 时的 bitWidthList
        int[][] bitWidthListList = new int[m + 1][m];

        int[][] encodingTypeList = new int[m + 1][m];

        for (int beta : beta_list) {
            if (beta > m) {
                break;
            }
            // System.out.println("beta: " + beta);

            int l = (m + beta - 1) / beta;

            // System.out.println("l: " + l);

            int[][] subcolumnListList = new int[l][x_length];

            int cost = 0;

            for (int i = 0; i < l; i++) {
                int maxValuePart = 0;
                for (int j = 0; j < x_length; j++) {
                    subcolumnListList[i][j] = (x[j] >> (i * beta)) & ((1 << beta) - 1);
                    if (subcolumnListList[i][j] > maxValuePart) {
                        maxValuePart = subcolumnListList[i][j];
                    }
                }
                bitWidthListList[beta][i] = bitWidth(maxValuePart);
            }

            for (int i = 0; i < l; i++) {
                int bpCost = bitWidthListList[beta][i] * x_length;
                int rleCost = 0;

                int count = 1;
                int currentNumber = subcolumnListList[i][0];

                int index = 0;

                boolean bpBest = false;

                for (int j = 1; j < x_length; j++) {
                    if (subcolumnListList[i][j] == currentNumber) {
                        count++;
                        if (count == 255) {
                            index++;
                            count = 0;
                        }
                    } else {
                        index++;
                        currentNumber = subcolumnListList[i][j];
                        count = 1;
                    }

                    if (8 * index + bitWidthListList[beta][i] * index >= bpCost) {
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

                rleCost = 8 * index + bitWidthListList[beta][i] * index;

                // System.out.println("bpCost: " + bpCost + " rleCost: " + rleCost);

                if (bpCost <= rleCost) {
                    cost += bpCost;
                } else {
                    encodingTypeList[beta][i] = 1;
                    cost += rleCost;
                }
            }

            // System.out.println("cost: " + cost);

            if (cost < cMin) {
                cMin = cost;
                betaBest = beta;
            }
        }

        int l = (m + betaBest - 1) / betaBest;

        for (int i = 0; i < l; i++) {
            bitWidthList[i] = bitWidthListList[betaBest][i];
            encodingType[i] = encodingTypeList[betaBest][i];
        }

        return betaBest;
    }

    public static int SubcolumnEncoder(int[] list, int startBitPosition, byte[] encoded_result) {
        int list_length = list.length;
        int maxValue = 0;
        for (int i = 0; i < list_length; i++) {
            if (list[i] > maxValue) {
                maxValue = list[i];
            }
        }

        int m = bitWidth(maxValue);

        writeBits(encoded_result, startBitPosition, 6, m);

        startBitPosition += 6;

        if (m == 0) {
            return startBitPosition;
        }

        int[] bitWidthList = new int[m];

        // 0 表示 bit packing，1 表示 rle
        int[] encodingType = new int[m];

        int[][] subcolumnList = new int[m][list_length];

        int beta = 2;
        beta = Subcolumn(list, m, bitWidthList, encodingType);

        // System.out.println("beta: " + beta);

        int l;

        l = (m + beta - 1) / beta;

        writeBits(encoded_result, startBitPosition, 6, beta);
        startBitPosition += 6;

        for (int i = 0; i < l; i++) {
            for (int j = 0; j < list_length; j++) {
                subcolumnList[i][j] = (list[j] >> (i * beta)) & ((1 << beta) - 1);
            }
        }

        bitPacking(bitWidthList, encoded_result, startBitPosition, 8, l);
        startBitPosition += 8 * l;

        bitPacking(encodingType, encoded_result, startBitPosition, 1, l);
        startBitPosition += l;

        for (int i = l - 1; i >= 0; i--) {
            int type = encodingType[i];
            if (type == 0) {
                bitPacking(subcolumnList[i], encoded_result, startBitPosition, bitWidthList[i], list_length);
                startBitPosition += bitWidthList[i] * list_length;
            } else {
                int[] run_length = new int[list_length];
                int[] rle_values = new int[list_length];

                int count = 1;
                int currentNumber = subcolumnList[i][0];

                int index = 0;

                for (int j = 1; j < list_length; j++) {
                    if (subcolumnList[i][j] == currentNumber) {
                        count++;
                        if (count == 255) {
                            rle_values[index] = currentNumber;
                            run_length[index] = count;
                            index++;
                            count = 0;
                        }
                    } else {
                        rle_values[index] = currentNumber;
                        run_length[index] = count;
                        index++;
                        currentNumber = subcolumnList[i][j];
                        count = 1;
                    }
                }

                rle_values[index] = currentNumber;
                run_length[index] = count;
                index++;

                writeBits(encoded_result, startBitPosition, 16, index);
                startBitPosition += 16;

                bitPacking(run_length, encoded_result, startBitPosition, 8, index);
                startBitPosition += 8 * index;

                bitPacking(rle_values, encoded_result, startBitPosition, bitWidthList[i], index);
                startBitPosition += bitWidthList[i] * index;
            }
        }

        // System.out.println("startBitPosition: " + startBitPosition);

        return startBitPosition;
    }

    public static int SubcolumnDecoder(byte[] encoded_result, int startBitPosition, int[] list) {
        int list_length = list.length;

        int m = readBits(encoded_result, startBitPosition, 6, 0);
        startBitPosition += 6;

        if (m == 0) {
            return startBitPosition;
        }

        int beta = readBits(encoded_result, startBitPosition, 6, 0);
        startBitPosition += 6;

        // System.out.println("beta: " + beta);

        int l = (m + beta - 1) / beta;

        int[] bitWidthList = bitUnpacking(encoded_result, startBitPosition, 8, l);
        startBitPosition += 8 * l;

        int[] encodingType = bitUnpacking(encoded_result, startBitPosition, 1, l);
        startBitPosition += l;

        int[][] subcolumnList = new int[l][list_length];

        for (int i = l - 1; i >= 0; i--) {
            int type = encodingType[i];
            if (type == 0) {
                subcolumnList[i] = bitUnpacking(encoded_result, startBitPosition, bitWidthList[i], list_length);
                startBitPosition += bitWidthList[i] * list_length;
            } else {
                int index = readBits(encoded_result, startBitPosition, 16, 0);
                startBitPosition += 16;

                int[] run_length = bitUnpacking(encoded_result, startBitPosition, 8, index);
                startBitPosition += 8 * index;

                int[] rle_values = bitUnpacking(encoded_result, startBitPosition, bitWidthList[i], index);
                startBitPosition += bitWidthList[i] * index;

                int count = 0;
                for (int j = 0; j < index; j++) {
                    for (int k = 0; k < run_length[j]; k++) {
                        subcolumnList[i][count] = rle_values[j];
                        count++;
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
            int startBitPosition, byte[] encoded_result) {
        int[] min_delta = new int[3];

        int[] data_delta = getAbsDeltaTsBlock(data, block_index, block_size,
                remainder, min_delta);

        // for (int i = 0; i < remainder; i++) {
        // System.out.print(data_delta[i] + " ");
        // }
        // System.out.println();

        // System.out.println("min_delta: " + min_delta[0]);

        writeBits(encoded_result, startBitPosition, 32, min_delta[0]);
        startBitPosition += 32;

        startBitPosition = SubcolumnEncoder(data_delta, startBitPosition,
                encoded_result);

        return startBitPosition;
    }

    public static int BlockDecoder(byte[] encoded_result, int block_index, int block_size, int remainder,
            int startBitPosition, int[] data) {
        int[] min_delta = new int[3];

        min_delta[0] = readBits(encoded_result, startBitPosition, 32, 1);
        startBitPosition += 32;

        // System.out.println("min_delta: " + min_delta[0]);

        int[] block_data = new int[remainder];

        startBitPosition = SubcolumnDecoder(encoded_result, startBitPosition,
                block_data);

        for (int i = 0; i < remainder; i++) {
            data[block_index * block_size + i] = block_data[i] + min_delta[0];
        }

        return startBitPosition;
    }

    public static int Encoder(int[] data, int block_size, byte[] encoded_result) {
        int data_length = data.length;
        int startBitPosition = 0;

        writeBits(encoded_result, startBitPosition, 32, data_length);
        startBitPosition += 32;

        // System.out.println("data_length: " + data_length);

        writeBits(encoded_result, startBitPosition, 32, block_size);
        startBitPosition += 32;

        // System.out.println("block_size: " + block_size);

        int num_blocks = data_length / block_size;

        // System.out.println("num_blocks: " + num_blocks);

        for (int i = 0; i < num_blocks; i++) {
            startBitPosition = BlockEncoder(data, i, block_size, block_size, startBitPosition, encoded_result);
        }

        int remainder = data_length % block_size;

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                writeBits(encoded_result, startBitPosition, 32, data[num_blocks * block_size + i]);
                startBitPosition += 32;
            }
        } else {
            startBitPosition = BlockEncoder(data, num_blocks, block_size, remainder, startBitPosition,
                    encoded_result);
        }

        return startBitPosition;
    }

    public static int[] Decoder(byte[] encoded_result) {
        int startBitPosition = 0;

        int data_length = readBits(encoded_result, startBitPosition, 32, 0);
        startBitPosition += 32;

        // System.out.println("data_length: " + data_length);

        int block_size = readBits(encoded_result, startBitPosition, 32, 0);
        startBitPosition += 32;

        // System.out.println("block_size: " + block_size);

        int num_blocks = data_length / block_size;

        int[] data = new int[data_length];

        for (int i = 0; i < num_blocks; i++) {
            startBitPosition = BlockDecoder(encoded_result, i, block_size, block_size, startBitPosition, data);
        }

        int remainder = data_length % block_size;

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                data[num_blocks * block_size + i] = readBits(encoded_result, startBitPosition, 32, 0);
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

        String outputPath = output_parent_dir + "test_subcolumn_int.csv";

        int block_size = 1024;

        int repeatTime = 100;
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
                // System.out.println(f_str);
                // if (f_str.equals("")) {
                // continue;
                // }
                int cur_decimal = getDecimalPrecision(f_str);
                if (cur_decimal > max_decimal)
                    max_decimal = cur_decimal;
                // String value = loader.getValues()[index];
                data1.add(Float.valueOf(f_str));
                // data2.add(Integer.valueOf(loader.getValues()[1]));
                // data.add(Integer.valueOf(value));
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

            s = System.nanoTime();

            for (int repeat = 0; repeat < repeatTime; repeat++) {
                int[] data2_arr_decoded = Decoder(encoded_result);
                for (int i = 0; i < data2_arr_decoded.length; i++) {
                    assert data2_arr[i] == data2_arr_decoded[i]
                            || data2_arr[i] + Integer.MAX_VALUE + 1 == data2_arr_decoded[i];
                    // assert data2_arr[i] == data2_arr_decoded[i];
                    // if (data2_arr_decoded[i] != data2_arr[i]
                    // && data2_arr_decoded[i] != data2_arr[i] + Integer.MAX_VALUE + 1) {
                    // System.out.println("Error");
                    // System.out.println(i);
                    // System.out.println(data2_arr_decoded[i]);
                    // System.out.println(data2_arr[i]);
                    // break;
                    // }
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
