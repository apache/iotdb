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

public class SubcolumnAltTSDIFFTest {
    /**
     * 位宽
     * 
     * @param value
     * @return
     */
    public static int bitWidth(int value) {
        return value == 0 ? 1 : 32 - Integer.numberOfLeadingZeros(value);
    }

    /**
     * 将 int value 以 bitWidth 位宽，写入 byte array 的 startBitPosition 比特位置
     * 
     * @param array
     * @param startBitPosition
     * @param bitWidth
     * @param value
     */
    public static void writeBits(byte[] array, int startBitPosition, int bitWidth, int value) {
        int bytePosition = startBitPosition / 8;
        int bitOffset = startBitPosition % 8;
        int bitsLeft = bitWidth;
        int bitsWritten = 0;

        while (bitsLeft > 0) {
            int bitsToWrite = Math.min(8 - bitOffset, bitsLeft);
            int mask = (1 << bitsToWrite) - 1;
            int shift = 8 - bitOffset - bitsToWrite;
            int bits = (value >> (bitsLeft - bitsToWrite)) & mask;
            array[bytePosition] |= bits << shift;

            bitsLeft -= bitsToWrite;
            bitsWritten += bitsToWrite;
            bytePosition++;
            bitOffset = 0;
        }
    }

    /**
     * 从 byte array 的 startBitPosition 比特位置，读取 bitWidth 位宽的 int 值
     * 
     * @param array
     * @param startBitPosition
     * @param bitWidth
     * @param signed           1 表示有符号，0 表示无符号
     * @return
     */
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

    /**
     * 将 values 数组中的值，按 bitWidth 位宽，写入 byte array 的 startBitPosition 比特位置
     * 
     * @param values
     * @param array
     * @param startBitPosition
     * @param bitWidth
     */
    public static void bitPacking(int[] values, byte[] array, int startBitPosition, int bitWidth) {
        for (int i = 0; i < values.length; i++) {
            writeBits(array, startBitPosition + i * bitWidth, bitWidth, values[i]);
        }
    }

    /**
     * 从 byte array 的 startBitPosition 比特位置开始，读取 bitWidth 位宽的 int 值，存入数组
     * 
     * @param array
     * @param startBitPosition
     * @param bitWidth
     * @param numValues
     * @return
     */
    public static int[] bitUnpacking(byte[] array, int startBitPosition, int bitWidth, int numValues) {
        int[] values = new int[numValues];
        for (int i = 0; i < numValues; i++) {
            values[i] = readBits(array, startBitPosition + i * bitWidth, bitWidth, 0);
        }
        return values;
    }

    public static int[] subcolumnAlt(int[] x, int m) {
        int x_length = x.length;
        if (x_length == 0) {
            return new int[] { 0, 0, 0 };
        }

        int cMin = m * x_length;

        int lBest = 0;
        int betaBest = 0;

        int[] beta_list = new int[] { 7, 5, 3, 2 };
        // int[] beta_list = new int[] { 4, 3, 2, 1 };

        // costHB[l] 表示高 m - l 位的 RLE 的 cost
        int[] costHB = new int[m + 1];

        for (int l = 1; l <= m; l++) {
            int count = 0;
            int index = 0;
            int currentNumber = (x[0] >> l) & ((1 << (m - l)) - 1);

            for (int i = 0; i < x_length; i++) {
                int newNumber = (x[i] >> l) & ((1 << (m - l)) - 1);
                if (newNumber == currentNumber) {
                    count++;
                    if (count == 255) {
                        index++;
                        count = 0;
                    }
                } else {
                    index++;
                    currentNumber = newNumber;
                    count = 1;
                }
            }

            index++;

            costHB[l] = index * ((m - l) + 8);
        }

        // for (int i = 1; i <= m; i++) {
        // System.out.print(costHB[i] + " ");
        // }
        // System.out.println();

        // 表示第 i 位是否全为 0，最低位是第 1 位
        boolean[] isAllZero = new boolean[m + 1];
        for (int i = 1; i <= m; i++) {
            isAllZero[i] = true;
            for (int j = 0; j < x_length; j++) {
                if (((x[j] >> (i - 1)) & 1) == 1) {
                    isAllZero[i] = false;
                    break;
                }
            }
        }

        // for (int i = 1; i <= m; i++) {
        // System.out.print(isAllZero[i] + " ");
        // }
        // System.out.println();

        // costLB[l][beta] 表示低 l 位按 beta 划分进行比特打包的 cost
        int[][] costLB = new int[m + 1][10];

        for (int l = 1; l <= m; l++) {
            costLB[l][1] = l * x_length;

            // for (int beta = 2; beta <= 4; beta++) {
            for (int beta : beta_list) {
                int parts = (l + beta - 1) / beta;
                for (int p = 1; p <= parts; p++) {
                    int currentBit = p * beta;
                    if (currentBit > l) {
                        currentBit = l;
                    }
                    if (isAllZero[currentBit] && currentBit > beta * (p - 1) + 1) {
                        currentBit--;
                    }
                    costLB[l][beta] += x_length * (currentBit - beta * (p - 1));
                }
            }
        }

        // for (int i = 1; i <= m; i++) {
        // for (int j = 1; j <= 4; j++) {
        // System.out.print(costLB[i][j] + " ");
        // }
        // System.out.println();
        // }

        for (int l = 1; l <= m; l++) {
            // for (int beta = 4; beta >= 1; beta--) {
            for (int beta : beta_list) {
                if (costHB[l] + costLB[l][beta] < cMin) {
                    cMin = costHB[l] + costLB[l][beta];
                    lBest = l;
                    betaBest = beta;
                }
            }
        }

        if (cMin == m * x_length) {
            return new int[] { 0, 0, cMin };
        }

        return new int[] { lBest, betaBest, cMin };

    }

    public static int SubcolumnAltEncoder(int[] list, int startBitPosition, byte[] encoded_result) {
        int list_length = list.length;
        int maxValue = 0;
        for (int i = 0; i < list_length; i++) {
            if (list[i] > maxValue) {
                maxValue = list[i];
            }
        }

        int m = bitWidth(maxValue);

        int[] subcolumn_cost = subcolumnAlt(list, m);
        int l = subcolumn_cost[0];
        int beta = subcolumn_cost[1];

        writeBits(encoded_result, startBitPosition, 8, m);
        startBitPosition += 8;

        // System.out.println("m: " + m);

        writeBits(encoded_result, startBitPosition, 8, l);
        startBitPosition += 8;

        // System.out.println("l: " + l);

        writeBits(encoded_result, startBitPosition, 3, beta);
        startBitPosition += 3;

        // System.out.println("beta: " + beta);

        if (beta == 0) {
            bitPacking(list, encoded_result, startBitPosition, m);
            startBitPosition += m * list_length;

            // for (int i = 0; i < list_length; i++) {
            // System.out.print(list[i] + " ");
            // }
            // System.out.println();

            return startBitPosition;
        }

        int[] highBitsList = new int[list_length];
        for (int i = 0; i < list_length; i++) {
            highBitsList[i] = (list[i] >> l) & ((1 << (m - l)) - 1);
        }

        int[] lowBitsList = new int[list_length];
        for (int i = 0; i < list_length; i++) {
            lowBitsList[i] = list[i] & ((1 << l) - 1);
        }

        int parts = (l + beta - 1) / beta;

        int[] bitsWidthList = new int[parts];

        int[][] bpListList = new int[parts][list_length];

        for (int p = 0; p < parts; p++) {
            int maxValuePart = 0;
            for (int i = 0; i < list_length; i++) {
                bpListList[p][i] = (lowBitsList[i] >> (p * beta)) & ((1 << beta) - 1);
                if (bpListList[p][i] > maxValuePart) {
                    maxValuePart = bpListList[p][i];
                }
            }
            bitsWidthList[p] = bitWidth(maxValuePart);
        }

        bitPacking(bitsWidthList, encoded_result, startBitPosition, 3);
        startBitPosition += 3 * parts;

        // System.out.println("startBitPosition1: " + startBitPosition);

        int[] run_length = new int[list_length];
        int[] rle_values = new int[list_length];

        int count = 1;
        int currentNumber = highBitsList[0];
        int index = 0;

        for (int i = 1; i < list_length; i++) {
            if (highBitsList[i] == currentNumber) {
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
                currentNumber = highBitsList[i];
                count = 1;
            }
        }

        rle_values[index] = currentNumber;
        run_length[index] = count;
        index++;

        int[] final_rle_values = new int[index];
        int[] final_run_length = new int[index];
        System.arraycopy(rle_values, 0, final_rle_values, 0, index);
        System.arraycopy(run_length, 0, final_run_length, 0, index);

        writeBits(encoded_result, startBitPosition, 16, index);
        startBitPosition += 16;

        bitPacking(final_run_length, encoded_result, startBitPosition, 8);
        startBitPosition += 8 * index;

        writeBits(encoded_result, startBitPosition, 8, m - l);
        startBitPosition += 8;

        bitPacking(final_rle_values, encoded_result, startBitPosition, m - l);
        startBitPosition += (m - l) * index;

        for (int p = 0; p < parts; p++) {
            bitPacking(bpListList[p], encoded_result, startBitPosition, bitsWidthList[p]);
            startBitPosition += bitsWidthList[p] * list_length;
        }

        return startBitPosition;

    }

    public static int SubcolumnAltDecoder(byte[] encoded_result, int startBitPosition, int[] list) {
        int list_length = list.length;

        int m = readBits(encoded_result, startBitPosition, 8, 0);
        startBitPosition += 8;

        // System.out.println("m: " + m);

        int l = readBits(encoded_result, startBitPosition, 8, 0);
        startBitPosition += 8;

        // System.out.println("l: " + l);

        int beta = readBits(encoded_result, startBitPosition, 3, 0);
        startBitPosition += 3;

        // System.out.println("beta: " + beta);

        if (beta == 0) {
            int[] new_list = bitUnpacking(encoded_result, startBitPosition, m, list_length);
            startBitPosition += m * list_length;

            // for (int i = 0; i < list_length; i++) {
            // System.out.print(new_list[i] + " ");
            // }
            // System.out.println();

            System.arraycopy(new_list, 0, list, 0, list_length);
            // for (int i = 0; i < list_length; i++) {
            // list[i] = new_list[i];
            // }

            return startBitPosition;
        }

        int parts = (l + beta - 1) / beta;

        int[] bitsWidthList = bitUnpacking(encoded_result, startBitPosition, 3, parts);
        startBitPosition += 3 * parts;

        int index = readBits(encoded_result, startBitPosition, 16, 0);
        startBitPosition += 16;

        int[] run_length = bitUnpacking(encoded_result, startBitPosition, 8, index);
        startBitPosition += 8 * index;

        int maxBits = readBits(encoded_result, startBitPosition, 8, 0);
        startBitPosition += 8;

        int[] rle_values = bitUnpacking(encoded_result, startBitPosition, maxBits, index);
        startBitPosition += maxBits * index;

        int[] highBitsList = new int[list_length];

        int count = 0;
        for (int i = 0; i < index; i++) {
            for (int j = 0; j < run_length[i]; j++) {
                highBitsList[count] = rle_values[i];
                count++;
            }
        }

        int[][] bpListList = new int[parts][list_length];

        for (int p = 0; p < parts; p++) {
            bpListList[p] = bitUnpacking(encoded_result, startBitPosition, bitsWidthList[p], list_length);
            startBitPosition += bitsWidthList[p] * list_length;
        }

        int[] lowBitsList = new int[list_length];

        for (int i = 0; i < list_length; i++) {
            lowBitsList[i] = 0;
            for (int p = 0; p < parts; p++) {
                lowBitsList[i] |= bpListList[p][i] << (p * beta);
            }
        }

        for (int i = 0; i < list_length; i++) {
            list[i] = (highBitsList[i] << l) | lowBitsList[i];
        }

        return startBitPosition;
    }

    public static int BOSEncoder(int[] data, int block_size, byte[] encoded_result) {
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
            startBitPosition = BOSBlockEncoder(data, i, block_size, block_size, startBitPosition, encoded_result);
        }

        int remainder = data_length % block_size;

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                writeBits(encoded_result, startBitPosition, 32, data[num_blocks * block_size + i]);
                startBitPosition += 32;
            }
        } else {
            startBitPosition = BOSBlockEncoder(data, num_blocks, block_size, remainder, startBitPosition,
                    encoded_result);
        }

        return startBitPosition;
    }

    public static int[] BOSDecoder(byte[] encoded_result) {
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
            startBitPosition = BOSBlockDecoder(encoded_result, i, block_size, block_size, startBitPosition, data);
        }

        int remainder = data_length % block_size;

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                data[num_blocks * block_size + i] = readBits(encoded_result, startBitPosition, 32, 0);
                startBitPosition += 32;
            }
        } else {
            startBitPosition = BOSBlockDecoder(encoded_result, num_blocks, block_size, remainder,
                    startBitPosition, data);
        }

        return data;
    }

    public static int[] getAbsDeltaTsBlock(
            int[] ts_block,
            int i,
            int block_size,
            int remaining,
            int[] min_delta) {
        int[] ts_block_delta = new int[remaining - 1];

        int value_delta_min = Integer.MAX_VALUE;
        int value_delta_max = Integer.MIN_VALUE;
        int base = i * block_size + 1;
        int end = i * block_size + remaining;

        int tmp_j_1 = ts_block[base - 1];
        min_delta[0] = tmp_j_1;
        int j = base;
        int tmp_j;

        while (j < end) {
            tmp_j = ts_block[j];
            int epsilon_v = tmp_j - tmp_j_1;
            ts_block_delta[j - base] = epsilon_v;
            if (epsilon_v < value_delta_min) {
                value_delta_min = epsilon_v;
            }
            if (epsilon_v > value_delta_max) {
                value_delta_max = epsilon_v;
            }
            tmp_j_1 = tmp_j;
            j++;
        }
        j = 0;
        end = remaining - 1;
        while (j < end) {
            ts_block_delta[j] = ts_block_delta[j] - value_delta_min;
            j++;
        }

        min_delta[1] = value_delta_min;
        min_delta[2] = (value_delta_max - value_delta_min);

        return ts_block_delta;
    }

    public static int BOSBlockEncoder(int[] data, int block_index, int block_size, int remainder,
            int startBitPosition, byte[] encoded_result) {
        int[] min_delta = new int[3];

        int[] data_delta = getAbsDeltaTsBlock(data, block_index, block_size, remainder, min_delta);

        // System.out.println("data_delta_length: " + data_delta.length);

        // for (int i = 0; i < data_delta.length; i++) {
        // System.out.print(data_delta[i] + " ");
        // }
        // System.out.println();

        writeBits(encoded_result, startBitPosition, 32, min_delta[0]);
        startBitPosition += 32;

        // System.out.println("min_delta[0]: " + min_delta[0]);

        writeBits(encoded_result, startBitPosition, 32, min_delta[1]);
        startBitPosition += 32;

        // System.out.println("min_delta[1]: " + min_delta[1]);

        // int m = bitWidth(min_delta[2]);

        startBitPosition = SubcolumnAltEncoder(data_delta, startBitPosition, encoded_result);

        return startBitPosition;
    }

    public static int BOSBlockDecoder(byte[] encoded_result, int block_index, int block_size, int remainder,
            int startBitPosition, int[] data) {
        int[] min_delta = new int[3];

        min_delta[0] = readBits(encoded_result, startBitPosition, 32, 1);
        startBitPosition += 32;

        // System.out.println("min_delta[0]: " + min_delta[0]);

        min_delta[1] = readBits(encoded_result, startBitPosition, 32, 1);
        startBitPosition += 32;

        // System.out.println("min_delta[1]: " + min_delta[1]);

        int[] data_delta = new int[remainder - 1];

        startBitPosition = SubcolumnAltDecoder(encoded_result, startBitPosition, data_delta);

        // System.out.println("data_delta_length: " + data_delta.length);

        // for (int i = 0; i < data_delta.length; i++) {
        // System.out.print(data_delta[i] + " ");
        // }
        // System.out.println();

        for (int i = 0; i < remainder - 1; i++) {
            data_delta[i] = data_delta[i] + min_delta[1];
        }

        data[block_index * block_size] = min_delta[0];

        for (int i = 0; i < remainder - 1; i++) {
            data[block_index * block_size + i + 1] = data[block_index * block_size + i] + data_delta[i];
        }

        return startBitPosition;
    }

    @Test
    public void testWriteBits() {
        System.out.println("testWriteBits");
        byte[] array1 = new byte[10];
        writeBits(array1, 3, 8, 211);
        assert readBits(array1, 3, 8, 0) == 211;

        byte[] array2 = new byte[10];
        writeBits(array2, 3, 8, 211);
        System.out.println(readBits(array2, 3, 8, 0));

        writeBits(array2, 16, 8, 232);
        System.out.println(readBits(array2, 16, 8, 0));

        writeBits(array2, 24, 32, 8321);
        System.out.println(readBits(array2, 24, 32, 0));

        for (byte b : array2) {
            System.out.println(Integer.toBinaryString(b & 0xFF));
        }

        byte[] array3 = new byte[10];
        writeBits(array3, 4, 5, -6);
        System.out.println(readBits(array3, 4, 5, 1));
    }

    @Test
    public void testBitPacking() {
        byte[] array = new byte[4];
        int[] values1 = new int[5];
        values1[0] = 5;
        values1[1] = 3;
        values1[2] = 7;
        values1[3] = 1;
        values1[4] = 2;
        bitPacking(values1, array, 0, 3);

        for (byte b : array) {
            System.out.println(Integer.toBinaryString(b & 0xFF));
        }

        int[] values2 = bitUnpacking(array, 0, 3, 5);
        for (int i = 0; i < values2.length; i++) {
            System.out.println(values2[i]);
            assert values1[i] == values2[i];
        }
    }

    @Test
    public void testSubcolumn() {
        int[] list = new int[8];

        list[0] = 154; // 10011010
        list[1] = 176; // 10110000
        list[2] = 179; // 10110011
        list[3] = 161; // 10100001
        list[4] = 152; // 10011000
        list[5] = 184; // 10111000
        list[6] = 193; // 11000001
        list[7] = 203; // 11001011

        // int[] result = subcolumn(list, 8);
        byte[] encoded_result = new byte[8 * 4];
        int startBitPosition = 0;
        startBitPosition = SubcolumnAltEncoder(list, startBitPosition, encoded_result);

        int[] decoded_list = new int[8];
        startBitPosition = SubcolumnAltDecoder(encoded_result, 0, decoded_list);

        for (int i = 0; i < 8; i++) {
            System.out.println(decoded_list[i]);
            assert list[i] == decoded_list[i];
        }
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
        // 找到最后一个斜杠的位置，从而提取文件名
        int lastSlashIndex = path.lastIndexOf('/');

        // 提取文件名（从最后一个斜杠之后开始）
        String fileNameWithExtension = path.substring(lastSlashIndex + 1);

        // 去掉文件扩展名（.csv）
        int dotIndex = fileNameWithExtension.lastIndexOf('.');
        if (dotIndex != -1) {
            return fileNameWithExtension.substring(0, dotIndex);
        }

        // 如果没有扩展名，直接返回文件名
        return fileNameWithExtension;
    }

    @Test
    public void BOSTest() throws IOException {
        String parent_dir = "/Users/allen/Documents/github/xjz17/subcolumn/elf_resources/";
        // String parent_dir = "/Users/allen/Documents/compress-subcolumn/";
        String output_parent_dir = "/Users/allen/Documents/compress-subcolumn/";
        // String output_parent_dir =
        // "/Users/allen/Documents/github/xjz17/subcolumn/elf_resources";
        // String input_parent_dir = parent_dir + "ElfData_Short/";
        String input_parent_dir = parent_dir + "dataset/";
        // String input_parent_dir = parent_dir + "testdata/";
        ArrayList<String> input_path_list = new ArrayList<>();
        ArrayList<String> output_path_list = new ArrayList<>();
        ArrayList<String> dataset_name = new ArrayList<>();
        ArrayList<Integer> dataset_block_size = new ArrayList<>();
        // dataset_name.add("CS-Sensors");
        // dataset_name.add("Metro-Traffic");
        // dataset_name.add("USGS-Earthquakes");
        // dataset_name.add("YZ-Electricity");
        // dataset_name.add("GW-Magnetic");
        // dataset_name.add("TY-Fuel");
        // dataset_name.add("Cyber-Vehicle");
        // dataset_name.add("Vehicle-Charge");
        // dataset_name.add("Nifty-Stocks");
        // dataset_name.add("TH-Climate");
        // dataset_name.add("TY-Transport");
        // dataset_name.add("EPM-Education");
        input_path_list.add(input_parent_dir);
        dataset_block_size.add(1024);
        // output_path_list.add(output_parent_dir + "compress_ratio.csv");
        // output_path_list.add(output_parent_dir + "subcolumn.csv");
        output_path_list.add(output_parent_dir + "/subcolumn_alt_ts2diff.csv");
        // for (String value : dataset_name) {
        // input_path_list.add(input_parent_dir + value);
        // dataset_block_size.add(1024);
        // }

        // output_path_list.add(output_parent_dir + "/CS-Sensors_ratio.csv"); // 0
        //// dataset_block_size.add(1024);
        // output_path_list.add(output_parent_dir + "/Metro-Traffic_ratio.csv");// 1
        //// dataset_block_size.add(2048);
        // output_path_list.add(output_parent_dir + "/USGS-Earthquakes_ratio.csv");// 2
        //// dataset_block_size.add(2048);
        // output_path_list.add(output_parent_dir + "/YZ-Electricity_ratio.csv"); // 3
        //// dataset_block_size.add(2048);
        // output_path_list.add(output_parent_dir + "/GW-Magnetic_ratio.csv"); //4
        //// dataset_block_size.add(1024);
        // output_path_list.add(output_parent_dir + "/TY-Fuel_ratio.csv");//5
        //// dataset_block_size.add(2048);
        // output_path_list.add(output_parent_dir + "/Cyber-Vehicle_ratio.csv"); //6
        //// dataset_block_size.add(2048);
        // output_path_list.add(output_parent_dir + "/Vehicle-Charge_ratio.csv");//7
        //// dataset_block_size.add(2048);
        // output_path_list.add(output_parent_dir + "/Nifty-Stocks_ratio.csv");//8
        //// dataset_block_size.add(1024);
        // output_path_list.add(output_parent_dir + "/TH-Climate_ratio.csv");//9
        //// dataset_block_size.add(2048);
        // output_path_list.add(output_parent_dir + "/TY-Transport_ratio.csv");//10
        //// dataset_block_size.add(2048);
        // output_path_list.add(output_parent_dir + "/EPM-Education_ratio.csv");//11
        //// dataset_block_size.add(1024);

        int repeatTime2 = 100;
        // TODO 真正计算时，记得注释掉将下面的内容
        // repeatTime2 = 1;

        // for (int file_i = 1; file_i < 2; file_i++) {

        for (int file_i = 0; file_i < input_path_list.size(); file_i++) {

            String inputPath = input_path_list.get(file_i);
            System.out.println(inputPath);
            String Output = output_path_list.get(file_i);

            File file = new File(inputPath);
            File[] tempList = file.listFiles();

            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Dataset",
                    "Encoding Algorithm",
                    "Encoding Time",
                    "Decoding Time",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file

            assert tempList != null;

            for (File f : tempList) {
                // f=tempList[2];

                // System.out.println(f);
                String datasetName = extractFileName(f.toString());
                System.out.println(datasetName);
                InputStream inputStream = Files.newInputStream(f.toPath());

                CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                ArrayList<Float> data1 = new ArrayList<>();
                // ArrayList<Integer> data2 = new ArrayList<>();

                // loader.readHeaders();
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

                // System.out.println("initial data");
                // for (int i = 0; i < data2_arr.length; i++) {
                // System.out.print(data2_arr[i] + " ");
                // }
                // System.out.println();

                System.out.println(max_decimal);
                byte[] encoded_result = new byte[data2_arr.length * 4];

                long encodeTime = 0;
                long decodeTime = 0;
                double ratio = 0;
                double compressed_size = 0;

                int length = 0;

                long s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime2; repeat++) {
                    length = BOSEncoder(data2_arr, dataset_block_size.get(file_i),
                            encoded_result);
                }

                long e = System.nanoTime();
                encodeTime += ((e - s) / repeatTime2);
                compressed_size += length / 8;
                double ratioTmp = compressed_size / (double) (data1.size() * Long.BYTES);
                ratio += ratioTmp;
                s = System.nanoTime();
                // System.out.println("Decode");
                for (int repeat = 0; repeat < repeatTime2; repeat++) {
                    // SubcolumnDecoder(encoded_result);
                    int[] data2_arr_decoded = BOSDecoder(encoded_result);

                    for (int i = 0; i < data2_arr_decoded.length; i++) {
                        // System.out.print(data2_arr_decoded[i] + " ");
                        assert data2_arr[i] == data2_arr_decoded[i]
                                || data2_arr[i] + Integer.MAX_VALUE + 1 == data2_arr_decoded[i];
                        // assert data2_arr[i] == data2_arr_decoded[i];
                        // if (data2_arr_decoded[i] != data2_arr[i]) {
                        // System.out.println("Error");
                        // System.out.println(i);
                        // System.out.println(data2_arr_decoded[i]);
                        // System.out.println(data2_arr[i]);
                        // break;
                        // }
                    }
                    // System.out.println();

                    // for (int i = 0; i < data2_arr.length; i++) {
                    // System.out.print(data2_arr[i] + " ");
                    // }
                    // System.out.println();
                }
                e = System.nanoTime();
                decodeTime += ((e - s) / repeatTime2);

                String[] record = {
                        datasetName,
                        "TS2DIFF+Subcolumn*",
                        String.valueOf(encodeTime),
                        String.valueOf(decodeTime),
                        String.valueOf(data1.size()),
                        String.valueOf(compressed_size),
                        String.valueOf(ratio)
                };
                writer.writeRecord(record);
                System.out.println(ratio);

                // break;
            }
            writer.close();
        }
    }
}
