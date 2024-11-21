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

public class SubcolumnCostTest {
    public static int bitWidth(int value) {
        return value == 0 ? 1 : 32 - Integer.numberOfLeadingZeros(value);
    }

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

    public static void bitPacking(int[] values, byte[] array, int startBitPosition, int bitWidth) {
        for (int i = 0; i < values.length; i++) {
            writeBits(array, startBitPosition + i * bitWidth, bitWidth, values[i]);
        }
    }

    public static int[] bitUnpacking(byte[] array, int startBitPosition, int bitWidth, int numValues) {
        int[] values = new int[numValues];
        for (int i = 0; i < numValues; i++) {
            values[i] = readBits(array, startBitPosition + i * bitWidth, bitWidth, 0);
        }
        return values;
    }
    public static int[] subcolumn(int[] x) {
        int x_length = x.length;
        if (x_length == 0) {
            return new int[] { 0, 0, 0, 0 };
        }

        int xmax = Integer.MIN_VALUE;

        for (int i = 0; i < x_length; i++) {
            if (x[i] > xmax) {
                xmax = x[i];
            }
        }

        int m = bitWidth(xmax);

        int cMin = m * x_length;

        int lBest = 0;
        int betaBest = 0;

        for (int l = 1; l <= m; l++) {
            int highCost = 0;

            // if (l != m) {
            int[] highBitsList = new int[x_length];
            for (int i = 0; i < x_length; i++) {
                highBitsList[i] = (x[i] >> l) & ((1 << (m - l)) - 1);
            }

            // for (int i = 0; i < highBitsList.length; i++) {
            // System.out.print(highBitsList[i] + " ");
            // }
            // System.out.println();

            int[] rle_values = new int[x_length];
            int[] run_length = new int[x_length];
    
            int count = 1;
            int currentNumber = highBitsList[0];
            int index = 0;
    
            for (int i = 1; i < x_length; i++) {
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
    
            // for (int i = 0; i < index; i++) {
            //     System.out.print(rle_values[i] + " ");
            // }
            // System.out.println();
    
            // for (int i = 0; i < index; i++) {
            //     System.out.print(run_length[i] + " ");
            // }
            // System.out.println();
    
            int maxValue = Integer.MIN_VALUE;
            for (int i = 0; i < index; i++) {
                if (rle_values[i] > maxValue) {
                    maxValue = rle_values[i];
                }
            }
    
            int maxBits = bitWidth(maxValue);

            highCost = maxBits * index + 8 * index;
    
            // highCost += RLECost(highBitsList);
            // System.out.println("highCost: " + highCost);
            // }

            int[] lowBitsList = new int[x_length];
            for (int i = 0; i < x_length; i++) {
                lowBitsList[i] = x[i] & ((1 << l) - 1);
            }

            // for (int i = 0; i < lowBitsList.length; i++) {
            // System.out.print(lowBitsList[i] + " ");
            // }
            // System.out.println();

            for (int beta = 4; beta >= 1; beta--) {
                // System.out.println("beta: " + beta);

                int lowCost = 0;

                // int parts = (int) Math.ceil((double) l / beta);
                int parts = (l + beta - 1) / beta;

                for (int p = 0; p < parts; p++) {
                    int[] bpList = new int[x_length];
                    int maxValuePart = 0;
                    for (int i = 0; i < x_length; i++) {
                        bpList[i] = (lowBitsList[i] >> (p * beta)) & ((1 << beta) - 1);
                        if (bpList[i] > maxValuePart) {
                            maxValuePart = bpList[i];
                        }
                    }

                    // for (int i = 0; i < bpList.length; i++) {
                    // System.out.print(bpList[i] + " ");
                    // }
                    // System.out.println();

                    int maxBitsPart = bitWidth(maxValuePart);
                    // System.out.println("maxBitsPart: " + maxBitsPart);

                    lowCost += x_length * maxBitsPart;
                }

                // System.out.println("lowCost: " + lowCost);

                if (highCost + lowCost < cMin) {
                    cMin = highCost + lowCost;
                    lBest = l;
                    betaBest = beta;
                }
            }
        }

        if (cMin == m * x_length) {
            return new int[] { m, 0, 0, cMin };
        }

        return new int[] { m, lBest, betaBest, cMin };

    }

    public static int[] getAbsDeltaTsBlock(int[] data, int index, int block_size, int remainder, int[] min_delta) {
        int[] data_delta = new int[remainder - 1];

        int value_delta_min = Integer.MAX_VALUE;
        int value_delta_max = Integer.MIN_VALUE;
        int base = index * block_size + 1;
        int end = index * block_size + remainder;

        int tmp_j_1 = data[base - 1];
        min_delta[0] = tmp_j_1;
        int j = base;
        int tmp_j;

        while (j < end) {
            tmp_j = data[j];
            int epsilon_v = tmp_j - tmp_j_1;
            data_delta[j - base] = epsilon_v;
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
        end = remainder - 1;
        while (j < end) {
            data_delta[j] = data_delta[j] - value_delta_min;
            j++;
        }

        min_delta[1] = value_delta_min;
        min_delta[2] = (value_delta_max - value_delta_min);

        return data_delta;
    }

    public static int SubcolumnEncoder(int[] data, int block_size, byte[] encoded_result) {
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
            startBitPosition = SubcolumnBlockEncoder(data, i, block_size, block_size, startBitPosition, encoded_result);
        }

        int remainder = data_length % block_size;

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                writeBits(encoded_result, startBitPosition, 32, data[num_blocks * block_size + i]);
                startBitPosition += 32;
            }
        } else {
            startBitPosition = SubcolumnBlockEncoder(data, num_blocks, block_size, remainder, startBitPosition, encoded_result);
        }

        return startBitPosition;
    }

    public static void SubcolumnDecoder(byte[] encoded_result) {
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
            startBitPosition = SubcolumnBlockDecoder(encoded_result, i, block_size, block_size, startBitPosition, data);
        }

        int remainder = data_length % block_size;

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                data[num_blocks * block_size + i] = readBits(encoded_result, startBitPosition, 32, 0);
                startBitPosition += 32;
            }
        } else {
            startBitPosition = SubcolumnBlockDecoder(encoded_result, num_blocks, block_size, remainder, startBitPosition, data);
        }
    }

    public static int SubcolumnBlockEncoder(int[] data, int block_index, int block_size, int remainder, int startBitPosition, byte[] encoded_result) {
        int[] min_delta = new int[3];

        // data_delta 长度为 remainder - 1
        int[] data_delta = getAbsDeltaTsBlock(data, block_index, block_size, remainder, min_delta);

        for (int i = 0; i < 3; i++) {
            writeBits(encoded_result, startBitPosition, 32, min_delta[i]);
            startBitPosition += 32;
        }

        // for (int i = 0; i < 3; i++) {
        //     System.out.print(min_delta[i] + " ");
        // }
        // System.out.println();

        int[] subcolumn_result = subcolumn(data_delta);
        int m = subcolumn_result[0];
        int l = subcolumn_result[1];
        int beta = subcolumn_result[2];
        int cMin = subcolumn_result[3];

        // TODO 测试，下面的要注释
        // int m = bitWidth(data_delta[2]);
        // int l = m - 1;
        // int beta = 4;

        // System.out.println("m: " + m);
        // System.out.println("l: " + l);
        // System.out.println("beta: " + beta);
        // System.out.println("cMin: " + cMin);

        writeBits(encoded_result, startBitPosition, 8, m);
        startBitPosition += 8;

        writeBits(encoded_result, startBitPosition, 8, l);
        startBitPosition += 8;

        writeBits(encoded_result, startBitPosition, 8, beta);
        startBitPosition += 8;

        if (beta == 0) {
            bitPacking(data_delta, encoded_result, startBitPosition, m);
            startBitPosition += m * (remainder - 1);
            return startBitPosition;
        }

        int[] highBitsList = new int[remainder - 1];
        for (int i = 0; i < remainder - 1; i++) {
            highBitsList[i] = (data_delta[i] >> l) & ((1 << (m - l)) - 1);
        }

        int[] lowBitsList = new int[remainder - 1];
        for (int i = 0; i < remainder - 1; i++) {
            lowBitsList[i] = data_delta[i] & ((1 << l) - 1);
        }

        int parts = (l + beta - 1) / beta;

        int[] bitsWidthList = new int[parts];

        int[][] bpListList = new int[parts][remainder - 1];

        for (int p = 0; p < parts; p++) {
            int maxValuePart = 0;
            for (int i = 0; i < remainder - 1; i++) {
                bpListList[p][i] = (lowBitsList[i] >> (p * beta)) & ((1 << beta) - 1);
                if (bpListList[p][i] > maxValuePart) {
                    maxValuePart = bpListList[p][i];
                }
            }
            bitsWidthList[p] = bitWidth(maxValuePart);
        }

        // System.out.println("parts: " + parts);
        // for (int i = 0; i < parts; i++) {
        //     System.out.print(bitsWidthList[i] + " ");
        // }
        // System.out.println();

        bitPacking(bitsWidthList, encoded_result, startBitPosition, 3);
        startBitPosition += 3 * parts;

        int[] run_length = new int[remainder - 1];
        int[] rle_values = new int[remainder - 1];

        int count = 1;
        int currentNumber = highBitsList[0];
        int index = 0;

        for (int i = 1; i < remainder - 1; i++) {
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
        // writeBits(encoded_result, startBitPosition, 6, index);
        // startBitPosition += 6;

        // System.out.println("index: " + index);

        bitPacking(final_run_length, encoded_result, startBitPosition, 8);
        startBitPosition += 8 * index;

        // for (int i = 0; i < index; i++) {
        //     System.out.print(final_run_length[i] + " ");
        // }
        // System.out.println();

        int maxValue = Integer.MIN_VALUE;
        for (int i = 0; i < index; i++) {
            if (rle_values[i] > maxValue) {
                maxValue = rle_values[i];
            }
        }

        int maxBits = bitWidth(maxValue);

        writeBits(encoded_result, startBitPosition, 8, maxBits);
        startBitPosition += 8;

        // System.out.println("maxBits: " + maxBits);

        bitPacking(final_rle_values, encoded_result, startBitPosition, maxBits);
        startBitPosition += maxBits * index;

        for (int p = 0; p < parts; p++) {
            bitPacking(bpListList[p], encoded_result, startBitPosition, bitsWidthList[p]);
            startBitPosition += bitsWidthList[p] * (remainder - 1);
        }

        return startBitPosition;
    }

    public static int SubcolumnBlockDecoder(byte[] encoded_result, int block_index, int block_size, int remainder, int startBitPosition, int[] data) {
        int[] min_delta = new int[3];

        int[] data_delta = new int[remainder - 1];

        for (int i = 0; i < 3; i++) {
            min_delta[i] = readBits(encoded_result, startBitPosition, 32, 1);
            startBitPosition += 32;
        }

        // for (int i = 0; i < 3; i++) {
        //     System.out.print(min_delta[i] + " ");
        // }
        // System.out.println();

        int m = readBits(encoded_result, startBitPosition, 8, 0);
        startBitPosition += 8;

        // System.out.println("m: " + m);

        int l = readBits(encoded_result, startBitPosition, 8, 0);
        startBitPosition += 8;

        // System.out.println("l: " + l);

        int beta = readBits(encoded_result, startBitPosition, 8, 0);
        startBitPosition += 8;

        // System.out.println("beta: " + beta);

        if (beta == 0) {
            data_delta = bitUnpacking(encoded_result, startBitPosition, m, remainder - 1);
            startBitPosition += m * (remainder - 1);
            return startBitPosition;
        }

        int parts = (l + beta - 1) / beta;

        // System.out.println("parts: " + parts);

        int[] bitsWidthList = bitUnpacking(encoded_result, startBitPosition, 3, parts);
        startBitPosition += 3 * parts;

        // for (int i = 0; i < parts; i++) {
        //     System.out.print(bitsWidthList[i] + " ");
        // }
        // System.out.println();

        int index = readBits(encoded_result, startBitPosition, 16, 0);
        startBitPosition += 16;
        // int index = readBits(encoded_result, startBitPosition, 6, 0);
        // startBitPosition += 6;

        // System.out.println("index: " + index);

        int[] run_length = bitUnpacking(encoded_result, startBitPosition, 8, index);
        startBitPosition += 8 * index;

        // System.out.println("run_length_length: " + run_length.length);

        // for (int i = 0; i < index; i++) {
        //     System.out.print(run_length[i] + " ");
        // }
        // System.out.println();

        int maxBits = readBits(encoded_result, startBitPosition, 8, 0);
        startBitPosition += 8;

        // System.out.println("maxBits: " + maxBits);

        int[] rle_values = bitUnpacking(encoded_result, startBitPosition, maxBits, index);
        startBitPosition += maxBits * index;

        // for (int i = 0; i < index; i++) {
        //     System.out.print(rle_values[i] + " ");
        // }
        // System.out.println();

        int[] highBitsList = new int[remainder - 1];

        int count = 0;
        for (int i = 0; i < index; i++) {
            for (int j = 0; j < run_length[i]; j++) {
                highBitsList[count] = rle_values[i];
                count++;
            }
        }

        int[][] bpListList = new int[parts][remainder - 1];

        for (int p = 0; p < parts; p++) {
            bpListList[p] = bitUnpacking(encoded_result, startBitPosition, bitsWidthList[p], remainder - 1);
            startBitPosition += bitsWidthList[p] * (remainder - 1);
        }

        int[] lowBitsList = new int[remainder - 1];

        for (int i = 0; i < remainder - 1; i++) {
            lowBitsList[i] = 0;
            for (int p = 0; p < parts; p++) {
                lowBitsList[i] |= bpListList[p][i] << (p * beta);
            }
        }

        for (int i = 0; i < remainder - 1; i++) {
            data_delta[i] = (highBitsList[i] << l) | lowBitsList[i];
        }

        for (int i = 0; i < remainder - 1; i++) {
            data_delta[i] = data_delta[i] + min_delta[1];
        }

        for (int i = 1; i < remainder; i++) {
            data[block_index * block_size + i] = data[block_index * block_size + i - 1] + data_delta[i - 1];
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
    public void test0() {
        int[] list = new int[8];

        list[0] = 154;
        list[1] = 176;
        list[2] = 179;
        list[3] = 161;
        list[4] = 152;
        list[5] = 184;
        list[6] = 193;
        list[7] = 203;

        int[] result = subcolumn(list);

        byte[] test = new byte[100];
        int c = 255;
        test[1] = (byte) c;
        int c1 = test[1] & 0xFF;
        // System.out.println(test[1]);
        // System.out.println(c1);
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
    public void BOSOptimalTest() throws IOException {
        // String parent_dir = "/Users/xiaojinzhao/Documents/GitHub/"; // your data path
        String parent_dir = "/Users/allen/Documents/github/xjz17/subcolumn/elf_resources/";
        // String parent_dir = "/Users/allen/Documents/compress-subcolumn/";
        // String parent_dir =
        // "/Users/zihanguo/Downloads/R/outlier/outliier_code/encoding-outlier/";
        // String output_parent_dir = parent_dir;
        String output_parent_dir = "/Users/allen/Documents/compress-subcolumn";
        // String input_parent_dir = parent_dir +
        // "elf/src/test/resources/ElfData_Short";
        String input_parent_dir = parent_dir + "ElfData_Short/";
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
        // output_path_list.add(output_parent_dir + "/compress_ratio.csv"); // 0
        // output_path_list.add(output_parent_dir + "/subcolumn.csv"); // 0
        output_path_list.add(output_parent_dir + "/subcolumn.csv"); // 0
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
        // TODO 将下面这个去掉
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

                // for (int i = 0; i < data2_arr.length; i++) {
                // System.out.print(data2_arr[i] + " ");
                // }
                // System.out.println();

                System.out.println(max_decimal);
                byte[] encoded_result = new byte[data2_arr.length * 4];

                // for (int div = 2; div < 11; div++) {
                // System.out.println(div);
                long encodeTime = 0;
                long decodeTime = 0;
                double ratio = 0;
                double compressed_size = 0;

                int length = 0;

                long s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime2; repeat++) {
                    length = SubcolumnEncoder(data2_arr, dataset_block_size.get(file_i),
                            encoded_result);
                }

                long e = System.nanoTime();
                encodeTime += ((e - s) / repeatTime2);
                compressed_size += length / 8;
                double ratioTmp = compressed_size / (double) (data1.size() * Long.BYTES);
                ratio += ratioTmp;
                s = System.nanoTime();
                System.out.println("Decode");
                for (int repeat = 0; repeat < repeatTime2; repeat++) {
                    SubcolumnDecoder(encoded_result);
                }
                e = System.nanoTime();
                decodeTime += ((e - s) / repeatTime2);

                String[] record = {
                        datasetName,
                        "TS_2DIFF+Sucolumn",
                        String.valueOf(encodeTime),
                        String.valueOf(decodeTime),
                        // String.valueOf(div),
                        String.valueOf(data1.size()),
                        String.valueOf(compressed_size),
                        String.valueOf(ratio)
                };
                writer.writeRecord(record);
                System.out.println(ratio);
                // }

                // break;
            }
            writer.close();
        }
    }
}
