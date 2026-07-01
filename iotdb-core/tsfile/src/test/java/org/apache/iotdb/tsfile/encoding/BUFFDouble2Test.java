package org.apache.iotdb.tsfile.encoding;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.Test;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

public class BUFFDouble2Test {

    public static int bitWidth(int value) {
        return 32 - Integer.numberOfLeadingZeros(value);
    }

    public static int bitWidth(long value) {
        return 64 - Long.numberOfLeadingZeros(value);
    }

    // 逐位写入整数到字节数组
    public static void writeBits(int srcNum, byte[] result, int bitPos, int width) {
        for (int i = 0; i < width; i++) {
            int bit = (srcNum >> (width - 1 - i)) & 1;
            int byteIndex = bitPos / 8;
            int bitOffset = 7 - (bitPos % 8); // 从最高位开始
            
            if (bit == 1) {
                result[byteIndex] |= (1 << bitOffset);
            } else {
                result[byteIndex] &= ~(1 << bitOffset);
            }
            bitPos++;
        }
    }

    // 从字节数组逐位读取整数
    public static int readBits(byte[] result, int bitPos, int width) {
        int ret = 0;
        for (int i = 0; i < width; i++) {
            int byteIndex = bitPos / 8;
            int bitOffset = 7 - (bitPos % 8);
            int bit = (result[byteIndex] >> bitOffset) & 1;
            ret = (ret << 1) | bit;
            bitPos++;
        }
        return ret;
    }

    // 逐位写入长整数到字节数组
    public static void writeBits(long srcNum, byte[] result, int bitPos, int width) {
        for (int i = 0; i < width; i++) {
            long bit = (srcNum >> (width - 1 - i)) & 1L;
            int byteIndex = bitPos / 8;
            int bitOffset = 7 - (bitPos % 8); // 从最高位开始
            
            if (bit == 1) {
                result[byteIndex] |= (1 << bitOffset);
            } else {
                result[byteIndex] &= ~(1 << bitOffset);
            }
            bitPos++;
        }
    }

    // 从字节数组逐位读取长整数
    public static long readBitsLong(byte[] result, int bitPos, int width) {
        long ret = 0;
        for (int i = 0; i < width; i++) {
            int byteIndex = bitPos / 8;
            int bitOffset = 7 - (bitPos % 8);
            long bit = (result[byteIndex] >> bitOffset) & 1L;
            ret = (ret << 1) | bit;
            bitPos++;
        }
        return ret;
    }

    // 逐位打包整数数组
    public static int bitPacking(int[] numbers, int bit_width, int startBitPos,
            byte[] encoded_result, int num_values) {
        int currentBitPos = startBitPos;
        
        for (int i = 0; i < num_values; i++) {
            writeBits(numbers[i], encoded_result, currentBitPos, bit_width);
            currentBitPos += bit_width;
        }
        
        return (currentBitPos + 7) / 8; // 返回字节位置
    }

    // 逐位打包长整数数组
    public static int bitPacking(long[] numbers, int bit_width, int startBitPos,
            byte[] encoded_result, int num_values) {
        int currentBitPos = startBitPos;
        
        for (int i = 0; i < num_values; i++) {
            writeBits(numbers[i], encoded_result, currentBitPos, bit_width);
            currentBitPos += bit_width;
        }
        
        return (currentBitPos + 7) / 8; // 返回字节位置
    }

    // 逐位解包整数数组
    public static int decodeBitPacking(
            byte[] encoded, int startBitPos, int bit_width, int num_values, int[] result_list) {
        int currentBitPos = startBitPos;
        
        for (int i = 0; i < num_values; i++) {
            result_list[i] = readBits(encoded, currentBitPos, bit_width);
            currentBitPos += bit_width;
        }
        
        return (currentBitPos + 7) / 8; // 返回字节位置
    }

    // 逐位解包长整数数组
    public static int decodeBitPacking(
            byte[] encoded, int startBitPos, int bit_width, int num_values, long[] result_list) {
        int currentBitPos = startBitPos;
        
        for (int i = 0; i < num_values; i++) {
            result_list[i] = readBitsLong(encoded, currentBitPos, bit_width);
            currentBitPos += bit_width;
        }
        
        return (currentBitPos + 7) / 8; // 返回字节位置
    }

    // 辅助函数：将整数写入字节数组（字节对齐）
    public static void int2Bytes(int integer, int encode_pos, byte[] cur_byte) {
        cur_byte[encode_pos] = (byte) (integer >> 24);
        cur_byte[encode_pos + 1] = (byte) (integer >> 16);
        cur_byte[encode_pos + 2] = (byte) (integer >> 8);
        cur_byte[encode_pos + 3] = (byte) (integer);
    }

    // 辅助函数：将长整数写入字节数组（字节对齐）
    public static void long2Bytes(long integer, int encode_pos, byte[] cur_byte) {
        cur_byte[encode_pos] = (byte) (integer >> 56);
        cur_byte[encode_pos + 1] = (byte) (integer >> 48);
        cur_byte[encode_pos + 2] = (byte) (integer >> 40);
        cur_byte[encode_pos + 3] = (byte) (integer >> 32);
        cur_byte[encode_pos + 4] = (byte) (integer >> 24);
        cur_byte[encode_pos + 5] = (byte) (integer >> 16);
        cur_byte[encode_pos + 6] = (byte) (integer >> 8);
        cur_byte[encode_pos + 7] = (byte) (integer);
    }

    // 从字节数组读取整数
    public static int bytes2Integer(byte[] encoded, int start, int num) {
        int value = 0;
        for (int i = 0; i < num; i++) {
            value <<= 8;
            int b = encoded[i + start] & 0xFF;
            value |= b;
        }
        return value;
    }

    // 从字节数组读取长整数
    public static long bytes2Long(byte[] encoded, int start, int num) {
        long value = 0;
        for (int i = 0; i < num; i++) {
            value <<= 8;
            int b = encoded[i + start] & 0xFF;
            value |= b;
        }
        return value;
    }

    public static int[] bits_needed = { 0, 5, 8, 11, 15, 18, 21, 25, 28, 31, 35,
        38, 41, 45, 48, 51, 55, 58
    };

    public static int BlockEncoder(double[] data, int block_index, int block_size, int remainder, int max_decimal,
            int encode_pos, byte[] encoded_result) {

        long[] sign_bits = new long[remainder];
        long[] integer_parts = new long[remainder];
        long[] decimal_parts = new long[remainder];

        long min_integer_part = Long.MAX_VALUE;
        long max_integer_part = Long.MIN_VALUE;

        // 提取每个双精度值的符号、整数部分和小数部分
        for (int i = 0; i < remainder; i++) {
            double value = data[block_index * block_size + i];

            // 符号位
            if (value < 0) {
                sign_bits[i] = 1;
            }

            // 整数部分
            long currentInt = (long) Math.abs(value);
            integer_parts[i] = currentInt;

            // 更新最小和最大整数部分
            if (currentInt < min_integer_part) {
                min_integer_part = currentInt;
            }
            if (currentInt > max_integer_part) {
                max_integer_part = currentInt;
            }

            // 提取小数部分
            long bits = Double.doubleToLongBits(value);
            long exponent = (bits >> 52) & 0x7FF;
            long mantissa = bits & (long) ((1L << 52) - 1);
            long actualExponent = exponent - 1023;

            if (actualExponent >= 0) {
                long mask = (1L << (52 - actualExponent)) - 1;
                mantissa &= mask;
            } else {
                mantissa += 1L << 52;
            }

            long shift = 52 - actualExponent - bits_needed[max_decimal];
            if (shift < 0) {
                mantissa <<= -shift;
            } else {
                mantissa >>= shift;
            }

            if (exponent == 0) {
                mantissa = 0;
            }

            decimal_parts[i] = mantissa;
        }

        // 写入最小整数部分（8字节）
        long2Bytes(min_integer_part, encode_pos, encoded_result);
        encode_pos += 8;

        // 计算整数部分所需的位数并写入
        int bw = bitWidth(max_integer_part - min_integer_part);
        encoded_result[encode_pos] = (byte) bw;
        encode_pos += 1;

        // 对整数部分进行差分编码
        for (int i = 0; i < remainder; i++) {
            integer_parts[i] -= min_integer_part;
        }

        // 计算总位宽
        int totalBitWidth = 1 + bw + bits_needed[max_decimal];

        // 为每个值分配一个长整数数组来存储组合位
        long[] combinedValues = new long[remainder];
        
        // 将符号位、整数部分和小数部分组合成一个长整数
        for (int i = 0; i < remainder; i++) {
            long combined = (sign_bits[i] << (bw + bits_needed[max_decimal]))
                    | (integer_parts[i] << bits_needed[max_decimal]) | decimal_parts[i];
            combinedValues[i] = combined;
        }

        // 使用逐位打包方式编码组合值
        int bitPos = encode_pos * 8; // 转换为位位置
        encode_pos = bitPacking(combinedValues, totalBitWidth, bitPos, encoded_result, remainder);
        
        return encode_pos;
    }

    // 块解码器
    public static int BlockDecoder(byte[] encoded_result, int block_index, int block_size, int remainder,
            int max_decimal, int encode_pos, double[] data) {

        // 读取最小整数部分
        long min_integer_part = bytes2Long(encoded_result, encode_pos, 8);
        encode_pos += 8;

        // 读取整数部分位宽
        int bw = encoded_result[encode_pos] & 0xFF;
        encode_pos += 1;

        // 计算总位宽
        int totalBitWidth = 1 + bw + bits_needed[max_decimal];

        // 解码组合值
        long[] combinedValues = new long[remainder];
        int bitPos = encode_pos * 8; // 转换为位位置
        encode_pos = decodeBitPacking(encoded_result, bitPos, totalBitWidth, remainder, combinedValues);

        // 解码每个值
        for (int i = 0; i < remainder; i++) {
            long combined = combinedValues[i];
            
            // 提取符号位、整数部分和小数部分
            long sign_bit = (combined >> (bw + bits_needed[max_decimal])) & 1L;
            long integer_part = (combined >> bits_needed[max_decimal]) & ((1L << bw) - 1);
            long decimal_part = combined & ((1L << bits_needed[max_decimal]) - 1);
            
            // 恢复原始整数部分
            integer_part += min_integer_part;
            
            // 将小数部分转换为double
            double decimal = decimal_part;
            for (int j = 0; j < bits_needed[max_decimal]; j++) {
                decimal /= 2;
            }
            
            // 组合成最终的双精度值
            double value = integer_part + decimal;
            value = sign_bit == 1 ? -value : value;
            data[block_index * block_size + i] = value;
        }

        return encode_pos;
    }

    // 主编码器
    public static int Encoder(double[] data, int block_size, int max_decimal, byte[] encoded_result) {
        int data_length = data.length;
        int encode_pos = 0;

        // 写入数据长度（4字节）
        encoded_result[0] = (byte) (data_length >> 24);
        encoded_result[1] = (byte) (data_length >> 16);
        encoded_result[2] = (byte) (data_length >> 8);
        encoded_result[3] = (byte) data_length;
        encode_pos += 4;

        // 写入块大小（4字节）
        encoded_result[4] = (byte) (block_size >> 24);
        encoded_result[5] = (byte) (block_size >> 16);
        encoded_result[6] = (byte) (block_size >> 8);
        encoded_result[7] = (byte) block_size;
        encode_pos += 4;

        // 写入最大小数位数（1字节）
        encoded_result[8] = (byte) max_decimal;
        encode_pos += 1;

        // 计算块数
        int num_blocks = data_length / block_size;
        int remainder = data_length % block_size;

        // 编码完整块
        for (int i = 0; i < num_blocks; i++) {
            encode_pos = BlockEncoder(data, i, block_size, block_size, max_decimal, encode_pos, encoded_result);
        }

        // 编码剩余部分
        if (remainder > 0) {
            encode_pos = BlockEncoder(data, num_blocks, block_size, remainder, max_decimal, encode_pos, encoded_result);
        }

        return encode_pos;
    }

    // 主解码器
    public static double[] Decoder(byte[] encoded_result) {
        int encode_pos = 0;

        // 读取数据长度
        int data_length = ((encoded_result[encode_pos] & 0xFF) << 24) | 
                         ((encoded_result[encode_pos + 1] & 0xFF) << 16) |
                         ((encoded_result[encode_pos + 2] & 0xFF) << 8) | 
                         (encoded_result[encode_pos + 3] & 0xFF);
        encode_pos += 4;

        // 读取块大小
        int block_size = ((encoded_result[encode_pos] & 0xFF) << 24) | 
                        ((encoded_result[encode_pos + 1] & 0xFF) << 16) |
                        ((encoded_result[encode_pos + 2] & 0xFF) << 8) | 
                        (encoded_result[encode_pos + 3] & 0xFF);
        encode_pos += 4;

        // 读取最大小数位数
        int max_decimal = encoded_result[encode_pos] & 0xFF;
        encode_pos += 1;

        // 计算块数
        int num_blocks = data_length / block_size;
        int remainder = data_length % block_size;

        // 分配结果数组
        double[] data = new double[data_length];

        // 解码完整块
        for (int i = 0; i < num_blocks; i++) {
            encode_pos = BlockDecoder(encoded_result, i, block_size, block_size, max_decimal, encode_pos, data);
        }

        // 解码剩余部分
        if (remainder > 0) {
            encode_pos = BlockDecoder(encoded_result, num_blocks, block_size, remainder, max_decimal, encode_pos, data);
        }

        return data;
    }

    public static int getDecimalPrecision(String str) {
        int decimalIndex = str.indexOf(".");

        if (decimalIndex == -1) {
            return 0;
        }

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
    public void test0() throws IOException {
        // String parent_dir = "path/to/your/directory/";
        String parent_dir = "D:/encoding-subcolumn/";

        // String input_parent_dir = parent_dir + "dataset/";
        String input_parent_dir = parent_dir + "ElfTestData_camel/";

        String output_parent_dir = parent_dir + "result/";

        // String outputPath = output_parent_dir + "buff_long0.csv";
        String outputPath = output_parent_dir + "buff_long_repeat50.csv";

        int block_size = 1024;

        int repeatTime = 50;

        CsvWriter writer = new CsvWriter(outputPath, ',', StandardCharsets.UTF_8);
        writer.setRecordDelimiter('\n');

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

        File directory = new File(input_parent_dir);
        File[] csvFiles = directory.listFiles((dir, name) -> name.endsWith(".csv"));

        for (File file : csvFiles) {
            String datasetName = extractFileName(file.toString());
            System.out.println(datasetName);

            InputStream inputStream = Files.newInputStream(file.toPath());

            CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
            ArrayList<Double> data1 = new ArrayList<>();

            int max_decimal = 0;
            while (loader.readRecord()) {
                String f_str = loader.getValues()[0];
                if (f_str.isEmpty()) {
                    continue;
                }
                int cur_decimal = getDecimalPrecision(f_str);
                if (cur_decimal > max_decimal)
                    max_decimal = cur_decimal;
                data1.add(Double.valueOf(f_str));
            }
            inputStream.close();
            double[] data2_arr = new double[data1.size()];

            for (int i = 0; i < data1.size(); i++) {
                data2_arr[i] = data1.get(i);
            }

            System.out.println(max_decimal);

            if (max_decimal > 17) {
                max_decimal = 17;
            }

            byte[] encoded_result = new byte[data2_arr.length * 8];

            long encodeTime = 0;
            long decodeTime = 0;
            double ratio = 0;
            double compressed_size = 0;

            int length = 0;

            long s = System.nanoTime();
            for (int repeat = 0; repeat < repeatTime; repeat++) {
                length = Encoder(data2_arr, block_size, max_decimal, encoded_result);
            }

            long e = System.nanoTime();
            encodeTime += ((e - s) / repeatTime);
            compressed_size += length;

            double ratioTmp;

            ratioTmp = compressed_size / (double) (data1.size() * Long.BYTES);

            ratio += ratioTmp;

            System.out.println("Decode");

            double[] data2_arr_decoded = new double[data1.size()];

            s = System.nanoTime();

            for (int repeat = 0; repeat < repeatTime; repeat++) {
                data2_arr_decoded = Decoder(encoded_result);
            }

            e = System.nanoTime();
            decodeTime += ((e - s) / repeatTime);

            String[] record = {
                    datasetName,
                    "BUFF",
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
