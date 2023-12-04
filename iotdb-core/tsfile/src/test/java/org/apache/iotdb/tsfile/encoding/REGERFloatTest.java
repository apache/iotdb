package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;

import static java.lang.Math.abs;

public class REGERFloatTest {
    public static int getBitWith(int num) {
        if (num == 0) return 1;
        else return 32 - Integer.numberOfLeadingZeros(num);
    }

    public static int zigzag(int num){
        if(num < 0) return 2*(-num) - 1;
        else return 2*num;
    }

    public static boolean containsValue(int[] array, int targetValue) {
        for (int value : array) {
            if (value == targetValue) {
                return true;
            }
        }
        return false;
    }

    public static int[] removeElement(int[] array, int position) {
        if (position < 0 || position >= array.length) {
            return array;
        }

        int[] newArray = new int[array.length - 1];
        int newIndex = 0;

        for (int i = 0; i < array.length; i++) {
            if (i != position) {
                newArray[newIndex] = array[i];
                newIndex++;
            }
        }

        return newArray;
    }

    public static int min3(int a, int b, int c) {
        if (a < b && a < c) {
            return 0;
        } else if (b < c) {
            return 1;
        } else {
            return 2;
        }
    }

    public static void int2Bytes(int integer, int encode_pos, byte[] cur_byte) {
        cur_byte[encode_pos] = (byte) (integer >> 24);
        cur_byte[encode_pos + 1] = (byte) (integer >> 16);
        cur_byte[encode_pos + 2] = (byte) (integer >> 8);
        cur_byte[encode_pos + 3] = (byte) (integer);
    }

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


    public static void intByte2Bytes(int integer, int encode_pos, byte[] cur_byte) {
        cur_byte[encode_pos] = (byte) (integer);
    }

    public static void intWord2Bytes(int integer, int encode_pos, byte[] cur_byte) {
        cur_byte[encode_pos] = (byte) (integer >> 8);
        cur_byte[encode_pos + 1] = (byte) (integer);
    }

    public static int bytes2Integer(byte[] encoded, int start, int num) {
        int value = 0;
        if (num > 4) {
            System.out.println("bytes2Integer error");
            return 0;
        }
        for (int i = 0; i < num; i++) {
            value <<= 8;
            int b = encoded[i + start] & 0xFF;
            value |= b;
        }
        return value;
    }

    private static int byte2Integer(byte[] encoded, int decode_pos) {
        int value = 0;
        int b = encoded[decode_pos] & 0xFF;
        value |= b;
        if (value == 0)
            return 256;
        return value % 256;
    }


    public static void pack8Values(long[] values, int index, int offset, int width, int encode_pos, byte[] encoded_result) {
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

                buffer |= (getInt(values[valueIdx], index) << (32 - leftBit));
                leftSize -= leftBit;
                leftBit = 0;
                valueIdx++;
            }

            while (leftSize >= width && valueIdx < 8 + offset) {
                // encode one Integer to the 'buffer'
                buffer |= (getInt(values[valueIdx], index) << (leftSize - width));
                leftSize -= width;
                valueIdx++;
            }
            // If the remaining space of the buffer can not save the bits for one Integer,
            if (leftSize > 0 && valueIdx < 8 + offset) {
                // put the first 'leftSize' bits of the Integer into remaining space of the
                // buffer
                buffer |= (getInt(values[valueIdx], index) >>> (width - leftSize));
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
//        return encode_pos;
    }

    public static int unpack8Values(byte[] encoded, int offset, int width, int value_pos, int[] result_list) {
        int byteIdx = offset;
//        int pos_encode = 0;
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
                result_list[value_pos] = (int) (buffer >>> (totalBits - width));
                value_pos++;
                valueIdx++;
                totalBits -= width;
                buffer = buffer & ((1L << totalBits) - 1);
            }
        }
        return value_pos;
    }

    public static int bitPacking(long[] numbers, int index, int start, int block_size, int bit_width, int encode_pos, byte[] encoded_result) {
        int block_num = block_size / 8;
        for (int i = 0; i < block_num; i++) {
            pack8Values(numbers, index, start + i * 8, bit_width, encode_pos, encoded_result);
            encode_pos += bit_width;
        }

        return encode_pos;

    }

    public static int decodeBitPacking(
            byte[] encoded, int decode_pos, int bit_width, int block_size, int[] result_list) {
        int block_num = block_size / 8;
        int value_pos = 0;

        for (int i = 0; i < block_num; i++) {
            value_pos = unpack8Values(encoded, decode_pos, bit_width, value_pos, result_list);
            decode_pos += bit_width;
        }
        return decode_pos;
    }


    public static void float2bytes(float f, int pos_encode, byte[] encode_result) {
        int fbit = Float.floatToIntBits(f);
        byte[] b = new byte[4];
        for (int i = 0; i < 4; i++) {
            b[i] = (byte) (fbit >> (24 - i * 8));
        }
        int len = b.length;

        System.arraycopy(b, 0, encode_result, pos_encode, len);
        byte temp;
        for (int i = 0; i < len / 2; ++i) {
            temp = encode_result[i + pos_encode];
            encode_result[i + pos_encode] = encode_result[len - i - 1 + pos_encode];
            encode_result[len - i - 1 + pos_encode] = temp;
        }
    }

    public static float bytes2float(byte[] b, int index) {
        int l;
        l = b[index];
        l &= 0xff;
        l |= ((long) b[index + 1] << 8);
        l &= 0xffff;
        l |= ((long) b[index + 2] << 16);
        l &= 0xffffff;
        l |= ((long) b[index + 3] << 24);
        return Float.intBitsToFloat(l);
    }


    public static long combine2Int(int int1, int int2) {
        return ((long) int1 << 32) | (int2 & 0xFFFFFFFFL);
    }

    public static int getTime(long long1) {
        return ((int) (long1 >> 32));
    }

    public static int getValue(long long1) {
        return ((int) (long1));
    }

    public static int getInt(long long1, int index) {
        if (index == 0) return getTime(long1);
        else return getValue(long1);
    }

    public static int getCommon(int m, int n) {
        int z;
        while (m % n != 0) {
            z = m % n;
            m = n;
            n = z;
        }
        return n;
    }

    public static void splitTimeStamp3(
            ArrayList<Long> ts_block, ArrayList<Integer> result) {
        int td_common = 0;

        for (int i = 1; i < ts_block.size(); i++) {
            int cur_value = getTime(ts_block.get(i));
            int pre_value = getTime(ts_block.get(i - 1));
            int time_diffi = cur_value - pre_value;

            if (td_common == 0) {
                if (time_diffi != 0) {
                    td_common = time_diffi;
                }
                continue;
            }
            if (time_diffi != 0) {
                td_common = getCommon(time_diffi, td_common);
                if (td_common == 1) {
                    break;
                }
            }
        }
        if (td_common == 0) {
            td_common = 1;
        }

        int t0 = getTime(ts_block.get(0));
        for (int i = 0; i < ts_block.size(); i++) {
            int cur_value = getTime(ts_block.get(i));
            int interval_i = ((cur_value - t0) / td_common);
            ts_block.set(i, combine2Int(t0 + interval_i, getValue(ts_block.get(i))));
        }
        result.add(td_common);
    }

    private static void adjust1TimeCost(long[] ts_block, int i, int[] raw_length, ArrayList<Integer> min_index, float[] theta) {


        int block_size = ts_block.length;
        float theta0_t = theta[0];
        float theta1_t = theta[1];
        long tmp_i = ts_block[i];
        long tmp_i_1 = ts_block[i - 1];

        int min_delta_time = getTime(tmp_i) - (int) (theta0_t + theta1_t * (float) getTime(tmp_i_1));
        int min_delta_time_i = min_delta_time;
        int min_delta_time_index = i;
        int j = 1;
        long tmp_j_1 = ts_block[0];
        long tmp_j;
        while (j < block_size) {
            tmp_j = ts_block[j];
            int epsilon_r_j =
                    getTime(tmp_j)
                            - (int) (theta0_t + theta1_t * (float) getTime(tmp_j_1));

            if (epsilon_r_j < min_delta_time) {
                min_delta_time_index = j;
                min_delta_time = epsilon_r_j;
            }
            tmp_j_1 = tmp_j;
            j++;
        }

        raw_length[0] += (getBitWith(min_delta_time_i - min_delta_time) * (block_size - 1));
        raw_length[3] = min_delta_time;
        min_index.set(0, min_delta_time_index);


    }

    private static void adjust1ValueCost(long[] ts_block, int i, int[] raw_length, ArrayList<Integer> min_index, float[] theta) {

        int block_size = ts_block.length;
        float theta0_v = theta[2];
        float theta1_v = theta[3];


        int min_delta_value = getValue(ts_block[i]) - (int) (theta0_v + theta1_v * (float) getValue(ts_block[i - 1]));
        int min_delta_value_i = min_delta_value;
        int min_delta_value_index = i;
        int j = 1;
        long tmp_j_1 = ts_block[0];
        long tmp_j;
        while (j < block_size) {
            tmp_j = ts_block[j];
            int epsilon_r_j =
                    getValue(tmp_j)
                            - (int) (theta0_v + theta1_v * (float) getValue(tmp_j_1));

            if (epsilon_r_j < min_delta_value) {
                min_delta_value_index = j;
                min_delta_value = epsilon_r_j;
            }
            tmp_j_1 = tmp_j;
            j++;
        }


        raw_length[0] += (getBitWith(min_delta_value_i - min_delta_value) * (block_size - 1));
        raw_length[3] = min_delta_value;
        min_index.set(0, min_delta_value_index);


    }

    private static int[] adjust0MinChange(
            long[] ts_block, int j, float[] theta) {
        int block_size = ts_block.length;
        assert j != block_size;

        int[] b = new int[3];
        int timestamp_delta_min = Integer.MAX_VALUE;
        int value_delta_min = Integer.MAX_VALUE;
        int timestamp_delta_max = Integer.MIN_VALUE;
        int value_delta_max = Integer.MIN_VALUE;
        long[] ts_block_delta = new long[block_size - 1];

        float theta0_t = theta[0];
        float theta1_t = theta[1];
        float theta0_v = theta[2];
        float theta1_v = theta[3];


        int pos_ts_block_delta = 0;
        for (int i = 2; i < block_size; i++) {
            int timestamp_delta_i;
            int value_delta_i;
            if (i != j) {
                long tmp_i = ts_block[i];
                long tmp_i_1 = ts_block[i - 1];
                timestamp_delta_i =
                        getTime(tmp_i)
                                - (int) (theta0_t + theta1_t * (float) getTime(tmp_i_1));
                value_delta_i =
                        getValue(tmp_i)
                                - (int) (theta0_v + theta1_v * (float) getValue(tmp_i_1));

            } else {
                long tmp_j = ts_block[j];
                long tmp_0 = ts_block[0];
                long tmp_j_1 = ts_block[j - 1];
                timestamp_delta_i =
                        getTime(tmp_j)
                                - (int) (theta0_t + theta1_t * (float) getTime(tmp_0));
                value_delta_i =
                        getValue(tmp_j)
                                - (int) (theta0_v + theta1_v * (float) getValue(tmp_0));

                long delta_i = combine2Int(timestamp_delta_i, value_delta_i);
                ts_block_delta[pos_ts_block_delta] = delta_i;
                pos_ts_block_delta++;

                if (timestamp_delta_i > timestamp_delta_max) {
                    timestamp_delta_max = timestamp_delta_i;
                }
                if (timestamp_delta_i < timestamp_delta_min) {
                    timestamp_delta_min = timestamp_delta_i;
                }
                if (value_delta_i > value_delta_max) {
                    value_delta_max = value_delta_i;
                }
                if (value_delta_i < value_delta_min) {
                    value_delta_min = value_delta_i;
                }
                timestamp_delta_i = getTime(tmp_0) - (int) (theta0_t + theta1_t * (float) getTime(tmp_j_1));
                value_delta_i = getValue(tmp_0) - (int) (theta0_v + theta1_v * (float) getValue(tmp_j_1));
            }
            long delta_i = combine2Int(timestamp_delta_i, value_delta_i);
            ts_block_delta[pos_ts_block_delta] = delta_i;
            pos_ts_block_delta++;

            if (timestamp_delta_i > timestamp_delta_max) {
                timestamp_delta_max = timestamp_delta_i;
            }
            if (timestamp_delta_i < timestamp_delta_min) {
                timestamp_delta_min = timestamp_delta_i;
            }
            if (value_delta_i > value_delta_max) {
                value_delta_max = value_delta_i;
            }
            if (value_delta_i < value_delta_min) {
                value_delta_min = value_delta_i;
            }

        }
        int length = 0;
        for (long segment_max : ts_block_delta) {
            length += getBitWith(getTime(segment_max) - timestamp_delta_min);
            length += getBitWith(getValue(segment_max) - value_delta_min);
        }

        b[0] = length;
        b[1] = timestamp_delta_min;
        b[2] = value_delta_min;

        return b;
    }

    private static int[] adjust0MinChangeNo(
            long[] ts_block, int[] raw_length, int j, float[] theta) {
        int block_size = ts_block.length;
        assert j != block_size;

        int[] b = new int[3];
        int timestamp_delta_min = raw_length[3];
        int value_delta_min = raw_length[4];


        float theta0_t = theta[0];
        float theta1_t = theta[1];
        float theta0_v = theta[2];
        float theta1_v = theta[3];
        int length = raw_length[0];
        int timestamp_delta_i;
        int value_delta_i;
        long tmp_j_plus_1 = ts_block[j + 1];
        long tmp_j_minus_1 = ts_block[j - 1];
        long tmp_1 = ts_block[1];
        long tmp_0 = ts_block[0];

        timestamp_delta_i = getTime(tmp_j_plus_1) - (int) (theta0_t + theta1_t * (float) getTime(tmp_j_minus_1));
        value_delta_i = getValue(tmp_j_plus_1) - (int) (theta0_v + theta1_v * (float) getValue(tmp_j_minus_1));

        length -= getBitWith(timestamp_delta_i - timestamp_delta_min);
        length -= getBitWith(value_delta_i - value_delta_min);
        timestamp_delta_i = getTime(tmp_1) - (int) (theta0_t + theta1_t * (float) getTime(tmp_0));
        value_delta_i = getValue(tmp_1) - (int) (theta0_v + theta1_v * (float) getValue(tmp_0));

        length -= getBitWith(timestamp_delta_i - timestamp_delta_min);
        length -= getBitWith(value_delta_i - value_delta_min);

        timestamp_delta_i = getTime(tmp_0) - (int) (theta0_t + theta1_t * (float) getTime(tmp_j_minus_1));
        value_delta_i = getValue(tmp_0) - (int) (theta0_v + theta1_v * (float) getValue(tmp_j_minus_1));

        if (timestamp_delta_i < timestamp_delta_min) {
            length += getBitWith(timestamp_delta_min - timestamp_delta_i) * (block_size - 1);
            timestamp_delta_min = timestamp_delta_i;
        } else {
            length += getBitWith(timestamp_delta_i - timestamp_delta_min);
        }
        if (value_delta_i < value_delta_min) {
            length += getBitWith(value_delta_min - value_delta_i) * (block_size - 1);
            value_delta_min = value_delta_i;
        } else {
            length += getBitWith(value_delta_i - value_delta_min);
        }

        timestamp_delta_i = getTime(tmp_j_plus_1) - (int) (theta0_t + theta1_t * (float) getTime(tmp_0));
        value_delta_i = getValue(tmp_j_plus_1) - (int) (theta0_v + theta1_v * (float) getValue(tmp_0));

        if (timestamp_delta_i < timestamp_delta_min) {
            length += getBitWith(timestamp_delta_min - timestamp_delta_i) * (block_size - 1);
            timestamp_delta_min = timestamp_delta_i;
        } else {
            length += getBitWith(timestamp_delta_i - timestamp_delta_min);
        }

        if (value_delta_i < value_delta_min) {
            length += getBitWith(value_delta_min - value_delta_i) * (block_size - 1);
            value_delta_min = value_delta_i;
        } else {
            length += getBitWith(value_delta_i - value_delta_min);
        }

        b[0] = length;
        b[1] = timestamp_delta_min;
        b[2] = value_delta_min;

        return b;
    }


    // adjust 0 to n
    private static int[] adjust0n1MinChange(
            long[] ts_block, float[] theta) {
        int block_size = ts_block.length;
        int[] b = new int[3];
        int timestamp_delta_min = Integer.MAX_VALUE;
        int value_delta_min = Integer.MAX_VALUE;

        float theta0_t = theta[0];
        float theta1_t = theta[1];
        float theta0_v = theta[2];
        float theta1_v = theta[3];

        long[] ts_block_delta = new long[block_size - 1];
        int pos_ts_block_delta = 0;
        int length = 0;
        for (int i = 2; i < block_size; i++) {
            int timestamp_delta_i;
            int value_delta_i;
            long tmp_i = ts_block[i];
            long tmp_i_1 = ts_block[i - 1];

            timestamp_delta_i = getTime(tmp_i) - (int) (theta0_t + theta1_t * (float) getTime(tmp_i_1));
            value_delta_i = getValue(tmp_i) - (int) (theta0_v + theta1_v * (float) getValue(tmp_i_1));
            ts_block_delta[pos_ts_block_delta] = combine2Int(timestamp_delta_i, value_delta_i);
            pos_ts_block_delta++;


            if (timestamp_delta_i < timestamp_delta_min) {
                timestamp_delta_min = timestamp_delta_i;
            }
            if (value_delta_i < value_delta_min) {
                value_delta_min = value_delta_i;
            }
        }
        int timestamp_delta_i;
        int value_delta_i;
        long tmp_0 = ts_block[0];
        long tmp_block_size_1 = ts_block[block_size - 1];

        timestamp_delta_i = getTime(tmp_0) - (int) (theta0_t + theta1_t * (float) getTime(tmp_block_size_1));
        value_delta_i = getValue(tmp_0) - (int) (theta0_v + theta1_v * (float) getValue(tmp_block_size_1));
        ts_block_delta[pos_ts_block_delta] = combine2Int(timestamp_delta_i, value_delta_i);
        pos_ts_block_delta++;

        if (timestamp_delta_i < timestamp_delta_min) {
            timestamp_delta_min = timestamp_delta_i;
        }
        if (value_delta_i < value_delta_min) {
            value_delta_min = value_delta_i;
        }

        for (long segment_max : ts_block_delta) {
            length += getBitWith(getTime(segment_max) - timestamp_delta_min);
            length += getBitWith(getValue(segment_max) - value_delta_min);
        }
        b[0] = length;
        b[1] = timestamp_delta_min;
        b[2] = value_delta_min;

        return b;
    }

    private static int[] adjust0n1MinChangeNo(
            long[] ts_block, int[] raw_length, float[] theta) {
        int block_size = ts_block.length;
        int[] b = new int[3];
        int timestamp_delta_min = raw_length[3];
        int value_delta_min = raw_length[4];

        float theta0_t = theta[0];
        float theta1_t = theta[1];
        float theta0_v = theta[2];
        float theta1_v = theta[3];

        int length = raw_length[0];
        int timestamp_delta_i;
        int value_delta_i;
        long tmp_1 = ts_block[1];
        long tmp_0 = ts_block[1];
        long tmp_block_size_1 = ts_block[1];


        timestamp_delta_i = getTime(tmp_1) - (int) (theta0_t + theta1_t * (float) getTime(tmp_0));
        value_delta_i = getValue(tmp_1) - (int) (theta0_v + theta1_v * (float) getValue(tmp_0));

        length -= getBitWith(timestamp_delta_i - timestamp_delta_min);
        length -= getBitWith(value_delta_i - value_delta_min);
        timestamp_delta_i = getTime(tmp_0) - (int) (theta0_t + theta1_t * (float) getTime(tmp_block_size_1));
        value_delta_i = getValue(tmp_0) - (int) (theta0_v + theta1_v * (float) getValue(tmp_block_size_1));

        if (timestamp_delta_i < timestamp_delta_min) {
            length += getBitWith(timestamp_delta_min - timestamp_delta_i) * (block_size - 1);
            timestamp_delta_min = timestamp_delta_i;
        } else {
            length += getBitWith(timestamp_delta_i - timestamp_delta_min);
        }

        if (value_delta_i < value_delta_min) {
            length += getBitWith(value_delta_min - value_delta_i) * (block_size - 1);
            value_delta_min = value_delta_i;
        } else {
            length += getBitWith(value_delta_i - value_delta_min);
        }


        b[0] = length;
        b[1] = timestamp_delta_min;
        b[2] = value_delta_min;

        return b;
    }


    // adjust n to no 0
    private static int[] adjustnMinChange(
            long[] ts_block, int j, float[] theta) {
        int block_size = ts_block.length;
        assert j != 0;
        int[] b = new int[3];
        int timestamp_delta_min = Integer.MAX_VALUE;
        int value_delta_min = Integer.MAX_VALUE;
        long[] ts_block_delta = new long[block_size - 1];

        float theta0_t = theta[0];
        float theta1_t = theta[1];
        float theta0_v = theta[2];
        float theta1_v = theta[3];

        int length = 0;

        int pos_ts_block_delta = 0;
        for (int i = 1; i < block_size - 1; i++) {
            int timestamp_delta_i;
            int value_delta_i;
            if (i != j) {
                long tmp_i = ts_block[i];
                long tmp_i_1 = ts_block[i - 1];
                timestamp_delta_i = getTime(tmp_i) - (int) (theta0_t + theta1_t * (float) getTime(tmp_i_1));
                value_delta_i = getValue(tmp_i) - (int) (theta0_v + theta1_v * (float) getValue(tmp_i_1));
            } else {
                long tmp_j = ts_block[j];
                long tmp_block_size_1 = ts_block[block_size - 1];
                long tmp_j_1 = ts_block[j - 1];

                timestamp_delta_i = getTime(tmp_j) - (int) (theta0_t + theta1_t * (float) getTime(tmp_block_size_1));
                value_delta_i = getValue(tmp_j) - (int) (theta0_v + theta1_v * (float) getValue(tmp_block_size_1));
                ts_block_delta[pos_ts_block_delta] = combine2Int(timestamp_delta_i, value_delta_i);
                pos_ts_block_delta++;
                if (timestamp_delta_i < timestamp_delta_min) {
                    timestamp_delta_min = timestamp_delta_i;
                }
                if (value_delta_i < value_delta_min) {
                    value_delta_min = value_delta_i;
                }

                timestamp_delta_i = getTime(tmp_block_size_1) - (int) (theta0_t + theta1_t * (float) getTime(tmp_j_1));
                value_delta_i = getValue(tmp_block_size_1) - (int) (theta0_v + theta1_v * (float) getValue(tmp_j_1));

            }
            ts_block_delta[pos_ts_block_delta] = combine2Int(timestamp_delta_i, value_delta_i);
            pos_ts_block_delta++;

            if (timestamp_delta_i < timestamp_delta_min) {
                timestamp_delta_min = timestamp_delta_i;
            }
            if (value_delta_i < value_delta_min) {
                value_delta_min = value_delta_i;
            }

        }

        for (long segment_max : ts_block_delta) {
            length += getBitWith(getTime(segment_max) - timestamp_delta_min);
            length += getBitWith(getValue(segment_max) - value_delta_min);
        }
        b[0] = length;
        b[1] = timestamp_delta_min;
        b[2] = value_delta_min;

        return b;
    }

    private static int[] adjustnMinChangeNo(
            long[] ts_block, int[] raw_length, int j, float[] theta) {
        int block_size = ts_block.length;
        assert j != 0;
        int[] b = new int[3];
        int timestamp_delta_min = raw_length[3];
        int value_delta_min = raw_length[4];

        float theta0_t = theta[0];
        float theta1_t = theta[1];
        float theta0_v = theta[2];
        float theta1_v = theta[3];
        int length = raw_length[0];
        int timestamp_delta_i;
        int value_delta_i;

        long tmp_j = ts_block[j];
        long tmp_block_size_1 = ts_block[block_size - 1];
        long tmp_block_size_2 = ts_block[block_size - 2];
        long tmp_j_1 = ts_block[j - 1];


        timestamp_delta_i = getTime(tmp_j) - (int) (theta0_t + theta1_t * (float) getTime(tmp_j_1));
        value_delta_i = getValue(tmp_j) - (int) (theta0_v + theta1_v * (float) getValue(tmp_j_1));

        length -= getBitWith(timestamp_delta_i - timestamp_delta_min);
        length -= getBitWith(value_delta_i - value_delta_min);

        timestamp_delta_i = getTime(tmp_block_size_1) - (int) (theta0_t + theta1_t * (float) getTime(tmp_block_size_2));
        value_delta_i = getValue(tmp_block_size_1) - (int) (theta0_v + theta1_v * (float) getValue(tmp_block_size_2));

        length -= getBitWith(timestamp_delta_i - timestamp_delta_min);
        length -= getBitWith(value_delta_i - value_delta_min);

        timestamp_delta_i = getTime(tmp_j) - (int) (theta0_t + theta1_t * (float) getTime(tmp_block_size_1));
        value_delta_i = getValue(tmp_j) - (int) (theta0_v + theta1_v * (float) getValue(tmp_block_size_1));


        if (timestamp_delta_i < timestamp_delta_min) {
            length += getBitWith(timestamp_delta_min - timestamp_delta_i) * (block_size - 1);
            timestamp_delta_min = timestamp_delta_i;
        } else {
            length += getBitWith(timestamp_delta_i - timestamp_delta_min);
        }

        if (value_delta_i < value_delta_min) {
            length += getBitWith(value_delta_min - value_delta_i) * (block_size - 1);
            value_delta_min = value_delta_i;
        } else {
            length += getBitWith(value_delta_i - value_delta_min);
        }


        timestamp_delta_i = getTime(tmp_block_size_1) - (int) (theta0_t + theta1_t * (float) getTime(tmp_j_1));
        value_delta_i = getValue(tmp_block_size_1) - (int) (theta0_v + theta1_v * (float) getValue(tmp_j_1));


        if (timestamp_delta_i < timestamp_delta_min) {
            length += getBitWith(timestamp_delta_min - timestamp_delta_i) * (block_size - 1);
            timestamp_delta_min = timestamp_delta_i;
        } else {
            length += getBitWith(timestamp_delta_i - timestamp_delta_min);
        }


        if (value_delta_i < value_delta_min) {
            length += getBitWith(value_delta_min - value_delta_i) * (block_size - 1);
            value_delta_min = value_delta_i;
        } else {
            length += getBitWith(value_delta_i - value_delta_min);
        }


        b[0] = length;
        b[1] = timestamp_delta_min;
        b[2] = value_delta_min;

        return b;
    }

    // adjust n to 0
    private static int[] adjustn0MinChange(long[] ts_block, float[] theta) {
        int block_size = ts_block.length;
        int[] b = new int[3];
        int timestamp_delta_min = Integer.MAX_VALUE;
        int value_delta_min = Integer.MAX_VALUE;

        float theta0_t = theta[0];
        float theta1_t = theta[1];
        float theta0_v = theta[2];
        float theta1_v = theta[3];
        long[] ts_block_delta = new long[block_size - 1];

        int length = 0;

        int pos_ts_block_delta = 0;
        for (int i = 1; i < block_size - 1; i++) {
            int timestamp_delta_i;
            int value_delta_i;
            long tmp_i = ts_block[i];
            long tmp_i_1 = ts_block[i - 1];
            timestamp_delta_i = getTime(tmp_i) - (int) (theta0_t + theta1_t * (float) getTime(tmp_i_1));
            value_delta_i = getValue(tmp_i) - (int) (theta0_v + theta1_v * (float) getValue(tmp_i_1));
            ts_block_delta[pos_ts_block_delta] = combine2Int(timestamp_delta_i, value_delta_i);

            pos_ts_block_delta++;
            if (timestamp_delta_i < timestamp_delta_min) {
                timestamp_delta_min = timestamp_delta_i;
            }
            if (value_delta_i < value_delta_min) {
                value_delta_min = value_delta_i;
            }

        }
        long tmp_block_size_1 = ts_block[block_size - 1];
        long tmp_0 = ts_block[0];
        int timestamp_delta_i;
        int value_delta_i;
        timestamp_delta_i = getTime(tmp_0) - (int) (theta0_t + theta1_t * (float) getTime(tmp_block_size_1));
        value_delta_i = getValue(tmp_0) - (int) (theta0_v + theta1_v * (float) getValue(tmp_block_size_1));
        ts_block_delta[pos_ts_block_delta] = combine2Int(timestamp_delta_i, value_delta_i);

        pos_ts_block_delta++;

        if (timestamp_delta_i < timestamp_delta_min) {
            timestamp_delta_min = timestamp_delta_i;
        }
        if (value_delta_i < value_delta_min) {
            value_delta_min = value_delta_i;
        }
        for (long segment_max : ts_block_delta) {
            length += getBitWith(getTime(segment_max) - timestamp_delta_min);
            length += getBitWith(getValue(segment_max) - value_delta_min);
        }
        b[0] = length;
        b[1] = timestamp_delta_min;
        b[2] = value_delta_min;

        return b;
    }

    private static int[] adjustn0MinChangeNo(
            long[] ts_block, int[] raw_length, float[] theta) {
        int block_size = ts_block.length;
        int[] b = new int[3];
        int timestamp_delta_min = raw_length[3];
        int value_delta_min = raw_length[4];
        float theta0_t = theta[0];
        float theta1_t = theta[1];
        float theta0_v = theta[2];
        float theta1_v = theta[3];
        int length = raw_length[0];
        int timestamp_delta_i;
        int value_delta_i;
        long tmp_block_size_1 = ts_block[block_size - 1];
        long tmp_block_size_2 = ts_block[block_size - 2];
        long tmp_0 = ts_block[0];


        timestamp_delta_i = getTime(tmp_block_size_1) - (int) (theta0_t + theta1_t * (float) getTime(tmp_block_size_2));
        value_delta_i = getValue(tmp_block_size_1) - (int) (theta0_v + theta1_v * (float) getValue(tmp_block_size_2));

        length -= getBitWith(timestamp_delta_i - timestamp_delta_min);
        length -= getBitWith(value_delta_i - value_delta_min);

        timestamp_delta_i = getTime(tmp_0) - (int) (theta0_t + theta1_t * (float) getTime(tmp_block_size_1));
        value_delta_i = getValue(tmp_0) - (int) (theta0_v + theta1_v * (float) getValue(tmp_block_size_1));


        if (timestamp_delta_i < timestamp_delta_min) {
            length += getBitWith(timestamp_delta_min - timestamp_delta_i) * (block_size - 1);
            timestamp_delta_min = timestamp_delta_i;
        } else {
            length += getBitWith(timestamp_delta_i - timestamp_delta_min);
        }

        if (value_delta_i < value_delta_min) {
            length += getBitWith(value_delta_min - value_delta_i) * (block_size - 1);
            value_delta_min = value_delta_i;
        } else {
            length += getBitWith(value_delta_i - value_delta_min);
        }


        b[0] = length;
        b[1] = timestamp_delta_min;
        b[2] = value_delta_min;

        return b;
    }


    // adjust alpha to j

    private static int[] adjustAlphaToJMinChange(
            long[] ts_block, int alpha, int j, float[] theta,
            long tmp_alpha, long tmp_alpha_plus_1, long tmp_alpha_minus_1) {

        int block_size = ts_block.length;
        assert alpha != block_size - 1;
        assert alpha != 0;
        assert j != 0;
        assert j != block_size;
        int[] b = new int[3];
        int timestamp_delta_min = Integer.MAX_VALUE;
        int value_delta_min = Integer.MAX_VALUE;
        float theta0_t = theta[0];
        float theta1_t = theta[1];
        float theta0_v = theta[2];
        float theta1_v = theta[3];
        long[] ts_block_delta = new long[block_size - 1];

        long tmp_j = ts_block[j];
        long tmp_j_minus_1 = ts_block[j - 1];

        int length = 0;
        int pos_ts_block_delta = 0;
        for (int i = 1; i < block_size; i++) {
            int timestamp_delta_i;
            int value_delta_i;


            if (i == j) {
                timestamp_delta_i = getTime(tmp_j) - (int) (theta0_t + theta1_t * (float) getTime(tmp_alpha));
                value_delta_i = getValue(tmp_j) - (int) (theta0_v + theta1_v * (float) getValue(tmp_alpha));
            } else if (i == alpha) {
                timestamp_delta_i = getTime(tmp_alpha) - (int) (theta0_t + theta1_t * (float) getTime(tmp_j_minus_1));
                value_delta_i = getValue(tmp_alpha) - (int) (theta0_v + theta1_v * (float) getValue(tmp_j_minus_1));
            } else if (i == alpha + 1) {
                timestamp_delta_i = getTime(tmp_alpha_plus_1) - (int) (theta0_t + theta1_t * (float) getTime(tmp_alpha_minus_1));
                value_delta_i = getValue(tmp_alpha_plus_1) - (int) (theta0_v + theta1_v * (float) getValue(tmp_alpha_minus_1));
            } else {
                long tmp_i = ts_block[i];
                long tmp_i_1 = ts_block[i - 1];
                timestamp_delta_i = getTime(tmp_i) - (int) (theta0_t + theta1_t * (float) getTime(tmp_i_1));
                value_delta_i = getValue(tmp_i) - (int) (theta0_v + theta1_v * (float) getValue(tmp_i_1));
            }
            ts_block_delta[pos_ts_block_delta] = combine2Int(timestamp_delta_i, value_delta_i);
            pos_ts_block_delta++;
            if (timestamp_delta_i < timestamp_delta_min) {
                timestamp_delta_min = timestamp_delta_i;
            }
            if (value_delta_i < value_delta_min) {
                value_delta_min = value_delta_i;
            }

        }

        for (long segment_max : ts_block_delta) {
            length += getBitWith(getTime(segment_max) - timestamp_delta_min);
            length += getBitWith(getValue(segment_max) - value_delta_min);
        }
        b[0] = length;
        b[1] = timestamp_delta_min;
        b[2] = value_delta_min;


        return b;
    }

    private static int[] adjustAlphaToJMinChangeNo(
            long[] ts_block, int[] raw_length, int alpha, int j, float[] theta,
            long tmp_alpha, long tmp_alpha_plus_1, long tmp_alpha_minus_1) {

        int block_size = ts_block.length;
        assert alpha != block_size - 1;
        assert alpha != 0;
        assert j != 0;
        assert j != block_size;
        int[] b = new int[3];
        int timestamp_delta_min = raw_length[3];
        int value_delta_min = raw_length[4];

        float theta0_t = theta[0];
        float theta1_t = theta[1];
        float theta0_v = theta[2];
        float theta1_v = theta[3];
        int length = raw_length[0];
        int timestamp_delta_i;
        int value_delta_i;
        long tmp_j = ts_block[j];
        long tmp_j_minus_1 = ts_block[j - 1];

        timestamp_delta_i = getTime(tmp_alpha_plus_1) - (int) (theta0_t + theta1_t * (float) getTime(tmp_alpha));
        value_delta_i = getValue(tmp_alpha_plus_1) - (int) (theta0_v + theta1_v * (float) getValue(tmp_alpha));
        length -= getBitWith(timestamp_delta_i - timestamp_delta_min);
        length -= getBitWith(value_delta_i - value_delta_min);

        timestamp_delta_i = getTime(tmp_alpha) - (int) (theta0_t + theta1_t * (float) getTime(tmp_alpha_minus_1));
        value_delta_i = getValue(tmp_alpha) - (int) (theta0_v + theta1_v * (float) getValue(tmp_alpha_minus_1));
        length -= getBitWith(timestamp_delta_i - timestamp_delta_min);
        length -= getBitWith(value_delta_i - value_delta_min);

        timestamp_delta_i = getTime(tmp_j) - (int) (theta0_t + theta1_t * (float) getTime(tmp_j_minus_1));
        value_delta_i = getValue(tmp_j) - (int) (theta0_v + theta1_v * (float) getValue(tmp_j_minus_1));


        length -= getBitWith(timestamp_delta_i - timestamp_delta_min);
        length -= getBitWith(value_delta_i - value_delta_min);

        timestamp_delta_i = getTime(tmp_j) - (int) (theta0_t + theta1_t * (float) getTime(tmp_alpha));
        value_delta_i = getValue(tmp_j) - (int) (theta0_v + theta1_v * (float) getValue(tmp_alpha));

        if (timestamp_delta_i < timestamp_delta_min) {
            length += getBitWith(timestamp_delta_min - timestamp_delta_i) * (block_size - 1);
            timestamp_delta_min = timestamp_delta_i;
        } else {
            length += getBitWith(timestamp_delta_i - timestamp_delta_min);
        }
        if (value_delta_i < value_delta_min) {
            length += getBitWith(value_delta_min - value_delta_i) * (block_size - 1);
            value_delta_min = value_delta_i;
        } else {
            length += getBitWith(value_delta_i - value_delta_min);
        }

        timestamp_delta_i = getTime(tmp_alpha) - (int) (theta0_t + theta1_t * (float) getTime(tmp_j_minus_1));
        value_delta_i = getValue(tmp_alpha) - (int) (theta0_v + theta1_v * (float) getValue(tmp_j_minus_1));

        if (timestamp_delta_i < timestamp_delta_min) {
            length += getBitWith(timestamp_delta_min - timestamp_delta_i) * (block_size - 1);
            timestamp_delta_min = timestamp_delta_i;
        } else {
            length += getBitWith(timestamp_delta_i - timestamp_delta_min);
        }
        if (value_delta_i < value_delta_min) {
            length += getBitWith(value_delta_min - value_delta_i) * (block_size - 1);
            value_delta_min = value_delta_i;
        } else {
            length += getBitWith(value_delta_i - value_delta_min);
        }

        timestamp_delta_i = getTime(tmp_alpha_plus_1) - (int) (theta0_t + theta1_t * (float) getTime(tmp_alpha_minus_1));
        value_delta_i = getValue(tmp_alpha_plus_1) - (int) (theta0_v + theta1_v * (float) getValue(tmp_alpha_minus_1));


        if (timestamp_delta_i < timestamp_delta_min) {
            length += getBitWith(timestamp_delta_min - timestamp_delta_i) * (block_size - 1);
            timestamp_delta_min = timestamp_delta_i;
        } else {
            length += getBitWith(timestamp_delta_i - timestamp_delta_min);
        }
        if (value_delta_i < value_delta_min) {
            length += getBitWith(value_delta_min - value_delta_i) * (block_size - 1);
            value_delta_min = value_delta_i;
        } else {
            length += getBitWith(value_delta_i - value_delta_min);
        }


        b[0] = length;
        b[1] = timestamp_delta_min;
        b[2] = value_delta_min;

        return b;
    }


    // move alpha to 0
    private static int[] adjustTo0MinChange(long[] ts_block, int alpha, float[] theta,
                                            long tmp_alpha, long tmp_alpha_plus_1, long tmp_alpha_minus_1) {
        int block_size = ts_block.length;
        assert alpha != block_size - 1;
        assert alpha != 0;
        int[] b = new int[3];
        int timestamp_delta_min = Integer.MAX_VALUE;
        int value_delta_min = Integer.MAX_VALUE;

        float theta0_t = theta[0];
        float theta1_t = theta[1];
        float theta0_v = theta[2];
        float theta1_v = theta[3];

        long[] ts_block_delta = new long[block_size - 1];

        int pos_ts_block_delta = 0;
        for (int i = 1; i < block_size; i++) {
            int timestamp_delta_i;
            int value_delta_i;
            if (i == (alpha + 1)) {
                timestamp_delta_i = getTime(tmp_alpha_plus_1) - (int) (theta0_t + theta1_t * (float) getTime(tmp_alpha_minus_1));
                value_delta_i = getValue(tmp_alpha_plus_1) - (int) (theta0_v + theta1_v * (float) getValue(tmp_alpha_minus_1));
            } else if (i == alpha) {
                long tmp_0 = ts_block[0];
                timestamp_delta_i = getTime(tmp_0) - (int) (theta0_t + theta1_t * (float) getTime(tmp_alpha));
                value_delta_i = getValue(tmp_0) - (int) (theta0_v + theta1_v * (float) getValue(tmp_alpha));
            } else {
                long tmp_i = ts_block[i];
                long tmp_i_1 = ts_block[i - 1];
                timestamp_delta_i = getTime(tmp_i) - (int) (theta0_t + theta1_t * (float) getTime(tmp_i_1));
                value_delta_i = getValue(tmp_i) - (int) (theta0_v + theta1_v * (float) getValue(tmp_i_1));
            }

            ts_block_delta[pos_ts_block_delta] = combine2Int(timestamp_delta_i, value_delta_i);
            pos_ts_block_delta++;

            if (timestamp_delta_i < timestamp_delta_min) {
                timestamp_delta_min = timestamp_delta_i;
            }

            if (value_delta_i < value_delta_min) {
                value_delta_min = value_delta_i;
            }

        }
        int length = 0;
        for (long segment_max : ts_block_delta) {
            length += getBitWith(getTime(segment_max) - timestamp_delta_min);
            length += getBitWith(getValue(segment_max) - value_delta_min);
        }

        b[0] = length;
        b[1] = timestamp_delta_min;
        b[2] = value_delta_min;

        return b;
    }

    private static int[] adjustTo0MinChangeNo(
            long[] ts_block, int[] raw_length, int alpha, float[] theta,
            long tmp_alpha, long tmp_alpha_plus_1, long tmp_alpha_minus_1) {
        int block_size = ts_block.length;
        assert alpha != block_size - 1;
        assert alpha != 0;
        int[] b = new int[3];
        int timestamp_delta_min = raw_length[3];
        int value_delta_min = raw_length[4];
        float theta0_t = theta[0];
        float theta1_t = theta[1];
        float theta0_v = theta[2];
        float theta1_v = theta[3];
        int length = raw_length[0];
        int timestamp_delta_i;
        int value_delta_i;


        timestamp_delta_i = getTime(tmp_alpha_plus_1) - (int) (theta0_t + theta1_t * (float) getTime(tmp_alpha));
        value_delta_i = getValue(tmp_alpha_plus_1) - (int) (theta0_v + theta1_v * (float) getValue(tmp_alpha));
        length -= getBitWith(timestamp_delta_i - timestamp_delta_min);
        length -= getBitWith(value_delta_i - value_delta_min);


        timestamp_delta_i = getTime(tmp_alpha) - (int) (theta0_t + theta1_t * (float) getTime(tmp_alpha_minus_1));
        value_delta_i = getValue(tmp_alpha) - (int) (theta0_v + theta1_v * (float) getValue(tmp_alpha_minus_1));
        length -= getBitWith(timestamp_delta_i - timestamp_delta_min);
        length -= getBitWith(value_delta_i - value_delta_min);

        timestamp_delta_i = getTime(tmp_alpha_plus_1) - (int) (theta0_t + theta1_t * (float) getTime(tmp_alpha_minus_1));
        value_delta_i = getValue(tmp_alpha_plus_1) - (int) (theta0_v + theta1_v * (float) getValue(tmp_alpha_minus_1));
        if (timestamp_delta_i < timestamp_delta_min) {
            length += getBitWith(timestamp_delta_min - timestamp_delta_i) * (block_size - 1);
            timestamp_delta_min = timestamp_delta_i;
        } else {
            length += getBitWith(timestamp_delta_i - timestamp_delta_min);
        }
        if (value_delta_i < value_delta_min) {
            length += getBitWith(value_delta_min - value_delta_i) * (block_size - 1);
            value_delta_min = value_delta_i;
        } else {
            length += getBitWith(value_delta_i - value_delta_min);
        }


        long tmp_0 = ts_block[0];

        timestamp_delta_i = getTime(tmp_0) - (int) (theta0_t + theta1_t * (float) getTime(tmp_alpha));
        value_delta_i = getValue(tmp_0) - (int) (theta0_v + theta1_v * (float) getValue(tmp_alpha));
        if (timestamp_delta_i < timestamp_delta_min) {
            length += getBitWith(timestamp_delta_min - timestamp_delta_i) * (block_size - 1);
            timestamp_delta_min = timestamp_delta_i;
        } else {
            length += getBitWith(timestamp_delta_i - timestamp_delta_min);
        }
        if (value_delta_i < value_delta_min) {
            length += getBitWith(value_delta_min - value_delta_i) * (block_size - 1);
            value_delta_min = value_delta_i;
        } else {
            length += getBitWith(value_delta_i - value_delta_min);
        }

        b[0] = length;
        b[1] = timestamp_delta_min;
        b[2] = value_delta_min;

        return b;
    }


    // move alpha to n
    private static int[] adjustTonMinChange(
            long[] ts_block, int alpha, float[] theta,
            long tmp_alpha, long tmp_alpha_plus_1, long tmp_alpha_minus_1) {
        int block_size = ts_block.length;
        assert alpha != block_size - 1;
        assert alpha != 0;
        int[] b = new int[3];
        int timestamp_delta_min = Integer.MAX_VALUE;
        int value_delta_min = Integer.MAX_VALUE;

        float theta0_t = theta[0];
        float theta1_t = theta[1];
        float theta0_v = theta[2];
        float theta1_v = theta[3];
        long[] ts_block_delta = new long[block_size - 1];

        int pos_ts_block_delta = 0;
        int length = 0;

        for (int i = 1; i < block_size; i++) {
            int timestamp_delta_i;
            int value_delta_i;
            if (i == (alpha + 1)) {
                timestamp_delta_i = getTime(tmp_alpha_plus_1) - (int) (theta0_t + theta1_t * (float) getTime(tmp_alpha_minus_1));
                value_delta_i = getValue(tmp_alpha_plus_1) - (int) (theta0_v + theta1_v * (float) getValue(tmp_alpha_minus_1));
            } else if (i == alpha) {
                long tmp_block_size_1 = ts_block[block_size - 1];
                timestamp_delta_i = getTime(tmp_alpha) - (int) (theta0_t + theta1_t * (float) getTime(tmp_block_size_1));
                value_delta_i = getValue(tmp_alpha) - (int) (theta0_v + theta1_v * (float) getValue(tmp_block_size_1));
            } else {
                long tmp_i = ts_block[i];
                long tmp_i_1 = ts_block[i - 1];
                timestamp_delta_i = getTime(tmp_i) - (int) (theta0_t + theta1_t * (float) getTime(tmp_i_1));
                value_delta_i = getValue(tmp_i) - (int) (theta0_v + theta1_v * (float) getValue(tmp_i_1));
            }
            ts_block_delta[pos_ts_block_delta] = combine2Int(timestamp_delta_i, value_delta_i);
            pos_ts_block_delta++;
            if (timestamp_delta_i < timestamp_delta_min) {
                timestamp_delta_min = timestamp_delta_i;
            }
            if (value_delta_i < value_delta_min) {
                value_delta_min = value_delta_i;
            }
        }

        for (long segment_max : ts_block_delta) {
            length += getBitWith(getTime(segment_max) - timestamp_delta_min);
            length += getBitWith(getValue(segment_max) - value_delta_min);
        }
        b[0] = length;
        b[1] = timestamp_delta_min;
        b[2] = value_delta_min;


        return b;
    }

    private static int[] adjustTonMinChangeNo(
            long[] ts_block, int[] raw_length, int alpha, float[] theta,
            long tmp_alpha, long tmp_alpha_plus_1, long tmp_alpha_minus_1) {
        int block_size = ts_block.length;
        assert alpha != block_size - 1;
        assert alpha != 0;
        int[] b = new int[3];
        int timestamp_delta_min = raw_length[3];
        int value_delta_min = raw_length[4];
        float theta0_t = theta[0];
        float theta1_t = theta[1];
        float theta0_v = theta[2];
        float theta1_v = theta[3];
        int length = raw_length[0];

        int timestamp_delta_i;
        int value_delta_i;

        timestamp_delta_i = getTime(tmp_alpha_plus_1) - (int) (theta0_t + theta1_t * (float) getTime(tmp_alpha));
        value_delta_i = getValue(tmp_alpha_plus_1) - (int) (theta0_v + theta1_v * (float) getValue(tmp_alpha));
        length -= getBitWith(timestamp_delta_i - timestamp_delta_min);
        length -= getBitWith(value_delta_i - value_delta_min);


        timestamp_delta_i = getTime(tmp_alpha) - (int) (theta0_t + theta1_t * (float) getTime(tmp_alpha_minus_1));
        value_delta_i = getValue(tmp_alpha) - (int) (theta0_v + theta1_v * (float) getValue(tmp_alpha_minus_1));
        length -= getBitWith(timestamp_delta_i - timestamp_delta_min);
        length -= getBitWith(value_delta_i - value_delta_min);

        timestamp_delta_i = getTime(tmp_alpha_plus_1) - (int) (theta0_t + theta1_t * (float) getTime(tmp_alpha_minus_1));
        value_delta_i = getValue(tmp_alpha_plus_1) - (int) (theta0_v + theta1_v * (float) getValue(tmp_alpha_minus_1));
        if (timestamp_delta_i < timestamp_delta_min) {
            length += getBitWith(timestamp_delta_min - timestamp_delta_i) * (block_size - 1);
            timestamp_delta_min = timestamp_delta_i;
        } else {
            length += getBitWith(timestamp_delta_i - timestamp_delta_min);
        }
        if (value_delta_i < value_delta_min) {
            length += getBitWith(value_delta_min - value_delta_i) * (block_size - 1);
            value_delta_min = value_delta_i;
        } else {
            length += getBitWith(value_delta_i - value_delta_min);
        }


        long tmp_block_size_1 = ts_block[block_size - 1];
        timestamp_delta_i = getTime(tmp_alpha) - (int) (theta0_t + theta1_t * (float) getTime(tmp_block_size_1));
        value_delta_i = getValue(tmp_alpha) - (int) (theta0_v + theta1_v * (float) getValue(tmp_block_size_1));

        if (timestamp_delta_i < timestamp_delta_min) {
            length += getBitWith(timestamp_delta_min - timestamp_delta_i) * (block_size - 1);
            timestamp_delta_min = timestamp_delta_i;
        } else {
            length += getBitWith(timestamp_delta_i - timestamp_delta_min);
        }
        if (value_delta_i < value_delta_min) {
            length += getBitWith(value_delta_min - value_delta_i) * (block_size - 1);
            value_delta_min = value_delta_i;
        } else {
            length += getBitWith(value_delta_i - value_delta_min);
        }

        b[0] = length;
        b[1] = timestamp_delta_min;
        b[2] = value_delta_min;

        return b;
    }


    private static int getIstarClose(int alpha, ArrayList<Integer> j_star_list, int[][] new_length_list, int[] raw_length) {
        int min_i = 0;
        int min_dis = Integer.MAX_VALUE;
        for (int i = 0; i < j_star_list.size(); i++) {
            if (abs(alpha - j_star_list.get(i)) < min_dis) {
                min_i = j_star_list.get(i);
                min_dis = abs(alpha - j_star_list.get(i));
                raw_length[0] = new_length_list[i][0];
                raw_length[3] = new_length_list[i][1];
                raw_length[4] = new_length_list[i][2];
            }
        }
        if (min_dis == 0) {
            System.out.println("get IstarClose error");
            return 0;
        }
        return min_i;
    }

    public static void trainParameter(long[] ts_block, int block_size, float[] theta) {
        long sum_X_r = 0;
        long sum_Y_r = 0;
        long sum_squ_X_r = 0;
        long sum_squ_XY_r = 0;
        long sum_X_v = 0;
        long sum_Y_v = 0;
        long sum_squ_X_v = 0;
        long sum_squ_XY_v = 0;

        for (int i = 1; i < block_size; i++) {
            long ts_block_i_1 = ts_block[i - 1];
            long ts_block_i = ts_block[i];
            long ts_block_i_1_time = getTime(ts_block_i_1);
            long ts_block_i_1_value = getValue(ts_block_i_1);
            long ts_block_i_time = getTime(ts_block_i);
            long ts_block_i_value = getValue(ts_block_i);


            sum_X_r += (ts_block_i_1_time);
            sum_X_v += (ts_block_i_1_value);
            sum_Y_r += (ts_block_i_time);
            sum_Y_v += (ts_block_i_value);
            sum_squ_X_r += (ts_block_i_1_time * (ts_block_i_1_time));
            sum_squ_X_v += (ts_block_i_1_value * ts_block_i_1_value);
            sum_squ_XY_r += (ts_block_i_1_time * ts_block_i_time);
            sum_squ_XY_v += (ts_block_i_1_value * ts_block_i_value);
        }

        int m_reg = block_size - 1;
        float theta0_r = 0.0F;
        float theta1_r = 1.0F;
        if (m_reg * sum_squ_X_r != sum_X_r * sum_X_r) {
            theta0_r =
                    (float) (sum_squ_X_r * sum_Y_r - sum_X_r * sum_squ_XY_r)
                            / (float) (m_reg * sum_squ_X_r - sum_X_r * sum_X_r);
            theta1_r =
                    (float) (m_reg * sum_squ_XY_r - sum_X_r * sum_Y_r)
                            / (float) (m_reg * sum_squ_X_r - sum_X_r * sum_X_r);
        }

        float theta0_v = 0.0F;
        float theta1_v = 1.0F;
        if (m_reg * sum_squ_X_v != sum_X_v * sum_X_v) {
            theta0_v =
                    (float) (sum_squ_X_v * sum_Y_v - sum_X_v * sum_squ_XY_v)
                            / (float) (m_reg * sum_squ_X_v - sum_X_v * sum_X_v);
            theta1_v =
                    (float) (m_reg * sum_squ_XY_v - sum_X_v * sum_Y_v)
                            / (float) (m_reg * sum_squ_X_v - sum_X_v * sum_X_v);
        }
        theta[0] = theta0_r;
        theta[1] = theta1_r;
        theta[2] = theta0_v;
        theta[3] = theta1_v;
    }

    public static long[] getEncodeBitsRegressionNoTrain(
            long[] ts_block,
            int block_size,
            int[] raw_length,
            float[] theta,
            int segment_size) {

        long[] ts_block_delta = new long[ts_block.length];

        float theta0_r = theta[0];
        float theta1_r = theta[1];
        float theta0_v = theta[2];
        float theta1_v = theta[3];

        ts_block_delta[0] = ts_block[0];
        int timestamp_delta_min = Integer.MAX_VALUE;
        int value_delta_min = Integer.MAX_VALUE;

        int j = 1;
        long tmp_j_1 = ts_block[0];
        long tmp_j;
        while (j < block_size) {
            tmp_j = ts_block[j];

            int epsilon_r_j =
                    getTime(tmp_j)
                            - (int) (theta0_r + theta1_r * (float) getTime(tmp_j_1));
            int epsilon_v_j =
                    getValue(tmp_j)
                            - (int) (theta0_v + theta1_v * (float) getValue(tmp_j_1));

            if (epsilon_r_j < timestamp_delta_min) {
                timestamp_delta_min = epsilon_r_j;
            }
            if (epsilon_v_j < value_delta_min) {
                value_delta_min = epsilon_v_j;
            }
            ts_block_delta[j] = combine2Int(epsilon_r_j, epsilon_v_j);
            tmp_j_1 = tmp_j;
            j++;
        }


        int max_interval = Integer.MIN_VALUE;
        int max_value = Integer.MIN_VALUE;
        int max_interval_segment = Integer.MIN_VALUE;
        int max_value_segment = Integer.MIN_VALUE;
        int length = 0;

        for (int i = 1; i < block_size; i++) {
            tmp_j = ts_block_delta[i];
            int epsilon_r = getTime(tmp_j) - timestamp_delta_min;
            int epsilon_v = getValue(tmp_j) - value_delta_min;

            ts_block_delta[i] = combine2Int(epsilon_r, epsilon_v);

            if (epsilon_r > max_interval) {
                max_interval = epsilon_r;
            }
            if (epsilon_v > max_value) {
                max_value = epsilon_v;
            }

            if (epsilon_r > max_interval_segment) {
                max_interval_segment = epsilon_r;
            }
            if (epsilon_v > max_value_segment) {
                max_value_segment = epsilon_v;
            }
            if (i % segment_size == 0) {
                length += getBitWith(max_interval_segment) * segment_size;
                length += getBitWith(max_value_segment) * segment_size;
                max_interval_segment = Integer.MIN_VALUE;
                max_value_segment = Integer.MIN_VALUE;
            }
        }

        int max_bit_width_interval = getBitWith(max_interval);
        int max_bit_width_value = getBitWith(max_value);


        raw_length[0] = length;
        raw_length[1] = max_bit_width_interval;
        raw_length[2] = max_bit_width_value;
        raw_length[3] = timestamp_delta_min;
        raw_length[4] = value_delta_min;

        return ts_block_delta;


    }

    public static int getBeta(
            long[] ts_block,
            int alpha,
            ArrayList<Integer> min_index,
            int block_size,
            int[] raw_length,
            float[] theta) {

        int raw_abs_sum = raw_length[0];
        int[][] new_length_list = new int[block_size][3];
        int pos_new_length_list = 0;
        int range = block_size / 16;

        ArrayList<Integer> j_star_list = new ArrayList<>(); // beta list of min b phi alpha to j
        int j_star = -1;
        int[] b;
        if (alpha == -1) {
            return j_star;
        }

        if (alpha == 0) {
            if (min_index.get(0) == 1) {
                adjust1TimeCost(ts_block, 1, raw_length, min_index, theta);
            }
            if (min_index.get(1) == 1) {
                adjust1ValueCost(ts_block, 1, raw_length, min_index, theta);
            }

            for (int j = 2; j < range - 1; j++) {
//            for (int j = 2; j < block_size - 1; j++) {
                // if j, alpha+1, alpha points are min residuals, need to recalculate min residuals
                if (min_index.contains(j)) { //|| min_index.contains(1)
                    b = adjust0MinChange(ts_block, j, theta);
                } else {
                    b = adjust0MinChangeNo(ts_block, raw_length, j, theta);
                }
                if (b[0] < raw_abs_sum) {
                    raw_abs_sum = b[0];
                    j_star_list = new ArrayList<>();
                    pos_new_length_list = 0;
                    j_star_list.add(j);
                    System.arraycopy(b, 0, new_length_list[pos_new_length_list], 0, 3);
                    pos_new_length_list++;
                } else if (b[0] == raw_abs_sum) {
                    j_star_list.add(j);
                    System.arraycopy(b, 0, new_length_list[pos_new_length_list], 0, 3);
                    pos_new_length_list++;
                }
            }
            if (min_index.contains(0)) {
                b = adjust0n1MinChange(ts_block, theta);
            } else {
                b = adjust0n1MinChangeNo(ts_block, raw_length, theta);
            }
            if (b[0] < raw_abs_sum) {
                raw_abs_sum = b[0];
                j_star_list = new ArrayList<>();

                j_star_list.add(block_size);
                pos_new_length_list = 0;
                System.arraycopy(b, 0, new_length_list[pos_new_length_list], 0, 3);

                pos_new_length_list++;
            } else if (b[0] == raw_abs_sum) {
                j_star_list.add(block_size);
                System.arraycopy(b, 0, new_length_list[pos_new_length_list], 0, 3);
                pos_new_length_list++;
            }
        } // alpha == n
        else if (alpha == block_size - 1) {
            if (min_index.get(0) == block_size - 1) {
                adjust1TimeCost(ts_block, block_size - 1, raw_length, min_index, theta);
            }
            if (min_index.get(1) == block_size - 1) {
                adjust1ValueCost(ts_block, block_size - 1, raw_length, min_index, theta);
            }
            for (int j = block_size - range; j < block_size - 1; j++) {
//            for (int j = 1; j < block_size - 1; j++) {
                if (min_index.contains(j)) { //min_index.contains(block_size - 1) ||
                    b = adjustnMinChange(ts_block, j, theta);
                } else {
                    b = adjustnMinChangeNo(ts_block, raw_length, j, theta);
                }
                if (b[0] < raw_abs_sum) {
                    raw_abs_sum = b[0];
                    j_star_list = new ArrayList<>();
                    j_star_list.add(j);
                    pos_new_length_list = 0;
                    System.arraycopy(b, 0, new_length_list[pos_new_length_list], 0, 3);
                    pos_new_length_list++;
                } else if (b[0] == raw_abs_sum) {
                    j_star_list.add(j);
                    System.arraycopy(b, 0, new_length_list[pos_new_length_list], 0, 3);
                    pos_new_length_list++;
                }
            }
            if (min_index.contains(0)) {//min_index.contains(block_size - 1) ||
                b = adjustn0MinChange(ts_block, theta);
            } else {
                b = adjustn0MinChangeNo(ts_block, raw_length, theta);
            }
            if (b[0] < raw_abs_sum) {
                raw_abs_sum = b[0];
                j_star_list.clear();
                j_star_list = new ArrayList<>();
                j_star_list.add(0);

                pos_new_length_list = 0;
                System.arraycopy(b, 0, new_length_list[pos_new_length_list], 0, 3);
                pos_new_length_list++;
            } else if (b[0] == raw_abs_sum) {
                j_star_list.add(0);
                System.arraycopy(b, 0, new_length_list[pos_new_length_list], 0, 3);
                pos_new_length_list++;
            }
        } // alpha != 1 and alpha != n
        else {
            if (min_index.get(0) == alpha) {
                adjust1TimeCost(ts_block, alpha, raw_length, min_index, theta);
            }
            if (min_index.get(1) == alpha) {
                adjust1ValueCost(ts_block, alpha, raw_length, min_index, theta);
            }
            if (min_index.get(0) == alpha + 1) {
                adjust1TimeCost(ts_block, alpha + 1, raw_length, min_index, theta);
            }
            if (min_index.get(1) == alpha + 1) {
                adjust1ValueCost(ts_block, alpha + 1, raw_length, min_index, theta);
            }

            long tmp_alpha = ts_block[alpha];
            long tmp_alpha_plus_1 = ts_block[alpha + 1];
            long tmp_alpha_minus_1 = ts_block[alpha - 1];

            int start_j = Math.max(alpha - range / 2, 1);
            int end_j = Math.min(alpha + range / 2, block_size - 1);
            for (int j = start_j; j < end_j; j++) {
//            for (int j = 1; j < block_size - 1; j++) {
                if (alpha != j && (alpha + 1) != j) {
                    if (min_index.contains(j)) { //|| min_index.contains(alpha) || min_index.contains(alpha + 1)
                        b = adjustAlphaToJMinChange(ts_block, alpha, j, theta, tmp_alpha, tmp_alpha_plus_1, tmp_alpha_minus_1);
                    } else {
                        b = adjustAlphaToJMinChangeNo(ts_block, raw_length, alpha, j, theta, tmp_alpha, tmp_alpha_plus_1, tmp_alpha_minus_1);
                    }
                    if (b[0] < raw_abs_sum) {
                        raw_abs_sum = b[0];
                        j_star_list = new ArrayList<>();
                        j_star_list.add(j);
                        pos_new_length_list = 0;
                        System.arraycopy(b, 0, new_length_list[pos_new_length_list], 0, 3);
                        pos_new_length_list++;
                    } else if (b[0] == raw_abs_sum) {
                        j_star_list.add(j);
                        System.arraycopy(b, 0, new_length_list[pos_new_length_list], 0, 3);
                        pos_new_length_list++;
                    }
                }
            }

            if (min_index.contains(0)) {//|| min_index.contains(alpha) || min_index.contains(alpha + 1)
                b = adjustTo0MinChange(ts_block, alpha, theta, tmp_alpha, tmp_alpha_plus_1, tmp_alpha_minus_1);
            } else {
                b = adjustTo0MinChangeNo(ts_block, raw_length, alpha, theta, tmp_alpha, tmp_alpha_plus_1, tmp_alpha_minus_1);
            }
            if (b[0] < raw_abs_sum) {
                raw_abs_sum = b[0];
                j_star_list = new ArrayList<>();
                j_star_list.add(0);
                pos_new_length_list = 0;
                System.arraycopy(b, 0, new_length_list[pos_new_length_list], 0, 3);
                pos_new_length_list++;
            } else if (b[0] == raw_abs_sum) {
                j_star_list.add(0);
                System.arraycopy(b, 0, new_length_list[pos_new_length_list], 0, 3);
                pos_new_length_list++;
            }
            if (min_index.contains(block_size - 1)) {//|| min_index.contains(alpha) || min_index.contains(alpha + 1)
                b = adjustTonMinChange(ts_block, alpha, theta, tmp_alpha, tmp_alpha_plus_1, tmp_alpha_minus_1);
            } else {
                b = adjustTonMinChangeNo(ts_block, raw_length, alpha, theta, tmp_alpha, tmp_alpha_plus_1, tmp_alpha_minus_1);
            }

            if (b[0] < raw_abs_sum) {
                raw_abs_sum = b[0];
                j_star_list = new ArrayList<>();
                j_star_list.add(block_size);
                pos_new_length_list = 0;
                System.arraycopy(b, 0, new_length_list[pos_new_length_list], 0, 3);
                pos_new_length_list++;
            } else if (b[0] == raw_abs_sum) {
                j_star_list.add(block_size);
                System.arraycopy(b, 0, new_length_list[pos_new_length_list], 0, 3);
                pos_new_length_list++;
            }
        }
        int[][] new_new_length_list = new int[pos_new_length_list][5];
        for (int i = 0; i < pos_new_length_list; i++) {
            System.arraycopy(new_new_length_list[i], 0, new_length_list[i], 0, 3);
        }

        if (j_star_list.size() != 0) {
            j_star = getIstarClose(alpha, j_star_list, new_new_length_list, raw_length);
        }
        return j_star;
    }


    public static int[] getIStar(
            long[] ts_block, ArrayList<Integer> min_index, int block_size, int index, float[] theta) {

        int timestamp_delta_min = Integer.MAX_VALUE;
        int value_delta_min = Integer.MAX_VALUE;
        int timestamp_delta_min_index = -1;
        int value_delta_min_index = -1;
        int timestamp_delta_max = Integer.MIN_VALUE;
        int value_delta_max = Integer.MIN_VALUE;
        int timestamp_delta_max_index = -1;
        int value_delta_max_index = -1;

        float theta0_t = theta[0];
        float theta1_t = theta[1];
        float theta0_v = theta[2];
        float theta1_v = theta[3];
//        int[][] ts_block_delta = new int[block_size - 1][2];

        int j = 1;
        long tmp_j_1 = ts_block[0];
        long tmp_j;
        while (j < block_size) {
            tmp_j = ts_block[j];
            int epsilon_r_j =
                    getTime(tmp_j)
                            - (int) (theta0_t + theta1_t * (float) getTime(tmp_j_1));
            int epsilon_v_j =
                    getValue(tmp_j)
                            - (int) (theta0_v + theta1_v * (float) getValue(tmp_j_1));

            if (epsilon_r_j < timestamp_delta_min) {
                timestamp_delta_min = epsilon_r_j;
                timestamp_delta_min_index = j;
            }
            if (epsilon_v_j < value_delta_min) {
                value_delta_min = epsilon_v_j;
                value_delta_min_index = j;
            }
            if (epsilon_r_j > timestamp_delta_max) {
                timestamp_delta_max = epsilon_r_j;
                timestamp_delta_max_index = j;
            }
            if (epsilon_v_j > value_delta_max) {
                value_delta_max = epsilon_v_j;
                value_delta_max_index = j;
            }
            tmp_j_1 = tmp_j;
            j++;
        }

        min_index.add(timestamp_delta_min_index);
        min_index.add(value_delta_min_index);


        int[] alpha_list = new int[2];
        if (index == 0) {
            alpha_list[0] = value_delta_min_index;
            alpha_list[1] = value_delta_max_index;
        } else {
            alpha_list[0] = timestamp_delta_min_index;
            alpha_list[1] = timestamp_delta_max_index;
        }

        return alpha_list;
    }

    public static int[] getIStar(
            long[] ts_block,
            ArrayList<Integer> min_index,
            int block_size,
            float[] theta) {
        int timestamp_delta_max = Integer.MIN_VALUE;
        int value_delta_max = Integer.MIN_VALUE;
        int timestamp_delta_min = Integer.MAX_VALUE;
        int value_delta_min = Integer.MAX_VALUE;
        int timestamp_delta_min_index = -1;
        int value_delta_min_index = -1;
        int timestamp_delta_max_index = -1;
        int value_delta_max_index = -1;

        int[] alpha_list = new int[4];

        float theta0_t = theta[0];
        float theta1_t = theta[1];
        float theta0_v = theta[2];
        float theta1_v = theta[3];

        int j = 1;
        long tmp_j_1 = ts_block[0];
        long tmp_j;
        while (j < block_size) {
            tmp_j = ts_block[j];
            int epsilon_r_j =
                    getTime(tmp_j)
                            - (int) (theta0_t + theta1_t * (float) getTime(tmp_j_1));
            int epsilon_v_j =
                    getValue(tmp_j)
                            - (int) (theta0_v + theta1_v * (float) getValue(tmp_j_1));

            if (epsilon_r_j < timestamp_delta_min) {
                timestamp_delta_min = epsilon_r_j;
                timestamp_delta_min_index = j;
            }
            if (epsilon_v_j < value_delta_min) {
                value_delta_min = epsilon_v_j;
                value_delta_min_index = j;
            }
            if (epsilon_r_j > timestamp_delta_max) {
                timestamp_delta_max = epsilon_r_j;
                timestamp_delta_max_index = j;
            }
            if (epsilon_v_j > value_delta_max) {
                value_delta_max = epsilon_v_j;
                value_delta_max_index = j;
            }
            tmp_j_1 = tmp_j;
            j++;
        }


        min_index.add(timestamp_delta_min_index);
        alpha_list[0] = timestamp_delta_min_index;
        alpha_list[1] = timestamp_delta_max_index;


        int pos_alpha_list = 2;
        min_index.add(value_delta_min_index);
        if (!containsValue(alpha_list, value_delta_min_index)) {
            alpha_list[pos_alpha_list] = value_delta_min_index;
            pos_alpha_list++;
        }
        if (!containsValue(alpha_list, value_delta_max_index)) {
            alpha_list[pos_alpha_list] = value_delta_max_index;
            pos_alpha_list++;
        }


        int[] new_alpha_list = new int[pos_alpha_list];
        System.arraycopy(alpha_list, 0, new_alpha_list, 0, pos_alpha_list);

        return new_alpha_list;
    }


    public static int encodeRLEBitWidth2Bytes(
            long[] bit_width_segments) {
        int encoded_result = 0;


        int count_of_time = 1;
        int count_of_value = 1;
        long pre_bit_width_segments = bit_width_segments[0];
        int pre_time = getTime(pre_bit_width_segments);
        int pre_value = getValue(pre_bit_width_segments);
        int size = bit_width_segments.length;


        int pos_time = 0;
        int pos_value = 0;

        for (int i = 1; i < size; i++) {
            long cur_bit_width_segments = bit_width_segments[i];
            int cur_time = getTime(cur_bit_width_segments);
            int cur_value = getValue(cur_bit_width_segments);
            if (cur_time != pre_time && count_of_time != 0) {
                pos_time++;
                pre_time = cur_time;
                count_of_time = 1;
            } else {
                count_of_time++;
                pre_time = cur_time;
                if (count_of_time == 256) {
                    pos_time++;
                    count_of_time = 1;
                }
            }

            if (cur_value != pre_value && count_of_value != 0) {
                pos_value++;

                pre_value = cur_value;
                count_of_value = 1;
            } else {
                count_of_value++;
                pre_value = cur_value;
                if (count_of_value == 256) {
                    pos_value++;
                    count_of_value = 0;
                }
            }

        }
        if (count_of_time != 0) {
            pos_time++;
        }
        if (count_of_value != 0) {
            pos_value++;
        }

        encoded_result += (pos_time * 2);
        encoded_result += (pos_value * 2);

        return encoded_result;
    }

    public static int encodeRLEBitWidth2Bytes(
            long[] bit_width_segments, int pos_encode, byte[] encoded_result) {

        int count_of_time = 1;
        int count_of_value = 1;
        long pre_bit_width_segments = bit_width_segments[0];
        int pre_time = getTime(pre_bit_width_segments);
        int pre_value = getValue(pre_bit_width_segments);

        int size = bit_width_segments.length;
        int[][] run_length_time = new int[size][2];
        int[][] run_length_value = new int[size][2];

        int pos_time = 0;
        int pos_value = 0;

        for (int i = 1; i < size; i++) {
            long cur_bit_width_segments = bit_width_segments[i];
            int cur_time = getTime(cur_bit_width_segments);
            int cur_value = getValue(cur_bit_width_segments);
            if (cur_time != pre_time && count_of_time != 0) {
                run_length_time[pos_time][0] = count_of_time;
                run_length_time[pos_time][1] = pre_time;
                pos_time++;
                pre_time = cur_time;
                count_of_time = 1;
            } else {
                count_of_time++;
                pre_time = cur_time;
                if (count_of_time == 256) {
                    run_length_time[pos_time][0] = count_of_time;
                    run_length_time[pos_time][1] = pre_time;
                    pos_time++;
                    count_of_time = 0;
                }
            }

            if (cur_value != pre_value && count_of_value != 0) {
                run_length_value[pos_value][0] = count_of_value;
                run_length_value[pos_value][1] = pre_value;
                pos_value++;

                pre_value = cur_value;
                count_of_value = 1;
            } else {
                count_of_value++;
                pre_value = cur_value;
                if (count_of_value == 256) {
                    run_length_value[pos_value][0] = count_of_value;
                    run_length_value[pos_value][1] = pre_value;
                    pos_value++;
                    count_of_value = 0;
                }
            }

        }
        if (count_of_time != 0) {
            run_length_time[pos_time][0] = count_of_time;
            run_length_time[pos_time][1] = pre_time;
            pos_time++;
        }
        if (count_of_value != 0) {
            run_length_value[pos_value][0] = count_of_value;
            run_length_value[pos_value][1] = pre_value;
            pos_value++;
        }
        intWord2Bytes(pos_time, pos_encode, encoded_result);
        pos_encode += 2;
        intWord2Bytes(pos_value, pos_encode, encoded_result);
        pos_encode += 2;

        for (int i = 0; i < pos_time; i++) {
            int[] bit_width_time = run_length_time[i];
            intByte2Bytes(bit_width_time[0], pos_encode, encoded_result);
            pos_encode++;
            intByte2Bytes(bit_width_time[1], pos_encode, encoded_result);
            pos_encode++;
        }
//        pos_encode += Math.ceil((double) (pos_time*5)/8);
        for (int i = 0; i < pos_value; i++) {
            int[] bit_width_value = run_length_value[i];
            intByte2Bytes(bit_width_value[0], pos_encode, encoded_result);
            pos_encode++;
            intByte2Bytes(bit_width_value[1], pos_encode, encoded_result);
            pos_encode++;

        }
//        pos_encode += Math.ceil((double) (pos_value*5)/8);

        return pos_encode;
    }

    public static long[] segmentBitPacking(long[] ts_block_delta, int block_size, int segment_size) {

        int segment_n = (block_size - 1) / segment_size;
        long[] bit_width_segments = new long[segment_n];
        for (int segment_i = 0; segment_i < segment_n; segment_i++) {
            int bit_width_time = Integer.MIN_VALUE;
            int bit_width_value = Integer.MIN_VALUE;

            for (int data_i = segment_i * segment_size + 1; data_i < (segment_i + 1) * segment_size + 1; data_i++) {
                long cur_data_i = ts_block_delta[data_i];
                int cur_bit_width_time = getBitWith(getTime(cur_data_i));
                int cur_bit_width_value = getBitWith(getValue(cur_data_i));
                if (cur_bit_width_time > bit_width_time) {
                    bit_width_time = cur_bit_width_time;
                }
                if (cur_bit_width_value > bit_width_value) {
                    bit_width_value = cur_bit_width_value;
                }
            }
            bit_width_segments[segment_i] = combine2Int(bit_width_time, bit_width_value);
        }
        return bit_width_segments;
    }

    public static void moveAlphaToBeta(long[] ts_block, int alpha, int beta) {
        long tmp_tv = ts_block[alpha];
        if (beta < alpha) {
            for (int u = alpha - 1; u >= beta; u--) {
                ts_block[u + 1] = ts_block[u];
            }
        } else {
            for (int u = alpha + 1; u < beta; u++) {
                ts_block[u - 1] = ts_block[u];
            }
            beta--;
        }
        ts_block[beta] = tmp_tv;
    }

    private static int encodeSegment2Bytes(long[] delta_segments, long[] bit_width_segments, int[] raw_length, int segment_size, float[] theta, int pos_encode, byte[] encoded_result) {

        int block_size = delta_segments.length;
        int segment_n = block_size / segment_size;
        long2Bytes(delta_segments[0], pos_encode, encoded_result);
        pos_encode += 8;
        float2bytes(theta[0], pos_encode, encoded_result);
        pos_encode += 4;
        float2bytes(theta[1], pos_encode, encoded_result);
        pos_encode += 4;
        float2bytes(theta[2], pos_encode, encoded_result);
        pos_encode += 4;
        float2bytes(theta[3], pos_encode, encoded_result);
        pos_encode += 4;

        int2Bytes(raw_length[3], pos_encode, encoded_result);
        pos_encode += 4;
        int2Bytes(raw_length[4], pos_encode, encoded_result);
        pos_encode += 4;



        pos_encode = encodeRLEBitWidth2Bytes(bit_width_segments, pos_encode, encoded_result);
        for (int segment_i = 0; segment_i < segment_n; segment_i++) {
            long tmp_bit_width_segments = bit_width_segments[segment_i];
            int bit_width_time = getTime(tmp_bit_width_segments);
            int bit_width_value = getValue(tmp_bit_width_segments);
            pos_encode = bitPacking(delta_segments, 0, segment_i * segment_size + 1, segment_size, bit_width_time, pos_encode, encoded_result);
            pos_encode = bitPacking(delta_segments, 1, segment_i * segment_size + 1, segment_size, bit_width_value, pos_encode, encoded_result);
        }

        return pos_encode;
    }

    private static void printTSBlock(long[] ts_block) {
        for (long ts : ts_block) {
            System.out.println("[" + getTime(ts) + "," + getValue(ts) + "]");
        }
    }

    private static long[] ReorderingTimeSeries(long[] ts_block, int[] raw_length, float[] theta, int segment_size) {


        int block_size = ts_block.length;
        long[] ts_block_delta = new long[block_size];

        ArrayList<Integer> min_index = new ArrayList<>();
        int index_alpha_list = 0;


        getEncodeBitsRegressionNoTrain(ts_block, block_size, raw_length, theta, segment_size);

        int[] alpha_list = getIStar(ts_block, min_index, block_size, 0, theta);
        int[] beta_list = new int[alpha_list.length];
        int[][] new_length_list = new int[alpha_list.length][5];
        int pos_new_length_list = 0;
        int[] new_alpha_list = new int[alpha_list.length];


        for (int alpha : alpha_list) {
            if (alpha == -1)
                continue;
            new_alpha_list[pos_new_length_list] = alpha;

            ArrayList<Integer> new_min_index = (ArrayList<Integer>) min_index.clone();
            int[] new_length = raw_length.clone();

            beta_list[pos_new_length_list] = getBeta(ts_block, alpha, new_min_index, block_size, new_length, theta);
            if (beta_list[pos_new_length_list] == -1)
                continue;
            System.arraycopy(new_length, 0, new_length_list[pos_new_length_list], 0, 5);
            pos_new_length_list++;
        }

        int adjust_count = 0;


        while (pos_new_length_list != 0) {
            if (adjust_count < block_size / 2 && adjust_count < 20) {
                adjust_count++;
            } else {
                break;
            }
//            all_length = new int[pos_new_length_list][2];
            int min_length_index = -1;
            int min_length = Integer.MAX_VALUE;


            for (int pos_new_length_list_j = 0; pos_new_length_list_j < pos_new_length_list; pos_new_length_list_j++) {
                if (new_length_list[pos_new_length_list_j][0] < min_length) {
                    min_length = new_length_list[pos_new_length_list_j][0];
                    min_length_index = pos_new_length_list_j;
                }
            }

            if (min_length <= raw_length[0]) {
                long[] new_ts_block = ts_block.clone();
                moveAlphaToBeta(new_ts_block, new_alpha_list[min_length_index], beta_list[min_length_index]);
                int[] new_length = raw_length.clone();
                getEncodeBitsRegressionNoTrain(new_ts_block, block_size, new_length, theta, segment_size);
                if (new_length[0] < raw_length[0]) {
                    System.arraycopy(new_length, 0, raw_length, 0, 5);
                    ts_block = new_ts_block.clone();
                } else {
                    break;
                }
            } else {
                break;
            }

            alpha_list = getIStar(ts_block, min_index, block_size, theta);

            int alpha_size = alpha_list.length;
            for (int alpha_i = alpha_size - 1; alpha_i >= 0; alpha_i--) {
                if (containsValue(beta_list, alpha_list[alpha_i])) {
                    alpha_list = removeElement(alpha_list, alpha_i);
                }
            }
            beta_list = new int[alpha_list.length];
            new_length_list = new int[alpha_list.length][5];
            pos_new_length_list = 0;
            new_alpha_list = new int[alpha_list.length];

            for (int alpha : alpha_list) {
                if (alpha == -1)
                    continue;
                new_alpha_list[pos_new_length_list] = alpha;

                int[] new_length = raw_length.clone();
//            beta_list[j] = 0;
                beta_list[pos_new_length_list] = getBeta(ts_block, alpha, min_index, block_size, new_length, theta);
                if (beta_list[pos_new_length_list] == -1)
                    continue;
                System.arraycopy(new_length, 0, new_length_list[pos_new_length_list], 0, 5);
                pos_new_length_list++;
            }

        }


        ts_block_delta = getEncodeBitsRegressionNoTrain(ts_block, block_size, raw_length, theta, segment_size);

        return ts_block_delta;

    }


    private static int REGERBlockEncoder(long[] data, int i, int block_size, int supply_length, int[] third_value, int segment_size, int encode_pos, byte[] cur_byte, int[] block_sort) {


        long min_time = (long) getTime(data[i * block_size]) << 32;
        long[] ts_block;
        long[] ts_block_value;
        long[] ts_block_partition;

        if (supply_length == 0) {
            ts_block = new long[block_size];
            ts_block_value = new long[block_size];
            ts_block_partition = new long[block_size];

            for (int j = 0; j < block_size; j++) {
                long tmp_j = data[j + i * block_size] - min_time;
                ts_block[j] = tmp_j;
                ts_block_value[j] = combine2Int(getValue(tmp_j), getTime(tmp_j));

            }

        } else {
            ts_block = new long[supply_length];
            ts_block_value = new long[supply_length];
            ts_block_partition = new long[supply_length];
            int end = data.length - i * block_size;
            for (int j = 0; j < end; j++) {
                long tmp_j = data[j + i * block_size] - min_time;
                ts_block[j] = tmp_j;
                ts_block_value[j] = combine2Int(getValue(tmp_j), getTime(tmp_j));

            }
            for (int j = end; j < supply_length; j++) {
                ts_block[j] = 0;
                ts_block_value[j] = 0;
            }
            block_size = supply_length;
        }


        int[] reorder_length = new int[5];
        float[] theta_reorder = new float[4];
        int[] time_length = new int[5];// length,max_bit_width_interval,max_bit_width_value,max_bit_width_deviation
        float[] theta_time = new float[4];
        int[] partition_length = new int[5]; // length,max_bit_width_interval,max_bit_width_value,max_bit_width_deviation
        float[] theta_partition = new float[4];

        trainParameter(ts_block, block_size, theta_time);

        long[] ts_block_delta_time = getEncodeBitsRegressionNoTrain(ts_block, block_size, time_length, theta_time, segment_size);


        int pos_ts_block_partition = 0;
        if (third_value.length > 0) {
            for(int j=block_size-1;j>=0;j--){
                long datum = ts_block[j];
                if (getValue(datum) <= third_value[0]) {
                    ts_block_partition[pos_ts_block_partition] = datum;
                    pos_ts_block_partition++;
                }
            }
            for (int third_i = 1; third_i <= third_value.length - 1; third_i++) {
                for (int j = block_size - 1; j >= 0; j--) {
                    long datum = ts_block[j];
                    if (getValue(datum) <= third_value[third_i] && getValue(datum) > third_value[third_i - 1]) {
                        ts_block_partition[pos_ts_block_partition] = datum;
                        pos_ts_block_partition++;
                    }
                }
            }
            for (int j = block_size - 1; j >= 0; j--) {
                long datum = ts_block[j];
                if (getValue(datum) > third_value[third_value.length - 1]) {
                    ts_block_partition[pos_ts_block_partition] = datum;
                    pos_ts_block_partition++;
                }
            }

        }


        trainParameter(ts_block_partition, block_size, theta_partition);
        long[] ts_block_delta_partition = getEncodeBitsRegressionNoTrain(ts_block_partition, block_size, partition_length, theta_partition, segment_size);

        Arrays.sort(ts_block_value);
        trainParameter(ts_block_value, block_size, theta_reorder);
        long[] ts_block_delta_reorder = getEncodeBitsRegressionNoTrain(ts_block_value, block_size, reorder_length, theta_reorder, segment_size);


        ReorderingTimeSeries(ts_block_value, reorder_length,  theta_reorder, segment_size);

        int segment_n = (block_size - 1) / segment_size;
        long[] bit_width_segments = new long[segment_n];
        int2Bytes(getTime(min_time),encode_pos,cur_byte);
        encode_pos += 4;

        int choose = min3(time_length[0], partition_length[0], reorder_length[0]);
        if (choose == 0) {

            block_sort[i] = 0;
            ts_block_delta_time = ReorderingTimeSeries(ts_block, time_length, theta_time, segment_size);
            bit_width_segments = segmentBitPacking(ts_block_delta_time, block_size, segment_size);


            encode_pos = encodeSegment2Bytes(ts_block_delta_time, bit_width_segments, time_length, segment_size, theta_time, encode_pos, cur_byte);
//            System.out.println(Arrays.toString(time_length));
        } else if (choose == 1) {

            block_sort[i] = 0;
            ts_block_delta_partition = ReorderingTimeSeries(ts_block_partition, partition_length, theta_partition, segment_size);
            bit_width_segments = segmentBitPacking(ts_block_delta_partition, block_size, segment_size);

            encode_pos = encodeSegment2Bytes(ts_block_delta_partition, bit_width_segments, partition_length, segment_size, theta_partition, encode_pos, cur_byte);
        } else if (choose == 2) {

            block_sort[i] = 1;
            ts_block_delta_reorder = ReorderingTimeSeries(ts_block_value, reorder_length, theta_reorder, segment_size);

            bit_width_segments = segmentBitPacking(ts_block_delta_reorder, block_size, segment_size);
            encode_pos = encodeSegment2Bytes(ts_block_delta_reorder, bit_width_segments, reorder_length, segment_size, theta_reorder, encode_pos, cur_byte);
        }


        return encode_pos;

    }

    public static int ReorderingRegressionEncoder(long[] data, int block_size, int[] third_value, int segment_size, byte[] encoded_result) {
        block_size++;
//    ArrayList<Byte> encoded_result = new ArrayList<Byte>();
        int length_all = data.length;
//        System.out.println(length_all);
        int encode_pos = 0;
        int2Bytes(length_all, encode_pos, encoded_result);
        encode_pos += 4;

        int block_num = length_all / block_size;
        int2Bytes(block_size, encode_pos, encoded_result);
        encode_pos += 4;

        int2Bytes(segment_size, encode_pos, encoded_result);
        encode_pos += 4;

        int[] block_sort = new int[block_num+1];
        int encode_pos_block_sort = encode_pos;
        int length_block_sort = (int) Math.ceil((double)(block_num+1)/(double) 8);
        encode_pos += length_block_sort;

//        for (int i = 44; i < 45; i++) {
        for (int i = 0; i < block_num; i++) {
//            System.out.println(i);
            encode_pos = REGERBlockEncoder(data, i, block_size, 0, third_value, segment_size, encode_pos, encoded_result, block_sort);
        }

        int remaining_length = length_all - block_num * block_size;
        if (remaining_length == 1) {
            long2Bytes(data[data.length - 1], encode_pos, encoded_result);
            encode_pos += 8;
        }
        if (remaining_length != 0 && remaining_length != 1) {
            int supple_length;
            if (remaining_length % segment_size == 0) {
                supple_length = 1;
            } else if (remaining_length % segment_size == 1) {
                supple_length = 0;
            } else {
                supple_length = segment_size + 1 - remaining_length % segment_size;
            }
            encode_pos = REGERBlockEncoder(data, block_num, block_size, supple_length + remaining_length, third_value, segment_size, encode_pos, encoded_result, block_sort);

        }
        encodeSort(block_sort,encode_pos_block_sort,encoded_result);

        return encode_pos;
    }

    private static void encodeSort(int[] block_sort,int encode_pos_block_sort, byte[] encoded_result) {
        int length = block_sort.length;
        int cur_num = 0;
        for(int i=1;i<=length;i++){
            cur_num <<=1;
            cur_num += block_sort[i-1];
            if(i%8==0){
                intByte2Bytes(cur_num, encode_pos_block_sort, encoded_result);
                encode_pos_block_sort ++;
                cur_num = 0;
            }
        }
        if(length%8!=0){
            intByte2Bytes(cur_num, encode_pos_block_sort, encoded_result);
        }
    }

    public static int REGERBlockDecoder(byte[] encoded, int decode_pos, int[][] value_list, int block_size, int segment_size, int[] value_pos_arr) {

        int min_time_0 = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;

        int time0 = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;
        value_list[value_pos_arr[0]][0] = time0+min_time_0;
        int value0 = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;
        value_list[value_pos_arr[0]][1] = value0;

        value_pos_arr[0]++;

        float theta_time0 = bytes2float(encoded, decode_pos);
        decode_pos += 4;
        float theta_time1 = bytes2float(encoded, decode_pos);
        decode_pos += 4;

        float theta_value0 = bytes2float(encoded, decode_pos);
        decode_pos += 4;
        float theta_value1 = bytes2float(encoded, decode_pos);
        decode_pos += 4;

        int min_time = bytes2Integer(encoded, decode_pos,4);
        decode_pos += 4;
        int min_value = bytes2Integer(encoded, decode_pos,4);
        decode_pos += 4;

        int bit_width_time_count = bytes2Integer(encoded, decode_pos, 2);
        decode_pos += 2;
        int bit_width_value_count = bytes2Integer(encoded, decode_pos, 2);
        decode_pos += 2;

        int count;
        int num;
        int segment_n = block_size / segment_size;
        int[][] bit_width_segments = new int[segment_n][2];
        int pos_bit_width_segments = 0;
        for (int i = 0; i < bit_width_time_count; i++) {
            count = byte2Integer(encoded, decode_pos);
            decode_pos++;
            num = byte2Integer(encoded, decode_pos);

            decode_pos++;
            for (int j = 0; j < count; j++) {
                bit_width_segments[pos_bit_width_segments][0] = num;
                pos_bit_width_segments++;
            }

        }

        pos_bit_width_segments = 0;
        for (int i = 0; i < bit_width_value_count; i++) {
            count = byte2Integer(encoded, decode_pos);
            decode_pos++;
            num = byte2Integer(encoded, decode_pos);
            decode_pos++;
            for (int j = 0; j < count; j++) {
                bit_width_segments[pos_bit_width_segments][1] = num;
                pos_bit_width_segments++;
            }
        }

        int pre_time = time0;
        int pre_value = value0;


        for (int segment_i = 0; segment_i < segment_n; segment_i++) {
            int bit_width_time = bit_width_segments[segment_i][0];
            int bit_width_value = bit_width_segments[segment_i][1];
            int[] decode_time_result = new int[segment_size];
            int[] decode_value_result = new int[segment_size];

            decode_pos = decodeBitPacking(encoded, decode_pos, bit_width_time, segment_size, decode_time_result);
            decode_pos = decodeBitPacking(encoded, decode_pos, bit_width_value, segment_size, decode_value_result);
            int pos_decode_time_result = 0;
            int length_decode_time_result = decode_time_result.length;
            for(;pos_decode_time_result<length_decode_time_result;pos_decode_time_result++){
                pre_time = (int) (theta_time0 + theta_time1 * (float)pre_time ) + decode_time_result[pos_decode_time_result] + min_time;
                pre_value = (int) (theta_value0 + theta_value1 * (float)pre_value ) +  decode_value_result[pos_decode_time_result] + min_value;
                value_list[value_pos_arr[0]][0] = pre_time+min_time_0;
                value_list[value_pos_arr[0]][1] = pre_value;
                value_pos_arr[0] ++;
            }
        }

        return decode_pos;
    }

    public static int REGERBlockDecoderValue(byte[] encoded, int decode_pos, int[][] value_list, int block_size, int segment_size, int[] value_pos_arr) {

        int min_time_0 = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;

        int time0 = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;
        value_list[value_pos_arr[0]][1] = time0 ;
        int value0 = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;
        value_list[value_pos_arr[0]][0] = value0+min_time_0;



        value_pos_arr[0]++;

        float theta_time0 = bytes2float(encoded, decode_pos);
        decode_pos += 4;
        float theta_time1 = bytes2float(encoded, decode_pos);
        decode_pos += 4;

        float theta_value0 = bytes2float(encoded, decode_pos);
        decode_pos += 4;
        float theta_value1 = bytes2float(encoded, decode_pos);
        decode_pos += 4;

        int min_time = bytes2Integer(encoded, decode_pos,4);
        decode_pos += 4;
        int min_value = bytes2Integer(encoded, decode_pos,4);
        decode_pos += 4;

        int bit_width_time_count = bytes2Integer(encoded, decode_pos, 2);
        decode_pos += 2;
        int bit_width_value_count = bytes2Integer(encoded, decode_pos, 2);
        decode_pos += 2;

        int count;
        int num;
        int segment_n = block_size / segment_size;
        int[][] bit_width_segments = new int[segment_n][2];
        int pos_bit_width_segments = 0;
        for (int i = 0; i < bit_width_time_count; i++) {
            count = byte2Integer(encoded, decode_pos);
            decode_pos++;
            num = byte2Integer(encoded, decode_pos);

            decode_pos++;
            for (int j = 0; j < count; j++) {
                bit_width_segments[pos_bit_width_segments][0] = num;
                pos_bit_width_segments++;
            }

        }

        pos_bit_width_segments = 0;
        for (int i = 0; i < bit_width_value_count; i++) {
            count = byte2Integer(encoded, decode_pos);
            decode_pos++;
            num = byte2Integer(encoded, decode_pos);
            decode_pos++;
            for (int j = 0; j < count; j++) {
                bit_width_segments[pos_bit_width_segments][1] = num;
                pos_bit_width_segments++;
            }
        }

        int pre_time = time0;
        int pre_value = value0;


        for (int segment_i = 0; segment_i < segment_n; segment_i++) {
            int bit_width_time = bit_width_segments[segment_i][0];
            int bit_width_value = bit_width_segments[segment_i][1];
            int[] decode_time_result = new int[segment_size];
            int[] decode_value_result = new int[segment_size];

            decode_pos = decodeBitPacking(encoded, decode_pos, bit_width_time, segment_size, decode_time_result);
            decode_pos = decodeBitPacking(encoded, decode_pos, bit_width_value, segment_size, decode_value_result);
            int pos_decode_time_result = 0;
            int length_decode_time_result = decode_time_result.length;
            for(;pos_decode_time_result<length_decode_time_result;pos_decode_time_result++){
                pre_time = (int) (theta_time0 + theta_time1 * (float)pre_time ) + decode_time_result[pos_decode_time_result] + min_time;
                pre_value = (int) (theta_value0 + theta_value1 * (float)pre_value ) +  decode_value_result[pos_decode_time_result] + min_value;
                value_list[value_pos_arr[0]][1] = pre_time;
                value_list[value_pos_arr[0]][0] = pre_value+min_time_0;
                value_pos_arr[0] ++;
            }
        }


        return decode_pos;
    }

    public static int[][] REGERDecoder(byte[] encoded) {

        int decode_pos = 0;
        int length_all = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;
        int block_size = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;
        int segment_size = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;


        int block_num = length_all / block_size;
        int remain_length = length_all - block_num * block_size;
        int zero_number;

        int length_block_sort = (int) Math.ceil((double)(block_num+1)/(double) 8);
        int[] block_sort = new int[block_num+1];
        for(int i=0;i<length_block_sort-1;i++){
            int sort = byte2Integer(encoded, decode_pos);
            decode_pos ++;
            for(int j=0;j<8;j++){
               block_sort[i*8+7-j] = sort & 1;
               sort >>= 1;
            }
        }
        int block_sort_end =(block_num +1)- (length_block_sort*8-8);
        int sort = byte2Integer(encoded, decode_pos);
        decode_pos ++;
        for(int j=0;j<block_sort_end;j++){
            block_sort[8*length_block_sort-8+block_sort_end-j-1] = sort & 1;
            sort >>= 1;
        }

        if (remain_length % segment_size == 0) {
            zero_number = 1;
        } else if (remain_length % segment_size == 1) {
            zero_number = 0;
        } else {
            zero_number = segment_size + 1 - remain_length % segment_size;
        }
        int[][] value_list = new int[length_all + segment_size][2];

        int[] value_pos_arr = new int[1];

        for (int k = 0; k < block_num; k++) {
            int cur_block_sort = block_sort[k];
            if(cur_block_sort==0)
                decode_pos = REGERBlockDecoder(encoded, decode_pos, value_list, block_size, segment_size, value_pos_arr);
            else if (cur_block_sort == 1){
                decode_pos = REGERBlockDecoderValue(encoded, decode_pos, value_list, block_size, segment_size, value_pos_arr);
            }

        }

        if (remain_length == 1) {
            for (int i = 0; i < remain_length; i++) {
                int value_end = bytes2Integer(encoded, decode_pos, 4);
                decode_pos += 4;
                value_list[value_pos_arr[0]][0] = value_end;
                value_end = bytes2Integer(encoded, decode_pos, 4);
                decode_pos += 4;
                value_list[value_pos_arr[0]][1] = value_end;
                value_pos_arr[0]++;
            }
        } else {
            int cur_block_sort = block_sort[block_num];
            if(cur_block_sort==0)
                REGERBlockDecoder(encoded, decode_pos, value_list, remain_length + zero_number, segment_size, value_pos_arr);
            else if (cur_block_sort == 1){
                REGERBlockDecoderValue(encoded, decode_pos, value_list, remain_length + zero_number, segment_size, value_pos_arr);
            }
        }
        return value_list;
    }

    @Test
    public void REGER() throws IOException {

        String parent_dir = "/Users/xiaojinzhao/Documents/GitHub/iotdb/iotdb-core/tsfile/src/test/resources/";
        String output_parent_dir = "/Users/xiaojinzhao/Documents/GitHub/encoding-reorder/compression_ratio/reger_float";
        int pack_size = 16;
        int block_size = 512;

        String input_parent_dir = parent_dir + "trans_data/";
        ArrayList<String> input_path_list = new ArrayList<>();
        ArrayList<String> output_path_list = new ArrayList<>();
        ArrayList<String> dataset_name = new ArrayList<>();
        ArrayList<Integer> dataset_block_size = new ArrayList<>();
        ArrayList<int[]> dataset_third = new ArrayList<>();

        dataset_name.add("CS-Sensors");
        dataset_name.add("Metro-Traffic");
        dataset_name.add("USGS-Earthquakes");
        dataset_name.add("YZ-Electricity");
        dataset_name.add("GW-Magnetic");
        dataset_name.add("TY-Fuel");
        dataset_name.add("Cyber-Vehicle");
        dataset_name.add("Vehicle-Charge");
        dataset_name.add("Nifty-Stocks");
        dataset_name.add("TH-Climate");
        dataset_name.add("TY-Transport");
        dataset_name.add("EPM-Education");
        dataset_name.add("FANYP-Sensors");
        dataset_name.add("TRAJET-Transport");

        int[] dataset_0 = {547, 2816};
        int[] dataset_1 = {1719, 3731};
        int[] dataset_2 = {-48, -11, 6, 25, 52};
        int[] dataset_3 = {8681, 13584};
        int[] dataset_4 = {79, 184, 274};
        int[] dataset_5 = {17, 68};
        int[] dataset_6 = {677};
        int[] dataset_7 = {1047, 1725};
        int[] dataset_8 = {227, 499, 614, 1013};
        int[] dataset_9 = {474, 678};
        int[] dataset_10 = {4, 30, 38, 49, 58};
        int[] dataset_11 = {5182, 8206};
        int[] dataset_12 = {652477};
        int[] dataset_13 = {581388};

        dataset_third.add(dataset_0);
        dataset_third.add(dataset_1);
        dataset_third.add(dataset_2);
        dataset_third.add(dataset_3);
        dataset_third.add(dataset_4);
        dataset_third.add(dataset_5);
        dataset_third.add(dataset_6);
        dataset_third.add(dataset_7);
        dataset_third.add(dataset_8);
        dataset_third.add(dataset_9);
        dataset_third.add(dataset_10);
        dataset_third.add(dataset_11);
        dataset_third.add(dataset_12);
        dataset_third.add(dataset_13);

        for (String value : dataset_name) {
            input_path_list.add(input_parent_dir + value);
            dataset_block_size.add(block_size);
        }

        output_path_list.add(output_parent_dir + "/CS-Sensors_ratio.csv"); // 0
        output_path_list.add(output_parent_dir + "/Metro-Traffic_ratio.csv");// 1
        output_path_list.add(output_parent_dir + "/USGS-Earthquakes_ratio.csv");// 2
        output_path_list.add(output_parent_dir + "/YZ-Electricity_ratio.csv"); // 3
        output_path_list.add(output_parent_dir + "/GW-Magnetic_ratio.csv"); //4
        output_path_list.add(output_parent_dir + "/TY-Fuel_ratio.csv");//5
        output_path_list.add(output_parent_dir + "/Cyber-Vehicle_ratio.csv"); //6
        output_path_list.add(output_parent_dir + "/Vehicle-Charge_ratio.csv");//7
        output_path_list.add(output_parent_dir + "/Nifty-Stocks_ratio.csv");//8
        output_path_list.add(output_parent_dir + "/TH-Climate_ratio.csv");//9
        output_path_list.add(output_parent_dir + "/TY-Transport_ratio.csv");//10
        output_path_list.add(output_parent_dir + "/EPM-Education_ratio.csv");//11
        output_path_list.add(output_parent_dir + "/FANYP-Sensors_ratio.csv"); // 12
        output_path_list.add(output_parent_dir + "/TRAJET-Transport_ratio.csv"); // 13


        for (int file_i = 0; file_i < input_path_list.size(); file_i++) {
            String inputPath = input_path_list.get(file_i);
            String Output = output_path_list.get(file_i);

            File file = new File(inputPath);
            File[] tempList = file.listFiles();

            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Encoding Time",
                    "Decoding Time",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio",
            };
            writer.writeRecord(head); // write header to output file

            assert tempList != null;

            int count_csv =0;
            for (File f : tempList) {
                System.out.println(count_csv);
                count_csv ++;
                System.out.println(f);
                InputStream inputStream = Files.newInputStream(f.toPath());
                CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                ArrayList<Long> data = new ArrayList<>();

                loader.readHeaders();
                loader.readRecord();
                int time0 = Integer.parseInt(loader.getValues()[0]);
                int value0 = Integer.parseInt(loader.getValues()[1]);
                data.add(combine2Int(0, value0));

                while (loader.readRecord()) {

                    int time_tmp = Integer.parseInt(loader.getValues()[0]) - time0;
                    int value_tmp = Integer.parseInt(loader.getValues()[1]);

                    data.add(combine2Int(time_tmp, value_tmp));
                }


                inputStream.close();
                ArrayList<Integer> result2 = new ArrayList<>();
                splitTimeStamp3(data, result2);

                long[] data2_arr = new long[data.size()];

                for (int i = 0; i < data.size(); i++) {
                    data2_arr[i] = data.get(i);
                }

                byte[] encoded_result = new byte[data2_arr.length * 8];
                long encodeTime = 0;
                long decodeTime = 0;
                double ratio = 0;
                double compressed_size = 0;
                int repeatTime2 = 100;
                long s = System.nanoTime();
                int length = 0;
                for (int repeat = 0; repeat < repeatTime2; repeat++)
                    length = ReorderingRegressionEncoder(data2_arr, dataset_block_size.get(file_i), dataset_third.get(file_i), pack_size, encoded_result);
                long e = System.nanoTime();
                encodeTime += ((e - s) / repeatTime2);
                compressed_size += length;
                double ratioTmp = compressed_size / (double) (data.size() * Integer.BYTES * 2);
                ratio += ratioTmp;
                s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime2; repeat++)
                    REGERDecoder(encoded_result);
                e = System.nanoTime();
                decodeTime += ((e - s) / repeatTime2);


                String[] record = {
                        f.toString(),
                        "REGER",
                        String.valueOf(encodeTime),
                        String.valueOf(decodeTime),
                        String.valueOf(data.size()),
                        String.valueOf(compressed_size),
                        String.valueOf(ratio),
//                        String.valueOf(best_order[0]),
//                        String.valueOf(best_order[1]),
//                        String.valueOf(best_order[2])
                };
                writer.writeRecord(record);
//                System.out.println(Arrays.toString(best_order));
                System.out.println(ratio);

//                break;
            }
            writer.close();
        }
    }

    @Test
    public void REGERVaryBlockSize() throws IOException {
        //        String parent_dir = "C:/Users/xiaoj/Desktop/test";
        String parent_dir =
                "/Users/xiaojinzhao/Documents/GitHub/iotdb/iotdb-core/tsfile/src/test/resources/";
        String output_parent_dir =
                "/Users/xiaojinzhao/Documents/GitHub/encoding-reorder/compression_ratio/block_size_float";
        int pack_size = 16;

        String input_parent_dir = parent_dir + "trans_data/";
        ArrayList<String> input_path_list = new ArrayList<>();
        ArrayList<String> output_path_list = new ArrayList<>();
        ArrayList<String> dataset_name = new ArrayList<>();
        ArrayList<int[]> dataset_third = new ArrayList<>();
        dataset_name.add("CS-Sensors");
        dataset_name.add("Metro-Traffic");
        dataset_name.add("USGS-Earthquakes");
        dataset_name.add("YZ-Electricity");
        dataset_name.add("GW-Magnetic");
        dataset_name.add("TY-Fuel");
        dataset_name.add("Cyber-Vehicle");
        dataset_name.add("Vehicle-Charge");
        dataset_name.add("Nifty-Stocks");
        dataset_name.add("TH-Climate");
        dataset_name.add("TY-Transport");
        dataset_name.add("EPM-Education");
        dataset_name.add("FANYP-Sensors");
        dataset_name.add("TRAJET-Transport");

        int[] dataset_0 = {547, 2816};
        int[] dataset_1 = {1719, 3731};
        int[] dataset_2 = {-48, -11, 6, 25, 52};
        int[] dataset_3 = {8681, 13584};
        int[] dataset_4 = {79, 184, 274};
        int[] dataset_5 = {17, 68};
        int[] dataset_6 = {677};
        int[] dataset_7 = {1047, 1725};
        int[] dataset_8 = {227, 499, 614, 1013};
        int[] dataset_9 = {474, 678};
        int[] dataset_10 = {4, 30, 38, 49, 58};
        int[] dataset_11 = {5182, 8206};
        int[] dataset_12 = {652477};
        int[] dataset_13 = {581388};

        dataset_third.add(dataset_0);
        dataset_third.add(dataset_1);
        dataset_third.add(dataset_2);
        dataset_third.add(dataset_3);
        dataset_third.add(dataset_4);
        dataset_third.add(dataset_5);
        dataset_third.add(dataset_6);
        dataset_third.add(dataset_7);
        dataset_third.add(dataset_8);
        dataset_third.add(dataset_9);
        dataset_third.add(dataset_10);
        dataset_third.add(dataset_11);
        dataset_third.add(dataset_12);
        dataset_third.add(dataset_13);

        for (String value : dataset_name) {
            input_path_list.add(input_parent_dir + value);
        }

        output_path_list.add(output_parent_dir + "/CS-Sensors_ratio.csv"); // 0
        output_path_list.add(output_parent_dir + "/Metro-Traffic_ratio.csv"); // 1
        output_path_list.add(output_parent_dir + "/USGS-Earthquakes_ratio.csv"); // 2
        output_path_list.add(output_parent_dir + "/YZ-Electricity_ratio.csv"); // 3
        output_path_list.add(output_parent_dir + "/GW-Magnetic_ratio.csv"); // 4
        output_path_list.add(output_parent_dir + "/TY-Fuel_ratio.csv"); // 5
        output_path_list.add(output_parent_dir + "/Cyber-Vehicle_ratio.csv"); // 6
        output_path_list.add(output_parent_dir + "/Vehicle-Charge_ratio.csv"); // 7
        output_path_list.add(output_parent_dir + "/Nifty-Stocks_ratio.csv"); // 8
        output_path_list.add(output_parent_dir + "/TH-Climate_ratio.csv"); // 9
        output_path_list.add(output_parent_dir + "/TY-Transport_ratio.csv"); // 10
        output_path_list.add(output_parent_dir + "/EPM-Education_ratio.csv"); // 11
        output_path_list.add(output_parent_dir + "/FANYP-Sensors_ratio.csv"); // 12
        output_path_list.add(output_parent_dir + "/TRAJET-Transport_ratio.csv"); // 13

//        int[] file_lists = {5,6,8,10};
//        for (int file_i : file_lists) {
        for (int file_i = 0; file_i < input_path_list.size(); file_i++) {
//        for (int file_i = 12; file_i < 14; file_i++) {
            String inputPath = input_path_list.get(file_i);
            String Output = output_path_list.get(file_i);

            File file = new File(inputPath);
            File[] tempList = file.listFiles();

            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Encoding Time",
                    "Decoding Time",
                    "Points",
                    "Compressed Size",
                    "Block Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file

            assert tempList != null;

            int count_csv =0;
            for (File f : tempList) {
                System.out.println(count_csv);
                count_csv ++;
                System.out.println(f);
                InputStream inputStream = Files.newInputStream(f.toPath());
                CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                ArrayList<Long> data = new ArrayList<>();

                // add a column to "data"
                loader.readHeaders();
                loader.readRecord();
                int time0 = Integer.parseInt(loader.getValues()[0]);
                int value0 = Integer.parseInt(loader.getValues()[1]);
                data.add(combine2Int(0, value0));

                while (loader.readRecord()) {
                    int time_tmp = Integer.parseInt(loader.getValues()[0]) - time0;
                    int value_tmp = Integer.parseInt(loader.getValues()[1]);
                    data.add(combine2Int(time_tmp, value_tmp));
                }
                ArrayList<Integer> result2 = new ArrayList<>();
                splitTimeStamp3(data, result2);


                long[] data2_arr = new long[data.size()];
                for (int i = 0; i < data.size(); i++) {
                    data2_arr[i] = data.get(i);
                }
                for (int block_size_exp = 13; block_size_exp >= 4; block_size_exp--) {
                    int block_size = (int) Math.pow(2, block_size_exp);
                    System.out.println(block_size);

                    byte[] encoded_result = new byte[data2_arr.length * 12];
                    long encodeTime = 0;
                    long decodeTime = 0;
                    double ratio = 0;
                    double compressed_size = 0;
                    int repeatTime2 = 1;
                    long s = System.nanoTime();
                    int length = 0;
                    for (int repeat = 0; repeat < repeatTime2; repeat++)
                        length =
                                ReorderingRegressionEncoder(
                                        data2_arr,
                                        block_size,
                                        dataset_third.get(file_i),
                                        pack_size,
                                        encoded_result);
                    long e = System.nanoTime();
                    encodeTime += ((e - s) / repeatTime2);
                    compressed_size += length;
                    double ratioTmp = compressed_size / (double) (data.size() * Integer.BYTES * 2);
                    ratio += ratioTmp;
                    s = System.nanoTime();
                    for (int repeat = 0; repeat < repeatTime2; repeat++) REGERDecoder(encoded_result);
                    e = System.nanoTime();
                    decodeTime += ((e - s) / repeatTime2);

                    String[] record = {
                            f.toString(),
                            "REGER-32-FLOAT",
                            String.valueOf(encodeTime),
                            String.valueOf(decodeTime),
                            String.valueOf(data.size()),
                            String.valueOf(compressed_size),
                            String.valueOf(block_size_exp),
                            String.valueOf(ratio)
                    };
                    writer.writeRecord(record);
                    System.out.println(ratio);
                }
            }
            writer.close();
        }
    }

    @Test
    public void REGERVaryPackSize() throws IOException {
        //        String parent_dir = "C:/Users/xiaoj/Desktop/test";
        String parent_dir =
                "/Users/xiaojinzhao/Documents/GitHub/iotdb/iotdb-core/tsfile/src/test/resources/";
        String output_parent_dir =
                "/Users/xiaojinzhao/Documents/GitHub/encoding-reorder/compression_ratio/pack_size_float";

        int block_size = 512;

        String input_parent_dir = parent_dir + "trans_data/";
        ArrayList<String> input_path_list = new ArrayList<>();
        ArrayList<String> output_path_list = new ArrayList<>();
        ArrayList<String> dataset_name = new ArrayList<>();
        ArrayList<Integer> dataset_block_size = new ArrayList<>();
        ArrayList<int[]> dataset_third = new ArrayList<>();
        dataset_name.add("CS-Sensors");
        dataset_name.add("Metro-Traffic");
        dataset_name.add("USGS-Earthquakes");
        dataset_name.add("YZ-Electricity");
        dataset_name.add("GW-Magnetic");
        dataset_name.add("TY-Fuel");
        dataset_name.add("Cyber-Vehicle");
        dataset_name.add("Vehicle-Charge");
        dataset_name.add("Nifty-Stocks");
        dataset_name.add("TH-Climate");
        dataset_name.add("TY-Transport");
        dataset_name.add("EPM-Education");
        dataset_name.add("FANYP-Sensors");
        dataset_name.add("TRAJET-Transport");

        int[] dataset_0 = {547, 2816};
        int[] dataset_1 = {1719, 3731};
        int[] dataset_2 = {-48, -11, 6, 25, 52};
        int[] dataset_3 = {8681, 13584};
        int[] dataset_4 = {79, 184, 274};
        int[] dataset_5 = {17, 68};
        int[] dataset_6 = {677};
        int[] dataset_7 = {1047, 1725};
        int[] dataset_8 = {227, 499, 614, 1013};
        int[] dataset_9 = {474, 678};
        int[] dataset_10 = {4, 30, 38, 49, 58};
        int[] dataset_11 = {5182, 8206};
        int[] dataset_12 = {652477};
        int[] dataset_13 = {581388};

        dataset_third.add(dataset_0);
        dataset_third.add(dataset_1);
        dataset_third.add(dataset_2);
        dataset_third.add(dataset_3);
        dataset_third.add(dataset_4);
        dataset_third.add(dataset_5);
        dataset_third.add(dataset_6);
        dataset_third.add(dataset_7);
        dataset_third.add(dataset_8);
        dataset_third.add(dataset_9);
        dataset_third.add(dataset_10);
        dataset_third.add(dataset_11);
        dataset_third.add(dataset_12);
        dataset_third.add(dataset_13);

        for (String value : dataset_name) {
            input_path_list.add(input_parent_dir + value);
            dataset_block_size.add(block_size);
        }

        output_path_list.add(output_parent_dir + "/CS-Sensors_ratio.csv"); // 0
        output_path_list.add(output_parent_dir + "/Metro-Traffic_ratio.csv"); // 1
        output_path_list.add(output_parent_dir + "/USGS-Earthquakes_ratio.csv"); // 2
        output_path_list.add(output_parent_dir + "/YZ-Electricity_ratio.csv"); // 3
        output_path_list.add(output_parent_dir + "/GW-Magnetic_ratio.csv"); // 4
        output_path_list.add(output_parent_dir + "/TY-Fuel_ratio.csv"); // 5
        output_path_list.add(output_parent_dir + "/Cyber-Vehicle_ratio.csv"); // 6
        output_path_list.add(output_parent_dir + "/Vehicle-Charge_ratio.csv"); // 7
        output_path_list.add(output_parent_dir + "/Nifty-Stocks_ratio.csv"); // 8
        output_path_list.add(output_parent_dir + "/TH-Climate_ratio.csv"); // 9
        output_path_list.add(output_parent_dir + "/TY-Transport_ratio.csv"); // 10
        output_path_list.add(output_parent_dir + "/EPM-Education_ratio.csv"); // 11
        output_path_list.add(output_parent_dir + "/FANYP-Sensors_ratio.csv"); // 12
        output_path_list.add(output_parent_dir + "/TRAJET-Transport_ratio.csv"); // 13

        for (int file_i = 0; file_i < input_path_list.size(); file_i++) {
//        for (int file_i = 12; file_i < 14; file_i++) {
            String inputPath = input_path_list.get(file_i);
            String Output = output_path_list.get(file_i);

            File file = new File(inputPath);
            File[] tempList = file.listFiles();

            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Encoding Time",
                    "Decoding Time",
                    "Points",
                    "Compressed Size",
                    "Block Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file

            assert tempList != null;

            int count_csv =0;
            for (File f : tempList) {
                System.out.println(count_csv);
                count_csv ++;
                System.out.println(f);
                InputStream inputStream = Files.newInputStream(f.toPath());
                CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                ArrayList<Long> data = new ArrayList<>();

                // add a column to "data"
                loader.readHeaders();
                loader.readRecord();
                int time0 = Integer.parseInt(loader.getValues()[0]);
                int value0 = Integer.parseInt(loader.getValues()[1]);
                data.add(combine2Int(0, value0));

                while (loader.readRecord()) {
                    int time_tmp = Integer.parseInt(loader.getValues()[0]) - time0;
                    int value_tmp = Integer.parseInt(loader.getValues()[1]);
                    data.add(combine2Int(time_tmp, value_tmp));
                }
                ArrayList<Integer> result2 = new ArrayList<>();
                splitTimeStamp3(data, result2);


                long[] data2_arr = new long[data.size()];
                for (int i = 0; i < data.size(); i++) {
                    data2_arr[i] = data.get(i);
                }
                for (int segment_size_exp = 8; segment_size_exp > 2; segment_size_exp--) {
                    int segment_size = (int) Math.pow(2, segment_size_exp);
                    System.out.println(segment_size);

                    byte[] encoded_result = new byte[data2_arr.length * 12];
                    long encodeTime = 0;
                    long decodeTime = 0;
                    double ratio = 0;
                    double compressed_size = 0;
                    int repeatTime2 = 1;
                    long s = System.nanoTime();
                    int length = 0;
                    for (int repeat = 0; repeat < repeatTime2; repeat++)
                        length =
                                ReorderingRegressionEncoder(
                                        data2_arr,
                                        dataset_block_size.get(file_i),
                                        dataset_third.get(file_i),
                                        segment_size,
                                        encoded_result);
                    long e = System.nanoTime();
                    encodeTime += ((e - s) / repeatTime2);
                    compressed_size += length;
                    double ratioTmp = compressed_size / (double) (data.size() * Integer.BYTES * 2);
                    ratio += ratioTmp;
                    s = System.nanoTime();
                    for (int repeat = 0; repeat < repeatTime2; repeat++) REGERDecoder(encoded_result);
                    e = System.nanoTime();
                    decodeTime += ((e - s) / repeatTime2);

                    String[] record = {
                            f.toString(),
                            "REGER-32-FLOAT",
                            String.valueOf(encodeTime),
                            String.valueOf(decodeTime),
                            String.valueOf(data.size()),
                            String.valueOf(compressed_size),
                            String.valueOf(segment_size_exp),
                            String.valueOf(ratio)
                    };
                    writer.writeRecord(record);
                    System.out.println(ratio);
                }
            }
            writer.close();
        }
    }


}
