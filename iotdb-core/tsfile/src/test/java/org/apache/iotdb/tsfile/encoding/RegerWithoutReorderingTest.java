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
import java.util.Objects;

public class RegerWithoutReorderingTest {
  public static int getBitWith(int num) {
    if (num == 0) return 1;
    else return 32 - Integer.numberOfLeadingZeros(num);
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
    if (value == 0) return 256;
    return value % 256;
  }

  public static void pack8Values(
      ArrayList<Integer> values, int offset, int width, int encode_pos, byte[] encoded_result) {
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
        buffer |= (values.get(valueIdx) << (32 - leftBit));
        leftSize -= leftBit;
        leftBit = 0;
        valueIdx++;
      }

      while (leftSize >= width && valueIdx < 8 + offset) {
        // encode one Integer to the 'buffer'
        buffer |= (values.get(valueIdx) << (leftSize - width));
        leftSize -= width;
        valueIdx++;
      }
      // If the remaining space of the buffer can not save the bits for one Integer,
      if (leftSize > 0 && valueIdx < 8 + offset) {
        // put the first 'leftSize' bits of the Integer into remaining space of the
        // buffer
        buffer |= (values.get(valueIdx) >>> (width - leftSize));
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

  public static void pack8Values(
      int[][] values, int index, int offset, int width, int encode_pos, byte[] encoded_result) {
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
        buffer |= (values[valueIdx][index] << (32 - leftBit));
        leftSize -= leftBit;
        leftBit = 0;
        valueIdx++;
      }

      while (leftSize >= width && valueIdx < 8 + offset) {
        // encode one Integer to the 'buffer'
        buffer |= (values[valueIdx][index] << (leftSize - width));
        leftSize -= width;
        valueIdx++;
      }
      // If the remaining space of the buffer can not save the bits for one Integer,
      if (leftSize > 0 && valueIdx < 8 + offset) {
        // put the first 'leftSize' bits of the Integer into remaining space of the
        // buffer
        buffer |= (values[valueIdx][index] >>> (width - leftSize));
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

  public static int unpack8Values(
      byte[] encoded, int offset, int width, int value_pos, int[] result_list) {
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

  public static int bitPacking(
      int[][] numbers,
      int index,
      int start,
      int block_size,
      int bit_width,
      int encode_pos,
      byte[] encoded_result) {
    int block_num = block_size / 8;
    for (int i = 0; i < block_num; i++) {
      pack8Values(numbers, index, start + i * 8, bit_width, encode_pos, encoded_result);
      encode_pos += bit_width;
    }

    return encode_pos;
  }

  public static int decodeBitPacking(
      byte[] encoded, int decode_pos, int bit_width, int block_size, int[] result_list) {
    //        int[] result_list = new int[];
    int block_num = block_size / 8;
    int value_pos = 0;

    for (int i = 0; i < block_num; i++) { // bitpacking
      value_pos = unpack8Values(encoded, decode_pos, bit_width, value_pos, result_list);
      decode_pos += bit_width;
    }
    //        decode_pos_result.add(decode_pos);
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

  public static int bytes2Integer(ArrayList<Byte> encoded, int start, int num) {
    int value = 0;
    if (num > 4) {
      System.out.println("bytes2Integer error");
      return 0;
    }
    for (int i = 0; i < num; i++) {
      value <<= 8;
      int b = encoded.get(i + start) & 0xFF;
      value |= b;
    }
    return value;
  }

  public static int part(ArrayList<ArrayList<Integer>> arr, int index, int low, int high) {
    ArrayList<Integer> tmp = arr.get(low);
    while (low < high) {
      while (low < high
          && (arr.get(high).get(index) > tmp.get(index)
              || (Objects.equals(arr.get(high).get(index), tmp.get(index))
                  && arr.get(high).get(index ^ 1) >= tmp.get(index ^ 1)))) {
        high--;
      }
      arr.set(low, arr.get(high));
      while (low < high
          && (arr.get(low).get(index) < tmp.get(index)
              || (Objects.equals(arr.get(low).get(index), tmp.get(index))
                  && arr.get(low).get(index ^ 1) <= tmp.get(index ^ 1)))) {
        low++;
      }
      arr.set(high, arr.get(low));
    }
    arr.set(low, tmp);
    return low;
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
      ArrayList<ArrayList<Integer>> ts_block, ArrayList<Integer> result) {
    int td_common = 0;
    for (int i = 1; i < ts_block.size(); i++) {
      int time_diffi = ts_block.get(i).get(0) - ts_block.get(i - 1).get(0);
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

    int t0 = ts_block.get(0).get(0);
    for (int i = 0; i < ts_block.size(); i++) {
      ArrayList<Integer> tmp = new ArrayList<>();
      int interval_i = (ts_block.get(i).get(0) - t0) / td_common;
      tmp.add(t0 + interval_i);
      tmp.add(ts_block.get(i).get(1));
      ts_block.set(i, tmp);
    }
    result.add(td_common);
  }

  // ------------------------------------------------basic
  // function-------------------------------------------------------
  public static int[][] getEncodeBitsRegression(
      int[][] ts_block, int block_size, int[] raw_length, float[] theta) {
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    int[][] ts_block_delta = new int[ts_block.length][2];
    //        theta = new float[4];

    long sum_X_r = 0;
    long sum_Y_r = 0;
    long sum_squ_X_r = 0;
    long sum_squ_XY_r = 0;
    long sum_X_v = 0;
    long sum_Y_v = 0;
    long sum_squ_X_v = 0;
    long sum_squ_XY_v = 0;

    for (int i = 1; i < block_size; i++) {
      sum_X_r += ts_block[i - 1][0];
      sum_X_v += ts_block[i - 1][1];
      sum_Y_r += ts_block[i][0];
      sum_Y_v += ts_block[i][1];
      sum_squ_X_r += ((long) (ts_block[i - 1][0]) * (ts_block[i - 1][0]));
      sum_squ_X_v += ((long) ts_block[i - 1][1] * ts_block[i - 1][1]);
      sum_squ_XY_r += ((long) (ts_block[i - 1][0]) * (ts_block[i][0]));
      sum_squ_XY_v += ((long) ts_block[i - 1][1] * ts_block[i][1]);
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

    ts_block_delta[0][0] = ts_block[0][0];
    ts_block_delta[0][1] = ts_block[0][1];

    // delta to Regression
    for (int j = 1; j < block_size; j++) {

      int epsilon_r = (ts_block[j][0] - (int) (theta0_r + theta1_r * (float) ts_block[j - 1][0]));
      int epsilon_v = (ts_block[j][1] - (int) (theta0_v + theta1_v * (float) ts_block[j - 1][1]));

      if (epsilon_r < timestamp_delta_min) {
        timestamp_delta_min = epsilon_r;
      }
      if (epsilon_v < value_delta_min) {
        value_delta_min = epsilon_v;
      }
      ts_block_delta[j][0] = epsilon_r;
      ts_block_delta[j][1] = epsilon_v;
    }

    int max_interval = Integer.MIN_VALUE;
    int max_value = Integer.MIN_VALUE;
    int length = 0;
    for (int j = block_size - 1; j > 0; j--) {
      ts_block_delta[j][0] -= timestamp_delta_min;
      int epsilon_r = ts_block_delta[j][0];
      ts_block_delta[j][1] -= value_delta_min;
      int epsilon_v = ts_block_delta[j][1];

      length += getBitWith(ts_block_delta[j][0]);
      length += getBitWith(ts_block_delta[j][1]);

      if (epsilon_r > max_interval) {
        max_interval = epsilon_r;
      }
      if (epsilon_v > max_value) {
        max_value = epsilon_v;
      }
    }

    int max_bit_width_interval = getBitWith(max_interval);
    int max_bit_width_value = getBitWith(max_value);

    //        raw_length = new int[5];
    raw_length[0] = length;
    raw_length[1] = max_bit_width_interval;
    raw_length[2] = max_bit_width_value;
    raw_length[3] = timestamp_delta_min;
    raw_length[4] = value_delta_min;

    theta[0] = theta0_r;
    theta[1] = theta1_r;
    theta[2] = theta0_v;
    theta[3] = theta1_v;

    return ts_block_delta;
  }

  public static int encodeRLEBitWidth2Bytes(int[][] bit_width_segments) {
    int encoded_result = 0;

    int count_of_time = 1;
    int count_of_value = 1;
    int pre_time = bit_width_segments[0][0];
    int pre_value = bit_width_segments[0][1];
    int size = bit_width_segments.length;
    int[][] run_length_time = new int[size][2];
    int[][] run_length_value = new int[size][2];

    int pos_time = 0;
    int pos_value = 0;

    for (int i = 1; i < size; i++) {
      int cur_time = bit_width_segments[i][0];
      int cur_value = bit_width_segments[i][1];
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
          count_of_time = 1;
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

    encoded_result += (pos_time * 2);
    encoded_result += (pos_value * 2);

    return encoded_result;
  }

  public static int encodeRLEBitWidth2Bytes(
      int[][] bit_width_segments, int pos_encode, byte[] encoded_result) {

    int count_of_time = 1;
    int count_of_value = 1;
    int pre_time = bit_width_segments[0][0];
    int pre_value = bit_width_segments[0][1];
    int size = bit_width_segments.length;
    int[][] run_length_time = new int[size][2];
    int[][] run_length_value = new int[size][2];

    int pos_time = 0;
    int pos_value = 0;

    for (int i = 1; i < size; i++) {
      int cur_time = bit_width_segments[i][0];
      int cur_value = bit_width_segments[i][1];
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

      //            System.out.println("bit_width_time[0]="+bit_width_time[0]);
      //            System.out.println("bit_width_time[1]="+bit_width_time[1]);
    }
    for (int i = 0; i < pos_value; i++) {
      int[] bit_width_value = run_length_value[i];
      intByte2Bytes(bit_width_value[0], pos_encode, encoded_result);
      pos_encode++;
      intByte2Bytes(bit_width_value[1], pos_encode, encoded_result);
      pos_encode++;

      //            System.out.println("bit_width_value[0]="+bit_width_value[0]);
      //            System.out.println("bit_width_value[1]="+bit_width_value[1]);
    }

    return pos_encode;
  }

  public static int[][] segmentBitPacking(
      int[][] ts_block_delta, int block_size, int segment_size) {

    int segment_n = (block_size - 1) / segment_size;
    int[][] bit_width_segments = new int[segment_n][2];
    for (int segment_i = 0; segment_i < segment_n; segment_i++) {
      int bit_width_time = Integer.MIN_VALUE;
      int bit_width_value = Integer.MIN_VALUE;

      for (int data_i = segment_i * segment_size + 1;
          data_i < (segment_i + 1) * segment_size + 1;
          data_i++) {
        int cur_bit_width_time = getBitWith(ts_block_delta[data_i][0]);
        int cur_bit_width_value = getBitWith(ts_block_delta[data_i][1]);
        if (cur_bit_width_time > bit_width_time) {
          bit_width_time = cur_bit_width_time;
        }
        if (cur_bit_width_value > bit_width_value) {
          bit_width_value = cur_bit_width_value;
        }
      }
      bit_width_segments[segment_i][0] = bit_width_time;
      bit_width_segments[segment_i][1] = bit_width_value;
    }
    return bit_width_segments;
  }

  private static int numberOfEncodeSegment2Bytes(
      int[][] delta_segments, int[][] bit_width_segments, int segment_size) {
    int block_size = delta_segments.length;
    int segment_n = block_size / segment_size;
    int result = 0;
    result += 8; // encode interval0 and value0
    result += 16; // encode theta
    result += encodeRLEBitWidth2Bytes(bit_width_segments);

    for (int segment_i = 0; segment_i < segment_n; segment_i++) {
      int bit_width_time = bit_width_segments[segment_i][0];
      int bit_width_value = bit_width_segments[segment_i][1];
      result += (segment_size * bit_width_time / 8);
      result += (segment_size * bit_width_value / 8);
    }

    return result;
  }

  private static int encodeSegment2Bytes(
      int[][] delta_segments,
      int[][] bit_width_segments,
      int[] raw_length,
      int segment_size,
      float[] theta,
      int pos_encode,
      byte[] encoded_result) {

    int block_size = delta_segments.length;
    int segment_n = block_size / segment_size;
    int2Bytes(delta_segments[0][0], pos_encode, encoded_result);
    pos_encode += 4;
    int2Bytes(delta_segments[0][1], pos_encode, encoded_result);
    pos_encode += 4;
    float2bytes(theta[0] + raw_length[3], pos_encode, encoded_result);
    pos_encode += 4;
    float2bytes(theta[1], pos_encode, encoded_result);
    pos_encode += 4;
    float2bytes(theta[2] + raw_length[4], pos_encode, encoded_result);
    pos_encode += 4;
    float2bytes(theta[3], pos_encode, encoded_result);
    pos_encode += 4;

    pos_encode = encodeRLEBitWidth2Bytes(bit_width_segments, pos_encode, encoded_result);
//    System.out.println(Arrays.deepToString(bit_width_segments));
//    System.out.println("theta:"+ Arrays.toString(theta));
//    System.out.println(pos_encode);
//    System.out.println("encodeRLEBitWidth2Bytes:"+ Arrays.deepToString(bit_width_segments));
    for (int segment_i = 0; segment_i < segment_n; segment_i++) {
      int bit_width_time = bit_width_segments[segment_i][0];
      int bit_width_value = bit_width_segments[segment_i][1];
      pos_encode =
          bitPacking(
              delta_segments,
              0,
              segment_i * segment_size + 1,
              segment_size,
              bit_width_time,
              pos_encode,
              encoded_result);
      pos_encode =
          bitPacking(
              delta_segments,
              1,
              segment_i * segment_size + 1,
              segment_size,
              bit_width_value,
              pos_encode,
              encoded_result);
    }
    //        System.out.println("value pos_encode:"+pos_encode);
    return pos_encode;
  }

  private static int REGERBlockEncoder(
      int[][] data,
      int i,
      int block_size,
      int supply_length,
      int segment_size,
      int encode_pos,
      byte[] cur_byte) {

    int min_time = data[i * block_size][0];
    int[][] ts_block;
    if (supply_length == 0) {
      ts_block = new int[block_size][2];
      for (int j = 0; j < block_size; j++) {
        //                data[j + i * block_size][0] -= min_time;
        ts_block[j][0] = (data[j + i * block_size][0] - min_time);
        ts_block[j][1] = data[j + i * block_size][1];
      }
    } else {
      ts_block = new int[supply_length][2];
      int end = data.length - i * block_size;
      for (int j = 0; j < end; j++) {
        //                data[j + i * block_size][0] -= min_time;
        ts_block[j][0] = (data[j + i * block_size][0] - min_time);
        ts_block[j][1] = data[j + i * block_size][1];
      }
      for (int j = end; j < supply_length; j++) {
        ts_block[j][0] = 0;
        ts_block[j][1] = 0;
      }
      block_size = supply_length;
    }

    int[] time_length =
        new int[5]; // length,max_bit_width_interval,max_bit_width_value,max_bit_width_deviation
    float[] theta_time = new float[4];
    int[][] ts_block_delta_time;
    int[][] bit_width_segments_time;

    ts_block_delta_time = getEncodeBitsRegression(ts_block, block_size, time_length, theta_time);
    bit_width_segments_time = segmentBitPacking(ts_block_delta_time, block_size, segment_size);
//    System.out.println(Arrays.toString(time_length));
    //    System.out.println(Arrays.deepToString(ts_block));
//    time_length[0] =
//        numberOfEncodeSegment2Bytes(ts_block_delta_time, bit_width_segments_time, segment_size);

    encode_pos =
        encodeSegment2Bytes(
            ts_block_delta_time,
            bit_width_segments_time,
            time_length,
            segment_size,
            theta_time,
            encode_pos,
            cur_byte);

//    System.out.println(encode_pos);
    return encode_pos;
  }

  public static int ReorderingRegressionEncoder(
      int[][] data, int block_size, int segment_size, byte[] encoded_result) {
    block_size++;

    int length_all = data.length;
    int encode_pos = 0;
    int2Bytes(length_all, encode_pos, encoded_result);
    encode_pos += 4;

    int block_num = length_all / block_size;
    int2Bytes(block_size, encode_pos, encoded_result);
    encode_pos += 4;

    int2Bytes(segment_size, encode_pos, encoded_result);
    encode_pos += 4;

    int remaining_length = length_all - block_num * block_size;

    for (int i = 0; i < block_num; i++) {
//    for (int i = 44; i < 45; i++) {
    encode_pos =
          REGERBlockEncoder(data, i, block_size, 0, segment_size, encode_pos, encoded_result);
      //            System.out.println(encode_pos);
    }

    if (remaining_length == 1) {
      int2Bytes(data[data.length - 1][0], encode_pos, encoded_result);
      encode_pos += 4;
      int2Bytes(data[data.length - 1][1], encode_pos, encoded_result);
      encode_pos += 4;
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
      encode_pos =
          REGERBlockEncoder(
              data,
              block_num,
              block_size,
              supple_length + remaining_length,
              segment_size,
              encode_pos,
              encoded_result);
    }
    return encode_pos;
  }

  public static int REGERBlockDecoder(
      byte[] encoded,
      int decode_pos,
      int[][] value_list,
      int block_size,
      int segment_size,
      int[] value_pos_arr) {

    int time0 = bytes2Integer(encoded, decode_pos, 4);
    decode_pos += 4;
    value_list[value_pos_arr[0]][0] = time0;
    int value0 = bytes2Integer(encoded, decode_pos, 4);
    decode_pos += 4;
    value_list[value_pos_arr[0]][0] = value0;

    value_pos_arr[0]++;

    float theta_time0 = bytes2float(encoded, decode_pos);
    decode_pos += 4;
    float theta_time1 = bytes2float(encoded, decode_pos);
    decode_pos += 4;

    float theta_value0 = bytes2float(encoded, decode_pos);
    decode_pos += 4;
    float theta_value1 = bytes2float(encoded, decode_pos);
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

      decode_pos =
          decodeBitPacking(encoded, decode_pos, bit_width_time, segment_size, decode_time_result);
      int pos_time = value_pos_arr[0];
      for (int delta_time : decode_time_result) {
        pre_time = (int) (theta_time0 + theta_time1 * (double) delta_time) + pre_time;
        value_list[pos_time][0] = pre_time;
        pos_time++;
      }
      int pos_value = value_pos_arr[0];
      decode_pos =
          decodeBitPacking(encoded, decode_pos, bit_width_value, segment_size, decode_value_result);
      for (int delta_value : decode_value_result) {
        pre_value = (int) (theta_value0 + theta_value1 * (double) delta_value) + pre_value;
        value_list[pos_value][1] = pre_value;
        pos_value++;
      }
      value_pos_arr[0] = pos_value;
    }

    return decode_pos;
  }

  public static void REGERDecoder(byte[] encoded) {

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
    if (remain_length % segment_size == 0) {
      zero_number = 1;
    } else if (remain_length % segment_size == 1) {
      zero_number = 0;
    } else {
      zero_number = segment_size + 1 - remain_length % segment_size;
    }
    int[][] value_list = new int[length_all + segment_size][2];

    int[] value_pos_arr = new int[1];

    //        for (int k = 0; k < 2; k++) {
    for (int k = 0; k < block_num; k++) {
      //            System.out.println("k="+k);
      decode_pos =
          REGERBlockDecoder(
              encoded, decode_pos, value_list, block_size, segment_size, value_pos_arr);
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
      REGERBlockDecoder(
          encoded,
          decode_pos,
          value_list,
          remain_length + zero_number,
          segment_size,
          value_pos_arr);
    }
  }

  @Test
  public void REGERWithoutReordering() throws IOException {
    //        String parent_dir = "C:/Users/xiaoj/Desktop/test";
    String parent_dir =
        "/Users/xiaojinzhao/Documents/GitHub/iotdb/iotdb-core/tsfile/src/test/resources/";
    String output_parent_dir =
        "/Users/xiaojinzhao/Documents/GitHub/encoding-reorder/compression_ratio/reger_only_segment";

    String input_parent_dir = parent_dir + "trans_data/";
    ArrayList<String> input_path_list = new ArrayList<>();
    ArrayList<String> output_path_list = new ArrayList<>();
    ArrayList<String> dataset_name = new ArrayList<>();
    ArrayList<Integer> dataset_block_size = new ArrayList<>();

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

    for (String value : dataset_name) {
      input_path_list.add(input_parent_dir + value);
      dataset_block_size.add(512);
    }

    output_path_list.add(output_parent_dir + "/CS-Sensors_ratio.csv"); // 0
    //        dataset_block_size.add(128);

    output_path_list.add(output_parent_dir + "/Metro-Traffic_ratio.csv"); // 1
    //        dataset_block_size.add(4096);
    output_path_list.add(output_parent_dir + "/USGS-Earthquakes_ratio.csv"); // 2
    //        dataset_block_size.add(8192);
    output_path_list.add(output_parent_dir + "/YZ-Electricity_ratio.csv"); // 3
    //        dataset_block_size.add(8192);
    output_path_list.add(output_parent_dir + "/GW-Magnetic_ratio.csv"); // 4

    //        dataset_block_size.add(2048);
    output_path_list.add(output_parent_dir + "/TY-Fuel_ratio.csv"); // 5
    //        dataset_block_size.add(8192);
    output_path_list.add(output_parent_dir + "/Cyber-Vehicle_ratio.csv"); // 6
    //        dataset_block_size.add(2048);
    output_path_list.add(output_parent_dir + "/Vehicle-Charge_ratio.csv"); // 7
    //        dataset_block_size.add(2048);
    output_path_list.add(output_parent_dir + "/Nifty-Stocks_ratio.csv"); // 8
    //        dataset_block_size.add(128);
    output_path_list.add(output_parent_dir + "/TH-Climate_ratio.csv"); // 9
    //        dataset_block_size.set(9,8192);
    //        dataset_block_size.add(64);
    output_path_list.add(output_parent_dir + "/TY-Transport_ratio.csv"); // 10
    //        dataset_block_size.add(64);
    output_path_list.add(output_parent_dir + "/EPM-Education_ratio.csv"); // 11
    //        dataset_block_size.add(256);

//    for (int file_i = 0; file_i < input_path_list.size(); file_i++) {
              for (int file_i = 2; file_i < 3; file_i++) {
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
        "Compression Ratio"
      };
      writer.writeRecord(head); // write header to output file

      assert tempList != null;

      for (File f : tempList) {
//        f = tempList[2];
        System.out.println(f);
        InputStream inputStream = Files.newInputStream(f.toPath());
        CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
        ArrayList<ArrayList<Integer>> data = new ArrayList<>();

        // add a column to "data"
        loader.readHeaders();
        while (loader.readRecord()) {
          ArrayList<Integer> tmp = new ArrayList<>();
          tmp.add(Integer.valueOf(loader.getValues()[0]));
          tmp.add(Integer.valueOf(loader.getValues()[1]));
          data.add(tmp);
        }
        inputStream.close();
        ArrayList<Integer> result2 = new ArrayList<>();
        splitTimeStamp3(data, result2);

        int[][] data2_arr = new int[data.size()][2];
        int min_time = data.get(0).get(0);
        for (int i = 0; i < data.size(); i++) {
          data2_arr[i][0] = data.get(i).get(0) - min_time;
          data2_arr[i][1] = data.get(i).get(1);
        }
//        System.out.println(data2_arr[0][0]);
        byte[] encoded_result = new byte[data2_arr.length * 8];
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
                  data2_arr, dataset_block_size.get(file_i), 8, encoded_result);
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
          "REGER-Without-Reordering",
          String.valueOf(encodeTime),
          String.valueOf(decodeTime),
          String.valueOf(data.size()),
          String.valueOf(compressed_size),
          String.valueOf(ratio)
        };
        writer.writeRecord(record);
        System.out.println(ratio);

                        break;
      }
      writer.close();
    }
  }

  @Test
  public void REGERWithoutReorderingVaryBlockSize() throws IOException {
    //        String parent_dir = "C:/Users/xiaoj/Desktop/test";
    String parent_dir =
        "/Users/xiaojinzhao/Documents/GitHub/iotdb/iotdb-core/tsfile/src/test/resources/";
    String output_parent_dir =
        "/Users/xiaojinzhao/Documents/GitHub/encoding-reorder/compression_ratio/block_size_only_segment";

    String input_parent_dir = parent_dir + "trans_data/";
    ArrayList<String> input_path_list = new ArrayList<>();
    ArrayList<String> output_path_list = new ArrayList<>();
    ArrayList<String> dataset_name = new ArrayList<>();

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

    for (String value : dataset_name) {
      input_path_list.add(input_parent_dir + value);
    }

    output_path_list.add(output_parent_dir + "/CS-Sensors_ratio.csv"); // 0
    //        dataset_block_size.add(1024);

    output_path_list.add(output_parent_dir + "/Metro-Traffic_ratio.csv"); // 1
    //        dataset_block_size.add(512);
    output_path_list.add(output_parent_dir + "/USGS-Earthquakes_ratio.csv"); // 2
    //        dataset_block_size.add(512);
    output_path_list.add(output_parent_dir + "/YZ-Electricity_ratio.csv"); // 3
    //        dataset_block_size.add(256);
    output_path_list.add(output_parent_dir + "/GW-Magnetic_ratio.csv"); // 4
    //        dataset_block_size.add(128);
    output_path_list.add(output_parent_dir + "/TY-Fuel_ratio.csv"); // 5
    //        dataset_block_size.add(64);
    output_path_list.add(output_parent_dir + "/Cyber-Vehicle_ratio.csv"); // 6
    //        dataset_block_size.add(128);
    output_path_list.add(output_parent_dir + "/Vehicle-Charge_ratio.csv"); // 7
    //        dataset_block_size.add(512);
    output_path_list.add(output_parent_dir + "/Nifty-Stocks_ratio.csv"); // 8
    //        dataset_block_size.add(256);
    output_path_list.add(output_parent_dir + "/TH-Climate_ratio.csv"); // 9
    //        dataset_block_size.add(512);
    output_path_list.add(output_parent_dir + "/TY-Transport_ratio.csv"); // 10
    //        dataset_block_size.add(512);
    output_path_list.add(output_parent_dir + "/EPM-Education_ratio.csv"); // 11
    //        dataset_block_size.add(512);

    int[] file_lists = {0,2,11};
    for (int file_i : file_lists) {
//    for (int file_i = 0; file_i < input_path_list.size(); file_i++) {
      //        for (int file_i = 9; file_i < 10; file_i++) {
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

      for (File f : tempList) {
        System.out.println(f);
        InputStream inputStream = Files.newInputStream(f.toPath());
        CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
        ArrayList<ArrayList<Integer>> data = new ArrayList<>();

        // add a column to "data"
        loader.readHeaders();
        while (loader.readRecord()) {
          ArrayList<Integer> tmp = new ArrayList<>();
          tmp.add(Integer.valueOf(loader.getValues()[0]));
          tmp.add(Integer.valueOf(loader.getValues()[1]));
          data.add(tmp);
        }
        inputStream.close();
        ArrayList<Integer> result2 = new ArrayList<>();
        splitTimeStamp3(data, result2);

        int[][] data2_arr = new int[data.size()][2];
        int min_time = data.get(0).get(0);
        for (int i = 0; i < data.size(); i++) {
          data2_arr[i][0] = data.get(i).get(0) - min_time;
          data2_arr[i][1] = data.get(i).get(1);
        }
//        System.out.println(data2_arr[0][0]);
        //                for (int block_size_exp = 13; block_size_exp >= 12; block_size_exp--) {
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
            length = ReorderingRegressionEncoder(data2_arr, block_size, 8, encoded_result);
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
            "REGER-Without-Reordering",
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
  public void REGERWithoutReorderingVaryPackSize() throws IOException {
    //        String parent_dir = "C:/Users/xiaoj/Desktop/test";
    String parent_dir =
        "/Users/xiaojinzhao/Documents/GitHub/iotdb/iotdb-core/tsfile/src/test/resources/";
    String output_parent_dir =
        "/Users/xiaojinzhao/Documents/GitHub/encoding-reorder/compression_ratio/pack_size_only_segment";

    String input_parent_dir = parent_dir + "trans_data/";
    ArrayList<String> input_path_list = new ArrayList<>();
    ArrayList<String> output_path_list = new ArrayList<>();
    ArrayList<String> dataset_name = new ArrayList<>();
    ArrayList<Integer> dataset_block_size = new ArrayList<>();
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

    for (String value : dataset_name) {
      input_path_list.add(input_parent_dir + value);
      dataset_block_size.add(1024);
    }

    output_path_list.add(output_parent_dir + "/CS-Sensors_ratio.csv"); // 0
    //        dataset_block_size.add(1024);

    output_path_list.add(output_parent_dir + "/Metro-Traffic_ratio.csv"); // 1
    //        dataset_block_size.add(512);
    output_path_list.add(output_parent_dir + "/USGS-Earthquakes_ratio.csv"); // 2
    //        dataset_block_size.add(512);
    output_path_list.add(output_parent_dir + "/YZ-Electricity_ratio.csv"); // 3
    //        dataset_block_size.add(256);
    output_path_list.add(output_parent_dir + "/GW-Magnetic_ratio.csv"); // 4
    //        dataset_block_size.add(128);
    output_path_list.add(output_parent_dir + "/TY-Fuel_ratio.csv"); // 5
    //        dataset_block_size.add(64);
    output_path_list.add(output_parent_dir + "/Cyber-Vehicle_ratio.csv"); // 6
    //        dataset_block_size.add(128);
    output_path_list.add(output_parent_dir + "/Vehicle-Charge_ratio.csv"); // 7
    //        dataset_block_size.add(512);
    output_path_list.add(output_parent_dir + "/Nifty-Stocks_ratio.csv"); // 8
    //        dataset_block_size.add(256);
    output_path_list.add(output_parent_dir + "/TH-Climate_ratio.csv"); // 9
    //        dataset_block_size.add(512);
    output_path_list.add(output_parent_dir + "/TY-Transport_ratio.csv"); // 10
    //        dataset_block_size.add(512);
    output_path_list.add(output_parent_dir + "/EPM-Education_ratio.csv"); // 11
    //        dataset_block_size.add(512);

    for (int file_i = 0; file_i < input_path_list.size(); file_i++) {
      //        for (int file_i = 6; file_i < input_path_list.size(); file_i++) {
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

      for (File f : tempList) {
        System.out.println(f);
        InputStream inputStream = Files.newInputStream(f.toPath());
        CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
        ArrayList<ArrayList<Integer>> data = new ArrayList<>();

        // add a column to "data"
        loader.readHeaders();
        while (loader.readRecord()) {
          ArrayList<Integer> tmp = new ArrayList<>();
          tmp.add(Integer.valueOf(loader.getValues()[0]));
          tmp.add(Integer.valueOf(loader.getValues()[1]));
          data.add(tmp);
        }
        inputStream.close();
        ArrayList<Integer> result2 = new ArrayList<>();
        splitTimeStamp3(data, result2);

        int[][] data2_arr = new int[data.size()][2];
        int min_time = data.get(0).get(0);
        for (int i = 0; i < data.size(); i++) {
          data2_arr[i][0] = data.get(i).get(0) - min_time;
          data2_arr[i][1] = data.get(i).get(1);
        }
        System.out.println(data2_arr[0][0]);
        for (int segment_size_exp = 6; segment_size_exp > 2; segment_size_exp--) {
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
                    data2_arr, dataset_block_size.get(file_i), segment_size, encoded_result);
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
            "REGER-Without-Reordering",
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
