package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;

import static java.lang.Math.abs;
import static java.lang.Math.pow;

public class Reger {
  public static int getBitWith(int num) {
    if (num == 0) return 1;
    else return 32 - Integer.numberOfLeadingZeros(num);
  }
  public static boolean containsValue(int[] array, int targetValue) {
    for (int value : array) {
      if (value == targetValue) {
        return true;
      }
    }
    return false;
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

  public static byte[] int2Bytes(int integer) {
    byte[] bytes = new byte[4];
    bytes[0] = (byte) (integer >> 24);
    bytes[1] = (byte) (integer >> 16);
    bytes[2] = (byte) (integer >> 8);
    bytes[3] = (byte) integer;
    return bytes;
  }
  public static void int2Bytes(int integer,int encode_pos , byte[] cur_byte) {
    cur_byte[encode_pos] = (byte) (integer >> 24);
    cur_byte[encode_pos+1] = (byte) (integer >> 16);
    cur_byte[encode_pos+2] = (byte) (integer >> 8);
    cur_byte[encode_pos+3] = (byte) (integer);
  }
  public static void intByte2Bytes(int integer, int encode_pos , byte[] cur_byte) {
    cur_byte[encode_pos] = (byte) (integer);
  }

  private static void long2intBytes(long integer, int encode_pos , byte[] cur_byte) {
    cur_byte[encode_pos] = (byte) (integer >> 24);
    cur_byte[encode_pos+1] = (byte) (integer >> 16);
    cur_byte[encode_pos+2] = (byte) (integer >> 8);
    cur_byte[encode_pos+3] = (byte) (integer);
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

  private static long bytesLong2Integer(byte[] encoded, int decode_pos) {
    long value = 0;
    for (int i = 0; i < 4; i++) {
      value <<= 8;
      int b = encoded[i + decode_pos] & 0xFF;
      value |= b;
    }
    return value;
  }

  public static void pack8Values(ArrayList<Integer> values, int offset, int width, int encode_pos,  byte[] encoded_result) {
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
        buffer |= (values.get(valueIdx)<< (leftSize - width));
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
        encode_pos ++;
        bufIdx++;
        if (bufIdx >= width) {
          return ;
        }
      }
    }
//        return encode_pos;
  }

  public static void unpack8Values(byte[] encoded, int offset,int width,  ArrayList<Integer> result_list) {
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
        result_list.add ((int) (buffer >>> (totalBits - width)));
        valueIdx++;
        totalBits -= width;
        buffer = buffer & ((1L << totalBits) - 1);
      }
    }
  }

  public static int bitPacking(ArrayList<Integer> numbers, int start, int bit_width,int encode_pos,  byte[] encoded_result) {
    int block_num = (numbers.size()-start) / 8;
    for(int i=0;i<block_num;i++){
      pack8Values( numbers, start+i*8, bit_width,encode_pos, encoded_result);
      encode_pos +=bit_width;
    }

    return encode_pos;

  }

  public static ArrayList<Integer> decodeBitPacking(
          byte[] encoded, int decode_pos, int bit_width, int block_size) {
    ArrayList<Integer> result_list = new ArrayList<>();
    int block_num = (block_size - 1) / 8;

    for (int i = 0; i < block_num; i++) { // bitpacking
      unpack8Values( encoded, decode_pos, bit_width,  result_list);
      decode_pos += bit_width;

    }
    return result_list;
  }

  public static byte[] bitWidth2Bytes(int integer) {
    byte[] bytes = new byte[1];
    bytes[0] = (byte) integer;
    return bytes;
  }

  public static byte[] double2Bytes(double dou) {
    long value = Double.doubleToRawLongBits(dou);
    byte[] bytes = new byte[8];
    for (int i = 0; i < 8; i++) {
      bytes[i] = (byte) ((value >> 8 * i) & 0xff);
    }
    return bytes;
  }

  public static double bytes2Double(ArrayList<Byte> encoded, int start, int num) {
    if (num > 8) {
      System.out.println("bytes2Doubleerror");
      return 0;
    }
    long value = 0;
    for (int i = 0; i < 8; i++) {
      value |= ((long) (encoded.get(i + start) & 0xff)) << (8 * i);
    }
    return Double.longBitsToDouble(value);
  }

  public static byte[] float2bytes(float f) {
    int fbit = Float.floatToIntBits(f);
    byte[] b = new byte[4];
    for (int i = 0; i < 4; i++) {
      b[i] = (byte) (fbit >> (24 - i * 8));
    }
    int len = b.length;
    byte[] dest = new byte[len];
    System.arraycopy(b, 0, dest, 0, len);
    byte temp;
    for (int i = 0; i < len / 2; ++i) {
      temp = dest[i];
      dest[i] = dest[len - i - 1];
      dest[len - i - 1] = temp;
    }
    return dest;
  }

  public static float bytes2float(ArrayList<Byte> b, int index) {
    int l;
    l = b.get(index);
    l &= 0xff;
    l |= ((long) b.get(index + 1) << 8);
    l &= 0xffff;
    l |= ((long) b.get(index + 2) << 16);
    l &= 0xffffff;
    l |= ((long) b.get(index + 3) << 24);
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

  public static int bytes2BitWidth(ArrayList<Byte> encoded, int start, int num) {
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

  public static byte[] bitPacking(ArrayList<Integer> numbers, int bit_width) {
    int block_num = numbers.size() / 8;
    byte[] result = new byte[bit_width * block_num];
    for (int i = 0; i < block_num; i++) {
      for (int j = 0; j < bit_width; j++) {
        int tmp_int = 0;
        for (int k = 0; k < 8; k++) {
          tmp_int += (((numbers.get(i * 8 + k) >> j) % 2) << k);
        }
        result[i * bit_width + j] = (byte) tmp_int;
      }
    }
    return result;
  }

  public static byte[] bitPacking(ArrayList<ArrayList<Integer>> numbers, int index, int bit_width) {
    int block_num = numbers.size() / 8;
    byte[] result = new byte[bit_width * block_num];
    for (int i = 0; i < block_num; i++) {
      for (int j = 0; j < bit_width; j++) {
        int tmp_int = 0;
        for (int k = 0; k < 8; k++) {
          tmp_int += (((numbers.get(i * 8 + k + 1).get(index) >> j) % 2) << k);
        }
        result[i * bit_width + j] = (byte) tmp_int;
      }
    }
    return result;
  }

  public static byte[] bitPacking(ArrayList<ArrayList<Integer>> numbers, int index, int start, int block_num, int bit_width) {
    block_num = block_num / 8;
    byte[] result = new byte[bit_width * block_num];

    for (int i = 0; i < block_num; i++) {
      for (int j = 0; j < bit_width; j++) {
        int tmp_int = 0;
        for (int k = 0; k < 8; k++) {
          tmp_int += (((numbers.get(start + i * 8 + k).get(index) >> j) % 2) << k);
        }
        result[i * bit_width + j] = (byte) tmp_int;
      }
    }
    return result;
  }

  public static ArrayList<Integer> decodebitPacking(
          ArrayList<Byte> encoded, int decode_pos, int bit_width, int min_delta, int block_size) {
    ArrayList<Integer> result_list = new ArrayList<>();
    for (int i = 0; i < (block_size - 1) / 8; i++) { // bitpacking  纵向8个，bit width是多少列
      int[] val8 = new int[8];
      for (int j = 0; j < 8; j++) {
        val8[j] = 0;
      }
      for (int j = 0; j < bit_width; j++) {
        byte tmp_byte = encoded.get(decode_pos + bit_width - 1 - j);
        byte[] bit8 = new byte[8];
        for (int k = 0; k < 8; k++) {
          bit8[k] = (byte) (tmp_byte & 1);
          tmp_byte = (byte) (tmp_byte >> 1);
        }
        for (int k = 0; k < 8; k++) {
          val8[k] = val8[k] * 2 + bit8[k];
        }
      }
      for (int j = 0; j < 8; j++) {
        result_list.add(val8[j] + min_delta);
      }
      decode_pos += bit_width;
    }
    return result_list;
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

  public static void quickSort(ArrayList<ArrayList<Integer>> arr, int index, int low, int high) {
    Stack<Integer> stack = new Stack<>();
    int mid = part(arr, index, low, high);
    if (mid + 1 < high) {
      stack.push(mid + 1);
      stack.push(high);
    }
    if (mid - 1 > low) {
      stack.push(low);
      stack.push(mid - 1);
    }
    while (stack.empty() == false) {
      high = stack.pop();
      low = stack.pop();
      mid = part(arr, index, low, high);
      if (mid + 1 < high) {
        stack.push(mid + 1);
        stack.push(high);
      }
      if (mid - 1 > low) {
        stack.push(low);
        stack.push(mid - 1);
      }
    }
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
          continue;
        } else {
          continue;
        }
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

  // ------------------------------------------------basic function-------------------------------------------------------
  public static ArrayList<ArrayList<Integer>> getEncodeBitsRegression(
          ArrayList<ArrayList<Integer>> ts_block,
          int block_size,
          ArrayList<Integer> result,
          ArrayList<Float> theta,
          int segment_size) {
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    ArrayList<ArrayList<Integer>> ts_block_delta = new ArrayList<>();
    theta.clear();

    long sum_X_r = 0;
    long sum_Y_r = 0;
    long sum_squ_X_r = 0;
    long sum_squ_XY_r = 0;
    long sum_X_v = 0;
    long sum_Y_v = 0;
    long sum_squ_X_v = 0;
    long sum_squ_XY_v = 0;

    for (int i = 1; i < block_size; i++) {
      sum_X_r += (ts_block.get(i - 1).get(0));
      sum_X_v += ts_block.get(i - 1).get(1);
      sum_Y_r += (ts_block.get(i).get(0));
      sum_Y_v += ts_block.get(i).get(1);
      sum_squ_X_r += ((long) (ts_block.get(i - 1).get(0)) * (ts_block.get(i - 1).get(0)));
      sum_squ_X_v += ((long) ts_block.get(i - 1).get(1) * ts_block.get(i - 1).get(1));
      sum_squ_XY_r += ((long) (ts_block.get(i - 1).get(0)) * (ts_block.get(i).get(0)));
      sum_squ_XY_v += ((long) ts_block.get(i - 1).get(1) * ts_block.get(i).get(1));
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


    ArrayList<Integer> tmp0 = new ArrayList<>();
    tmp0.add(ts_block.get(0).get(0));
    tmp0.add(ts_block.get(0).get(1));
    ts_block_delta.add(tmp0);

    // delta to Regression
    for (int j = 1; j < block_size; j++) {
      int epsilon_r =
               (ts_block.get(j).get(0)
                      - (int) (theta0_r + theta1_r * (float) ts_block.get(j - 1).get(0)));
      int epsilon_v =
               (ts_block.get(j).get(1)  - (int) ( theta0_v +  theta1_v * (float) ts_block.get(j - 1).get(1)));


      if (epsilon_r < timestamp_delta_min) {
        timestamp_delta_min = epsilon_r;
      }
      if (epsilon_v < value_delta_min) {
        value_delta_min = epsilon_v;
      }
      ArrayList<Integer> tmp = new ArrayList<>();
      tmp.add(epsilon_r);
      tmp.add(epsilon_v);
      ts_block_delta.add(tmp);



    }


    int max_interval = Integer.MIN_VALUE;
    int max_value = Integer.MIN_VALUE;
    int length = 0;
    for (int j = block_size - 1; j > 0; j--) {
      int epsilon_r =ts_block_delta.get(j).get(0) - timestamp_delta_min;
      int epsilon_v =ts_block_delta.get(j).get(1) - value_delta_min;

      length += getBitWith(epsilon_r);
      length += getBitWith(epsilon_v);
      if (epsilon_r > max_interval) {
        max_interval = epsilon_r;
      }
      if (epsilon_v > max_value) {
        max_value = epsilon_v;
      }
      ArrayList<Integer> tmp = new ArrayList<>();
      tmp.add(epsilon_r);
      tmp.add(epsilon_v);
      ts_block_delta.set(j, tmp);



    }

    int max_bit_width_interval = getBitWith(max_interval);
    int max_bit_width_value = getBitWith(max_value);

    // calculate error
    result.clear();

    result.add(length);
    result.add(max_bit_width_interval);
    result.add(max_bit_width_value);

    result.add(timestamp_delta_min);
    result.add(value_delta_min);

    theta.add(theta0_r);
    theta.add(theta1_r);
    theta.add(theta0_v);
    theta.add(theta1_v);

    return ts_block_delta;
  }
  public static int[][] getEncodeBitsRegression(
          int[][] ts_block,
          int block_size,
          int[] raw_length,
          float[] theta,
          int segment_size) {
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    int[][] ts_block_delta = new int[ts_block.length][2];
    theta= new float[4];

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
      int epsilon_r =
              (ts_block[j][0]
                      - (int) (theta0_r + theta1_r * (float) ts_block[j - 1][0]));
      int epsilon_v =
              (ts_block[j][1]  - (int) ( theta0_v +  theta1_v * (float) ts_block[j - 1][1]));


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

    // calculate error
//    result.clear();
    raw_length = new int[5];
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

  public static ArrayList<ArrayList<Integer>> getEncodeBitsRegressionNoTrain(
          ArrayList<ArrayList<Integer>> ts_block,
          int block_size,
          ArrayList<Integer> result,
          ArrayList<Float> theta,
          int segment_size) {
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    ArrayList<ArrayList<Integer>> ts_block_delta = new ArrayList<>();

    float theta0_r = theta.get(0);
    float theta1_r = theta.get(1);
    float theta0_v = theta.get(2);
    float theta1_v = theta.get(3);


    ArrayList<Integer> tmp0 = new ArrayList<>();
    tmp0.add(ts_block.get(0).get(0));
    tmp0.add(ts_block.get(0).get(1));
    ts_block_delta.add(tmp0);

    ArrayList<ArrayList<Integer>> ts_block_delta_segment = new ArrayList<>();
    ArrayList<Integer> tmp_segment = new ArrayList<>(2);
    int max_interval_segment = Integer.MIN_VALUE;
    int max_value_segment = Integer.MIN_VALUE;
    tmp_segment.add(max_interval_segment);
    tmp_segment.add(max_value_segment);


    // delta to Regression
    for (int j = 1; j < block_size; j++) {
      int epsilon_r =
               (ts_block.get(j).get(0)
                      -  (int)(theta0_r + theta1_r * (float) ts_block.get(j - 1).get(0)));
      int epsilon_v =
               (ts_block.get(j).get(1)
                      - (int)(theta0_v +  theta1_v * (float) ts_block.get(j - 1).get(1)));


      if (epsilon_r < timestamp_delta_min) {
        timestamp_delta_min = epsilon_r;
      }
      if (epsilon_v < value_delta_min) {
        value_delta_min = epsilon_v;
      }
      ArrayList<Integer> tmp = new ArrayList<>();
      tmp.add(epsilon_r);
      tmp.add(epsilon_v);
      ts_block_delta.add(tmp);
      if (epsilon_r > max_interval_segment) {
        max_interval_segment = epsilon_r;
        tmp_segment.set(0, max_interval_segment);
      }
      if (epsilon_v > max_value_segment) {
        max_value_segment = epsilon_v;
        tmp_segment.set(1, max_value_segment);
      }
      if (j % segment_size == 0) {
        ts_block_delta_segment.add(tmp_segment);
        tmp_segment = new ArrayList<>();
        max_interval_segment = Integer.MIN_VALUE;
        max_value_segment = Integer.MIN_VALUE;
        tmp_segment.add(max_interval_segment);
        tmp_segment.add(max_value_segment);
      }

    }

    int max_interval = Integer.MIN_VALUE;
    int max_value = Integer.MIN_VALUE;
    int length = 0;
    for (int j = block_size - 1; j > 0; j--) {
       int epsilon_r =ts_block_delta.get(j).get(0) - timestamp_delta_min;
      int epsilon_v =ts_block_delta.get(j).get(1) - value_delta_min;
      if (epsilon_r > max_interval) {
        max_interval = epsilon_r;
      }
      if (epsilon_v > max_value) {
        max_value = epsilon_v;
      }
      ArrayList<Integer> tmp = new ArrayList<>();
      tmp.add(epsilon_r);
      tmp.add(epsilon_v);
      ts_block_delta.set(j, tmp);
    }
    for (ArrayList<Integer> segment_max : ts_block_delta_segment) {
      length += getBitWith(segment_max.get(0) - timestamp_delta_min);
      length += getBitWith(segment_max.get(1) - value_delta_min);
    }

    int max_bit_width_interval = getBitWith(max_interval);
    int max_bit_width_value = getBitWith(max_value);

     result.clear();

    result.add(length);
    result.add(max_bit_width_interval);
    result.add(max_bit_width_value);

    result.add(timestamp_delta_min);
    result.add(value_delta_min);



    return ts_block_delta;
  }

  public static int getBeta(
          int[][] ts_block,
          int alpha,
          ArrayList<Integer> min_index,
          int block_size,
          int[] raw_length,
          float[] theta,
          int segment_size,
          int new_length_j,
          int[][] final_new_length_list) {

    int raw_abs_sum = raw_length[0];
    ArrayList<ArrayList<Integer>> new_length_list =new ArrayList<>();

    ArrayList<Integer> j_star_list = new ArrayList<>(); // beta list of min b phi alpha to j
    int j_star = -1;
    ArrayList<Integer> b = new ArrayList<>();
    if (alpha == -1) {
      return j_star;
    }

    if (alpha == 0) {
      for (int j = 2; j < block_size; j++) {
        // if j, alpha+1, alpha points are min residuals, need to recalculate min residuals
        if(min_index.contains(j)||min_index.contains(0)||min_index.contains(1)){
          b = adjust0MinChange(ts_block, raw_length, j, theta, segment_size);
        }else {
          b = adjust0MinChangeNo(ts_block, raw_length, j, theta, segment_size);
        }
        if (b.get(0) < raw_abs_sum) {
          raw_abs_sum = b.get(0);
          j_star_list = new ArrayList<>();
          new_length_list = new ArrayList<>();
          j_star_list.add(j);
          new_length_list.add(b);
        } else if (b.get(0) == raw_abs_sum) {
          j_star_list.add(j);
          new_length_list.add(b);
        }
      }
      if(min_index.contains(0)||min_index.contains(1)){
        b = adjust0n1MinChange(ts_block, raw_length, theta, segment_size);
      }else {
        b = adjust0n1MinChangeNo(ts_block, raw_length, theta, segment_size);
      }
      if (b.get(0) < raw_abs_sum) {
        raw_abs_sum = b.get(0);
        j_star_list = new ArrayList<>();
        new_length_list = new ArrayList<>();
        j_star_list.add(block_size);
        new_length_list.add(b);
      } else if (b.get(0) == raw_abs_sum) {
        j_star_list.add(block_size);
        new_length_list.add(b);
      }
    } // alpha == n
    else if (alpha == block_size - 1) {
      for (int j = 1; j < block_size - 1; j++) {
        if(min_index.contains(block_size - 1)||min_index.contains(j)){
          b = adjustnMinChange(ts_block, raw_length, j, theta, segment_size);
        }else {
          b = adjustnMinChangeNo(ts_block, raw_length, j, theta, segment_size);
        }
        if (b.get(0) < raw_abs_sum) {
          raw_abs_sum = b.get(0);
          j_star_list = new ArrayList<>();
          new_length_list = new ArrayList<>();
          j_star_list.add(j);
          new_length_list.add(b);
        } else if (b.get(0) == raw_abs_sum) {
          j_star_list.add(j);
          new_length_list.add(b);
        }
      }
      if(min_index.contains(block_size - 1)||min_index.contains(0)){
        b = adjustn0MinChange(ts_block, raw_length, theta, segment_size);
      }else {
        b = adjustn0MinChangeNo(ts_block, raw_length, theta, segment_size);
      }
      if (b.get(0) < raw_abs_sum) {
        raw_abs_sum = b.get(0);
        j_star_list.clear();
        j_star_list = new ArrayList<>();
        new_length_list = new ArrayList<>();
        j_star_list.add(0);
        new_length_list.add(b);
      } else if (b.get(0) == raw_abs_sum) {
        j_star_list.add(0);
        new_length_list.add(b);
      }
    } // alpha != 1 and alpha != n
    else {
      for (int j = 1; j < block_size; j++) {
        if (alpha != j && (alpha + 1) != j) {
          if(min_index.contains(j)||min_index.contains(alpha)||min_index.contains(alpha+1)){
            b = adjustAlphaToJMinChange(ts_block, raw_length, alpha, j, theta, segment_size);
          }else {
            b = adjustAlphaToJMinChangeNo(ts_block, raw_length, alpha, j, theta, segment_size);
          }
          if (b.get(0) < raw_abs_sum) {
            raw_abs_sum = b.get(0);
            j_star_list = new ArrayList<>();
            new_length_list = new ArrayList<>();
            j_star_list.add(j);
            new_length_list.add(b);
          } else if (b.get(0) == raw_abs_sum) {
            j_star_list.add(j);
            new_length_list.add(b);
          }
        }
      }
      if(min_index.contains(0)||min_index.contains(alpha)||min_index.contains(alpha+1)){
        b = adjustTo0MinChange(ts_block, raw_length, alpha, theta, segment_size);
      }else{
        b = adjustTo0MinChangeNo(ts_block, raw_length, alpha, theta,segment_size);
      }
      if (b.get(0) < raw_abs_sum) {
        raw_abs_sum = b.get(0);
        j_star_list = new ArrayList<>();
        new_length_list = new ArrayList<>();
        j_star_list.add(0);
        new_length_list.add(b);
      } else if (b.get(0) == raw_abs_sum) {
        j_star_list.add(0);
        new_length_list.add(b);
      }
      if(min_index.contains(block_size-1)||min_index.contains(alpha)||min_index.contains(alpha+1)){
        b = adjustTonMinChange(ts_block,raw_length, alpha, theta, segment_size);
      }else {
        b = adjustTonMinChangeNo(ts_block,raw_length, alpha, theta, segment_size);
      }

      if (b.get(0) < raw_abs_sum) {
        raw_abs_sum = b.get(0);
        j_star_list = new ArrayList<>();
        new_length_list = new ArrayList<>();
        j_star_list.add(block_size);
        new_length_list.add(b);
      } else if (b.get(0) == raw_abs_sum) {
        j_star_list.add(block_size);
        new_length_list.add(b);
      }
    }
    if (j_star_list.size() != 0) {
      j_star = getIstarClose(alpha, j_star_list, new_length_list, raw_length);
    }
    return j_star;
  }

  public static int getBeta(
          ArrayList<ArrayList<Integer>> ts_block,
          int alpha,
          ArrayList<Integer> min_index,
          int block_size,
          ArrayList<Integer> raw_length,
          ArrayList<Float> theta,
          int segment_size) {

    int raw_abs_sum = raw_length.get(0);
    ArrayList<ArrayList<Integer>> new_length_list =new ArrayList<>();

    ArrayList<Integer> j_star_list = new ArrayList<>(); // beta list of min b phi alpha to j
    int j_star = -1;
    ArrayList<Integer> b = new ArrayList<>();
    if (alpha == -1) {
      return j_star;
    }

    if (alpha == 0) {
      for (int j = 2; j < block_size; j++) {
        // if j, alpha+1, alpha points are min residuals, need to recalculate min residuals
        if(min_index.contains(j)||min_index.contains(0)||min_index.contains(1)){
          b = adjust0MinChange(ts_block, raw_length, j, theta, segment_size);
        }else {
          b = adjust0MinChangeNo(ts_block, raw_length, j, theta, segment_size);
        }
        if (b.get(0) < raw_abs_sum) {
          raw_abs_sum = b.get(0);
          j_star_list = new ArrayList<>();
          new_length_list = new ArrayList<>();
          j_star_list.add(j);
          new_length_list.add(b);
        } else if (b.get(0) == raw_abs_sum) {
          j_star_list.add(j);
          new_length_list.add(b);
        }
      }
      if(min_index.contains(0)||min_index.contains(1)){
        b = adjust0n1MinChange(ts_block, raw_length, theta, segment_size);
      }else {
        b = adjust0n1MinChangeNo(ts_block, raw_length, theta, segment_size);
      }
      if (b.get(0) < raw_abs_sum) {
        raw_abs_sum = b.get(0);
        j_star_list = new ArrayList<>();
        new_length_list = new ArrayList<>();
        j_star_list.add(block_size);
        new_length_list.add(b);
      } else if (b.get(0) == raw_abs_sum) {
        j_star_list.add(block_size);
        new_length_list.add(b);
      }
    } // alpha == n
    else if (alpha == block_size - 1) {
      for (int j = 1; j < block_size - 1; j++) {
        if(min_index.contains(block_size - 1)||min_index.contains(j)){
          b = adjustnMinChange(ts_block, raw_length, j, theta, segment_size);
        }else {
          b = adjustnMinChangeNo(ts_block, raw_length, j, theta, segment_size);
        }
        if (b.get(0) < raw_abs_sum) {
          raw_abs_sum = b.get(0);
          j_star_list = new ArrayList<>();
          new_length_list = new ArrayList<>();
          j_star_list.add(j);
          new_length_list.add(b);
        } else if (b.get(0) == raw_abs_sum) {
          j_star_list.add(j);
          new_length_list.add(b);
        }
      }
      if(min_index.contains(block_size - 1)||min_index.contains(0)){
        b = adjustn0MinChange(ts_block, raw_length, theta, segment_size);
      }else {
        b = adjustn0MinChangeNo(ts_block, raw_length, theta, segment_size);
      }
      if (b.get(0) < raw_abs_sum) {
        raw_abs_sum = b.get(0);
        j_star_list.clear();
        j_star_list = new ArrayList<>();
        new_length_list = new ArrayList<>();
        j_star_list.add(0);
        new_length_list.add(b);
      } else if (b.get(0) == raw_abs_sum) {
        j_star_list.add(0);
        new_length_list.add(b);
      }
    } // alpha != 1 and alpha != n
    else {
      for (int j = 1; j < block_size; j++) {
        if (alpha != j && (alpha + 1) != j) {
          if(min_index.contains(j)||min_index.contains(alpha)||min_index.contains(alpha+1)){
            b = adjustAlphaToJMinChange(ts_block, raw_length, alpha, j, theta, segment_size);
          }else {
            b = adjustAlphaToJMinChangeNo(ts_block, raw_length, alpha, j, theta, segment_size);
          }
          if (b.get(0) < raw_abs_sum) {
            raw_abs_sum = b.get(0);
            j_star_list = new ArrayList<>();
            new_length_list = new ArrayList<>();
            j_star_list.add(j);
            new_length_list.add(b);
          } else if (b.get(0) == raw_abs_sum) {
            j_star_list.add(j);
            new_length_list.add(b);
          }
        }
      }
      if(min_index.contains(0)||min_index.contains(alpha)||min_index.contains(alpha+1)){
        b = adjustTo0MinChange(ts_block, raw_length, alpha, theta, segment_size);
      }else{
        b = adjustTo0MinChangeNo(ts_block, raw_length, alpha, theta,segment_size);
      }
      if (b.get(0) < raw_abs_sum) {
        raw_abs_sum = b.get(0);
        j_star_list = new ArrayList<>();
        new_length_list = new ArrayList<>();
        j_star_list.add(0);
        new_length_list.add(b);
      } else if (b.get(0) == raw_abs_sum) {
        j_star_list.add(0);
        new_length_list.add(b);
      }
      if(min_index.contains(block_size-1)||min_index.contains(alpha)||min_index.contains(alpha+1)){
        b = adjustTonMinChange(ts_block,raw_length, alpha, theta, segment_size);
      }else {
        b = adjustTonMinChangeNo(ts_block,raw_length, alpha, theta, segment_size);
      }

      if (b.get(0) < raw_abs_sum) {
        raw_abs_sum = b.get(0);
        j_star_list = new ArrayList<>();
        new_length_list = new ArrayList<>();
        j_star_list.add(block_size);
        new_length_list.add(b);
      } else if (b.get(0) == raw_abs_sum) {
        j_star_list.add(block_size);
        new_length_list.add(b);
      }
    }
    if (j_star_list.size() != 0) {
      j_star = getIstarClose(alpha, j_star_list, new_length_list, raw_length);
    }
    return j_star;
  }

  // adjust 0 to no n
  private static int[] adjust0MinChange(
          int[][] ts_block, int[] raw_length,  int j, ArrayList<Float> theta, int segment_size) {
    int block_size = ts_block.length;
    assert j != block_size;

    int[] b = new int[3];
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    int timestamp_delta_max = Integer.MIN_VALUE;
    int value_delta_max = Integer.MIN_VALUE;
    int[][] ts_block_delta = new int[block_size-2][2];

    float theta0_t = theta.get(0);
    float theta1_t = theta.get(1);
    float theta0_v = theta.get(2);
    float theta1_v = theta.get(3);


    int pos_ts_block_delta = 0;
    for (int i = 2; i < block_size; i++) {
      int timestamp_delta_i;
      int value_delta_i;
      if (i != j) {
        timestamp_delta_i =
                ts_block[i][0]
                        - (int) (theta0_t + theta1_t * (float) ts_block[i - 1][0]);
        value_delta_i =
                ts_block[i][1]
                        - (int) (theta0_v + theta1_v * (float) ts_block[i - 1][1]);
      } else {
        timestamp_delta_i =
                ts_block[j][0]
                        - (int) (theta0_t + theta1_t * (float) ts_block[0][0]);
        value_delta_i =
                ts_block[j][1]
                        - (int) (theta0_v + theta1_v * (float) ts_block[0][1]);

        ts_block_delta[pos_ts_block_delta][0] = timestamp_delta_i;
        ts_block_delta[pos_ts_block_delta][1] = value_delta_i;
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
        timestamp_delta_i = ts_block[0][0] - (int) (theta0_t + theta1_t * (float) ts_block[j - 1][0]);
        value_delta_i = ts_block[0][1] - (int) (theta0_v + theta1_v * (float) ts_block[j - 1][1]);
      }
      ts_block_delta[pos_ts_block_delta][0] = timestamp_delta_i;
      ts_block_delta[pos_ts_block_delta][1] = value_delta_i;
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
    for (int[] segment_max : ts_block_delta) {
      length += getBitWith(segment_max[0] - timestamp_delta_min);
      length += getBitWith(segment_max[1] - value_delta_min);
    }

    b[0] = length;
    b[1] = timestamp_delta_min;
    b[2] = value_delta_min;

    return b;
  }
  private static int[] adjust0MinChangeNo(
          int[][] ts_block, int[] raw_length, int j, float[] theta, int segment_size) {
    int block_size = ts_block.length;
    assert j != block_size;

    ArrayList<Integer> b = new ArrayList<>();
    int timestamp_delta_min = raw_length[3];
    int value_delta_min = raw_length[4];


    float theta0_t = theta[0];
    float theta1_t = theta[1];
    float theta0_v = theta[2];
    float theta1_v = theta[3];
    int length = raw_length[0];
    int timestamp_delta_i;
    int value_delta_i;
    timestamp_delta_i = ts_block[j + 1][0] - (int) (theta0_t + theta1_t * (float) ts_block[j-1][0]);
    value_delta_i = ts_block[j + 1][1] - (int) (theta0_v + theta1_v * (float) ts_block[j-1][1]);

    length -= getBitWith(timestamp_delta_i-timestamp_delta_min);
    length -= getBitWith(value_delta_i-value_delta_min);
    timestamp_delta_i = ts_block[1][0] - (int) (theta0_t + theta1_t * (float) ts_block[0][0]);
    value_delta_i = ts_block[1][1] - (int) (theta0_v + theta1_v * (float) ts_block[0][1]);

    length -= getBitWith(timestamp_delta_i-timestamp_delta_min);
    length -= getBitWith(value_delta_i-value_delta_min);
    timestamp_delta_i =
            ts_block.get(0).get(0)
                    - (int) (theta0_t + theta1_t * (float) ts_block.get(j - 1).get(0));
    value_delta_i =
            ts_block.get(0).get(1)
                    - (int) (theta0_v + theta1_v * (float) ts_block.get(j - 1).get(1));
    if(timestamp_delta_i<timestamp_delta_min ||  value_delta_i < timestamp_delta_min){
      return adjust0MinChange( ts_block, raw_length,  j, theta,  segment_size);
    }

    length += getBitWith(timestamp_delta_i-timestamp_delta_min);
    length += getBitWith(value_delta_i-value_delta_min);

    timestamp_delta_i =
            ts_block.get(j + 1).get(0)
                    - (int) (theta0_t + theta1_t * (float) ts_block.get(0).get(0));
    value_delta_i =
            ts_block.get(j + 1).get(1)
                    - (int) (theta0_v + theta1_v * (float) ts_block.get(0).get(1));
    if(timestamp_delta_i<timestamp_delta_min ||  value_delta_i < timestamp_delta_min){
      return adjust0MinChange( ts_block, raw_length,  j, theta,  segment_size);
    }
    length += getBitWith(timestamp_delta_i-timestamp_delta_min);
    length += getBitWith(value_delta_i-value_delta_min);

    b.add(length);
    b.add(timestamp_delta_min);
    b.add(value_delta_min);

    return b;
  }

  private static ArrayList<Integer> adjust0MinChange(
          ArrayList<ArrayList<Integer>> ts_block,ArrayList<Integer> raw_length,  int j, ArrayList<Float> theta, int segment_size) {
    int block_size = ts_block.size();
    assert j != block_size;

    ArrayList<Integer> b = new ArrayList<>();
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    int timestamp_delta_max = Integer.MIN_VALUE;
    int value_delta_max = Integer.MIN_VALUE;
    ArrayList<ArrayList<Integer>> ts_block_delta = new ArrayList<>();

    float theta0_t = theta.get(0);
    float theta1_t = theta.get(1);
    float theta0_v = theta.get(2);
    float theta1_v = theta.get(3);


    for (int i = 2; i < block_size; i++) {
      int timestamp_delta_i;
      int value_delta_i;
      if (i != j) {
        timestamp_delta_i =
                ts_block.get(i).get(0)
                        - (int) (theta0_t + theta1_t * (float) ts_block.get(i - 1).get(0));
        value_delta_i =
                ts_block.get(i).get(1)
                        - (int) (theta0_v + theta1_v * (float) ts_block.get(i - 1).get(1));
      } else {
        timestamp_delta_i =
                ts_block.get(j).get(0)
                        - (int) (theta0_t + theta1_t * (float) ts_block.get(0).get(0));
        value_delta_i =
                ts_block.get(j).get(1)
                        - (int) (theta0_v + theta1_v * (float) ts_block.get(0).get(1));
        ArrayList<Integer> tmp0 = new ArrayList<>();
        tmp0.add(timestamp_delta_i);
        tmp0.add(value_delta_i);
        ts_block_delta.add(tmp0);
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
        timestamp_delta_i =
                ts_block.get(0).get(0)
                        - (int) (theta0_t + theta1_t * (float) ts_block.get(j - 1).get(0));
        value_delta_i =
                ts_block.get(0).get(1)
                        - (int) (theta0_v + theta1_v * (float) ts_block.get(j - 1).get(1));
      }
      ArrayList<Integer> tmp0 = new ArrayList<>();
      tmp0.add(timestamp_delta_i);
      tmp0.add(value_delta_i);
      ts_block_delta.add(tmp0);
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
    for (ArrayList<Integer> segment_max : ts_block_delta) {
      length += getBitWith(segment_max.get(0) - timestamp_delta_min);
      length += getBitWith(segment_max.get(1) - value_delta_min);
    }

    b.add(length);
    b.add(timestamp_delta_min);
    b.add(value_delta_min);

    return b;
  }
  private static ArrayList<Integer> adjust0MinChangeNo(
          ArrayList<ArrayList<Integer>> ts_block, ArrayList<Integer> raw_length, int j, ArrayList<Float> theta, int segment_size) {
    int block_size = ts_block.size();
    assert j != block_size;

    ArrayList<Integer> b = new ArrayList<>();
    int timestamp_delta_min = raw_length.get(3);
    int value_delta_min = raw_length.get(4);


    float theta0_t = theta.get(0);
    float theta1_t = theta.get(1);
    float theta0_v = theta.get(2);
    float theta1_v = theta.get(3);
    int length = raw_length.get(0);
    int timestamp_delta_i;
    int value_delta_i;
    timestamp_delta_i =
            ts_block.get(j + 1).get(0)
                    - (int) (theta0_t + theta1_t * (float) ts_block.get(j-1).get(0));
    value_delta_i =
            ts_block.get(j + 1).get(1)
                    - (int) (theta0_v + theta1_v * (float) ts_block.get(j-1).get(1));

    length -= getBitWith(timestamp_delta_i-timestamp_delta_min);
    length -= getBitWith(value_delta_i-value_delta_min);
    timestamp_delta_i =
            ts_block.get(1).get(0)
                    - (int) (theta0_t + theta1_t * (float) ts_block.get(0).get(0));
    value_delta_i =
            ts_block.get(1).get(1)
                    - (int) (theta0_v + theta1_v * (float) ts_block.get(0).get(1));

    length -= getBitWith(timestamp_delta_i-timestamp_delta_min);
    length -= getBitWith(value_delta_i-value_delta_min);
    timestamp_delta_i =
            ts_block.get(0).get(0)
                    - (int) (theta0_t + theta1_t * (float) ts_block.get(j - 1).get(0));
    value_delta_i =
            ts_block.get(0).get(1)
                    - (int) (theta0_v + theta1_v * (float) ts_block.get(j - 1).get(1));
    if(timestamp_delta_i<timestamp_delta_min ||  value_delta_i < timestamp_delta_min){
      return adjust0MinChange( ts_block, raw_length,  j, theta,  segment_size);
    }

    length += getBitWith(timestamp_delta_i-timestamp_delta_min);
    length += getBitWith(value_delta_i-value_delta_min);

    timestamp_delta_i =
            ts_block.get(j + 1).get(0)
                    - (int) (theta0_t + theta1_t * (float) ts_block.get(0).get(0));
    value_delta_i =
            ts_block.get(j + 1).get(1)
                    - (int) (theta0_v + theta1_v * (float) ts_block.get(0).get(1));
    if(timestamp_delta_i<timestamp_delta_min ||  value_delta_i < timestamp_delta_min){
      return adjust0MinChange( ts_block, raw_length,  j, theta,  segment_size);
    }
    length += getBitWith(timestamp_delta_i-timestamp_delta_min);
    length += getBitWith(value_delta_i-value_delta_min);

    b.add(length);
    b.add(timestamp_delta_min);
    b.add(value_delta_min);

    return b;
  }

  // adjust 0 to n
  private static ArrayList<Integer> adjust0n1MinChange(
          ArrayList<ArrayList<Integer>> ts_block,ArrayList<Integer> raw_length,  ArrayList<Float> theta, int segment_size) {
    int block_size = ts_block.size();
    ArrayList<Integer> b = new ArrayList<>();
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;

    float theta0_t = theta.get(0);
    float theta1_t = theta.get(1);
    float theta0_v = theta.get(2);
    float theta1_v = theta.get(3);
    ArrayList<ArrayList<Integer>> ts_block_delta = new ArrayList<>();
    int length = 0;
    for (int i = 2; i < block_size; i++) {
      int timestamp_delta_i;
      int value_delta_i;
      timestamp_delta_i =
              ts_block.get(i).get(0) - (int) (theta0_t + theta1_t * (float) ts_block.get(i - 1).get(0));
      value_delta_i =
              ts_block.get(i).get(1) - (int) (theta0_v + theta1_v * (float) ts_block.get(i - 1).get(1));
      ArrayList<Integer> tmp0 = new ArrayList<>();
      tmp0.add(timestamp_delta_i);
      tmp0.add(value_delta_i);
      ts_block_delta.add(tmp0);
      if (timestamp_delta_i < timestamp_delta_min) {
        timestamp_delta_min = timestamp_delta_i;
      }
      if (value_delta_i < value_delta_min) {
        value_delta_min = value_delta_i;
      }
    }
    int timestamp_delta_i;
    int value_delta_i;
    timestamp_delta_i =
            ts_block.get(0).get(0)
                    - (int) (theta0_t + theta1_t * (float) ts_block.get(block_size - 1).get(0));
    value_delta_i =
            ts_block.get(0).get(1)
                    - (int) (theta0_v + theta1_v * (float) ts_block.get(block_size - 1).get(1));
    ArrayList<Integer> tmp0 = new ArrayList<>();
    tmp0.add(timestamp_delta_i);
    tmp0.add(value_delta_i);
    ts_block_delta.add(tmp0);
    if (timestamp_delta_i < timestamp_delta_min) {
      timestamp_delta_min = timestamp_delta_i;
    }
    if (value_delta_i < value_delta_min) {
      value_delta_min = value_delta_i;
    }

    for (ArrayList<Integer> segment_max : ts_block_delta) {
      length += getBitWith(segment_max.get(0) - timestamp_delta_min);
      length += getBitWith(segment_max.get(1) - value_delta_min);
    }
    b.add(length);
    b.add(timestamp_delta_min);
    b.add(value_delta_min);

    return b;
  }
  private static ArrayList<Integer> adjust0n1MinChangeNo(
          ArrayList<ArrayList<Integer>> ts_block,ArrayList<Integer> raw_length,  ArrayList<Float> theta, int segment_size) {
    int block_size = ts_block.size();
    ArrayList<Integer> b = new ArrayList<>();
    int timestamp_delta_min = raw_length.get(3);
    int value_delta_min = raw_length.get(4);

    float theta0_t = theta.get(0);
    float theta1_t = theta.get(1);
    float theta0_v = theta.get(2);
    float theta1_v = theta.get(3);

    int length = raw_length.get(0);
    int timestamp_delta_i;
    int value_delta_i;
    timestamp_delta_i =
            ts_block.get(1).get(0)
                    - (int) (theta0_t + theta1_t * (float) ts_block.get(0).get(0));
    value_delta_i =
            ts_block.get(1).get(1)
                    - (int) (theta0_v + theta1_v * (float) ts_block.get(0).get(1));

    length -= getBitWith(timestamp_delta_i-timestamp_delta_min);
    length -= getBitWith(value_delta_i-value_delta_min);
    timestamp_delta_i =
            ts_block.get(0).get(0)
                    - (int) (theta0_t + theta1_t * (float) ts_block.get(block_size - 1).get(0));
    value_delta_i =
            ts_block.get(0).get(1)
                    - (int) (theta0_v + theta1_v * (float) ts_block.get(block_size - 1).get(1));
    if(timestamp_delta_i<timestamp_delta_min ||  value_delta_i < timestamp_delta_min){
      return adjust0n1MinChange( ts_block, raw_length,  theta,  segment_size);
    }
    length += getBitWith(timestamp_delta_i-timestamp_delta_min);
    length += getBitWith(value_delta_i-value_delta_min);


    b.add(length);
    b.add(timestamp_delta_min);
    b.add(value_delta_min);

    return b;
  }


  // adjust n to no 0
   private static ArrayList<Integer> adjustnMinChange(
          ArrayList<ArrayList<Integer>> ts_block, ArrayList<Integer> raw_length,  int j, ArrayList<Float> theta, int segment_size) {
    int block_size = ts_block.size();
    assert j != 0;
    ArrayList<Integer> b = new ArrayList<>();
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    ArrayList<ArrayList<Integer>> ts_block_delta = new ArrayList<>();

    float theta0_t = theta.get(0);
    float theta1_t = theta.get(1);
    float theta0_v = theta.get(2);
    float theta1_v = theta.get(3);

    int length = 0;


    for (int i = 1; i < block_size - 1; i++) {
      int timestamp_delta_i;
      int value_delta_i;
      if (i != j) {
        timestamp_delta_i =
                ts_block.get(i).get(0)
                        - (int) (theta0_t + theta1_t * (float) ts_block.get(i - 1).get(0));
        value_delta_i =
                ts_block.get(i).get(1)
                        - (int) (theta0_v + theta1_v * (float) ts_block.get(i - 1).get(1));
      } else {
        timestamp_delta_i =
                ts_block.get(j).get(0)
                        - (int) (theta0_t + theta1_t * (float) ts_block.get(block_size-1).get(0));
        value_delta_i =
                ts_block.get(j).get(1)
                        - (int) (theta0_v + theta1_v * (float) ts_block.get(block_size-1).get(1));
        ArrayList<Integer> tmp0 = new ArrayList<>();
        tmp0.add(timestamp_delta_i);
        tmp0.add(value_delta_i);
        ts_block_delta.add(tmp0);
        if (timestamp_delta_i < timestamp_delta_min) {
          timestamp_delta_min = timestamp_delta_i;
        }
        if (value_delta_i < value_delta_min) {
          value_delta_min = value_delta_i;
        }


        timestamp_delta_i =
                ts_block.get(block_size-1).get(0)
                        - (int) (theta0_t + theta1_t * (float) ts_block.get(j - 1).get(0));
        value_delta_i =
                ts_block.get(block_size-1).get(1)
                        - (int) (theta0_v + theta1_v * (float) ts_block.get(j - 1).get(1));
      }
      ArrayList<Integer> tmp0 = new ArrayList<>();
      tmp0.add(timestamp_delta_i);
      tmp0.add(value_delta_i);
      ts_block_delta.add(tmp0);

      if (timestamp_delta_i < timestamp_delta_min) {
        timestamp_delta_min = timestamp_delta_i;
      }
      if (value_delta_i < value_delta_min) {
        value_delta_min = value_delta_i;
      }

    }

    for (ArrayList<Integer> segment_max : ts_block_delta) {
      length += getBitWith(segment_max.get(0) - timestamp_delta_min);
      length += getBitWith(segment_max.get(1) - value_delta_min);
    }
    b.add(length);
    b.add(timestamp_delta_min);
    b.add(value_delta_min);

    return b;
  }
  private static ArrayList<Integer> adjustnMinChangeNo(
          ArrayList<ArrayList<Integer>> ts_block, ArrayList<Integer> raw_length,  int j, ArrayList<Float> theta, int segment_size) {
    int block_size = ts_block.size();
    assert j != 0;
    ArrayList<Integer> b = new ArrayList<>();
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;

    float theta0_t = theta.get(0);
    float theta1_t = theta.get(1);
    float theta0_v = theta.get(2);
    float theta1_v = theta.get(3);
    int length = raw_length.get(0);
    int timestamp_delta_i;
    int value_delta_i;
    timestamp_delta_i =
            ts_block.get(j).get(0)
                    - (int) (theta0_t + theta1_t * (float) ts_block.get(j-1).get(0));
    value_delta_i =
            ts_block.get(j).get(1)
                    - (int) (theta0_v + theta1_v * (float) ts_block.get(j-1).get(1));

    length -= getBitWith(timestamp_delta_i-timestamp_delta_min);
    length -= getBitWith(value_delta_i-value_delta_min);
    timestamp_delta_i =
            ts_block.get(block_size-1).get(0)
                    - (int) (theta0_t + theta1_t * (float) ts_block.get(block_size-2).get(0));
    value_delta_i =
            ts_block.get(block_size-1).get(1)
                    - (int) (theta0_v + theta1_v * (float) ts_block.get(block_size-2).get(1));

    length -= getBitWith(timestamp_delta_i-timestamp_delta_min);
    length -= getBitWith(value_delta_i-value_delta_min);

    timestamp_delta_i =
            ts_block.get(j).get(0)
                    - (int) (theta0_t + theta1_t * (float) ts_block.get(block_size - 1).get(0));
    value_delta_i =
            ts_block.get(j).get(1)
                    - (int) (theta0_v + theta1_v * (float) ts_block.get(block_size - 1).get(1));

    if(timestamp_delta_i<timestamp_delta_min ||  value_delta_i < timestamp_delta_min){
      return adjustnMinChange( ts_block, raw_length, j,  theta,  segment_size);
    }

    length += getBitWith(timestamp_delta_i-timestamp_delta_min);
    length += getBitWith(value_delta_i-value_delta_min);
    timestamp_delta_i =
            ts_block.get(block_size - 1).get(0)
                    - (int) (theta0_t + theta1_t * (float) ts_block.get(j-1).get(0));
    value_delta_i =
            ts_block.get(block_size - 1).get(1)
                    - (int) (theta0_v + theta1_v * (float) ts_block.get(j-1).get(1));

    if(timestamp_delta_i<timestamp_delta_min ||  value_delta_i < timestamp_delta_min){
      return adjustnMinChange( ts_block, raw_length, j,  theta,  segment_size);
    }

    length += getBitWith(timestamp_delta_i-timestamp_delta_min);
    length += getBitWith(value_delta_i-value_delta_min);

    b.add(length);
    b.add(timestamp_delta_min);
    b.add(value_delta_min);
    return b;
  }
  // adjust n to 0
   // adjust n to 0
  private static ArrayList<Integer> adjustn0MinChange(
          ArrayList<ArrayList<Integer>> ts_block,ArrayList<Integer> raw_length,  ArrayList<Float> theta, int segment_size) {
    int block_size = ts_block.size();
    ArrayList<Integer> b = new ArrayList<>();
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;

    float theta0_t = theta.get(0);
    float theta1_t = theta.get(1);
    float theta0_v = theta.get(2);
    float theta1_v = theta.get(3);
    ArrayList<ArrayList<Integer>> ts_block_delta = new ArrayList<>();
    int length = 0;

    for (int i = 1; i < block_size - 1; i++) {
      int timestamp_delta_i;
      int value_delta_i;
      timestamp_delta_i =
              ts_block.get(i).get(0) - (int) (theta0_t + theta1_t * (float) ts_block.get(i - 1).get(0));
      value_delta_i =
              ts_block.get(i).get(1) - (int) (theta0_v + theta1_v * (float) ts_block.get(i - 1).get(1));
      ArrayList<Integer> tmp0 = new ArrayList<>();
      tmp0.add(timestamp_delta_i);
      tmp0.add(value_delta_i);
      ts_block_delta.add(tmp0);
      if (timestamp_delta_i < timestamp_delta_min) {
        timestamp_delta_min = timestamp_delta_i;
      }
      if (value_delta_i < value_delta_min) {
        value_delta_min = value_delta_i;
      }

    }
    int timestamp_delta_i;
    int value_delta_i;
    timestamp_delta_i =
            ts_block.get(0).get(0)
                    - (int) (theta0_t + theta1_t * (float) ts_block.get(block_size - 1).get(0));
    value_delta_i =
            ts_block.get(0).get(1)
                    - (int) (theta0_v + theta1_v * (float) ts_block.get(block_size - 1).get(1));
    ArrayList<Integer> tmp0 = new ArrayList<>();
    tmp0.add(timestamp_delta_i);
    tmp0.add(value_delta_i);
    ts_block_delta.add(tmp0);

    if (timestamp_delta_i < timestamp_delta_min) {
      timestamp_delta_min = timestamp_delta_i;
    }
    if (value_delta_i < value_delta_min) {
      value_delta_min = value_delta_i;
    }
    for (ArrayList<Integer> segment_max : ts_block_delta) {
      length += getBitWith(segment_max.get(0) - timestamp_delta_min);
      length += getBitWith(segment_max.get(1) - value_delta_min);
    }
    b.add(length);
    b.add(timestamp_delta_min);
    b.add(value_delta_min);

    return b;
  }
  // adjust n to 0
  private static ArrayList<Integer> adjustn0MinChangeNo(
          ArrayList<ArrayList<Integer>> ts_block, ArrayList<Integer> raw_length,  ArrayList<Float> theta, int segment_size) {
    int block_size = ts_block.size();
    ArrayList<Integer> b = new ArrayList<>();
    int timestamp_delta_min = raw_length.get(3);
    int value_delta_min = raw_length.get(4);
    ArrayList<ArrayList<Integer>> ts_block_delta_segment = new ArrayList<>();
    float theta0_t = theta.get(0);
    float theta1_t = theta.get(1);
    float theta0_v = theta.get(2);
    float theta1_v = theta.get(3);
    int length = raw_length.get(0);
    int timestamp_delta_i;
    int value_delta_i;
    timestamp_delta_i =
            ts_block.get(block_size - 1).get(0)
                    - (int) (theta0_t + theta1_t * (float) ts_block.get(block_size - 2).get(0));
    value_delta_i =
            ts_block.get(block_size - 1).get(1)
                    - (int) (theta0_v + theta1_v * (float) ts_block.get(block_size - 2).get(1));

    length -= getBitWith(timestamp_delta_i-timestamp_delta_min);
    length -= getBitWith(value_delta_i-value_delta_min);

    timestamp_delta_i =
            ts_block.get(0).get(0)
                    - (int) (theta0_t + theta1_t * (float) ts_block.get(block_size - 1).get(0));
    value_delta_i =
            ts_block.get(0).get(1)
                    - (int) (theta0_v + theta1_v * (float) ts_block.get(block_size - 1).get(1));
    if(timestamp_delta_i<timestamp_delta_min ||  value_delta_i < timestamp_delta_min){
      return adjustn0MinChange( ts_block, raw_length, theta,  segment_size);
    }
    length += getBitWith(timestamp_delta_i-timestamp_delta_min);
    length += getBitWith(value_delta_i-value_delta_min);

    b.add(length);
    b.add(timestamp_delta_min);
    b.add(value_delta_min);
    return b;
  }

    private static ArrayList<Integer> adjustAlphaToJMinChange(
          ArrayList<ArrayList<Integer>> ts_block,  ArrayList<Integer> raw_length,int alpha, int j, ArrayList<Float> theta, int segment_size) {

    int block_size = ts_block.size();
    assert alpha != block_size - 1;
    assert alpha != 0;
    assert j != 0;
    assert j != block_size;
    ArrayList<Integer> b = new ArrayList<>();
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;

    float theta0_t = theta.get(0);
    float theta1_t = theta.get(1);
    float theta0_v = theta.get(2);
    float theta1_v = theta.get(3);
    ArrayList<ArrayList<Integer>> ts_block_delta = new ArrayList<>();
    int length = 0;
    for (int i = 1; i < block_size; i++) {
      int timestamp_delta_i;
      int value_delta_i;
      if (i == j) {
        timestamp_delta_i =
                ts_block.get(j).get(0)
                        - (int) (theta0_t + theta1_t * (float) ts_block.get(alpha).get(0));
        value_delta_i =
                ts_block.get(j).get(1)
                        - (int) (theta0_v + theta1_v * (float) ts_block.get(alpha).get(1));
      } else if (i == alpha) {
        timestamp_delta_i =
                ts_block.get(alpha).get(0)
                        - (int) (theta0_t + theta1_t * (float) ts_block.get(j - 1).get(0));
        value_delta_i =
                ts_block.get(alpha).get(1)
                        - (int) (theta0_v + theta1_v * (float) ts_block.get(j - 1).get(1));
      } else if (i == alpha + 1) {
        timestamp_delta_i =
                ts_block.get(alpha + 1).get(0)
                        - (int) (theta0_t + theta1_t * (float) ts_block.get(alpha - 1).get(0));
        value_delta_i =
                ts_block.get(alpha + 1).get(1)
                        - (int) (theta0_v + theta1_v * (float) ts_block.get(alpha - 1).get(1));
      } else {
        timestamp_delta_i =
                ts_block.get(i).get(0)
                        - (int) (theta0_t + theta1_t * (float) ts_block.get(i - 1).get(0));
        value_delta_i =
                ts_block.get(i).get(1)
                        - (int) (theta0_v + theta1_v * (float) ts_block.get(i - 1).get(1));
      }
      ArrayList<Integer> tmp0 = new ArrayList<>();
      tmp0.add(timestamp_delta_i);
      tmp0.add(value_delta_i);
      ts_block_delta.add(tmp0);
      if (timestamp_delta_i < timestamp_delta_min) {
        timestamp_delta_min = timestamp_delta_i;
      }
      if (value_delta_i < value_delta_min) {
        value_delta_min = value_delta_i;
      }

    }

    for (ArrayList<Integer> segment_max : ts_block_delta) {
      length += getBitWith(segment_max.get(0) - timestamp_delta_min);
      length += getBitWith(segment_max.get(1) - value_delta_min);
    }
    b.add(length);
    b.add(timestamp_delta_min);
    b.add(value_delta_min);

    return b;
  }
  private static ArrayList<Integer> adjustAlphaToJMinChangeNo(
          ArrayList<ArrayList<Integer>> ts_block,  ArrayList<Integer> raw_length, int alpha, int j, ArrayList<Float> theta, int segment_size) {

    int block_size = ts_block.size();
    assert alpha != block_size - 1;
    assert alpha != 0;
    assert j != 0;
    assert j != block_size;
    ArrayList<Integer> b = new ArrayList<>();
    int timestamp_delta_min = raw_length.get(3);
    int value_delta_min = raw_length.get(4);

    float theta0_t = theta.get(0);
    float theta1_t = theta.get(1);
    float theta0_v = theta.get(2);
    float theta1_v = theta.get(3);
    int length = raw_length.get(0);
    int timestamp_delta_i;
    int value_delta_i;
    timestamp_delta_i =
            ts_block.get(alpha + 1).get(0)
                    - (int) (theta0_t + theta1_t * (float) ts_block.get(alpha).get(0));
    value_delta_i =
            ts_block.get(alpha + 1).get(1)
                    - (int) (theta0_v + theta1_v * (float) ts_block.get(alpha).get(1));

    length -= getBitWith(timestamp_delta_i-timestamp_delta_min);
    length -= getBitWith(value_delta_i-value_delta_min);
    timestamp_delta_i =
            ts_block.get(alpha).get(0)
                    - (int) (theta0_t + theta1_t * (float) ts_block.get(alpha-1).get(0));
    value_delta_i =
            ts_block.get(alpha).get(1)
                    - (int) (theta0_v + theta1_v * (float) ts_block.get(alpha-1).get(1));

    length -= getBitWith(timestamp_delta_i-timestamp_delta_min);
    length -= getBitWith(value_delta_i-value_delta_min);
    timestamp_delta_i =
            ts_block.get(j).get(0)
                    - (int) (theta0_t + theta1_t * (float) ts_block.get(j-1).get(0));
    value_delta_i =
            ts_block.get(j).get(1)
                    - (int) (theta0_v + theta1_v * (float) ts_block.get(j-1).get(1));

    length -= getBitWith(timestamp_delta_i-timestamp_delta_min);
    length -= getBitWith(value_delta_i-value_delta_min);


    timestamp_delta_i =
            ts_block.get(alpha ).get(0)
                    - (int) (theta0_t + theta1_t * (float) ts_block.get(j - 1).get(0));
    value_delta_i =
            ts_block.get(alpha ).get(1)
                    - (int) (theta0_v + theta1_v * (float) ts_block.get(j - 1).get(1));
    if(timestamp_delta_i<timestamp_delta_min ||  value_delta_i < timestamp_delta_min){
      return adjustAlphaToJMinChange( ts_block, raw_length, alpha, j, theta,  segment_size);
    }
    length += getBitWith(timestamp_delta_i-timestamp_delta_min);
    length += getBitWith(value_delta_i-value_delta_min);

    timestamp_delta_i =
            ts_block.get(j).get(0)
                    - (int) (theta0_t + theta1_t * (float) ts_block.get(alpha).get(0));
    value_delta_i =
            ts_block.get(j).get(1)
                    - (int) (theta0_v + theta1_v * (float) ts_block.get(alpha).get(1));
    if(timestamp_delta_i<timestamp_delta_min ||  value_delta_i < timestamp_delta_min){
      return adjustAlphaToJMinChange( ts_block, raw_length, alpha, j, theta,  segment_size);
    }

    length += getBitWith(timestamp_delta_i-timestamp_delta_min);
    length += getBitWith(value_delta_i-value_delta_min);

    timestamp_delta_i =
            ts_block.get(alpha).get(0)
                    - (int) (theta0_t + theta1_t * (float) ts_block.get(j-1).get(0));
    value_delta_i =
            ts_block.get(alpha).get(1)
                    - (int) (theta0_v + theta1_v * (float) ts_block.get(j-1).get(1));
    if(timestamp_delta_i<timestamp_delta_min ||  value_delta_i < timestamp_delta_min){
      return adjustAlphaToJMinChange( ts_block, raw_length, alpha, j, theta,  segment_size);
    }
    length += getBitWith(timestamp_delta_i-timestamp_delta_min);
    length += getBitWith(value_delta_i-value_delta_min);

    b.add(length);
    b.add(timestamp_delta_min);
    b.add(value_delta_min);
    return b;
  }


  private static ArrayList<Integer> adjustTo0MinChange(
          ArrayList<ArrayList<Integer>> ts_block, ArrayList<Integer> raw_length, int alpha, ArrayList<Float> theta, int segment_size) {
    int block_size = ts_block.size();
    assert alpha != block_size - 1;
    assert alpha != 0;
    ArrayList<Integer> b = new ArrayList<>();
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    ArrayList<ArrayList<Integer>> ts_block_delta = new ArrayList<>();

    float theta0_t = theta.get(0);
    float theta1_t = theta.get(1);
    float theta0_v = theta.get(2);
    float theta1_v = theta.get(3);

    for (int i = 1; i < block_size; i++) {
      int timestamp_delta_i;
      int value_delta_i;
      if (i == (alpha + 1)) {
        timestamp_delta_i =
                ts_block.get(alpha + 1).get(0)
                        - (int) (theta0_t + theta1_t * (float) ts_block.get(alpha - 1).get(0));
        value_delta_i =
                ts_block.get(alpha + 1).get(1)
                        - (int) (theta0_v + theta1_v * (float) ts_block.get(alpha - 1).get(1));
      } else if (i == alpha) {
        timestamp_delta_i =
                ts_block.get(0).get(0)
                        - (int) (theta0_t + theta1_t * (float) ts_block.get(alpha).get(0));
        value_delta_i =
                ts_block.get(0).get(1)
                        - (int) (theta0_v + theta1_v * (float) ts_block.get(alpha).get(1));
      } else {
        timestamp_delta_i =
                ts_block.get(i).get(0)
                        - (int) (theta0_t + theta1_t * (float) ts_block.get(i - 1).get(0));
        value_delta_i =
                ts_block.get(i).get(1)
                        - (int) (theta0_v + theta1_v * (float) ts_block.get(i - 1).get(1));
      }
      ArrayList<Integer> tmp= new ArrayList<>();
      tmp.add(timestamp_delta_i);
      tmp.add(value_delta_i);
      ts_block_delta.add(tmp);

      if (timestamp_delta_i < timestamp_delta_min) {
        timestamp_delta_min = timestamp_delta_i;
      }

      if (value_delta_i < value_delta_min) {
        value_delta_min = value_delta_i;
      }

//            if (timestamp_delta_i > max_interval_segment) {
//                max_interval_segment = timestamp_delta_i;
//                tmp_segment.set(0, max_interval_segment);
//            }
//            if (value_delta_i > max_value_segment) {
//                max_value_segment = value_delta_i;
//                tmp_segment.set(1, max_value_segment);
//            }
//            if (i % segment_size == 0) {
//                ts_block_delta_segment.add(tmp_segment);
//                tmp_segment = new ArrayList<>();
//                max_interval_segment = Integer.MIN_VALUE;
//                max_value_segment = Integer.MIN_VALUE;
//                tmp_segment.add(max_interval_segment);
//                tmp_segment.add(max_value_segment);
//            }
    }
    int length = 0;
    for (ArrayList<Integer> segment_max : ts_block_delta) {
      length += getBitWith(segment_max.get(0) - timestamp_delta_min);
      length += getBitWith(segment_max.get(1) - value_delta_min);
    }
//        raw_length.set(3,timestamp_delta_min);
//        raw_length.set(4,value_delta_min);

    b.add(length);
    b.add(timestamp_delta_min);
    b.add(value_delta_min);

    return b;
  }
  private static ArrayList<Integer> adjustTo0MinChangeNo(
          ArrayList<ArrayList<Integer>> ts_block, ArrayList<Integer> raw_length, int alpha, ArrayList<Float> theta, int segment_size) {
    int block_size = ts_block.size();
    assert alpha != block_size - 1;
    assert alpha != 0;
    ArrayList<Integer> b = new ArrayList<>();
    int timestamp_delta_min = raw_length.get(3);
    int value_delta_min = raw_length.get(4);
    ArrayList<ArrayList<Integer>> ts_block_delta_segment = new ArrayList<>();
    float theta0_t = theta.get(0);
    float theta1_t = theta.get(1);
    float theta0_v = theta.get(2);
    float theta1_v = theta.get(3);
    int length = raw_length.get(0);
    int timestamp_delta_i;
    int value_delta_i;
    timestamp_delta_i =
            ts_block.get(alpha + 1).get(0)
                    - (int) (theta0_t + theta1_t * (float) ts_block.get(alpha).get(0));
    value_delta_i =
            ts_block.get(alpha + 1).get(1)
                    - (int) (theta0_v + theta1_v * (float) ts_block.get(alpha).get(1));

    length -= getBitWith(timestamp_delta_i-timestamp_delta_min);
    length -= getBitWith(value_delta_i-value_delta_min);
    timestamp_delta_i =
            ts_block.get(alpha).get(0)
                    - (int) (theta0_t + theta1_t * (float) ts_block.get(alpha-1).get(0));
    value_delta_i =
            ts_block.get(alpha).get(1)
                    - (int) (theta0_v + theta1_v * (float) ts_block.get(alpha-1).get(1));

    length -= getBitWith(timestamp_delta_i-timestamp_delta_min);
    length -= getBitWith(value_delta_i-value_delta_min);

    timestamp_delta_i =
            ts_block.get(alpha + 1).get(0)
                    - (int) (theta0_t + theta1_t * (float) ts_block.get(alpha - 1).get(0));
    value_delta_i =
            ts_block.get(alpha + 1).get(1)
                    - (int) (theta0_v + theta1_v * (float) ts_block.get(alpha - 1).get(1));
    if(timestamp_delta_i<timestamp_delta_min ||  value_delta_i < timestamp_delta_min){
      return adjustTo0MinChange( ts_block, raw_length, alpha,  theta,  segment_size);
    }
    length += getBitWith(timestamp_delta_i-timestamp_delta_min);
    length += getBitWith(value_delta_i-value_delta_min);
    timestamp_delta_i =
            ts_block.get(0).get(0)
                    - (int) (theta0_t + theta1_t * (float) ts_block.get(alpha).get(0));
    value_delta_i =
            ts_block.get(0).get(1)
                    - (int) (theta0_v + theta1_v * (float) ts_block.get(alpha).get(1));
    if(timestamp_delta_i<timestamp_delta_min ||  value_delta_i < timestamp_delta_min){
      return adjustTo0MinChange( ts_block, raw_length, alpha,  theta,  segment_size);
    }
    length += getBitWith(timestamp_delta_i-timestamp_delta_min);
    length += getBitWith(value_delta_i-value_delta_min);

    b.add(length);
    b.add(timestamp_delta_min);
    b.add(value_delta_min);

    return b;
  }

  private static ArrayList<Integer> adjustTonMinChange(
          ArrayList<ArrayList<Integer>> ts_block, ArrayList<Integer> raw_length, int alpha, ArrayList<Float> theta, int segment_size) {
    int block_size = ts_block.size();
    assert alpha != block_size - 1;
    assert alpha != 0;
    ArrayList<Integer> b = new ArrayList<>();
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;

    float theta0_t = theta.get(0);
    float theta1_t = theta.get(1);
    float theta0_v = theta.get(2);
    float theta1_v = theta.get(3);
    ArrayList<ArrayList<Integer>> ts_block_delta = new ArrayList<>();
    int length = 0;

    for (int i = 1; i < block_size; i++) {
      int timestamp_delta_i;
      int value_delta_i;
      if (i == (alpha + 1)) {
        timestamp_delta_i =
                ts_block.get(alpha + 1).get(0)
                        - (int) (theta0_t + theta1_t * (float) ts_block.get(alpha - 1).get(0));
        value_delta_i =
                ts_block.get(alpha + 1).get(1)
                        - (int) (theta0_v + theta1_v * (float) ts_block.get(alpha - 1).get(1));
      } else if (i == alpha) {
        timestamp_delta_i =
                ts_block.get(alpha).get(0)
                        - (int) (theta0_t + theta1_t * (float) ts_block.get(block_size - 1).get(0));
        value_delta_i =
                ts_block.get(alpha).get(1)
                        - (int) (theta0_v + theta1_v * (float) ts_block.get(block_size - 1).get(1));
      } else {
        timestamp_delta_i =
                ts_block.get(i).get(0)
                        - (int) (theta0_t + theta1_t * (float) ts_block.get(i - 1).get(0));
        value_delta_i =
                ts_block.get(i).get(1)
                        - (int) (theta0_v + theta1_v * (float) ts_block.get(i - 1).get(1));
      }
      ArrayList<Integer> tmp0 = new ArrayList<>();
      tmp0.add(timestamp_delta_i);
      tmp0.add(value_delta_i);
      ts_block_delta.add(tmp0);
      if (timestamp_delta_i < timestamp_delta_min) {
        timestamp_delta_min = timestamp_delta_i;
      }
      if (value_delta_i < value_delta_min) {
        value_delta_min = value_delta_i;
      }
    }

    for (ArrayList<Integer> segment_max : ts_block_delta) {
      length += getBitWith(segment_max.get(0) - timestamp_delta_min);
      length += getBitWith(segment_max.get(1) - value_delta_min);
    }
    b.add(length);
    b.add(timestamp_delta_min);
    b.add(value_delta_min);

    return b;
  }

  private static ArrayList<Integer> adjustTonMinChangeNo(
          ArrayList<ArrayList<Integer>> ts_block, ArrayList<Integer> raw_length, int alpha, ArrayList<Float> theta, int segment_size) {
    int block_size = ts_block.size();
    assert alpha != block_size - 1;
    assert alpha != 0;
    ArrayList<Integer> b = new ArrayList<>();
    int timestamp_delta_min = raw_length.get(3);
    int value_delta_min = raw_length.get(4);
    float theta0_t = theta.get(0);
    float theta1_t = theta.get(1);
    float theta0_v = theta.get(2);
    float theta1_v = theta.get(3);
    int length = raw_length.get(0);

    int timestamp_delta_i;
    int value_delta_i;
    timestamp_delta_i =
            ts_block.get(alpha + 1).get(0)
                    - (int) (theta0_t + theta1_t * (float) ts_block.get(alpha).get(0));
    value_delta_i =
            ts_block.get(alpha + 1).get(1)
                    - (int) (theta0_v + theta1_v * (float) ts_block.get(alpha).get(1));

    length -= getBitWith(timestamp_delta_i-timestamp_delta_min);
    length -= getBitWith(value_delta_i-value_delta_min);
    timestamp_delta_i =
            ts_block.get(alpha).get(0)
                    - (int) (theta0_t + theta1_t * (float) ts_block.get(alpha-1).get(0));
    value_delta_i =
            ts_block.get(alpha).get(1)
                    - (int) (theta0_v + theta1_v * (float) ts_block.get(alpha-1).get(1));

    length -= getBitWith(timestamp_delta_i-timestamp_delta_min);
    length -= getBitWith(value_delta_i-value_delta_min);

    timestamp_delta_i =
            ts_block.get(alpha + 1).get(0)
                    - (int) (theta0_t + theta1_t * (float) ts_block.get(alpha - 1).get(0));
    value_delta_i =
            ts_block.get(alpha + 1).get(1)
                    - (int) (theta0_v + theta1_v * (float) ts_block.get(alpha - 1).get(1));
    if(timestamp_delta_i<timestamp_delta_min ||  value_delta_i < timestamp_delta_min){
      return adjustTonMinChange( ts_block, raw_length, alpha,  theta,  segment_size);
    }
    length += getBitWith(timestamp_delta_i-timestamp_delta_min);
    length += getBitWith(value_delta_i-value_delta_min);
    timestamp_delta_i =
            ts_block.get(alpha).get(0)
                    - (int) (theta0_t + theta1_t * (float) ts_block.get(block_size-1).get(0));
    value_delta_i =
            ts_block.get(alpha).get(1)
                    - (int) (theta0_v + theta1_v * (float) ts_block.get(block_size-1).get(1));
    if(timestamp_delta_i<timestamp_delta_min ||  value_delta_i < timestamp_delta_min){
      return adjustTonMinChange( ts_block, raw_length, alpha,  theta,  segment_size);
    }
    length += getBitWith(timestamp_delta_i-timestamp_delta_min);
    length += getBitWith(value_delta_i-value_delta_min);

    b.add(length);
    b.add(timestamp_delta_min);
    b.add(value_delta_min);

    return b;
  }


  private static int getIstarClose(int alpha, ArrayList<Integer> j_star_list,ArrayList<ArrayList<Integer>> new_length_list,ArrayList<Integer> raw_length) {
    int min_i = 0;
    int min_dis = Integer.MAX_VALUE;
    for (int i= 0;i< j_star_list.size();i++) {
      if (abs(alpha - j_star_list.get(i)) < min_dis) {
        min_i = j_star_list.get(i);
        min_dis = abs(alpha - j_star_list.get(i));
        raw_length.set(0, new_length_list.get(i).get(0));
        raw_length.set(3, new_length_list.get(i).get(1));
        raw_length.set(4, new_length_list.get(i).get(2));
      }
    }
    if (min_dis == 0) {
      System.out.println("get IstarClose error");
      return 0;
    }
    return min_i;
  }

  public static ArrayList<Integer> getIStar(
          ArrayList<ArrayList<Integer>> ts_block, ArrayList<Integer> min_index, int block_size, int index, ArrayList<Float> theta, int k) {

    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    int timestamp_delta_min_index = -1;
    int value_delta_min_index = -1;
    int alpha = 0;
    ArrayList<Integer> alpha_list = new ArrayList<>();

    float theta0_t = theta.get(0);
    float theta1_t = theta.get(1);
    float theta0_v = theta.get(2);
    float theta1_v = theta.get(3);
    ArrayList<ArrayList<Integer>> ts_block_delta = new ArrayList<>();

    for (int j = 1; j < block_size; j++) {
      int epsilon_v_j =
              ts_block.get(j).get(1)
                      - (int) (theta0_v + theta1_v * (float) ts_block.get(j - 1).get(1));
      int epsilon_r_j =
              ts_block.get(j).get(0)
                      - (int) (theta0_t + theta1_t * (float) ts_block.get(j - 1).get(0));
      ArrayList<Integer> tmp = new ArrayList<>();
      tmp.add(j);
      if (index == 0) {
        tmp.add(epsilon_v_j);
      }else if (index == 1) {
        tmp.add(epsilon_r_j);
      }
      ts_block_delta.add(tmp);
      if(epsilon_r_j < timestamp_delta_min){
        timestamp_delta_min = epsilon_r_j;
        timestamp_delta_min_index = j;
      }
      if(epsilon_v_j < value_delta_min){
        value_delta_min = epsilon_v_j;
        value_delta_min_index = j;
      }
    }
    min_index.add(timestamp_delta_min_index);
    min_index.add(value_delta_min_index);

    quickSort(ts_block_delta, 1, 0, block_size - 2);
    alpha_list.add(ts_block_delta.get(0).get(0));
    for (int i = 0; i < k; i++) {
      alpha_list.add(ts_block_delta.get(block_size - 2 - k).get(0));
    }
    return alpha_list;
  }

  public static int[] getIStar(
          int[][] ts_block, ArrayList<Integer> min_index, int block_size, int index, float[] theta, int k) {

    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    int timestamp_delta_min_index = -1;
    int value_delta_min_index = -1;
    int alpha = 0;


    float theta0_t = theta[0];
    float theta1_t = theta[1];
    float theta0_v = theta[2];
    float theta1_v = theta[3];
    int[] ts_block_delta = new int[block_size-1];

    for (int j = 0; j < block_size-1; j++) {
      int epsilon_v_j =
              ts_block[j+1][1]
                      - (int) (theta0_v + theta1_v * (float) ts_block[j][1]);
      int epsilon_r_j =
              ts_block[j+1][0]
                      - (int) (theta0_t + theta1_t * (float) ts_block[j][0]);
//      ArrayList<Integer> tmp = new ArrayList<>();
//      tmp.add(j);
      if (index == 0) {
        ts_block_delta[j] = epsilon_v_j;

//        tmp.add(epsilon_v_j);
      }else if (index == 1) {
        ts_block_delta[j] = epsilon_r_j;
//        tmp.add(epsilon_r_j);
      }
//      ts_block_delta.add(tmp);
      if(epsilon_r_j < timestamp_delta_min){
        timestamp_delta_min = epsilon_r_j;
        timestamp_delta_min_index = j;
      }
      if(epsilon_v_j < value_delta_min){
        value_delta_min = epsilon_v_j;
        value_delta_min_index = j;
      }
    }
    min_index.add(timestamp_delta_min_index);
    min_index.add(value_delta_min_index);

    Arrays.sort(ts_block_delta);
//    quickSort(ts_block_delta, 1, 0, block_size - 2);
    int[] alpha_list = new int[k+1];
//    ArrayList<Integer> alpha_list = new ArrayList<>();
    alpha_list[0] = ts_block_delta[0];
    for (int i = 0; i < k; i++) {
      alpha_list[i+1] = ts_block_delta[block_size - 2 - k];
    }
    return alpha_list;
  }
  public static int[] getIStar(
          int[][] ts_block,
          ArrayList<Integer> min_index,
          int block_size,
          int[] raw_length,
          float[] theta,
          int k) {
    int timestamp_delta_max = Integer.MIN_VALUE;
    int value_delta_max = Integer.MIN_VALUE;
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    int timestamp_delta_max_index = -1;
    int value_delta_max_index = -1;
    int timestamp_delta_min_index = -1;
    int value_delta_min_index = -1;
    int[] alpha_list = new int[2*k+2];

    int[][] ts_block_delta_time = new int[block_size-1][2];
    int[][] ts_block_delta_value = new int[block_size-1][2];

//    ArrayList<ArrayList<Integer>> ts_block_delta_time = new ArrayList<>();
//    ArrayList<ArrayList<Integer>> ts_block_delta_value = new ArrayList<>();

    float theta0_t = theta[0];
    float theta1_t = theta[1];
    float theta0_v = theta[2];
    float theta1_v = theta[3];


    for (int j = 0; j < block_size-1; j++) {
      int epsilon_r_j = ts_block[j+1][0] - (int) (theta0_t + theta1_t * (float) ts_block[j][0]);
      int epsilon_v_j = ts_block[j+1][1] - (int) (theta0_v + theta1_v * (float) ts_block[j][1]);
      ts_block_delta_time[j][0] = j;
      ts_block_delta_time[j][1] = epsilon_r_j;
      ts_block_delta_value[j][0] = j;
      ts_block_delta_value[j][1] = epsilon_v_j;


      if (epsilon_v_j > value_delta_max) {
        value_delta_max = (int) epsilon_v_j;
      }
      if (epsilon_v_j < value_delta_min) {
        value_delta_min = (int) epsilon_v_j;
      }
      if (epsilon_r_j > timestamp_delta_max) {
        timestamp_delta_max = (int) epsilon_r_j;
      }
      if (epsilon_r_j < timestamp_delta_min) {
        timestamp_delta_min = (int) epsilon_r_j;
      }
    }
    Arrays.sort(ts_block_delta_time, (a, b) -> {
      if(a[1]==b[1])
        return Integer.compare(a[0], b[0]);
      return Integer.compare(a[1], b[1]);
    });

    min_index.add(ts_block_delta_time[0][0]);
    alpha_list[0]= ts_block_delta_time[0][0];
    for (int i = 0; i < k; i++) {
      alpha_list[i+1]=ts_block_delta_time[block_size - 2 - k][0];
    }

    Arrays.sort(ts_block_delta_value, (a, b) -> {
      if(a[1]==b[1])
        return Integer.compare(a[0], b[0]);
      return Integer.compare(a[1], b[1]);
    });

    int pos_alpha_list = k+1;
//    quickSort(ts_block_delta_value, 1, 0, block_size - 2);
    min_index.add(ts_block_delta_value[0][0]);
    if (!containsValue(alpha_list,ts_block_delta_value[0][0])){
      alpha_list[pos_alpha_list] = ts_block_delta_value[0][0];
      pos_alpha_list ++;
    }

    for (int i = 0; i < k; i++) {
      if (!containsValue(alpha_list,ts_block_delta_value[block_size - 2 - k][0])){
        alpha_list[pos_alpha_list] = ts_block_delta_value[block_size - 2 - k][0];
        pos_alpha_list ++;
      }
    }


    return alpha_list;
  }

//  public static ArrayList<Integer> getIStar(
//          ArrayList<ArrayList<Integer>> ts_block,
//          ArrayList<Integer> min_index,
//          int block_size,
//          ArrayList<Integer> raw_length,
//          ArrayList<Float> theta,
//          int k) {
//    int timestamp_delta_max = Integer.MIN_VALUE;
//    int value_delta_max = Integer.MIN_VALUE;
//    int timestamp_delta_min = Integer.MAX_VALUE;
//    int value_delta_min = Integer.MAX_VALUE;
//    int timestamp_delta_max_index = -1;
//    int value_delta_max_index = -1;
//    int timestamp_delta_min_index = -1;
//    int value_delta_min_index = -1;
//    ArrayList<Integer> alpha_list = new ArrayList<>();
//
//    ArrayList<ArrayList<Integer>> ts_block_delta_time = new ArrayList<>();
//    ArrayList<ArrayList<Integer>> ts_block_delta_value = new ArrayList<>();
//
//    float theta0_t = theta.get(0);
//    float theta1_t = theta.get(1);
//    float theta0_v = theta.get(2);
//    float theta1_v = theta.get(3);
//
//
//    for (int j = 1; j < block_size; j++) {
//      int epsilon_r_j =
//              ts_block.get(j).get(0) - (int) (theta0_t + theta1_t * (float) ts_block.get(j - 1).get(0));
//      int epsilon_v_j =
//              ts_block.get(j).get(1) - (int) (theta0_v + theta1_v * (float) ts_block.get(j - 1).get(1));
//      ArrayList<Integer> tmp = new ArrayList<>();
//      tmp.add(j);
//      tmp.add(epsilon_r_j);
//      ts_block_delta_time.add(tmp);
//
//      tmp = new ArrayList<>();
//      tmp.add(j);
//      tmp.add(epsilon_v_j);
//      ts_block_delta_value.add(tmp);
//
//
//      if (epsilon_v_j > value_delta_max) {
//        value_delta_max = (int) epsilon_v_j;
//        value_delta_max_index = j;
//      }
//      if (epsilon_v_j < value_delta_min) {
//        value_delta_min = (int) epsilon_v_j;
//        value_delta_min_index = j;
//      }
//      if (epsilon_r_j > timestamp_delta_max) {
//        timestamp_delta_max = (int) epsilon_r_j;
//        timestamp_delta_max_index = j;
//      }
//      if (epsilon_r_j < timestamp_delta_min) {
//        timestamp_delta_min = (int) epsilon_r_j;
//        timestamp_delta_min_index = j;
//      }
//    }
//    quickSort(ts_block_delta_time, 1, 0, block_size - 2);
//    min_index.add(ts_block_delta_time.get(0).get(0));
//    alpha_list.add(ts_block_delta_time.get(0).get(0));
//    for (int i = 0; i < k; i++) {
//      alpha_list.add(ts_block_delta_time.get(block_size - 2 - k).get(0));
//    }
//
//    quickSort(ts_block_delta_value, 1, 0, block_size - 2);
//    min_index.add(ts_block_delta_value.get(0).get(0));
//    if (!alpha_list.contains(ts_block_delta_value.get(0).get(0)))
//      alpha_list.add(ts_block_delta_value.get(0).get(0));
//    for (int i = 0; i < k; i++) {
//      if (!alpha_list.contains(ts_block_delta_value.get(block_size - 2 - k).get(0)))
//        alpha_list.add(ts_block_delta_value.get(block_size - 2 - k).get(0));
//    }
//
//
//    return alpha_list;
//  }

  public static ArrayList<Byte> encode2Bytes(
          ArrayList<ArrayList<Integer>> ts_block,
          ArrayList<Integer> raw_length,
          ArrayList<Float> theta,
          ArrayList<Integer> result2) {
    ArrayList<Byte> encoded_result = new ArrayList<>();



    // encode interval0 and value0
    byte[] interval0_byte = int2Bytes(ts_block.get(0).get(0));
    for (byte b : interval0_byte) encoded_result.add(b);
    byte[] value0_byte = int2Bytes(ts_block.get(0).get(1));
    for (byte b : value0_byte) encoded_result.add(b);

    // encode theta
    byte[] theta0_r_byte = float2bytes(theta.get(0) + raw_length.get(3));
    for (byte b : theta0_r_byte) encoded_result.add(b);
    byte[] theta1_r_byte = float2bytes(theta.get(1));
    for (byte b : theta1_r_byte) encoded_result.add(b);
    byte[] theta0_v_byte = float2bytes(theta.get(2) + raw_length.get(4));
    for (byte b : theta0_v_byte) encoded_result.add(b);
    byte[] theta1_v_byte = float2bytes(theta.get(3));
    for (byte b : theta1_v_byte) encoded_result.add(b);

    // encode interval
    byte[] max_bit_width_interval_byte = int2Bytes(raw_length.get(1));
    for (byte b : max_bit_width_interval_byte) encoded_result.add(b);
    byte[] timestamp_bytes = bitPacking(ts_block, 0, raw_length.get(1));
    for (byte b : timestamp_bytes) encoded_result.add(b);

    // encode value
    byte[] max_bit_width_value_byte = int2Bytes(raw_length.get(2));
    for (byte b : max_bit_width_value_byte) encoded_result.add(b);
    byte[] value_bytes = bitPacking(ts_block, 1, raw_length.get(2));
    for (byte b : value_bytes) encoded_result.add(b);

    byte[] td_common_byte = int2Bytes(result2.get(0));
    for (byte b : td_common_byte) encoded_result.add(b);

    return encoded_result;
  }

  public static ArrayList<Byte> encodeRLEBitWidth2Bytes(
          ArrayList<ArrayList<Integer>> bit_width_segments) {
    ArrayList<Byte> encoded_result = new ArrayList<>();

    ArrayList<ArrayList<Integer>> run_length_time = new ArrayList<>();
    ArrayList<ArrayList<Integer>> run_length_value = new ArrayList<>();

    int count_of_time = 0;
    int count_of_value = 0;
    int pre_time = bit_width_segments.get(0).get(0);
    int pre_value = bit_width_segments.get(0).get(1);
    int size = bit_width_segments.size();
    for (int i = 1; i < size; i++) {
      int cur_time = bit_width_segments.get(i).get(0);
      int cur_value = bit_width_segments.get(i).get(1);
      if (cur_time != pre_time) {
        ArrayList<Integer> tmp = new ArrayList<>();
        tmp.add(count_of_time);
        tmp.add(pre_time);
        run_length_time.add(tmp);
        pre_time = cur_time;
        count_of_time = 0;
      } else {
        count_of_time++;
        if (count_of_time == 256) {
          ArrayList<Integer> tmp = new ArrayList<>();
          tmp.add(count_of_time);
          tmp.add(pre_time);
          run_length_time.add(tmp);
          count_of_time = 0;
        }
      }

      if (cur_value != pre_value) {
        ArrayList<Integer> tmp = new ArrayList<>();
        tmp.add(count_of_value);
        tmp.add(pre_value);
        run_length_value.add(tmp);
        pre_value = cur_value;
        count_of_value = 0;
      } else {
        count_of_value++;
        if (count_of_value == 256) {
          ArrayList<Integer> tmp = new ArrayList<>();
          tmp.add(count_of_value);
          tmp.add(pre_value);
          run_length_value.add(tmp);
          count_of_value = 0;
        }
      }

    }
    if (count_of_time != 0) {
      ArrayList<Integer> tmp = new ArrayList<>();
      tmp.add(count_of_time);
      tmp.add(pre_time);
      run_length_time.add(tmp);
    }
    if (count_of_value != 0) {
      ArrayList<Integer> tmp = new ArrayList<>();
      tmp.add(count_of_value);
      tmp.add(pre_value);
      run_length_value.add(tmp);
    }

    for (ArrayList<Integer> bit_width_time : run_length_time) {
      byte[] timestamp_bytes = bitWidth2Bytes(bit_width_time.get(0));
      for (byte b : timestamp_bytes) encoded_result.add(b);
      byte[] value_bytes = bitWidth2Bytes(bit_width_time.get(1));
      for (byte b : value_bytes) encoded_result.add(b);
    }
    for (ArrayList<Integer> bit_width_value : run_length_value) {
      byte[] timestamp_bytes = bitWidth2Bytes(bit_width_value.get(0));
      for (byte b : timestamp_bytes) encoded_result.add(b);
      byte[] value_bytes = bitWidth2Bytes(bit_width_value.get(1));
      for (byte b : value_bytes) encoded_result.add(b);
    }
    return encoded_result;
  }

  public static int encodeRLEBitWidth2Bytes(
          int[][] bit_width_segments) {
    int encoded_result = 0;

//    ArrayList<ArrayList<Integer>> run_length_time = new ArrayList<>();
//    ArrayList<ArrayList<Integer>> run_length_value = new ArrayList<>();

    int count_of_time = 0;
    int count_of_value = 0;
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
      if (cur_time != pre_time) {
        run_length_time[pos_time][0] = count_of_time;
        run_length_time[pos_time][1] = pre_time;
        pos_time ++;
        pre_time = cur_time;
        count_of_time = 0;
      } else {
        count_of_time++;
        if (count_of_time == 256) {
          run_length_time[pos_time][0] = count_of_time;
          run_length_time[pos_time][1] = pre_time;
          pos_time ++;
          count_of_time = 0;
        }
      }

      if (cur_value != pre_value) {
        run_length_time[pos_value][0] = count_of_value;
        run_length_time[pos_value][1] = pre_value;
        pos_value ++;

        pre_value = cur_value;
        count_of_value = 0;
      } else {
        count_of_value++;
        if (count_of_value == 256) {
          run_length_time[pos_value][0] = count_of_value;
          run_length_time[pos_value][1] = pre_value;
          pos_value ++;
          count_of_value = 0;
        }
      }

    }
    if (count_of_time != 0) {
      run_length_time[pos_time][0] = count_of_time;
      run_length_time[pos_time][1] = pre_time;
      pos_time ++;
    }
    if (count_of_value != 0) {
      run_length_time[pos_value][0] = count_of_value;
      run_length_time[pos_value][1] = pre_value;
      pos_value ++;
    }

    encoded_result += (pos_time * 2);
    encoded_result += (pos_value * 2);

    return encoded_result;
  }

  public static int[][] segmentBitPacking(int[][] ts_block_delta, int block_size, int segment_size) {

    int segment_n = (block_size - 1) / segment_size;
    int[][] bit_width_segments = new int[segment_n][2];
    for (int segment_i = 0; segment_i < segment_n; segment_i++) {
      int bit_width_time = Integer.MIN_VALUE;
      int bit_width_value = Integer.MIN_VALUE;

      for (int data_i = segment_i * segment_size + 1; data_i < (segment_i + 1) * segment_size + 1; data_i++) {
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
  public static ArrayList<ArrayList<Integer>> segmentBitPacking(ArrayList<ArrayList<Integer>> ts_block_delta, int block_size, int segment_size) {
    ArrayList<ArrayList<Integer>> bit_width_segments = new ArrayList<>();
    int segment_n = (block_size - 1) / segment_size;
    for (int segment_i = 0; segment_i < segment_n; segment_i++) {
      int bit_width_time = Integer.MIN_VALUE;
      int bit_width_value = Integer.MIN_VALUE;

      for (int data_i = segment_i * segment_size + 1; data_i < (segment_i + 1) * segment_size + 1; data_i++) {
        int cur_bit_width_time = getBitWith(ts_block_delta.get(data_i).get(0));
        int cur_bit_width_value = getBitWith(ts_block_delta.get(data_i).get(1));
        if (cur_bit_width_time > bit_width_time) {
          bit_width_time = cur_bit_width_time;
        }
        if (cur_bit_width_value > bit_width_value) {
          bit_width_value = cur_bit_width_value;
        }
      }
      ArrayList<Integer> bit_width = new ArrayList<>();
      bit_width.add(bit_width_time);
      bit_width.add(bit_width_value);
      bit_width_segments.add(bit_width);
    }

    return bit_width_segments;
  }

  public static void moveAlphaToBeta(ArrayList<ArrayList<Integer>> ts_block, int alpha, int beta) {
    ArrayList<Integer> tmp_tv = ts_block.get(alpha);
    if (beta < alpha) {
      for (int u = alpha - 1; u >= beta; u--) {
        ArrayList<Integer> tmp_tv_cur = new ArrayList<>();
        tmp_tv_cur.add(ts_block.get(u).get(0));
        tmp_tv_cur.add(ts_block.get(u).get(1));
        ts_block.set(u + 1, tmp_tv_cur);
      }
    } else {
      for (int u = alpha + 1; u < beta; u++) {
        ArrayList<Integer> tmp_tv_cur = new ArrayList<>();
        tmp_tv_cur.add(ts_block.get(u).get(0));
        tmp_tv_cur.add(ts_block.get(u).get(1));
        ts_block.set(u - 1, tmp_tv_cur);
      }
      beta--;
    }
    ts_block.set(beta, tmp_tv);
  }

  private static int BOSBlockEncoderPartition(int[][] data, int i, int block_size,int segment_size,int k, int encode_pos , byte[] cur_byte) {


//    ArrayList<ArrayList<Integer>> ts_block = new ArrayList<>();
//    ArrayList<ArrayList<Integer>> ts_block_reorder = new ArrayList<>();
//    ArrayList<ArrayList<Integer>> ts_block_partition = new ArrayList<>();
    int min_time = data[i * block_size][0];
    int[][] ts_block = new int[block_size][2];
    int[][] ts_block_reorder = new int[block_size][2];
    int[][] ts_block_partition = new int[block_size][2];
    for (int j = 0; j < block_size; j++) {
      data[j + i * block_size][0]-=min_time;
      ts_block[j][0] = data[j + i * block_size][0];
      ts_block[j][1] = data[j + i * block_size][1];
      ts_block_reorder[j][0] = data[j + i * block_size][0];
      ts_block_reorder[j][1] = data[j + i * block_size][1];
    }



    // raw-order
    int[] raw_length = new int[5]; // length,max_bit_width_interval,max_bit_width_value,max_bit_width_deviation
    float[] theta = new float[4];
    int[][] ts_block_delta = getEncodeBitsRegression(ts_block, block_size, raw_length, theta, segment_size);
    int[][] bit_width_segments_partition = segmentBitPacking(ts_block_delta, block_size, segment_size);
    raw_length[0] = numberOfEncodeSegment2Bytes(ts_block_delta, bit_width_segments_partition, raw_length, segment_size, theta);

    Arrays.sort(ts_block, (a, b) -> {
      if(a[0]==b[0])
        return Integer.compare(a[1], b[1]);
      return Integer.compare(a[0], b[0]);
    });
//    quickSort(ts_block, 0, 0, block_size - 1);
    int[] time_length = new int[5];// length,max_bit_width_interval,max_bit_width_value,max_bit_width_deviation
    float[] theta_time = new float[4];
    int[][] ts_block_delta_time = getEncodeBitsRegression(ts_block, block_size, time_length, theta_time, segment_size);
    int[][] bit_width_segments_time = segmentBitPacking(ts_block_delta_time, block_size, segment_size);
    time_length[0] = numberOfEncodeSegment2Bytes(ts_block_delta_time, bit_width_segments_time, time_length, segment_size, theta_time);


    Arrays.sort(ts_block, (a, b) -> {
      if(a[1]==b[1])
        return Integer.compare(a[0], b[0]);
      return Integer.compare(a[1], b[1]);
    });
    // value-order
//    quickSort(ts_block, 1, 0, block_size - 1);

    int[] reorder_length = new int[5];
    float[] theta_reorder = new float[4];
    int[][] ts_block_delta_reorder = getEncodeBitsRegression(ts_block, block_size, reorder_length, theta_reorder, segment_size);
    int[][] bit_width_segments_value = segmentBitPacking(ts_block_delta_reorder, block_size, segment_size);
    reorder_length[0] = numberOfEncodeSegment2Bytes(ts_block_delta_reorder, bit_width_segments_value, reorder_length, segment_size, theta_reorder);


    int[] alpha_list;

    int choose = min3(time_length[0], raw_length[0], reorder_length[0]);
    ArrayList<Integer> min_index = new ArrayList<>();

    if (choose == 0) {
      raw_length = time_length;
      Arrays.sort(ts_block, (a, b) -> {
        if(a[0]==b[0])
          return Integer.compare(a[1], b[1]);
        return Integer.compare(a[0], b[0]);
      });
      theta = theta_time;
      ts_block_delta = ts_block_delta_time;
      alpha_list = getIStar(ts_block,min_index, block_size, 0, theta, k);
    } else if (choose == 1) {
      ts_block = ts_block_reorder;
      alpha_list = getIStar(ts_block,min_index, block_size, 0, theta, k);
    } else {
      raw_length = reorder_length;
      theta = theta_reorder;
      ts_block_delta = ts_block_delta_reorder;
      alpha_list = getIStar(ts_block,min_index, block_size, 1, theta, k);
    }
    int[] beta_list = new int[alpha_list.length];
    int[][] new_length_list = new int[alpha_list.length][5];


//    ArrayList<ArrayList<Integer>> new_length_list = new ArrayList<>();


    for (int j = 0; j< alpha_list.length;j++) {
      int alpha = alpha_list[j];
      int[] new_length = raw_length.clone();
      beta_list[j] = getBeta(ts_block, alpha, min_index,block_size, new_length, theta, segment_size, j, new_length_list);
//      new_length_list.add(new_length);
    }
    ArrayList<Integer> isMoveable = isMovable(alpha_list, beta_list);
    int adjust_count = 0;
    while (isMoveable.size() != 0) {
      if (adjust_count < block_size / 2 && adjust_count <= 20) {
        adjust_count++;
      } else {
        break;
      }
      ArrayList<ArrayList<Integer>> all_length = new ArrayList<>();

      for (int isMoveable_i : isMoveable) {
        ArrayList<Integer> tmp = new ArrayList<>();
        tmp.add(isMoveable_i);
        tmp.add(new_length_list.get(isMoveable_i).get(0));
        all_length.add(tmp);

      }
      quickSort(all_length, 1, 0, all_length.size() - 1);
      if (all_length.get(0).get(1) <= raw_length.get(0)) {
        ArrayList<ArrayList<Integer>> new_ts_block = (ArrayList<ArrayList<Integer>>) ts_block.clone();
        moveAlphaToBeta(new_ts_block, alpha_list.get(all_length.get(0).get(0)), beta_list.get(all_length.get(0).get(0)));
        ArrayList<Integer> new_length = new ArrayList<>();
        ts_block_delta = getEncodeBitsRegression(new_ts_block, block_size, new_length, theta, segment_size);
        ArrayList<ArrayList<Integer>> bit_width_segments= segmentBitPacking(ts_block_delta, block_size, segment_size);
        new_length.set(0, encodeSegment2Bytes(ts_block_delta, bit_width_segments, new_length, segment_size, theta, result2).size());

        if(new_length.get(0) <= raw_length.get(0)){
          raw_length = new_length;
          ts_block  = (ArrayList<ArrayList<Integer>>) new_ts_block.clone();
        }else {
          break;
        }
      } else {
        break;
      }
      alpha_list = getIStar(ts_block, min_index,block_size, raw_length, theta, k);

      int alpha_size = alpha_list.size();
      for (int alpha_i = alpha_size - 1; alpha_i >= 0; alpha_i--) {
        if (beta_list.contains(alpha_list.get(alpha_i))) {
          alpha_list.remove(alpha_i);
        }
      }
      beta_list = new ArrayList<>();

      for (int alpha : alpha_list) {
        ArrayList<Integer> new_length = (ArrayList<Integer>) raw_length.clone();
        beta_list.add(getBeta(ts_block,alpha, min_index,  block_size, raw_length, theta, segment_size));
        new_length_list.add(new_length);
      }
      isMoveable = isMovable(alpha_list, beta_list);
    }
    ts_block_delta = getEncodeBitsRegressionNoTrain(ts_block, block_size, raw_length, theta, segment_size);
    ArrayList<ArrayList<Integer>> bit_width_segments = new ArrayList<>();
    int segment_n = (block_size - 1) / segment_size;
    for (int segment_i = 0; segment_i < segment_n; segment_i++) {
      int bit_width_time = Integer.MIN_VALUE;
      int bit_width_value = Integer.MIN_VALUE;

      for (int data_i = segment_i * segment_size + 1; data_i < (segment_i + 1) * segment_size + 1; data_i++) {
        int cur_bit_width_time = getBitWith(ts_block_delta.get(data_i).get(0));
        int cur_bit_width_value = getBitWith(ts_block_delta.get(data_i).get(1));
        if (cur_bit_width_time > bit_width_time) {
          bit_width_time = cur_bit_width_time;
        }
        if (cur_bit_width_value > bit_width_value) {
          bit_width_value = cur_bit_width_value;
        }
      }
      ArrayList<Integer> bit_width = new ArrayList<>();
      bit_width.add(bit_width_time);
      bit_width.add(bit_width_value);
      bit_width_segments.add(bit_width);
    }


    ArrayList<Byte> cur_encoded_result = encodeSegment2Bytes(ts_block_delta, bit_width_segments, raw_length, segment_size, theta, result2);

    return encode_pos;
  }

  public static int ReorderingRegressionEncoder(
          int[][] data, int block_size, int[] third_value, int segment_size, int k,  byte[] encoded_result) {
    block_size++;
//    ArrayList<Byte> encoded_result = new ArrayList<Byte>();
    int length_all = data.length;
    int encode_pos = 0;
    int2Bytes(length_all,encode_pos,encoded_result);
    encode_pos += 4;

    int block_num = length_all / block_size;
    int2Bytes(block_size,encode_pos,encoded_result);
    encode_pos+= 4;

    // ----------------------- compare the whole time series order by time, value and partition ---------------------------
    int length_time = 0;
    int length_value = 0;
    int length_partition = 0;
    ArrayList<ArrayList<Integer>> data_value = (ArrayList<ArrayList<Integer>>) data.clone();
    quickSort(data_value, 1, 0, length_all - 1);

    ArrayList<ArrayList<Integer>> data_partition = new ArrayList<>();

    for (ArrayList<Integer> datum : data) {
      if (datum.get(1) > third_value[third_value.length - 1]) {
        data_partition.add(datum);
      }
    }
    for (int third_i = third_value.length - 1; third_i > 0; third_i--) {
      for (ArrayList<Integer> datum : data) {
        if (datum.get(1) <= third_value[third_i] && datum.get(1) > third_value[third_i - 1]) {
          data_partition.add(datum);
        }
      }
    }
    for (ArrayList<Integer> datum : data) {
      if (datum.get(1) <= third_value[0]) {
        data_partition.add(datum);
      }
    }
    for (int i = 0; i < block_num; i++) {
      ArrayList<ArrayList<Integer>> ts_block_time = new ArrayList<>();
      ArrayList<ArrayList<Integer>> ts_block_value = new ArrayList<>();
      ArrayList<ArrayList<Integer>> ts_block_partition = new ArrayList<>();

      for (int j = 0; j < block_size; j++) {
        ts_block_time.add(data.get(j + i * block_size));
        ts_block_value.add(data_value.get(j + i * block_size));
        ts_block_partition.add(data_partition.get(j + i * block_size));
      }
      ArrayList<Integer> result1 = new ArrayList<>();
      splitTimeStamp3(ts_block_time, result1);
      ArrayList<Integer> raw_length = new ArrayList<>();
      ArrayList<Float> theta = new ArrayList<>();
      ArrayList<ArrayList<Integer>> ts_block_delta = getEncodeBitsRegression(ts_block_time, block_size, raw_length, theta, segment_size);
      ArrayList<ArrayList<Integer>> bit_width_segments = segmentBitPacking(ts_block_delta, block_size, segment_size);
      length_time += encodeSegment2Bytes(ts_block_delta, bit_width_segments, raw_length, segment_size, theta, result1).size();


      ArrayList<Integer> result2 = new ArrayList<>();
      splitTimeStamp3(ts_block_value, result2);
      ArrayList<Integer> raw_length_value = new ArrayList<>();
      ArrayList<Float> theta_value = new ArrayList<>();
      ArrayList<ArrayList<Integer>> ts_block_delta_value = getEncodeBitsRegression(ts_block_value, block_size, raw_length_value, theta_value, segment_size);
      ArrayList<ArrayList<Integer>> bit_width_segments_value = segmentBitPacking(ts_block_delta_value, block_size, segment_size);
      length_value += encodeSegment2Bytes(ts_block_delta_value, bit_width_segments_value, raw_length_value, segment_size, theta_value, result2).size();

      ArrayList<Integer> result3 = new ArrayList<>();
      splitTimeStamp3(ts_block_partition, result3);
      ArrayList<Integer> raw_length_partition = new ArrayList<>();
      ArrayList<Float> theta_partition = new ArrayList<>();
      ArrayList<ArrayList<Integer>> ts_block_delta_partition = getEncodeBitsRegression(ts_block_partition, block_size, raw_length_partition, theta_partition, segment_size);
      ArrayList<ArrayList<Integer>> bit_width_segments_partition = segmentBitPacking(ts_block_delta_partition, block_size, segment_size);
      length_partition += encodeSegment2Bytes(ts_block_delta_partition, bit_width_segments_partition, raw_length_partition, segment_size, theta_partition, result3).size();

    }
    int remaining_length = length_all - block_num * block_size;


    if (length_partition < length_time && length_partition < length_value) { // partition performs better
      data = data_partition;


      for (int i = 0; i < block_num; i++) {
        encode_pos =  BOSBlockEncoderPartition(data, i, block_size, segment_size,k, encode_pos,encoded_result);

      }
    } else {
      if (length_value < length_time) { // order by value performs better
        System.out.println("type2");
        data = data_value;
      } else {
        System.out.println("type1");
      }
      for (int i = 0; i < block_num; i++) {
        ArrayList<ArrayList<Integer>> ts_block = new ArrayList<>();
        ArrayList<ArrayList<Integer>> ts_block_reorder = new ArrayList<>();
        ArrayList<ArrayList<Integer>> ts_block_partition = new ArrayList<>();
        int min_time = data.get(i * block_size).get(0);
        for (int j = 0; j < block_size; j++) {
          data.get(j + i * block_size).set(0,data.get(j + i * block_size).get(0)-min_time);
          ts_block.add(data.get(j + i * block_size));
          ts_block_reorder.add(data.get(j + i * block_size));
        }

        ArrayList<Integer> result2 = new ArrayList<>();
        //      result2.add(1);
        splitTimeStamp3(ts_block, result2);
        splitTimeStamp3(ts_block_reorder, result2);
        quickSort(ts_block, 0, 0, block_size - 1);
        ArrayList<Integer> raw_length = new ArrayList<>(); // length,max_bit_width_interval,max_bit_width_value,max_bit_width_deviation
        ArrayList<Float> theta = new ArrayList<>();
        ArrayList<ArrayList<Integer>> ts_block_delta = getEncodeBitsRegression(ts_block, block_size, raw_length, theta, segment_size);
        ArrayList<ArrayList<Integer>> bit_width_segments_time = segmentBitPacking(ts_block_delta, block_size, segment_size);
        raw_length.set(0, encodeSegment2Bytes(ts_block_delta, bit_width_segments_time, raw_length, segment_size, theta, result2).size());

        // value-order
        quickSort(ts_block, 1, 0, block_size - 1);

        ArrayList<Integer> reorder_length = new ArrayList<>();
        ArrayList<Float> theta_reorder = new ArrayList<>();
        ArrayList<ArrayList<Integer>> ts_block_delta_reorder = getEncodeBitsRegression(ts_block, block_size, reorder_length, theta_reorder, segment_size);
        ArrayList<ArrayList<Integer>> bit_width_segments_value = segmentBitPacking(ts_block_delta_reorder, block_size, segment_size);
        reorder_length.set(0, encodeSegment2Bytes(ts_block_delta_reorder, bit_width_segments_value, reorder_length, segment_size, theta_reorder, result2).size());

        for (ArrayList<Integer> datum : ts_block) {
          if (datum.get(1) > third_value[third_value.length - 1]) {
            ts_block_partition.add(datum);
          }
        }
        for (int third_i = third_value.length - 1; third_i > 0; third_i--) {
          for (ArrayList<Integer> datum : ts_block) {
            if (datum.get(1) <= third_value[third_i] && datum.get(1) > third_value[third_i - 1]) {
              ts_block_partition.add(datum);
            }
          }
        }
        for (ArrayList<Integer> datum : ts_block) {
          if (datum.get(1) <= third_value[0]) {
            ts_block_partition.add(datum);
          }
        }
        ArrayList<Integer> partition_length = new ArrayList<>();
        ArrayList<Float> theta_partition = new ArrayList<>();
        ArrayList<ArrayList<Integer>> ts_block_delta_partition = getEncodeBitsRegression(ts_block_partition, block_size, partition_length, theta_partition, segment_size);
        ArrayList<ArrayList<Integer>> bit_width_segments_partition = segmentBitPacking(ts_block_delta_partition, block_size, segment_size);
        partition_length.set(0, encodeSegment2Bytes(ts_block_delta_partition, bit_width_segments_partition, partition_length, segment_size, theta_partition, result2).size());


        int choose = min3(partition_length.get(0), reorder_length.get(0), raw_length.get(0));
        ArrayList<Integer> alpha_list;
        ArrayList<Integer> min_index = new ArrayList<>();
        if (choose == 0) {
          raw_length = partition_length;
          ts_block = ts_block_partition;
          ts_block_delta = ts_block_delta_partition;
          theta = theta_partition;
          alpha_list = getIStar(ts_block, min_index,block_size, 0, theta, k);
        } else if (choose == 1) {
          raw_length = reorder_length;
          quickSort(ts_block, 1, 0, block_size - 1);
          ts_block_delta = ts_block_delta_reorder;
          theta = theta_reorder;
          alpha_list = getIStar(ts_block,min_index, block_size, 1, theta, k);
        } else {
                    quickSort(ts_block, 0, 0, block_size - 1);
          alpha_list = getIStar(ts_block,min_index, block_size, 0, theta, k);
        }

        ArrayList<Integer> beta_list= new ArrayList<>();
        ArrayList<ArrayList<Integer>> new_length_list = new ArrayList<>();

        for (int alpha : alpha_list) {
          ArrayList<Integer> new_length = (ArrayList<Integer>) raw_length.clone();
          beta_list.add(getBeta(ts_block, alpha, min_index,block_size, new_length, theta, segment_size));
          new_length_list.add(new_length);
        }
        ArrayList<Integer> isMoveable = isMovable(alpha_list, beta_list);
        int adjust_count = 0;
        while (isMoveable.size() != 0) {
          if (adjust_count < block_size / 2 && adjust_count <= 20) {
            adjust_count++;
          } else {
            break;
          }
          ArrayList<ArrayList<Integer>> all_length = new ArrayList<>();

          for (int isMoveable_i : isMoveable) {
            ArrayList<Integer> tmp = new ArrayList<>();
            tmp.add(isMoveable_i);
            tmp.add(new_length_list.get(isMoveable_i).get(0));
            all_length.add(tmp);

          }
          quickSort(all_length, 1, 0, all_length.size() - 1);
          if (all_length.get(0).get(1) <= raw_length.get(0)) {
            ArrayList<ArrayList<Integer>> new_ts_block = (ArrayList<ArrayList<Integer>>) ts_block.clone();
            moveAlphaToBeta(new_ts_block, alpha_list.get(all_length.get(0).get(0)), beta_list.get(all_length.get(0).get(0)));
            ArrayList<Integer> new_length = new ArrayList<>();
            ts_block_delta = getEncodeBitsRegression(new_ts_block, block_size, new_length, theta, segment_size);
            ArrayList<ArrayList<Integer>> bit_width_segments= segmentBitPacking(ts_block_delta, block_size, segment_size);
            new_length.set(0, encodeSegment2Bytes(ts_block_delta, bit_width_segments, new_length, segment_size, theta, result2).size());

            if(new_length.get(0) <= raw_length.get(0)){
              raw_length = new_length;
              ts_block  = (ArrayList<ArrayList<Integer>>) new_ts_block.clone();
            }else {
              break;
            }
          } else {
            break;
          }
          alpha_list = getIStar(ts_block,min_index, block_size, raw_length, theta, k);
          int alpha_size = alpha_list.size();
          for (int alpha_i = alpha_size - 1; alpha_i >= 0; alpha_i--) {
            if (beta_list.contains(alpha_list.get(alpha_i))) {
              alpha_list.remove(alpha_i);
            }
          }
          beta_list = new ArrayList<>();
          for (int alpha : alpha_list) {
            ArrayList<Integer> new_length = (ArrayList<Integer>) raw_length.clone();
            beta_list.add(getBeta(ts_block, alpha, min_index,block_size, new_length, theta, segment_size));
            new_length_list.add(new_length);
          }
          isMoveable = isMovable(alpha_list, beta_list);
        }
        ts_block_delta = getEncodeBitsRegressionNoTrain(ts_block, block_size, raw_length, theta, segment_size);
        ArrayList<ArrayList<Integer>> bit_width_segments = new ArrayList<>();
        int segment_n = (block_size - 1) / segment_size;
        for (int segment_i = 0; segment_i < segment_n; segment_i++) {
          int bit_width_time = Integer.MIN_VALUE;
          int bit_width_value = Integer.MIN_VALUE;

          for (int data_i = segment_i * segment_size + 1; data_i < (segment_i + 1) * segment_size + 1; data_i++) {
            int cur_bit_width_time = getBitWith(ts_block_delta.get(data_i).get(0));
            int cur_bit_width_value = getBitWith(ts_block_delta.get(data_i).get(1));
            if (cur_bit_width_time > bit_width_time) {
              bit_width_time = cur_bit_width_time;
            }
            if (cur_bit_width_value > bit_width_value) {
              bit_width_value = cur_bit_width_value;
            }
          }
          ArrayList<Integer> bit_width = new ArrayList<>();
          bit_width.add(bit_width_time);
          bit_width.add(bit_width_value);
          bit_width_segments.add(bit_width);
        }

        ArrayList<Byte> cur_encoded_result = encodeSegment2Bytes(ts_block_delta, bit_width_segments, raw_length, segment_size, theta, result2);
        encoded_result.addAll(cur_encoded_result);

      }
    }


    remaining_length = length_all - block_num * block_size;
    if (remaining_length == 1) {
      byte[] timestamp_end_bytes = int2Bytes(data.get(data.size() - 1).get(0));
      for (byte b : timestamp_end_bytes) encoded_result.add(b);
      byte[] value_end_bytes = int2Bytes(data.get(data.size() - 1).get(1));
      for (byte b : value_end_bytes) encoded_result.add(b);
    }
    if (remaining_length != 0 && remaining_length != 1) {
      ArrayList<ArrayList<Integer>> ts_block = new ArrayList<>();
      ArrayList<ArrayList<Integer>> ts_block_reorder = new ArrayList<>();

      int min_time = data.get(block_num * block_size).get(0);
      for (int j = block_num * block_size; j < length_all; j++) {
        data.get(j).set(0,data.get(j).get(0) - min_time);
        ts_block.add(data.get(j));
        ts_block_reorder.add(data.get(j));
      }
      ArrayList<Integer> result2 = new ArrayList<>();
      splitTimeStamp3(ts_block, result2);

      quickSort(ts_block, 0, 0, remaining_length - 1);

      // time-order
      ArrayList<Integer> raw_length =
              new ArrayList<>(); // length,max_bit_width_interval,max_bit_width_value,max_bit_width_deviation
      ArrayList<Integer> i_star_ready = new ArrayList<>();
      ArrayList<Float> theta = new ArrayList<>();
      ArrayList<ArrayList<Integer>> ts_block_delta =
              getEncodeBitsRegression(ts_block, remaining_length, raw_length, theta, segment_size);

      // value-order
      quickSort(ts_block, 1, 0, remaining_length - 1);
      ArrayList<Integer> reorder_length = new ArrayList<>();
      ArrayList<Integer> i_star_ready_reorder = new ArrayList<>();
      ArrayList<Float> theta_reorder = new ArrayList<>();
      ArrayList<ArrayList<Integer>> ts_block_delta_reorder =
              getEncodeBitsRegression(
                      ts_block, remaining_length, reorder_length, theta_reorder, segment_size);

      if (raw_length.get(0) <= reorder_length.get(0)) {
        quickSort(ts_block, 0, 0, remaining_length - 1);
      } else {
        raw_length = reorder_length;
        theta = theta_reorder;
        quickSort(ts_block, 1, 0, remaining_length - 1);
      }
      ts_block_delta =
              getEncodeBitsRegression(ts_block, remaining_length, raw_length, theta, segment_size);
      int supple_length;
      if (remaining_length % 8 == 0) {
        supple_length = 1;
      } else if (remaining_length % 8 == 1) {
        supple_length = 0;
      } else {
        supple_length = 9 - remaining_length % 8;
      }
      for (int s = 0; s < supple_length; s++) {
        ArrayList<Integer> tmp = new ArrayList<>();
        tmp.add(0);
        tmp.add(0);
        ts_block_delta.add(tmp);
      }
      ArrayList<Byte> cur_encoded_result = encode2Bytes(ts_block_delta, raw_length, theta, result2);

      encoded_result.addAll(cur_encoded_result);
    }
    return encoded_result;
  }

  private static ArrayList<Integer> isMovable(ArrayList<Integer> alpha_list, ArrayList<Integer> beta_list) {
    ArrayList<Integer> isMoveable = new ArrayList<>();
    for (int i = 0; i < alpha_list.size(); i++) {
      if (alpha_list.get(i) != -1 && beta_list.get(i) != -1) {
        isMoveable.add(i);
      }
    }
    return isMoveable;
  }

  private static int numberOfEncodeSegment2Bytes(int[][] delta_segments,int[][]  bit_width_segments, int[] raw_length, int segment_size, float[] theta) {
    ArrayList<Byte> encoded_result = new ArrayList<>();
    int block_size = delta_segments.length;
    int segment_n = block_size / segment_size;
    int result = 0;
    result += 8; // encode interval0 and value0
    result += 16; // encode theta
    result += encodeRLEBitWidth2Bytes(bit_width_segments);
//    // encode interval0 and value0
//    byte[] interval0_byte = int2Bytes(delta_segments.get(0).get(0));
//    for (byte b : interval0_byte) encoded_result.add(b);
//    byte[] value0_byte = int2Bytes(delta_segments.get(0).get(1));
//    for (byte b : value0_byte) encoded_result.add(b);
//
//    // encode theta
//    byte[] theta0_r_byte = float2bytes(theta.get(0) + raw_length.get(3));
//    for (byte b : theta0_r_byte) encoded_result.add(b);
//    byte[] theta1_r_byte = float2bytes(theta.get(1));
//    for (byte b : theta1_r_byte) encoded_result.add(b);
//    byte[] theta0_v_byte = float2bytes(theta.get(2) + raw_length.get(4));
//    for (byte b : theta0_v_byte) encoded_result.add(b);
//    byte[] theta1_v_byte = float2bytes(theta.get(3));
//    for (byte b : theta1_v_byte) encoded_result.add(b);
//    encoded_result.addAll(encodeRLEBitWidth2Bytes(bit_width_segments));
    for (int segment_i = 0; segment_i < segment_n; segment_i++) {
      int bit_width_time = bit_width_segments[segment_i][0];
      int bit_width_value = bit_width_segments[segment_i][1];
      result += (segment_size*bit_width_time/8);
      result += (segment_size*bit_width_value/8);
    }

//    byte[] td_common_byte = int2Bytes(result2.get(0));
//    for (byte b : td_common_byte) encoded_result.add(b);

    return result;
  }
  private static ArrayList<Byte> encodeSegment2Bytes(ArrayList<ArrayList<Integer>> delta_segments, ArrayList<ArrayList<Integer>> bit_width_segments, ArrayList<Integer> raw_length, int segment_size, ArrayList<Float> theta) {
    ArrayList<Byte> encoded_result = new ArrayList<>();
    int block_size = delta_segments.size();
    int segment_n = block_size / segment_size;

    // encode interval0 and value0
    byte[] interval0_byte = int2Bytes(delta_segments.get(0).get(0));
    for (byte b : interval0_byte) encoded_result.add(b);
    byte[] value0_byte = int2Bytes(delta_segments.get(0).get(1));
    for (byte b : value0_byte) encoded_result.add(b);

    // encode theta
    byte[] theta0_r_byte = float2bytes(theta.get(0) + raw_length.get(3));
    for (byte b : theta0_r_byte) encoded_result.add(b);
    byte[] theta1_r_byte = float2bytes(theta.get(1));
    for (byte b : theta1_r_byte) encoded_result.add(b);
    byte[] theta0_v_byte = float2bytes(theta.get(2) + raw_length.get(4));
    for (byte b : theta0_v_byte) encoded_result.add(b);
    byte[] theta1_v_byte = float2bytes(theta.get(3));
    for (byte b : theta1_v_byte) encoded_result.add(b);
    encoded_result.addAll(encodeRLEBitWidth2Bytes(bit_width_segments));
    for (int segment_i = 0; segment_i < segment_n; segment_i++) {
      int bit_width_time = bit_width_segments.get(segment_i).get(0);
      int bit_width_value = bit_width_segments.get(segment_i).get(1);
      byte[] timestamp_bytes = bitPacking(delta_segments, 0, segment_i * segment_size + 1, segment_size, bit_width_time);
      for (byte b : timestamp_bytes) encoded_result.add(b);
      byte[] value_bytes = bitPacking(delta_segments, 1, segment_i * segment_size + 1, segment_size, bit_width_value);
      for (byte b : value_bytes) encoded_result.add(b);
    }

//    byte[] td_common_byte = int2Bytes(result2.get(0));
//    for (byte b : td_common_byte) encoded_result.add(b);

    return encoded_result;
  }

  public static ArrayList<ArrayList<Integer>> ReorderingRegressionDecoder(ArrayList<Byte> encoded) {
    ArrayList<ArrayList<Integer>> data = new ArrayList<>();
    int decode_pos = 0;
    int length_all = bytes2Integer(encoded, decode_pos, 4);
    decode_pos += 4;
    int block_size = bytes2Integer(encoded, decode_pos, 4);
    decode_pos += 4;

    int block_num = length_all / block_size;
    int remain_length = length_all - block_num * block_size;
    int zero_number;
    if (remain_length % 8 == 0) {
      zero_number = 1;
    } else if (remain_length % 8 == 1) {
      zero_number = 0;
    } else {
      zero_number = 9 - remain_length % 8;
    }

    for (int k = 0; k < block_num; k++) {
      ArrayList<Integer> time_list = new ArrayList<>();
      ArrayList<Integer> value_list = new ArrayList<>();

      ArrayList<ArrayList<Integer>> ts_block = new ArrayList<>();

      int time0 = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;
      int value0 = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;

      float theta0_r = bytes2float(encoded, decode_pos);
      decode_pos += 4;
      float theta1_r = bytes2float(encoded, decode_pos);
      decode_pos += 4;
      float theta0_v = bytes2float(encoded, decode_pos);
      decode_pos += 4;
      float theta1_v = bytes2float(encoded, decode_pos);
      decode_pos += 4;


      // get bit_width_segments
      ArrayList<Integer> bit_width_segments_time = new ArrayList<>();
      ArrayList<Integer> bit_width_segments_value = new ArrayList<>();
      int sum_time_bit_width = 0;
      int sum_value_bit_width = 0;
      int segment_n = (block_size - 1) / 8;
      while (sum_time_bit_width != segment_n) {
        int max_bit_width_time_nums = bytes2BitWidth(encoded, decode_pos, 1);
        decode_pos += 1;
        int max_bit_width_time = bytes2BitWidth(encoded, decode_pos, 1);
        decode_pos += 1;
        sum_time_bit_width += max_bit_width_time_nums;
        for (int i = 0; i < max_bit_width_time_nums; i++) {
          bit_width_segments_time.add(max_bit_width_time);
        }
      }
      while (sum_value_bit_width != segment_n) {
        int max_bit_width_value_nums = bytes2BitWidth(encoded, decode_pos, 1);
        decode_pos += 1;
        int max_bit_width_value = bytes2BitWidth(encoded, decode_pos, 1);
        decode_pos += 1;
        sum_value_bit_width += max_bit_width_value_nums;
        for (int i = 0; i < max_bit_width_value_nums; i++) {
          bit_width_segments_value.add(max_bit_width_value);
        }
      }


      for (int i = 0; i < segment_n; i++) {
        int max_bit_width_time = bit_width_segments_time.get(i);
        ArrayList<Integer> tmp_time_list = decodebitPacking(encoded, decode_pos, max_bit_width_time, 0, 9);
        decode_pos += max_bit_width_time * 8;
        time_list.addAll(tmp_time_list);
        int max_bit_width_value = bit_width_segments_value.get(i);
        ArrayList<Integer> tmp_value_list = decodebitPacking(encoded, decode_pos, max_bit_width_value, 0, 9);
        decode_pos += max_bit_width_value * 8;
        value_list.addAll(tmp_value_list);
      }


      int td_common = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;

      int ti_pre = time0;
      int vi_pre = value0;
      for (int i = 0; i < block_size - 1; i++) {
        int ti = (int) ((double) theta0_r + (double) theta1_r * (double) ti_pre) + time_list.get(i);
        time_list.set(i, ti);
        ti_pre = ti;

        int vi =
                (int) ((double) theta0_v + (double) theta1_v * (double) vi_pre) + value_list.get(i);
        value_list.set(i, vi);
        vi_pre = vi;
      }

      ArrayList<Integer> ts_block_tmp0 = new ArrayList<>();
      ts_block_tmp0.add(time0);
      ts_block_tmp0.add(value0);
      ts_block.add(ts_block_tmp0);
      for (int i = 0; i < block_size - 1; i++) {
        int ti = (time_list.get(i) - time0) * td_common + time0;
        ArrayList<Integer> ts_block_tmp = new ArrayList<>();
        ts_block_tmp.add(ti);
        ts_block_tmp.add(value_list.get(i));
        ts_block.add(ts_block_tmp);
      }
      quickSort(ts_block, 0, 0, block_size - 1);
      data.addAll(ts_block);
    }

    if (remain_length == 1) {
      int timestamp_end = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;
      int value_end = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;
      ArrayList<Integer> ts_block_end = new ArrayList<>();
      ts_block_end.add(timestamp_end);
      ts_block_end.add(value_end);
      data.add(ts_block_end);
    }
    if (remain_length != 0 && remain_length != 1) {
      ArrayList<Integer> time_list = new ArrayList<>();
      ArrayList<Integer> value_list = new ArrayList<>();

      ArrayList<ArrayList<Integer>> ts_block = new ArrayList<>();

      int time0 = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;
      int value0 = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;

      float theta0_r = bytes2float(encoded, decode_pos);
      decode_pos += 4;
      float theta1_r = bytes2float(encoded, decode_pos);
      decode_pos += 4;
      float theta0_v = bytes2float(encoded, decode_pos);
      decode_pos += 4;
      float theta1_v = bytes2float(encoded, decode_pos);
      decode_pos += 4;

      int max_bit_width_time = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;
      time_list =
              decodebitPacking(encoded, decode_pos, max_bit_width_time, 0, remain_length + zero_number);
      decode_pos += max_bit_width_time * (remain_length + zero_number - 1) / 8;

      int max_bit_width_value = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;
      value_list =
              decodebitPacking(
                      encoded, decode_pos, max_bit_width_value, 0, remain_length + zero_number);
      decode_pos += max_bit_width_value * (remain_length + zero_number - 1) / 8;

      int td_common = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;

      int ti_pre = time0;
      int vi_pre = value0;
      for (int i = 0; i < remain_length - 1; i++) {
        int ti = (int) ((double) theta0_r + (double) theta1_r * (double) ti_pre) + time_list.get(i);
        time_list.set(i, ti);
        ti_pre = ti;
        int vi =
                (int) ((double) theta0_v + (double) theta1_v * (double) vi_pre) + value_list.get(i);
        value_list.set(i, vi);
        vi_pre = vi;
      }

      ArrayList<Integer> ts_block_tmp0 = new ArrayList<>();
      ts_block_tmp0.add(time0);
      ts_block_tmp0.add(value0);
      ts_block.add(ts_block_tmp0);
      for (int i = 0; i < remain_length - 1; i++) {
        int ti = (time_list.get(i) - time0) * td_common + time0;
        ArrayList<Integer> ts_block_tmp = new ArrayList<>();
        ts_block_tmp.add(ti);
        ts_block_tmp.add(value_list.get(i));
        ts_block.add(ts_block_tmp);
      }

      quickSort(ts_block, 0, 0, remain_length - 1);
      for (int i = 0; i < remain_length; i++) {
        data.add(ts_block.get(i));
      }
    }
    return data;
  }


  public static void main(@org.jetbrains.annotations.NotNull String[] args) throws IOException {
//        String parent_dir = "C:/Users/xiaoj/Desktop/test";
    String parent_dir = "/Users/xiaojinzhao/Documents/GitHub/iotdb/iotdb-core/tsfile/src/test/resources/";
    String output_parent_dir = "/Users/xiaojinzhao/Documents/GitHub/encoding-reorder/compression_ratio";


    String input_parent_dir = parent_dir + "trans_data/";
    ArrayList<String> input_path_list = new ArrayList<>();
    ArrayList<String> output_path_list = new ArrayList<>();
    ArrayList<String> dataset_name = new ArrayList<>();
    ArrayList<Integer> dataset_block_size = new ArrayList<>();
    ArrayList<Integer> dataset_mid = new ArrayList<>();
    ArrayList<int[]> dataset_third = new ArrayList<>();
    ArrayList<Integer> dataset_k = new ArrayList<>();
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

    int[] dataset_0 = {547,2816};
    int[] dataset_1 = {1719,3731};
    int[] dataset_2 = {-48,-11,6,25,52};
    int[] dataset_3 = {8681,13584};
    int[] dataset_4 = {79,184,274};
    int[] dataset_5 = {17,68};
    int[] dataset_6 = {677};
    int[] dataset_7 = {1047,1725};
    int[] dataset_8 = {227,499,614,1013};
    int[] dataset_9 = {474,678};
    int[] dataset_10 = {4,30,38,49,58};
    int[] dataset_11 = {5182,8206};

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

    for (int i = 0; i < dataset_name.size(); i++) {
      input_path_list.add(input_parent_dir + dataset_name.get(i));
      dataset_k.add(1);
    }

    output_path_list.add(output_parent_dir + "/CS-Sensors_ratio.csv"); // 0
    dataset_block_size.add(1024);

    output_path_list.add(output_parent_dir + "/Metro-Traffic_ratio.csv");// 1
    dataset_block_size.add(512);
    output_path_list.add(output_parent_dir + "/USGS-Earthquakes_ratio.csv");// 2
    dataset_block_size.add(512);
    output_path_list.add(output_parent_dir + "/YZ-Electricity_ratio.csv"); // 3
    dataset_block_size.add(256);
    output_path_list.add(output_parent_dir + "/GW-Magnetic_ratio.csv"); //4
    dataset_block_size.add(128);
    output_path_list.add(output_parent_dir + "/TY-Fuel_ratio.csv");//5
    dataset_block_size.add(64);
    output_path_list.add(output_parent_dir + "/Cyber-Vehicle_ratio.csv"); //6
    dataset_block_size.add(128);
    output_path_list.add(output_parent_dir + "/Vehicle-Charge_ratio.csv");//7
    dataset_block_size.add(512);
    output_path_list.add(output_parent_dir + "/Nifty-Stocks_ratio.csv");//8
    dataset_block_size.add(256);
    output_path_list.add(output_parent_dir + "/TH-Climate_ratio.csv");//9
    dataset_block_size.add(512);
    output_path_list.add(output_parent_dir + "/TY-Transport_ratio.csv");//10
    dataset_block_size.add(512);
    output_path_list.add(output_parent_dir + "/EPM-Education_ratio.csv");//11
    dataset_block_size.add(512);

//    for (int file_i = 0; file_i < input_path_list.size(); file_i++) {
    for (int file_i = 9; file_i < 10; file_i++) {
      String inputPath = input_path_list.get(file_i);
      String Output = output_path_list.get(file_i);

      int repeatTime = 1; // set repeat time

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
        System.out.println(f);
        InputStream inputStream = Files.newInputStream(f.toPath());
        CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
        ArrayList<ArrayList<Integer>> data = new ArrayList<>();

        ArrayList<ArrayList<Integer>> data_decoded = new ArrayList<>();

        // add a column to "data"
        loader.readHeaders();
        data.clear();
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
        for(int i = 0;i<data.size();i++){
          data2_arr[i][0] = data.get(i).get(0);
          data2_arr[i][1] = data.get(i).get(1);
        }
        System.out.println(data2_arr[0][0]);
        byte[] encoded_result = new byte[data2_arr.length*8];
        long encodeTime = 0;
        long decodeTime = 0;
        double ratio = 0;
        double compressed_size = 0;
        int repeatTime2 = 1;
          long s = System.nanoTime();
          ArrayList<Byte> buffer = new ArrayList<>();
          int length = 0;
          for (int repeat = 0; repeat < repeatTime2; repeat++)
            length = ReorderingRegressionEncoder(data2_arr, dataset_block_size.get(file_i), dataset_third.get(file_i), 8, dataset_k.get(file_i), encoded_result);
          long e = System.nanoTime();
          encodeTime += ((e - s) / repeatTime2);
          compressed_size += length;
          double ratioTmp = (double) buffer.size() / (double) (data.size() * Integer.BYTES * 2);
          ratio += ratioTmp;
          s = System.nanoTime();
          e = System.nanoTime();
          decodeTime += ((e - s) / repeatTime2);


        String[] record = {
          f.toString(),
          "REGER",
          String.valueOf(encodeTime),
          String.valueOf(decodeTime),
          String.valueOf(data.size()),
          String.valueOf(compressed_size),
          String.valueOf(ratio)
        };
        writer.writeRecord(record);

      }
      writer.close();
    }
  }
}
