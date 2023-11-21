package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.apache.commons.math3.linear.*;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import org.junit.Test;
import java.text.DecimalFormat;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import java.util.Stack;

import static java.lang.Math.abs;

public class RegerPDoubleTest {

  public static int min3(int a, int b, int c) {
    if (a < b && a < c) {
      return 0;
    } else if (b < c) {
      return 1;
    } else {
      return 2;
    }
  }

  public static double decimalFloat(double f,int pre){
    StringBuilder pattern = new StringBuilder("#.");
    for(int i=0;i<pre;i++){
      pattern.append("#");
    }
    DecimalFormat decimalFormat = new DecimalFormat(pattern.toString());

    String formattedFloatStr = decimalFormat.format(f);
    return Double.parseDouble(formattedFloatStr);
  }
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

  public static void double2bytes(double f, int pos_encode, byte[] encode_result) {
    long fbit = Double.doubleToLongBits(f);
    byte[] b = new byte[8];
    for (int i = 0; i < 8; i++) {
      b[i] = (byte) (fbit >> (56 - i * 8));
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

  public static byte[] int2Bytes(int integer) {
    byte[] bytes = new byte[4];
    bytes[0] = (byte) (integer >> 24);
    bytes[1] = (byte) (integer >> 16);
    bytes[2] = (byte) (integer >> 8);
    bytes[3] = (byte) integer;
    return bytes;
  }

  public static byte[] bitWidth2Bytes(int integer) {
    byte[] bytes = new byte[1];
    bytes[0] = (byte) integer;
    return bytes;
  }

  public static byte[] bitPacking(
      ArrayList<ArrayList<Integer>> numbers, int index, int start, int block_num, int bit_width) {
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
    for (int i = 0; i < (block_size - 1) / 8; i++) {
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

      tmp.add(interval_i);
      tmp.add(ts_block.get(i).get(1));
      ts_block.set(i, tmp);
    }
    ArrayList<Integer> tmp = new ArrayList<>();

    tmp.add(0);
    tmp.add(ts_block.get(0).get(1));
    ts_block.set(0, tmp);

    result.add(td_common);
    result.add(t0);
  }

//  public static void terminate(int[][] ts_block, double[] coefficient, int p) {
//    int length = ts_block.length;
//    assert length > p;
//    int size = length - p;
//
//    double[] param;
//    try {
//      OLSMultipleLinearRegression ols1 = new OLSMultipleLinearRegression();
//      double[][] X1 = new double[size][p];
//      double[] Y1 = new double[size];
//      for (int i = 0; i < size; i++) {
//        X1[i] = new double[p];
//        for (int j = 0; j < p; j++) {
//          X1[i][j] = ts_block[i + j][0];
//        }
//        Y1[i] = ts_block[i + p][0];
//      }
//      ols1.newSampleData(Y1, X1);
//      param = ols1.estimateRegressionParameters(); // 结果的第1项是常数项， 之后依次序为各个特征的系数
//      // System.out.println(Arrays.toString(param));
//    } catch (Exception e) {
//      param = new double[p + 1];
//      for (int i = 0; i <= p; i++) {
//        param[i] = 0;
//      }
//    }
//
//    double[] param2;
//    try {
//      OLSMultipleLinearRegression ols2 = new OLSMultipleLinearRegression();
//      double[][] X2 = new double[size][p];
//      double[] Y2 = new double[size];
//      for (int i = 0; i < size; i++) {
//        X2[i] = new double[p];
//        for (int j = 0; j < p; j++) {
//          X2[i][j] = ts_block[i + j][1];
//        }
//        Y2[i] = ts_block[i + p][1];
//      }
//      ols2.newSampleData(Y2, X2);
//      param2 = ols2.estimateRegressionParameters(); // 结果的第1项是常数项， 之后依次序为各个特征的系数
//      // System.out.println(Arrays.toString(param2));
//    } catch (Exception exception) {
//      param2 = new double[p + 1];
//      for (int i = 0; i <= p; i++) {
//        param2[i] = 0;
//      }
//    }
//
//    for (int i = 0; i <= p; i++) {
//      coefficient[2 * i] = param[i];
//      coefficient[2 * i + 1] = param2[i];
//    }
//  }

  public static void terminate(int[][] ts_block, double[] coefficient, int p, int pre){
    int length = ts_block.length;
    assert length > p;
    int size = length - p;

    OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();

    RealMatrix matrix = new Array2DRowRealMatrix(size, p+1);
    RealVector vector = new ArrayRealVector(size);

    for (int i = 0; i < size; i++) {
      for (int j = 0; j < p; j++) {
        matrix.setEntry(i, j, ts_block[i + j][0]);
      }
      matrix.setEntry(i, p, 1);
      vector.setEntry(i, ts_block[i + p][0]);
    }


    DecompositionSolver solver = new SingularValueDecomposition(matrix).getSolver();
    RealVector solution1 = solver.solve(vector);

    matrix = new Array2DRowRealMatrix(size, p+1);
    vector = new ArrayRealVector(size);

    for (int i = 0; i < size; i++) {
      for (int j = 0; j < p; j++) {
        matrix.setEntry(i, j, ts_block[i + j][1]);
      }
      matrix.setEntry(i, p, 1);
      vector.setEntry(i, ts_block[i + p][1]);
    }

    DecompositionSolver solver1 = new SingularValueDecomposition(matrix).getSolver();
    RealVector solution2 = solver1.solve(vector);

//    // 输出系数
//    System.out.println("自回归模型系数：");
//    for (int i = 0; i < solution.getDimension(); i++) {
//      System.out.println("AR(" + (i + 1) + "): " + solution.getEntry(i));
//    }

//    int pre = 1;
//    for (int i = 0; i <= p; i++) {
//      coefficient[2 * i] =  decimalFloat(solution1.getEntry(p-i),pre);
//      coefficient[2 * i + 1] = decimalFloat(solution2.getEntry(p-i),pre);
//    }
    for (int i = 0; i <= p; i++) {
      coefficient[2 * i] =  solution1.getEntry(p-i);
      coefficient[2 * i + 1] = solution2.getEntry(p-i);
    }
//    System.out.println(Arrays.toString(coefficient));
  }

  public static int[][] deltaTSBlock(int[][] ts_block,
                                     int[] delta_index,
                                     int[] delta,
                                     double[] theta,
                                     int p){
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    int timestamp_delta_min_index = -1;
    int value_delta_min_index = -1;
    int timestamp_delta_max = Integer.MIN_VALUE;
    int value_delta_max = Integer.MIN_VALUE;
    int timestamp_delta_max_index = -1;
    int value_delta_max_index = -1;
    int block_size = ts_block.length;

    int[][] ts_block_delta = new int[block_size][2];
    ts_block_delta[0][0] = ts_block[0][0];
    ts_block_delta[0][1] = ts_block[0][1];

    // regression residual
    for (int j = 1; j < p; j++) {
      double epsilon_r = (double) ts_block[j][0] - theta[0];
      double epsilon_v = (double) ts_block[j][1] - theta[1];
      for (int pi = 1; pi <= j; pi++) {
        epsilon_r -= theta[2 * pi] * (double) ts_block[j - pi][0];
        epsilon_v -= theta[2 * pi + 1] * (double) ts_block[j - pi][1];
      }
      ts_block_delta[j][0] = (int) epsilon_r;
      ts_block_delta[j][1] = (int) epsilon_v;

      if (epsilon_r < timestamp_delta_min) {
        timestamp_delta_min = (int) epsilon_r;
        timestamp_delta_min_index = j;
      }
      if (epsilon_v < value_delta_min) {
        value_delta_min = (int) epsilon_v;
        value_delta_min_index = j;
      }
      if (epsilon_r > timestamp_delta_max) {
        timestamp_delta_max = (int) epsilon_r;
        timestamp_delta_max_index = j;
      }
      if (epsilon_v > value_delta_max) {
        value_delta_max = (int) epsilon_v;
        value_delta_max_index = j;
      }
    }
    for (int j = p; j < block_size; j++) {
      double epsilon_r = (double) ts_block[j][0] - theta[0];
      double epsilon_v = (double) ts_block[j][1] - theta[1];

      for (int pi = 1; pi <= p; pi++) {
        epsilon_r -=  (theta[2 * pi] * (double) ts_block[j - pi][0]);
        epsilon_v -=  (theta[2 * pi + 1] * (double) ts_block[j - pi][1]);
      }
      ts_block_delta[j][0] = (int) epsilon_r;
      ts_block_delta[j][1] = (int) epsilon_v;

      if (epsilon_r < timestamp_delta_min) {
        timestamp_delta_min = (int) epsilon_r;
        timestamp_delta_min_index = j;
      }
      if (epsilon_v < value_delta_min) {
        value_delta_min = (int) epsilon_v;
        value_delta_min_index = j;
      }
      if (epsilon_r > timestamp_delta_max) {
        timestamp_delta_max = (int) epsilon_r;
        timestamp_delta_max_index = j;
      }
      if (epsilon_v > value_delta_max) {
        value_delta_max = (int) epsilon_v;
        value_delta_max_index = j;
      }
    }
    delta_index[0] = timestamp_delta_max_index;
    delta_index[1] = value_delta_max_index;
    delta_index[2] = timestamp_delta_min_index;
    delta_index[3] = value_delta_min_index;

    delta[0] = timestamp_delta_max;
    delta[1] = value_delta_max;
    delta[2] = timestamp_delta_min;
    delta[3] = value_delta_min;

    return ts_block_delta;
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

  public static void moveAlphaToBeta(int[][] ts_block, int alpha, int beta) {
    int[] tmp_tv = ts_block[alpha];
    if (beta < alpha) {
      for (int u = alpha - 1; u >= beta; u--) {
        ts_block[u + 1][0] = ts_block[u][0];
        ts_block[u + 1][1] = ts_block[u][1];
      }
    } else {
      for (int u = alpha + 1; u < beta; u++) {
        ts_block[u - 1][0] = ts_block[u][0];
        ts_block[u - 1][1] = ts_block[u][1];
      }
      beta--;
    }
    ts_block[beta][0] = tmp_tv[0];
    ts_block[beta][1] = tmp_tv[1];
  }

  private static ArrayList<Integer> isMovable(int[] alpha_list, int[] beta_list) {
    ArrayList<Integer> isMoveable = new ArrayList<>();
    for (int i = 0; i < alpha_list.length; i++) {
      if (alpha_list[i] != -1 && beta_list[i] != -1) {
        isMoveable.add(i);
      }
    }
    return isMoveable;
  }

  private static int getIstarClose(
      int alpha, ArrayList<Integer> j_star_list, int[][] new_length_list, int[] raw_length) {
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

  public static int getBetaP(
      int[][] ts_block, int alpha, int block_size, int[] raw_length, double[] theta, int p) {
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    int max_timestamp = Integer.MIN_VALUE;
    int max_value = Integer.MIN_VALUE;
    int raw_timestamp_delta_max_index = -1;
    int raw_value_delta_max_index = -1;

    int raw_abs_sum = raw_length[0];

    ArrayList<Integer> j_star_list = new ArrayList<>(); // beta list of min b phi alpha to j

    int j_star = -1;

    if (alpha == -1) {
      return j_star;
    }

    int[] b;
    int[][] new_length_list = new int[block_size][3];
    int pos_new_length_list = 0;

    // alpha <= p
    if (alpha < p) {

      int j = 0;
      for (; j < alpha; j++) {

        b = adjustCase2(ts_block, alpha, j, theta, p);
        if (b[0] < raw_abs_sum) {
          raw_abs_sum = b[0];
          j_star_list.clear();
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
      for (j = alpha + 2; j < alpha + p && j < block_size; j++) {

        b = adjustCase3(ts_block, alpha, j, theta, p);
        if (b[0] < raw_abs_sum) {
          raw_abs_sum = b[0];
          j_star_list.clear();
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
      for (; j < block_size; j++) {

        b = adjustCase4(ts_block, alpha, j, theta, p);
        if (b[0] < raw_abs_sum) {
          raw_abs_sum = b[0];
          j_star_list.clear();
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
      b = adjustCase5(ts_block, alpha, theta, p);
      if (b[0] < raw_abs_sum) {
        raw_abs_sum = b[0];
        j_star_list.clear();
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
    else if (alpha < block_size && alpha >= block_size - p) {

      int j = 0;
      for (; j < alpha - p; j++) {

        b = adjustCase1(ts_block, alpha, j, theta, p);
        if (b[0] < raw_abs_sum) {
          raw_abs_sum = b[0];
          j_star_list.clear();
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
      for (; j < alpha; j++) {

        b = adjustCase2(ts_block, alpha, j, theta, p);
        if (b[0] < raw_abs_sum) {
          raw_abs_sum = b[0];
          j_star_list.clear();
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
      for (j = alpha + 2; j < alpha + p && j < block_size; j++) {

        b = adjustCase3(ts_block, alpha, j, theta, p);
        if (b[0] < raw_abs_sum) {
          raw_abs_sum = b[0];
          j_star_list.clear();
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
      for (; j < block_size; j++) {

        b = adjustCase4(ts_block, alpha, j, theta, p);
        if (b[0] < raw_abs_sum) {
          raw_abs_sum = b[0];
          j_star_list.clear();
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

      b = adjustCase5(ts_block, alpha, theta, p);
      if (b[0] < raw_abs_sum) {
        raw_abs_sum = b[0];
        j_star_list.clear();
        j_star_list.add(0);
        pos_new_length_list = 0;
        System.arraycopy(b, 0, new_length_list[pos_new_length_list], 0, 3);
        pos_new_length_list++;
      } else if (b[0] == raw_abs_sum) {
        j_star_list.add(0);
        System.arraycopy(b, 0, new_length_list[pos_new_length_list], 0, 3);
        pos_new_length_list++;
      }
    } // p < alpha <= n-p
    else {

      int j = 0;
      for (; j < alpha - p; j++) {

        b = adjustCase1(ts_block, alpha, j, theta, p);
        if (b[0] < raw_abs_sum) {
          raw_abs_sum = b[0];
          j_star_list.clear();
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
      for (; j < alpha; j++) {

        b = adjustCase2(ts_block, alpha, j, theta, p);
        if (b[0] < raw_abs_sum) {
          raw_abs_sum = b[0];
          j_star_list.clear();
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
      for (j = alpha + 2; j < alpha + p && j < block_size; j++) {

        b = adjustCase3(ts_block, alpha, j, theta, p);
        if (b[0] < raw_abs_sum) {
          raw_abs_sum = b[0];
          j_star_list.clear();
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
      for (; j < block_size; j++) {

        b = adjustCase4(ts_block, alpha, j, theta, p);
        if (b[0] < raw_abs_sum) {
          raw_abs_sum = b[0];
          j_star_list.clear();
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
      b = adjustCase5(ts_block, alpha, theta, p);
      if (b[0] < raw_abs_sum) {
        raw_abs_sum = b[0];
        j_star_list.clear();
        j_star_list.add(0);
        pos_new_length_list = 0;
        System.arraycopy(b, 0, new_length_list[pos_new_length_list], 0, 3);
        pos_new_length_list++;
      } else if (b[0] == raw_abs_sum) {
        j_star_list.add(0);
        System.arraycopy(b, 0, new_length_list[pos_new_length_list], 0, 3);
        pos_new_length_list++;
      }
    }

    if (j_star_list.size() != 0) {
      j_star = getIstarClose(alpha, j_star_list, new_length_list, raw_length);
    }
    return j_star;
  }
  private static void cloneTsblock(int[][] ts_block,int[][] tmp_ts_block){
    for(int i=0;i<ts_block.length;i++){
      tmp_ts_block[i][0] = ts_block[i][0];
      tmp_ts_block[i][1] = ts_block[i][1];
    }
  }
  private static int[] adjustCase1(int[][] ts_block, int alpha, int j_star, double[] theta, int p) {
    int[][] tmp_ts_block = new int[ts_block.length][2];
    cloneTsblock(ts_block,tmp_ts_block);
    int block_size = ts_block.length;
    int[] tmp_tv = tmp_ts_block[alpha].clone();
    int[] b = new int[3];
    for (int u = alpha - 1; u >= j_star; u--) {
      tmp_ts_block[u + 1][0] = tmp_ts_block[u][0];
      tmp_ts_block[u + 1][1] = tmp_ts_block[u][1];
    }
    tmp_ts_block[j_star][0] = tmp_tv[0];
    tmp_ts_block[j_star][1] = tmp_tv[1];

    int[] delta_index = new int[4];
    int[] delta = new int[4];
    int[][] ts_block_delta;
    ts_block_delta =deltaTSBlock(tmp_ts_block, delta_index,delta, theta, p);

    int timestamp_delta_min = delta[2];
    int value_delta_min = delta[3];

    int length = 0;
    for (int[] integers : ts_block_delta) {
      length += getBitWith(integers[0] - timestamp_delta_min);
      length += getBitWith(integers[1] - value_delta_min);
    }
    b[0] = length;
    b[1] = timestamp_delta_min;
    b[2] = value_delta_min;

    return b;
  }

  private static int[] adjustCase2(int[][] ts_block, int alpha, int j_star, double[] theta, int p) {
    int[][] tmp_ts_block = new int[ts_block.length][2];
    cloneTsblock(ts_block,tmp_ts_block);
    int block_size = ts_block.length;
    int[] tmp_tv = tmp_ts_block[alpha].clone();
    for (int u = alpha - 1; u >= j_star; u--) {
      tmp_ts_block[u + 1][0] = tmp_ts_block[u][0];
      tmp_ts_block[u + 1][1] = tmp_ts_block[u][1];
    }
    tmp_ts_block[j_star][0] = tmp_tv[0];
    tmp_ts_block[j_star][1] = tmp_tv[1];

    int[] delta_index = new int[4];
    int[] delta = new int[4];
    int[][] ts_block_delta;
    ts_block_delta =deltaTSBlock(tmp_ts_block, delta_index,delta, theta, p);

    int timestamp_delta_min = delta[2];
    int value_delta_min = delta[3];
    int length = 0;
    for (int[] integers : ts_block_delta) {
      length += getBitWith(integers[0] - timestamp_delta_min);
      length += getBitWith(integers[1] - value_delta_min);
    }
    int[] b = new int[3];
    b[0] = length;
    b[1] = timestamp_delta_min;
    b[2] = value_delta_min;
    return b;
  }

  private static int[] adjustCase3(int[][] ts_block, int alpha, int j_star, double[] theta, int p) {
    int[][] tmp_ts_block = new int[ts_block.length][2];
    cloneTsblock(ts_block,tmp_ts_block);
    int block_size = ts_block.length;
    int[] tmp_tv = tmp_ts_block[alpha].clone();
    for (int u = alpha + 1; u < j_star; u++) {
      tmp_ts_block[u - 1][0] = tmp_ts_block[u][0];
      tmp_ts_block[u - 1][1] = tmp_ts_block[u][1];
    }
    j_star--;
    tmp_ts_block[j_star][0] = tmp_tv[0];
    tmp_ts_block[j_star][1] = tmp_tv[1];

    int[] delta_index = new int[4];
    int[] delta = new int[4];
    int[][] ts_block_delta;
    ts_block_delta =deltaTSBlock(tmp_ts_block, delta_index,delta, theta, p);

    int timestamp_delta_min = delta[2];
    int value_delta_min = delta[3];
    int length = 0;
    for (int[] integers : ts_block_delta) {
      length += getBitWith(integers[0] - timestamp_delta_min);
      length += getBitWith(integers[1] - value_delta_min);
    }
    int[] b = new int[3];
    b[0] = length;
    b[1] = timestamp_delta_min;
    b[2] = value_delta_min;
    return b;
  }

  private static int[] adjustCase4(int[][] ts_block, int alpha, int j_star, double[] theta, int p) {
    int[][] tmp_ts_block = new int[ts_block.length][2];
    cloneTsblock(ts_block,tmp_ts_block);
    int block_size = ts_block.length;
    int[] tmp_tv = tmp_ts_block[alpha].clone();
    for (int u = alpha + 1; u < j_star; u++) {
      tmp_ts_block[u - 1][0] = tmp_ts_block[u][0];
      tmp_ts_block[u - 1][1] = tmp_ts_block[u][1];
    }
    j_star--;
    tmp_ts_block[j_star][0] = tmp_tv[0];
    tmp_ts_block[j_star][1] = tmp_tv[1];

    int[] delta_index = new int[4];
    int[] delta = new int[4];
    int[][] ts_block_delta;
    ts_block_delta =deltaTSBlock(tmp_ts_block, delta_index,delta, theta, p);

    int timestamp_delta_min = delta[2];
    int value_delta_min = delta[3];
    int length = 0;
    for (int[] integers : ts_block_delta) {
      length += getBitWith(integers[0] - timestamp_delta_min);
      length += getBitWith(integers[1] - value_delta_min);
    }
    int[] b = new int[3];
    b[0] = length;
    b[1] = timestamp_delta_min;
    b[2] = value_delta_min;
    return b;
  }

  private static int[] adjustCase5(int[][] ts_block, int alpha, double[] theta, int p) {
    int[][] tmp_ts_block = new int[ts_block.length][2];
    cloneTsblock(ts_block,tmp_ts_block);
    int block_size = ts_block.length;
    int[] tmp_tv = tmp_ts_block[alpha].clone();
    for (int u = alpha + 1; u < block_size; u++) {
      tmp_ts_block[u - 1][0] = tmp_ts_block[u][0];
      tmp_ts_block[u - 1][1] = tmp_ts_block[u][1];
    }
    tmp_ts_block[block_size - 1][0] = tmp_tv[0];
    tmp_ts_block[block_size - 1][1] = tmp_tv[1];
    int[] delta_index = new int[4];
    int[] delta = new int[4];
    int[][] ts_block_delta;
    ts_block_delta =deltaTSBlock(tmp_ts_block, delta_index,delta, theta, p);

    int timestamp_delta_min = delta[2];
    int value_delta_min = delta[3];
    int length = 0;
    for (int[] integers : ts_block_delta) {
      length += getBitWith(integers[0] - timestamp_delta_min);
      length += getBitWith(integers[1] - value_delta_min);
    }
    int[] b = new int[3];
    b[0] = length;
    b[1] = timestamp_delta_min;
    b[2] = value_delta_min;
    return b;
  }

  public static int encodeRLEBitWidth2Bytes(int[][] bit_width_segments) {
    int encoded_result = 0;

    //    ArrayList<ArrayList<Integer>> run_length_time = new ArrayList<>();
    //    ArrayList<ArrayList<Integer>> run_length_value = new ArrayList<>();

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
    //        System.out.println("pos_time="+pos_time);
    //        System.out.println("pos_value="+pos_value);

    //        System.out.println(Arrays.deepToString(run_length_time));
    //        System.out.println(Arrays.deepToString(run_length_value));
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

  private static int encodeSegment2Bytes(
      int[][] delta_segments,
      int[][] bit_width_segments,
      int[] raw_length,
      int segment_size,
      int p,
      double[] theta,
      int pos_encode,
      byte[] encoded_result) {

    int block_size = delta_segments.length;
    int segment_n = (block_size - p) / segment_size;
    int2Bytes(delta_segments[0][0], pos_encode, encoded_result);
    pos_encode += 4;
    int2Bytes(delta_segments[0][1], pos_encode, encoded_result);
    pos_encode += 4;
//    double2bytes(theta[0] + raw_length[3], pos_encode, encoded_result);
//    pos_encode += 8;
//
//    double2bytes(theta[1] + raw_length[4], pos_encode, encoded_result);
//    pos_encode += 8;

//    for (int i = 2; i < theta.length; i++) {
//      double2bytes(theta[i], pos_encode, encoded_result);
//      pos_encode += 8;
//    }
    //        System.out.println(delta_segments[0][0]);
    //        System.out.println(delta_segments[0][1]);
    //        System.out.println(theta[0] + raw_length[3]);
    //        System.out.println(theta[1]);
    //        System.out.println(theta[2] + raw_length[4]);
    //        System.out.println(theta[3]);

    pos_encode = encodeRLEBitWidth2Bytes(bit_width_segments, pos_encode, encoded_result);
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

    return pos_encode;
  }


  public static int[][] getEncodeBitsRegressionNoTrain(
      int[][] ts_block, int block_size, int[] raw_length, double[] theta, int segment_size, int p) {
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    int[][] ts_block_delta = new int[ts_block.length][2];

    ts_block_delta[0][0] = ts_block[0][0];
    ts_block_delta[0][1] = ts_block[0][1];

    for (int j = 1; j < p; j++) {
      double epsilon_r = ((double) ts_block[j][0] - theta[0]);
      double epsilon_v = ((double) ts_block[j][1] - theta[1]);
      for (int pi = 1; pi <= j; pi++) {
        epsilon_r -= (theta[2 * pi] * (double) ts_block[j - pi][0]);
        epsilon_v -= (theta[2 * pi + 1] * (double) ts_block[j - pi][1]);
      }
      if (epsilon_r < timestamp_delta_min) {
        timestamp_delta_min = (int) epsilon_r;
      }
      if (epsilon_v < value_delta_min) {
        value_delta_min = (int) epsilon_v;
      }

      ts_block_delta[j][0] = (int) epsilon_r;
      ts_block_delta[j][1] = (int) epsilon_v;
    }

    for (int j = p; j < block_size; j++) {
      double epsilon_r = (double) ts_block[j][0] - theta[0];
      double epsilon_v = (double) ts_block[j][1] - theta[1];
      for (int pi = 1; pi <= p; pi++) {
        epsilon_r -= theta[2 * pi] * (double) ts_block[j - pi][0];
        epsilon_v -= theta[2 * pi + 1] * (double) ts_block[j - pi][1];
      }

      if (epsilon_r < timestamp_delta_min) {
        timestamp_delta_min = (int) epsilon_r;
      }
      if (epsilon_v < value_delta_min) {
        value_delta_min = (int) epsilon_v;
      }
      ts_block_delta[j][0] = (int) epsilon_r;
      ts_block_delta[j][1] = (int) epsilon_v;
    }
    int max_interval = Integer.MIN_VALUE;
    int max_value = Integer.MIN_VALUE;
    int length = 0;
    for (int j = block_size - 1; j >= 1; j--) {
      ts_block_delta[j][0] -= timestamp_delta_min;
      int epsilon_r = ts_block_delta[j][0];
      ts_block_delta[j][1] -= value_delta_min;
      int epsilon_v = ts_block_delta[j][1];

      length += getBitWith(epsilon_r);
      length += getBitWith(epsilon_v);

      if (epsilon_r > max_interval) {
        max_interval = epsilon_r;
      }
      if (epsilon_v > max_value) {
        max_value = epsilon_v;
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

  private static int numberOfEncodeSegment2Bytes(
      int[][] delta_segments,
      int[][] bit_width_segments,
      int[] raw_length,
      int segment_size,
      int p,
      double[] theta) {
    ArrayList<Byte> encoded_result = new ArrayList<>();
    int block_size = delta_segments.length;
    int segment_n = (block_size - p) / segment_size;
    int result = 0;
    result += 8; // encode interval0 and value0
    result += ((2 * p + 1) * 8); // encode theta
    result += encodeRLEBitWidth2Bytes(bit_width_segments);

    for (int segment_i = 0; segment_i < segment_n; segment_i++) {
      int bit_width_time = bit_width_segments[segment_i][0];
      int bit_width_value = bit_width_segments[segment_i][1];
      result += (segment_size * bit_width_time / 8);
      result += (segment_size * bit_width_value / 8);
    }

    return result;
  }

  public static int[] getIStar(
      int[][] ts_block,
      ArrayList<Integer> min_index,
      int block_size,
      int index,
      double[] theta,
      int p,
      int k) {

    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    int timestamp_delta_min_index = -1;
    int value_delta_min_index = -1;
    int alpha = 0;

    int[][] ts_block_delta = new int[block_size - 1][2];

    // regression residual
    for (int j = 1; j < p; j++) {
      double epsilon_r = ((double) ts_block[j][0] - theta[0]);
      double epsilon_v = ((double) ts_block[j][1] - theta[1]);
      for (int pi = 1; pi <= j; pi++) {
        epsilon_r -= (theta[2 * pi] * (double) ts_block[j - pi][0]);
        epsilon_v -= (theta[2 * pi + 1] * (double) ts_block[j - pi][1]);
      }

      if (epsilon_r < timestamp_delta_min) {
        timestamp_delta_min = (int) epsilon_r;
        //                timestamp_delta_min_index
      }
      if (epsilon_v < value_delta_min) {
        value_delta_min = (int) epsilon_v;
      }
      if (index == 0) {
        ts_block_delta[j - 1][0] = j;
        ts_block_delta[j - 1][1] = (int) epsilon_v;
      } else if (index == 1) {
        ts_block_delta[j - 1][0] = j;
        ts_block_delta[j - 1][1] = (int) epsilon_r;
      }
    }
    for (int j = p; j < block_size; j++) {
      double epsilon_r = ((double) ts_block[j][0] - theta[0]);
      double epsilon_v = ((double) ts_block[j][1] - theta[1]);

      for (int pi = 1; pi <= p; pi++) {
        epsilon_r -= (theta[2 * pi] * (double) ts_block[j - pi][0]);
        epsilon_v -= (theta[2 * pi + 1] * (double) ts_block[j - pi][1]);
      }
      if (epsilon_r < timestamp_delta_min) {
        timestamp_delta_min = (int) epsilon_r;
      }
      if (epsilon_v < value_delta_min) {
        value_delta_min = (int) epsilon_v;
      }
      if (index == 0) {
        ts_block_delta[j - 1][0] = j;
        ts_block_delta[j - 1][1] = (int) epsilon_v;
      } else if (index == 1) {
        ts_block_delta[j - 1][0] = j;
        ts_block_delta[j - 1][1] = (int) epsilon_r;
      }
    }

    min_index.add(timestamp_delta_min_index);
    min_index.add(value_delta_min_index);
    Arrays.sort(
        ts_block_delta,
        (a, b) -> {
          if (a[1] == b[1]) return Integer.compare(a[0], b[0]);
          return Integer.compare(a[1], b[1]);
        });
    //        Arrays.sort(ts_block_delta);
    //    quickSort(ts_block_delta, 1, 0, block_size - 2);
    int[] alpha_list = new int[k + 1];
    //    ArrayList<Integer> alpha_list = new ArrayList<>();
    alpha_list[0] = ts_block_delta[0][0];
    for (int i = 0; i < k; i++) {
      alpha_list[i + 1] = ts_block_delta[block_size - 2 - k][0];
    }
    return alpha_list;
  }

  public static int[] getIStar(
      int[][] ts_block,
      ArrayList<Integer> min_index,
      int block_size,
      double[] theta,
      int p,
      int k) {
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    int timestamp_delta_max_index = -1;
    int value_delta_max_index = -1;
    int timestamp_delta_min_index = -1;
    int value_delta_min_index = -1;
    int[] alpha_list = new int[2 * k + 2];

    int[][] ts_block_delta_time = new int[block_size - 1][2];
    int[][] ts_block_delta_value = new int[block_size - 1][2];
    // regression residual
    for (int j = 1; j < p; j++) {
      double epsilon_r = ((double) ts_block[j][0] - theta[0]);
      double epsilon_v = ((double) ts_block[j][1] - theta[1]);
      for (int pi = 1; pi <= j; pi++) {
        epsilon_r -= (double) (theta[2 * pi] * (double) ts_block[j - pi][0]);
        epsilon_v -= (double) (theta[2 * pi + 1] * (double) ts_block[j - pi][1]);
      }

      if (epsilon_r < timestamp_delta_min) {
        timestamp_delta_min = (int) epsilon_r;
        //                timestamp_delta_min_index
      }
      if (epsilon_v < value_delta_min) {
        value_delta_min = (int) epsilon_v;
      }
      ts_block_delta_time[j - 1][0] = j;
      ts_block_delta_time[j - 1][1] = (int) epsilon_r;
      ts_block_delta_value[j - 1][0] = j;
      ts_block_delta_value[j - 1][1] = (int) epsilon_v;
    }
    for (int j = p; j < block_size; j++) {
      double epsilon_r = ((double) ts_block[j][0] - theta[0]);
      double epsilon_v = ((double) ts_block[j][1] - theta[1]);

      for (int pi = 1; pi <= p; pi++) {
        epsilon_r -= (theta[2 * pi] * (double) ts_block[j - pi][0]);
        epsilon_v -= (theta[2 * pi + 1] * (double) ts_block[j - pi][1]);
      }
      if (epsilon_r < timestamp_delta_min) {
        timestamp_delta_min = (int) epsilon_r;
      }
      if (epsilon_v < value_delta_min) {
        value_delta_min = (int) epsilon_v;
      }
      ts_block_delta_time[j - 1][0] = j;
      ts_block_delta_time[j - 1][1] = (int) epsilon_r;
      ts_block_delta_value[j - 1][0] = j;
      ts_block_delta_value[j - 1][1] = (int) epsilon_v;
    }

    Arrays.sort(
        ts_block_delta_time,
        (a, b) -> {
          if (a[1] == b[1]) return Integer.compare(a[0], b[0]);
          return Integer.compare(a[1], b[1]);
        });

    min_index.add(ts_block_delta_time[0][0]);
    alpha_list[0] = ts_block_delta_time[0][0];
    for (int i = 0; i < k; i++) {
      alpha_list[i + 1] = ts_block_delta_time[block_size - 2 - k][0];
    }

    Arrays.sort(
        ts_block_delta_value,
        (a, b) -> {
          if (a[1] == b[1]) return Integer.compare(a[0], b[0]);
          return Integer.compare(a[1], b[1]);
        });

    int pos_alpha_list = k + 1;
    //    quickSort(ts_block_delta_value, 1, 0, block_size - 2);
    min_index.add(ts_block_delta_value[0][0]);
    if (!containsValue(alpha_list, ts_block_delta_value[0][0])) {
      alpha_list[pos_alpha_list] = ts_block_delta_value[0][0];
      pos_alpha_list++;
    }

    for (int i = 0; i < k; i++) {
      if (!containsValue(alpha_list, ts_block_delta_value[block_size - 2 - k][0])) {
        alpha_list[pos_alpha_list] = ts_block_delta_value[block_size - 2 - k][0];
        pos_alpha_list++;
      }
    }

    return alpha_list;
  }


  private static int REGERBlockEncoder(
      int[][] data,
      int i,
      int block_size,
      int supply_length,
      int[] third_value,
      int segment_size,
      int k,
      int p,
      int pre,
      int encode_pos,
      byte[] cur_byte) {

    int min_time = data[i * block_size][0];
    int[][] ts_block;
    int[][] ts_block_partition;
    if (supply_length == 0) {
      ts_block = new int[block_size][2];
      ts_block_partition = new int[block_size][2];
      for (int j = 0; j < block_size; j++) {
//        data[j + i * block_size][0] -= min_time;
        ts_block[j][0] = data[j + i * block_size][0]- min_time;
        ts_block[j][1] = data[j + i * block_size][1];
        //      ts_block_reorder[j][0] = data[j + i * block_size][0];
        //      ts_block_reorder[j][1] = data[j + i * block_size][1];
      }
    } else {
      ts_block = new int[supply_length][2];
      ts_block_partition = new int[supply_length][2];
      int end = data.length - i * block_size;
      for (int j = 0; j < end; j++) {
//        data[j + i * block_size][0] ;
        ts_block[j][0] = data[j + i * block_size][0]- min_time;
        ts_block[j][1] = data[j + i * block_size][1];
      }
      for (int j = end; j < supply_length; j++) {
        ts_block[j][0] = 0;
        ts_block[j][1] = 0;
      }
      block_size = supply_length;
    }

    //        System.out.println(Arrays.deepToString(data));
    //        System.out.println(Arrays.deepToString(ts_block));

    int[] reorder_length = new int[5];
    double[] theta_reorder = new double[2 * p + 2];
    int[] time_length =
        new int[5]; // length,max_bit_width_interval,max_bit_width_value,max_bit_width_deviation
    double[] theta_time = new double[2 * p + 2];
    int[] raw_length =
        new int[5]; // length,max_bit_width_interval,max_bit_width_value,max_bit_width_deviation
    double[] theta = new double[2 * p + 2];
    int[][] ts_block_delta_reorder = new int[block_size][2];
    int[][] bit_width_segments_value;
    int[][] ts_block_delta_time = new int[block_size][2];
    int[][] bit_width_segments_time;
    int[][] ts_block_delta = new int[block_size][2];
    int[][] bit_width_segments_partition;

    terminate(ts_block, theta_time, p, pre);
    ts_block_delta_time =
        getEncodeBitsRegressionNoTrain(ts_block, block_size, time_length, theta_time, segment_size, p);
    bit_width_segments_time = segmentBitPacking(ts_block_delta_time, block_size, segment_size);
//      time_length[0] =
//          numberOfEncodeSegment2Bytes(
//              ts_block_delta_time,
//              bit_width_segments_time,
//              time_length,
//              segment_size,
//              p,
//              theta_time);

    int pos_ts_block_partition = 0;
    for (int[] datum : ts_block) {
      if (datum[1] > third_value[third_value.length - 1]) {
        ts_block_partition[pos_ts_block_partition][0] = datum[0];
        ts_block_partition[pos_ts_block_partition][1] = datum[1];
        pos_ts_block_partition++;
      }
    }
    for (int third_i = third_value.length - 1; third_i > 0; third_i--) {
      for (int[] datum : ts_block) {
        if (datum[1] <= third_value[third_i] && datum[1] > third_value[third_i - 1]) {
          ts_block_partition[pos_ts_block_partition][0] = datum[0];
          ts_block_partition[pos_ts_block_partition][1] = datum[1];
          pos_ts_block_partition++;
        }
      }
    }
    for (int[] datum : ts_block) {
      if (datum[1] <= third_value[0]) {
        ts_block_partition[pos_ts_block_partition][0] = datum[0];
        ts_block_partition[pos_ts_block_partition][1] = datum[1];
        pos_ts_block_partition++;
      }
    }

    terminate(ts_block_partition, theta, p, pre);
    ts_block_delta =
        getEncodeBitsRegressionNoTrain(
            ts_block_partition, block_size, raw_length, theta, segment_size, p);
    bit_width_segments_partition = segmentBitPacking(ts_block_delta, block_size, segment_size);
//      raw_length[0] =
//          numberOfEncodeSegment2Bytes(
//              ts_block_delta, bit_width_segments_partition, raw_length, segment_size, p, theta);

    Arrays.sort(
        ts_block,
        (a, b) -> {
          if (a[1] == b[1]) return Integer.compare(a[0], b[0]);
          return Integer.compare(a[1], b[1]);
        });
    terminate(ts_block, theta_reorder, p, pre);
    ts_block_delta_reorder =
        getEncodeBitsRegressionNoTrain(
            ts_block, block_size, reorder_length, theta_reorder, segment_size, p);
    bit_width_segments_value =
        segmentBitPacking(ts_block_delta_reorder, block_size, segment_size);
//      reorder_length[0] =
//          numberOfEncodeSegment2Bytes(
//              ts_block_delta_reorder,
//              bit_width_segments_value,
//              reorder_length,
//              segment_size,
//              p,
//              theta_reorder);
    //        System.out.println(Arrays.deepToString(ts_block_delta));

    int[] alpha_list;

    int choose = min3(time_length[0], raw_length[0], reorder_length[0]);
    ArrayList<Integer> min_index = new ArrayList<>();
    int index_alpha_list = 0;

    if (choose == 0) {
      raw_length = time_length;
      Arrays.sort(
          ts_block,
          (a, b) -> {
            if (a[0] == b[0]) return Integer.compare(a[1], b[1]);
            return Integer.compare(a[0], b[0]);
          });
      theta = theta_time;
    } else if (choose == 1) {
      ts_block = ts_block_partition;
    } else {
      raw_length = reorder_length;
      theta = theta_reorder;
      Arrays.sort(
          ts_block,
          (a, b) -> {
            if (a[1] == b[1]) return Integer.compare(a[0], b[0]);
            return Integer.compare(a[1], b[1]);
          });
      index_alpha_list = 1;
    }

    alpha_list = getIStar(ts_block, min_index, block_size, index_alpha_list, theta, p, k);
    int[] beta_list = new int[alpha_list.length];
    int[][] new_length_list = new int[alpha_list.length][5];
    int pos_new_length_list = 0;

    //    ArrayList<ArrayList<Integer>> new_length_list = new ArrayList<>();

    for (int j = 0; j < alpha_list.length; j++) {
      int alpha = alpha_list[j];
      int[] new_length = new int[5];
      System.arraycopy(raw_length, 0, new_length, 0, 5);
      beta_list[j] = getBetaP(ts_block, alpha, block_size, new_length, theta, p);
      System.arraycopy(new_length, 0, new_length_list[pos_new_length_list], 0, 5);
      pos_new_length_list++;
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
        tmp.add(new_length_list[isMoveable_i][0]);
        all_length.add(tmp);
      }
      quickSort(all_length, 1, 0, all_length.size() - 1);
      if (all_length.get(0).get(1) <= raw_length[0]) {
        int[][] new_ts_block = new int[ts_block.length][2];
        cloneTsblock(ts_block,new_ts_block);
//        int[][] new_ts_block = ts_block.clone();
        moveAlphaToBeta(
            new_ts_block,
            alpha_list[all_length.get(0).get(0)],
            beta_list[all_length.get(0).get(0)]);
        int[] new_length = new int[5];
        ts_block_delta =
            getEncodeBitsRegressionNoTrain(new_ts_block, block_size, new_length, theta, segment_size, p);
        int[][] bit_width_segments = segmentBitPacking(ts_block_delta, block_size, segment_size);
        new_length[0] =
            numberOfEncodeSegment2Bytes(
                ts_block_delta, bit_width_segments, new_length, segment_size, p, theta);

        if (new_length[0] <= raw_length[0]) {
          raw_length = new_length;
          ts_block = new_ts_block.clone();
        } else {
          break;
        }
      } else {
        break;
      }
      alpha_list = getIStar(ts_block, min_index, block_size, theta, p, k);

      int alpha_size = alpha_list.length;
      for (int alpha_i = alpha_size - 1; alpha_i >= 0; alpha_i--) {
        if (containsValue(beta_list, alpha_list[alpha_i])) {
          alpha_list = removeElement(alpha_list, alpha_i);
        }
      }
      beta_list = new int[alpha_list.length];
      new_length_list = new int[alpha_list.length][5];
      pos_new_length_list = 0;
      for (int alpha_i = 0; alpha_i < alpha_list.length; alpha_i++) {
        int alpha = alpha_list[alpha_i];
        int[] new_length = new int[5];
        System.arraycopy(raw_length, 0, new_length, 0, 5);
        beta_list[alpha_i] = (getBetaP(ts_block, alpha, block_size, raw_length, theta, p));
        System.arraycopy(new_length, 0, new_length_list[pos_new_length_list], 0, 5);
        pos_new_length_list++;
      }
      isMoveable = isMovable(alpha_list, beta_list);
    }

    //        System.out.println("getEncodeBitsRegressionNoTrain before:" +
    // Arrays.deepToString(ts_block_delta));

    ts_block_delta =
        getEncodeBitsRegressionNoTrain(ts_block, block_size, raw_length, theta, segment_size, p);

    //        System.out.println("getEncodeBitsRegressionNoTrain after:"
    // +Arrays.deepToString(ts_block_delta));

    //    ArrayList<ArrayList<Integer>> bit_width_segments = new ArrayList<>();
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

    //        System.out.println(Arrays.deepToString(ts_block_delta));
    //        System.out.println(Arrays.deepToString(bit_width_segments));

    encode_pos =
        encodeSegment2Bytes(
            ts_block_delta,
            bit_width_segments,
            raw_length,
            segment_size,
            p,
            theta,
            encode_pos,
            cur_byte);

    //        System.out.println("encode_pos="+encode_pos);
    return encode_pos;
  }

  public static int ReorderingRegressionEncoder(
      int[][] data,
      int block_size,
      int[] third_value,
      int segment_size,
      int p,
      int k,
      int pre,
      byte[] encoded_result) {
//    for (int i = 0; i < p; i++)
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

    intByte2Bytes(p, encode_pos, encoded_result);
    encode_pos += 1;

    // ----------------------- compare data order by time, value and partition
    // ---------------------------
//    int length_time = 0;
//    int length_value = 0;
//    int length_partition = 0;
//    int[][] data_value = data.clone();
//    Arrays.sort(
//        data_value,
//        (a, b) -> {
//          if (a[1] == b[1]) return Integer.compare(a[0], b[0]);
//          return Integer.compare(a[1], b[1]);
//        });
//
//    int[][] data_partition = new int[length_all][2];
//    int pos_data_partition = 0;
//
//    for (int[] datum : data) {
//      if (datum[1] > third_value[third_value.length - 1]) {
//        data_partition[pos_data_partition][0] = datum[0];
//        data_partition[pos_data_partition][1] = datum[1];
//        pos_data_partition++;
//      }
//    }
//    for (int third_i = third_value.length - 1; third_i > 0; third_i--) {
//      for (int[] datum : data) {
//        if (datum[1] <= third_value[third_i] && datum[1] > third_value[third_i - 1]) {
//          data_partition[pos_data_partition][0] = datum[0];
//          data_partition[pos_data_partition][1] = datum[1];
//          pos_data_partition++;
//        }
//      }
//    }
//    for (int[] datum : data) {
//      if (datum[1] <= third_value[0]) {
//        data_partition[pos_data_partition][0] = datum[0];
//        data_partition[pos_data_partition][1] = datum[1];
//        pos_data_partition++;
//      }
//    }
//    for (int i = 0; i < block_num; i++) {
//      int[][] ts_block_time = new int[block_size][2];
//      int[][] ts_block_value = new int[block_size][2];
//      int[][] ts_block_partition = new int[block_size][2];
//
//      for (int j = 0; j < block_size; j++) {
//        ts_block_time[j][0] = data[j + i * block_size][0];
//        ts_block_time[j][1] = data[j + i * block_size][1];
//        ts_block_value[j][0] = data_value[j + i * block_size][0];
//        ts_block_value[j][1] = data_value[j + i * block_size][1];
//        ts_block_partition[j][0] = data_partition[j + i * block_size][0];
//        ts_block_partition[j][1] = data_partition[j + i * block_size][1];
//      }
//
//      int[] raw_length = new int[5];
//      double[] theta = new double[2 * p + 2];
//      int[][] ts_block_delta =
//          getEncodeBitsRegression(ts_block_time, block_size, raw_length, theta, segment_size, p);
//      int[][] bit_width_segments = segmentBitPacking(ts_block_delta, block_size, segment_size);
//      length_time +=
//          numberOfEncodeSegment2Bytes(
//              ts_block_delta, bit_width_segments, raw_length, segment_size, p, theta);
//
//      int[] raw_length_value = new int[5];
//      double[] theta_value = new double[2 * p + 2];
//      int[][] ts_block_delta_value =
//          getEncodeBitsRegression(
//              ts_block_value, block_size, raw_length_value, theta_value, segment_size, p);
//      int[][] bit_width_segments_value =
//          segmentBitPacking(ts_block_delta_value, block_size, segment_size);
//      length_value +=
//          numberOfEncodeSegment2Bytes(
//              ts_block_delta_value,
//              bit_width_segments_value,
//              raw_length_value,
//              segment_size,
//              p,
//              theta_value);
//
//      int[] raw_length_partition = new int[5];
//      double[] theta_partition = new double[2 * p + 2];
//      int[][] ts_block_delta_partition =
//          getEncodeBitsRegression(
//              ts_block_partition,
//              block_size,
//              raw_length_partition,
//              theta_partition,
//              segment_size,
//              p);
//      int[][] bit_width_segments_partition =
//          segmentBitPacking(ts_block_delta_partition, block_size, segment_size);
//      length_partition +=
//          numberOfEncodeSegment2Bytes(
//              ts_block_delta_partition,
//              bit_width_segments_partition,
//              raw_length_partition,
//              segment_size,
//              p,
//              theta_partition);
//    }
//    //        int remaining_length = length_all - block_num * block_size;
//
//    if (length_partition < length_time
//        && length_partition < length_value) { // partition performs better
//      data = data_partition;
//      //            for (int i = 0; i < 2; i++) {
//      System.out.println("Partition");
//      for (int i = 0; i < block_num; i++) {
//        encode_pos =
//            REGERBlockEncoderPartition(
//                data, i, block_size, segment_size, k, p, encode_pos, encoded_result);
//      }
//    } else {
//      if (length_value < length_time) { // order by value performs better
//        System.out.println("Value");
//        data = data_value;
//        //                for (int i = 0; i < 2; i++) {
//        for (int i = 0; i < block_num; i++) {
//
//          encode_pos =
//              REGERBlockEncoder(
//                  data,
//                  1,
//                  i,
//                  block_size,
//                  0,
//                  third_value,
//                  segment_size,
//                  k,
//                  p,
//                  encode_pos,
//                  encoded_result);
//        }
//      } else {
//    System.out.println("Time");
//    for (int i = 0; i < 1; i++) {
        for (int i = 0; i < block_num; i++) {
      encode_pos =
          REGERBlockEncoder(
              data,
                  i,
              block_size,
              0,
              third_value,
              segment_size,
              k,
              p,
              pre,
              encode_pos,
              encoded_result);
    }
//      }
//    }

    int remaining_length = length_all - block_num * block_size;
    if (remaining_length == 1) {
      int2Bytes(data[data.length - 1][0], encode_pos, encoded_result);
      encode_pos += 4;
      int2Bytes(data[data.length - 1][1], encode_pos, encoded_result);
      encode_pos += 4;
    }
    if (remaining_length != 0 && remaining_length != 1) {
      int supple_length;
      while(p>=segment_size+1){
        segment_size *= 2;
      }
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
              third_value,
              segment_size,
              k,
              p,
              pre,
              encode_pos,
              encoded_result);
    }

    return encode_pos;
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

  @Test
  public void REGERPDoubleVaryP() throws IOException {

    String parent_dir =
        "/Users/xiaojinzhao/Documents/GitHub/iotdb/iotdb-core/tsfile/src/test/resources/";
    String output_parent_dir =
        "/Users/xiaojinzhao/Documents/GitHub/encoding-reorder/compression_ratio/p_double_vary_p/";
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
      dataset_block_size.add(128);
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

//    int[] file_lists = {0,2,6,7}; //
//    for (int file_i : file_lists) {
    for (int file_i = 0; file_i < input_path_list.size(); file_i++) {
      //        for (int file_i = 0; file_i < 1; file_i++) {

      String inputPath = input_path_list.get(file_i);
      String Output = output_path_list.get(file_i);
      System.out.println(inputPath);

      // speed
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
        "p",
        "Compressed Size",
        "Compression Ratio"
      };
      writer.writeRecord(head); // write header to output file

      assert tempList != null;

      for (File f : tempList) {
        System.out.println(f);
        //                for (int p = 8; p < 9; p++) {
        for (int p = 1; p < 10; p++) {
          System.out.println("p=" + p);

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
          int min_time = data.get(0).get(0);
          for (int i = 0; i < data.size(); i++) {
            data2_arr[i][0] = data.get(i).get(0) - min_time;
            data2_arr[i][1] = data.get(i).get(1);
          }
//          System.out.println(data2_arr[0][0]);
          byte[] encoded_result = new byte[data2_arr.length * 8];

          long encodeTime = 0;
          long decodeTime = 0;
          double ratio = 0;
          double compressed_size = 0;
          int length = 0;

          long s = System.nanoTime();

          for (int repeat_i = 0; repeat_i < 1; repeat_i++)
            length =
                ReorderingRegressionEncoder(
                    data2_arr,
                    dataset_block_size.get(file_i),
                    dataset_third.get(file_i),
                    8,
                    p,
                    1,
                    10,
                    encoded_result);

          long e = System.nanoTime();
          encodeTime += ((e - s));
          compressed_size += length;
          double ratioTmp = (double) compressed_size / (double) (data.size() * Integer.BYTES * 2);
          ratio += ratioTmp;
          s = System.nanoTime();
          e = System.nanoTime();
          decodeTime += ((e - s));

          String[] record = {
            f.toString(),
            "REGER-64-DOUBLE",
            String.valueOf(encodeTime),
            String.valueOf(decodeTime),
            String.valueOf(data.size()),
            String.valueOf(p),
            String.valueOf(compressed_size),
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
  public void REGERPDoubleVaryPrecision() throws IOException {

    String parent_dir =
            "/Users/xiaojinzhao/Documents/GitHub/iotdb/iotdb-core/tsfile/src/test/resources/";
    String output_parent_dir =
            "/Users/xiaojinzhao/Documents/GitHub/encoding-reorder/compression_ratio/double_vary_pre/";
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
      dataset_block_size.add(128);
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

//    int[] file_lists = {0,2,6,7}; //
//    for (int file_i : file_lists) {
    for (int file_i = 0; file_i < input_path_list.size(); file_i++) {
      //        for (int file_i = 0; file_i < 1; file_i++) {

      String inputPath = input_path_list.get(file_i);
      String Output = output_path_list.get(file_i);
      System.out.println(inputPath);

      // speed
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
              "precision",
              "Compressed Size",
              "Compression Ratio"
      };
      writer.writeRecord(head); // write header to output file

      assert tempList != null;

      for (File f : tempList) {
        System.out.println(f);
        //                for (int p = 8; p < 9; p++) {
        for (int pre = 1; pre < 10; pre++) {
          System.out.println("precision=" + pre);

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
          int min_time = data.get(0).get(0);
          for (int i = 0; i < data.size(); i++) {
            data2_arr[i][0] = data.get(i).get(0) - min_time;
            data2_arr[i][1] = data.get(i).get(1);
          }
//          System.out.println(data2_arr[0][0]);
          byte[] encoded_result = new byte[data2_arr.length * 8];

          long encodeTime = 0;
          long decodeTime = 0;
          double ratio = 0;
          double compressed_size = 0;
          int length = 0;

          long s = System.nanoTime();

          for (int repeat_i = 0; repeat_i < 1; repeat_i++)
            length =
                    ReorderingRegressionEncoder(
                            data2_arr,
                            dataset_block_size.get(file_i),
                            dataset_third.get(file_i),
                            8,
                            1,
                            1,
                            pre,
                            encoded_result);

          long e = System.nanoTime();
          encodeTime += ((e - s));
          compressed_size += length;
          double ratioTmp = (double) compressed_size / (double) (data.size() * Integer.BYTES * 2);
          ratio += ratioTmp;
          s = System.nanoTime();
          e = System.nanoTime();
          decodeTime += ((e - s));

          String[] record = {
                  f.toString(),
                  "REGER-64-DOUBLE",
                  String.valueOf(encodeTime),
                  String.valueOf(decodeTime),
                  String.valueOf(data.size()),
                  String.valueOf(pre),
                  String.valueOf(compressed_size),
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
