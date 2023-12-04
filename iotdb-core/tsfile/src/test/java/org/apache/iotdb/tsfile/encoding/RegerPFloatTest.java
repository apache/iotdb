package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.apache.commons.math3.linear.*;
import org.junit.Test;

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

public class RegerPFloatTest {

  public static int min3(int a, int b, int c) {
    if (a < b && a < c) {
      return 0;
    } else if (b < c) {
      return 1;
    } else {
      return 2;
    }
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
    while (!stack.empty()) {
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

  public static void terminate(int[][] ts_block, float[] coefficient, int p){
    int length = ts_block.length;
    assert length > p;
    int size = length - p;

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


    for (int i = 0; i <= p; i++) {
      coefficient[2 * i] = (float) solution1.getEntry(p-i);
      coefficient[2 * i + 1] = (float) solution2.getEntry(p-i);
    }

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
      int[][] ts_block, int alpha, int block_size, int[] raw_length, float[] theta, int p) {

    int raw_abs_sum = raw_length[0];
    int range = block_size / 32;


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
      int end_j = Math.min(alpha+range, block_size);
      for (j = alpha + 2; j < alpha + p && j < end_j; j++) {
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
      end_j = Math.min(alpha+range, block_size);
      for (; j < end_j; j++) {

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

    } // alpha > n-p
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
      int end_j = Math.min(alpha+range, block_size);
      for (j = alpha + 2; j < alpha + p && j < end_j; j++) {

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
      end_j = Math.min(alpha+range, block_size);
      for (; j < end_j; j++) {

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
      int start_j = Math.max(alpha - range / 2, 1);
      int end_j = Math.min(alpha + range / 2, block_size - 1);

      int j = start_j;

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


      for (j = alpha + 2; j < alpha + p && j < end_j; j++) {

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
      for (; j < end_j; j++) {

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
  private static int[] adjustCase1(int[][] ts_block, int alpha, int j_star, float[] theta, int p) {
    int[][] tmp_ts_block = new int[ts_block.length][2];
    cloneTsblock(ts_block,tmp_ts_block);
    int[] delta_index = new int[4];
    int[] delta = new int[4];



    int[] tmp_tv = tmp_ts_block[alpha].clone();
    int[] b = new int[3];
    for (int u = alpha - 1; u >= j_star; u--) {
      tmp_ts_block[u + 1][0] = tmp_ts_block[u][0];
      tmp_ts_block[u + 1][1] = tmp_ts_block[u][1];
    }
    tmp_ts_block[j_star][0] = tmp_tv[0];
    tmp_ts_block[j_star][1] = tmp_tv[1];
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

  private static int[] adjustCase2(int[][] ts_block, int alpha, int j_star, float[] theta, int p) {
    int[][] tmp_ts_block = new int[ts_block.length][2];
    cloneTsblock(ts_block,tmp_ts_block);
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

  private static int[] adjustCase3(int[][] ts_block, int alpha, int j_star, float[] theta, int p) {
    int[][] tmp_ts_block = new int[ts_block.length][2];
    cloneTsblock(ts_block,tmp_ts_block);
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

  private static int[] adjustCase4(int[][] ts_block, int alpha, int j_star, float[] theta, int p) {
    int[][] tmp_ts_block = new int[ts_block.length][2];
    cloneTsblock(ts_block,tmp_ts_block);
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

  private static int[] adjustCase5(int[][] ts_block, int alpha, float[] theta, int p) {
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

    int count_of_time = 1;
    int count_of_value = 1;
    int pre_time = bit_width_segments[0][0];
    int pre_value = bit_width_segments[0][1];
    int size = bit_width_segments.length;

    int pos_time = 0;
    int pos_value = 0;

    for (int i = 1; i < size; i++) {
      int cur_time = bit_width_segments[i][0];
      int cur_value = bit_width_segments[i][1];
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

    }
    for (int i = 0; i < pos_value; i++) {
      int[] bit_width_value = run_length_value[i];
      intByte2Bytes(bit_width_value[0], pos_encode, encoded_result);
      pos_encode++;
      intByte2Bytes(bit_width_value[1], pos_encode, encoded_result);
      pos_encode++;

    }

    return pos_encode;
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
    int segment_n = (block_size - 1) / segment_size;
    int2Bytes(delta_segments[0][0], pos_encode, encoded_result);
    pos_encode += 4;
    int2Bytes(delta_segments[0][1], pos_encode, encoded_result);
    pos_encode += 4;
    float2bytes(theta[0], pos_encode, encoded_result);
    pos_encode += 4;
    float2bytes(theta[1], pos_encode, encoded_result);
    pos_encode += 4;

    for (int i = 2; i < theta.length; i++) {
      float2bytes(theta[i], pos_encode, encoded_result);
      pos_encode += 4;
    }

    int2Bytes( raw_length[3], pos_encode, encoded_result);
    pos_encode += 4;
    int2Bytes(raw_length[4], pos_encode, encoded_result);
    pos_encode += 4;


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
          int[][] ts_block, int block_size, int[] raw_length, float[] theta, int p) {
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    int[][] ts_block_delta = new int[ts_block.length][2];
    //        theta = new float[4];

    ts_block_delta[0][0] = ts_block[0][0];
    ts_block_delta[0][1] = ts_block[0][1];

    for (int j = 1; j < p; j++) {
      float epsilon_r = (float) ts_block[j][0] - theta[0];
      float epsilon_v = (float) ts_block[j][1] - theta[1];
      for (int pi = 1; pi <= j; pi++) {
        epsilon_r -= theta[2 * pi] * (float) ts_block[j - pi][0];
        epsilon_v -= theta[2 * pi + 1] * (float) ts_block[j - pi][1];
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
      float epsilon_r = (float) ts_block[j][0] - theta[0];
      float epsilon_v = (float) ts_block[j][1] - theta[1];
      for (int pi = 1; pi <= p; pi++) {
        epsilon_r -= theta[2 * pi] * (float) ts_block[j - pi][0];
        epsilon_v -= theta[2 * pi + 1] * (float) ts_block[j - pi][1];
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
      int segment_size,
      int p) {
    int block_size = delta_segments.length;
    int segment_n = (block_size - p) / segment_size;
    int result = 0;
    result += 8; // encode interval0 and value0
    result += ((2 * p + 1) * 4); // encode theta
    result += encodeRLEBitWidth2Bytes(bit_width_segments);

    for (int segment_i = 0; segment_i < segment_n; segment_i++) {
      int bit_width_time = bit_width_segments[segment_i][0];
      int bit_width_value = bit_width_segments[segment_i][1];
      result += (segment_size * bit_width_time / 8);
      result += (segment_size * bit_width_value / 8);
    }

    return result;
  }

  public static int[][] deltaTSBlock(int[][] ts_block,
                                     int[] delta_index,
                                     int[] delta,
                                     float[] theta,
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
      float epsilon_r = (float) ts_block[j][0] - theta[0];
      float epsilon_v = (float) ts_block[j][1] - theta[1];
      for (int pi = 1; pi <= j; pi++) {
        epsilon_r -= theta[2 * pi] * (float) ts_block[j - pi][0];
        epsilon_v -= theta[2 * pi + 1] * (float) ts_block[j - pi][1];
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
      float epsilon_r = (float) ts_block[j][0] - theta[0];
      float epsilon_v = (float) ts_block[j][1] - theta[1];

      for (int pi = 1; pi <= p; pi++) {
        epsilon_r -= theta[2 * pi] * (float) ts_block[j - pi][0];
        epsilon_v -= theta[2 * pi + 1] * (float) ts_block[j - pi][1];
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

  public static int[] getIStar(
      int[][] ts_block,
      ArrayList<Integer> min_index,
      int index,
      float[] theta,
      int p) {

    int[] delta_index = new int[4];
    int[] delta = new int[4];

    deltaTSBlock(ts_block, delta_index,delta, theta, p);

    min_index.add(delta_index[2]);
    min_index.add(delta_index[3]);
    int[] alpha_list = new int[2];
    if(index==0){
      alpha_list[0] = delta_index[3];
      alpha_list[1] = delta_index[1];
    }else{
      alpha_list[0] = delta_index[2];
      alpha_list[1] = delta_index[0];
    }
    return alpha_list;
  }

  public static int[] getIStar(
          int[][] ts_block, ArrayList<Integer> min_index, float[] theta, int p) {

    int[] alpha_list = new int[4];

    int[] delta_index = new int[4];
    int[] delta = new int[4];

    deltaTSBlock(ts_block, delta_index,delta, theta, p);

    min_index.add(delta_index[2]);
    min_index.add(delta_index[3]);

    alpha_list[0] = delta_index[2];
    alpha_list[1] = delta_index[0];


    int pos_alpha_list = 2;
    if (!containsValue(alpha_list, delta_index[3])) {
      alpha_list[pos_alpha_list] = delta_index[3];
      pos_alpha_list++;
    }
    if (!containsValue(alpha_list, delta_index[1])) {
      alpha_list[pos_alpha_list] = delta_index[1];
      pos_alpha_list++;
    }

    int[] new_alpha_list = new int[pos_alpha_list];
    System.arraycopy(alpha_list, 0, new_alpha_list, 0, pos_alpha_list);


    return new_alpha_list;
  }


  private static int REGERBlockEncoder(
      int[][] data,
      int i,
      int block_size,
      int supply_length,
      int[] third_value,
      int segment_size,
      int p,
      int encode_pos,
      byte[] cur_byte) {

    int min_time = data[i * block_size][0];
    int[][] ts_block;
    int[][] ts_block_partition;
    if (supply_length == 0) {
      ts_block = new int[block_size][2];
      ts_block_partition = new int[block_size][2];
      for (int j = 0; j < block_size; j++) {
        ts_block[j][0] = data[j + i * block_size][0]-min_time;
        ts_block[j][1] = data[j + i * block_size][1];
      }
    } else {
      ts_block = new int[supply_length][2];
      ts_block_partition = new int[supply_length][2];
      int end = data.length - i * block_size;
      for (int j = 0; j < end; j++) {
//        data[j + i * block_size][0] -= min_time;
        ts_block[j][0] = data[j + i * block_size][0]- min_time;
        ts_block[j][1] = data[j + i * block_size][1];
      }
      for (int j = end; j < supply_length; j++) {
        ts_block[j][0] = 0;
        ts_block[j][1] = 0;
      }
      block_size = supply_length;
    }
    int2Bytes(min_time,encode_pos,cur_byte);
    encode_pos += 4;

    int[] reorder_length = new int[5];
    float[] theta_reorder = new float[2 * p + 2];
    int[] time_length =
        new int[5]; // length,max_bit_width_interval,max_bit_width_value,max_bit_width_deviation
    float[] theta_time = new float[2 * p + 2];
    int[] raw_length =
        new int[5]; // length,max_bit_width_interval,max_bit_width_value,max_bit_width_deviation
    float[] theta = new float[2 * p + 2];
    int[][] ts_block_delta_reorder;
    int[][] ts_block_delta_time;
    int[][] ts_block_delta;

    terminate(ts_block, theta_time, p);
    ts_block_delta_time =
        getEncodeBitsRegressionNoTrain(ts_block, block_size, time_length, theta_time, p);

    segmentBitPacking(ts_block_delta_time, block_size, segment_size);


    int pos_ts_block_partition = 0;
    if (third_value.length > 0) {

      for(int j=block_size-1;j>=0;j--){
        int[] datum = ts_block[j];
        if (datum[1] <= third_value[0]) {
          ts_block_partition[pos_ts_block_partition][0] = datum[0];
          ts_block_partition[pos_ts_block_partition][1] = datum[1];
          pos_ts_block_partition++;
        }
      }
    for (int third_i = 1; third_i <third_value.length ; third_i++) {
      for(int j=block_size-1;j>=0;j--){
        int[] datum = ts_block[j];
        if (datum[1] <= third_value[third_i] && datum[1] > third_value[third_i - 1]) {
          ts_block_partition[pos_ts_block_partition][0] = datum[0];
          ts_block_partition[pos_ts_block_partition][1] = datum[1];
          pos_ts_block_partition++;
        }
      }
    }
      for(int j=block_size-1;j>=0;j--){
        int[] datum = ts_block[j];
        if (datum[1] > third_value[third_value.length - 1]) {
          ts_block_partition[pos_ts_block_partition][0] = datum[0];
          ts_block_partition[pos_ts_block_partition][1] = datum[1];
          pos_ts_block_partition++;
        }
      }
    }
    terminate(ts_block_partition, theta, p);
    ts_block_delta =
        getEncodeBitsRegressionNoTrain(
            ts_block_partition, block_size, raw_length, theta, p);
    segmentBitPacking(ts_block_delta, block_size, segment_size);


    Arrays.sort(
        ts_block,
        (a, b) -> {
          if (a[1] == b[1]) return Integer.compare(a[0], b[0]);
          return Integer.compare(a[1], b[1]);
        });
    terminate(ts_block, theta_reorder, p);
    ts_block_delta_reorder =
        getEncodeBitsRegressionNoTrain(
            ts_block, block_size, reorder_length, theta_reorder, p);
    segmentBitPacking(ts_block_delta_reorder, block_size, segment_size);


    int[] alpha_list;
    int choose = min3(time_length[0], raw_length[0], reorder_length[0]);
    ArrayList<Integer> min_index = new ArrayList<>();
    int index_alpha_list = 0;

    if (choose == 0) {
      raw_length = time_length.clone();
      Arrays.sort(
          ts_block,
          (a, b) -> {
            if (a[0] == b[0]) return Integer.compare(a[1], b[1]);
            return Integer.compare(a[0], b[0]);
          });
      theta = theta_time.clone();
    } else if (choose == 1) {
      ts_block = ts_block_partition.clone();

    } else {
      raw_length = reorder_length.clone();
      theta = theta_reorder.clone();
      Arrays.sort(
          ts_block,
          (a, b) -> {
            if (a[1] == b[1]) return Integer.compare(a[0], b[0]);
            return Integer.compare(a[1], b[1]);
          });
      index_alpha_list = 1;
    }

    alpha_list = getIStar(ts_block, min_index, index_alpha_list, theta, p);


    int[] beta_list = new int[alpha_list.length];
    int[][] new_length_list = new int[alpha_list.length][5];
    int pos_new_length_list = 0;


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
        int[][] new_ts_block = ts_block.clone();
        moveAlphaToBeta(
            new_ts_block,
            alpha_list[all_length.get(0).get(0)],
            beta_list[all_length.get(0).get(0)]);
        int[] new_length = new int[5];
        ts_block_delta =
            getEncodeBitsRegressionNoTrain(new_ts_block, block_size, new_length, theta, p);
        int[][] bit_width_segments = segmentBitPacking(ts_block_delta, block_size, segment_size);
        new_length[0] =
            numberOfEncodeSegment2Bytes(
                ts_block_delta, bit_width_segments, segment_size, p);

        if (new_length[0] <= raw_length[0]) {
          raw_length = new_length.clone();

          ts_block = new_ts_block.clone();
        } else {
          break;
        }
      } else {
        break;
      }
      alpha_list = getIStar(ts_block, min_index, theta, p);

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



    ts_block_delta =
        getEncodeBitsRegressionNoTrain(ts_block, block_size, raw_length, theta, p);


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



    encode_pos =
        encodeSegment2Bytes(
            ts_block_delta,
            bit_width_segments,
            raw_length,
            segment_size,
                theta,
            encode_pos,
            cur_byte);


    return encode_pos;
  }

  public static int ReorderingRegressionEncoder(
      int[][] data,
      int block_size,
      int[] third_value,
      int segment_size,
      int p,
      byte[] encoded_result) {

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


    for (int i = 0; i < block_num; i++) {
          encode_pos =
              REGERBlockEncoder(
                  data,
                      i,
                  block_size,
                  0,
                  third_value,
                  segment_size,
                      p,
                  encode_pos,
                  encoded_result);
        }


    int remaining_length = length_all - block_num * block_size;
    if (remaining_length == 1) {
      int2Bytes(data[data.length - 1][0], encode_pos, encoded_result);
      encode_pos += 4;
      int2Bytes(data[data.length - 1][1], encode_pos, encoded_result);
      encode_pos += 4;
    }
    if (remaining_length != 0 && remaining_length != 1) {

      while(p>=segment_size+1){
        segment_size *= 2;
      }

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
              third_value,
              segment_size,
                  p,
              encode_pos,
              encoded_result);
    }

    return encode_pos;
  }


  @Test
  public void REGERPFloatVaryP() throws IOException {

    String parent_dir =
        "/Users/xiaojinzhao/Documents/GitHub/iotdb/iotdb-core/tsfile/src/test/resources/";
    String output_parent_dir =
        "/Users/xiaojinzhao/Documents/GitHub/encoding-reorder/compression_ratio/p_float_vary_p/";
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
      dataset_block_size.add(512);
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
      System.out.println(inputPath);


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

      int count_csv = 0;
      for (File f : tempList) {
        System.out.println(count_csv);
        count_csv ++;
        System.out.println(f);

        for (int p = 1; p < 10; p++) {
          System.out.println("p=" + p);

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
                    16,
                    p,
                        encoded_result);

          long e = System.nanoTime();
          encodeTime += ((e - s));
          compressed_size += length;
          double ratioTmp = compressed_size / (double) (data.size() * Integer.BYTES * 2);
          ratio += ratioTmp;
          s = System.nanoTime();
          e = System.nanoTime();
          decodeTime += ((e - s));

          String[] record = {
            f.toString(),
            "REGER-32-FLOAT",
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
}
