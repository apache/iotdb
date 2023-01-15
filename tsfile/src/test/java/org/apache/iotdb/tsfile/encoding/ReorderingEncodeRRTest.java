package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Objects;

public class ReorderingEncodeRRTest {

  static int DeviationOutlierThreshold = 8;
  static int OutlierThreshold = 0;
  public static int zigzag(int num){
    if(num<0){
      return 2*(-num)-1;
    }else{
      return 2*num;
    }
  }

  public static int max3(int a,int b,int c){
    if(a>=b && a >=c){
      return a;
    }else if(b >= a && b >= c){
      return b;
    }else{
      return c;
    }
  }
  public static int getBitWith(int num){
    return 32 - Integer.numberOfLeadingZeros(num);
  }
  public static byte[] int2Bytes(int integer)
  {
    byte[] bytes = new byte[4];
    bytes[0] = (byte) (integer >> 24);
    bytes[1] = (byte) (integer >> 16);
    bytes[2] = (byte) (integer >> 8);
    bytes[3] = (byte) integer;
    return bytes;
  }
  public static byte[] double2Bytes(double dou){
      long value = Double.doubleToRawLongBits(dou);
      byte[] bytes= new byte[8];
      for(int i=0;i<8;i++){
        bytes[i] = (byte) ((value >>8*i)& 0xff);
      }
      return bytes;
  }
  public static byte[] bitPacking(ArrayList<Integer> numbers,int bit_width){
    int block_num = numbers.size()/8;
    byte[] result = new byte[bit_width*block_num];
    for(int i=0;i<block_num;i++){
      for(int j=0;j<bit_width;j++){
        int tmp_int = 0;
        for(int k=0;k<8;k++){
          tmp_int += (((numbers.get(i*8+k) >>j) %2) << k);
        }
//             System.out.println(Integer.toBinaryString(tmp_int));
        result[i*bit_width+j] = (byte) tmp_int;
      }
    }
    return result;
  }
  public static byte[] bitPacking(ArrayList<ArrayList<Integer>> numbers,int index,int bit_width){
    int block_num = numbers.size()/8;
    byte[] result = new byte[bit_width*block_num];
    for(int i=0;i<block_num;i++){
      for(int j=0;j<bit_width;j++){
        int tmp_int = 0;
        for(int k=0;k<8;k++){
          tmp_int += (((numbers.get(i*8+k+1).get(index) >>j) %2) << k);
        }
//        System.out.println(Integer.toBinaryString(tmp_int));
        result[i*bit_width+j] = (byte) tmp_int;
      }
    }
    return result;
  }
  public static void quickSort(ArrayList<ArrayList<Integer>> ts_block, int index, int low, int high) {
    if(low>=high)
      return;
    ArrayList<Integer> pivot = ts_block.get(low);
    int l = low;
    int r = high;
    ArrayList<Integer> temp;
    while(l<r){
      while (l < r && ts_block.get(r).get(index) >= pivot.get(index)) {
        r--;
      }
      while (l < r && ts_block.get(l).get(index) <= pivot.get(index)) {
        l++;
      }
      if (l < r) {
        temp = ts_block.get(l);
        ts_block.set(l, ts_block.get(r));
        ts_block.set(r, temp);
      }
    }
    ts_block.set(low, ts_block.get(l));
    ts_block.set(l, pivot);
    if (low < l) {
      quickSort(ts_block,index, low, l - 1);
    }
    if (r < high) {
      quickSort(ts_block,index, r + 1, high);
    }
  }
  public static void quickSort2(ArrayList<ArrayList<Integer>> ts_block, int low, int high) {
    if(low>=high)
      return;
    ArrayList<Integer> pivot = ts_block.get(low);
    int l = low;
    int r = high;
    ArrayList<Integer> temp;
    while(l<r){
      while (l < r && (ts_block.get(r).get(2) > pivot.get(2)||
              (Objects.equals(ts_block.get(r).get(2), pivot.get(2)) &&ts_block.get(r).get(1) >= pivot.get(1)))) {
        r--;
      }
      while (l < r && ts_block.get(l).get(2) < pivot.get(2)||
              (Objects.equals(ts_block.get(l).get(2), pivot.get(2)) &&ts_block.get(l).get(1) < pivot.get(1))) {
        l++;
//        System.out.println(l);
      }
      if (l < r) {
        temp = ts_block.get(l);
        ts_block.set(l, ts_block.get(r));
        ts_block.set(r, temp);
      }
    }
    ts_block.set(low, ts_block.get(l));
    ts_block.set(l, pivot);
    if (low < l) {
      quickSort2(ts_block, low, l - 1);
    }
    if (r < high) {
      quickSort2(ts_block,r + 1, high);
    }
  }

  public static void splitTimeStamp(ArrayList<ArrayList<Integer>> ts_block, int block_size, int td,
                                    ArrayList<Integer> deviation_list,ArrayList<Integer> result){
    int deviation_max = Integer.MIN_VALUE;
    int max_bit_width_deviation;
    int r0;
    int d0;

    // split timestamp into intervals and deviations

    //address other timestamps and values
    for(int j=block_size-1;j>0;j--) {
      int delta_interval = (ts_block.get(j).get(0) - ts_block.get(j-1).get(0))/td;
      ArrayList<Integer> tmp = ts_block.get(j);
      tmp.add(delta_interval);
      ts_block.set(j,tmp);
    }


    // address timestamp0
    r0 = ts_block.get(0).get(0) /td;
    d0 = ts_block.get(0).get(0) %td;
    if(d0 >= (td/2)){
      d0 -= td;
      r0 ++;
    }
    d0 = zigzag(d0);
//    deviation_list.add(d0);
    if(d0 > deviation_max){
      deviation_max = d0;
    }
    ArrayList<Integer> tmp0 = ts_block.get(0);
    tmp0.add(0);
//    System.out.println(tmp0);
    ts_block.set(0,tmp0);

    for(int j=1;j<block_size;j++){
      int interval = ts_block.get(j).get(2) + ts_block.get(j-1).get(2);
      ArrayList<Integer> tmp;
      tmp = ts_block.get(j);
      tmp.set(2,interval);
      ts_block.set(j,tmp);
    }
//    System.out.println(ts_block);
    quickSort2(ts_block,0,block_size-1);

    for(int j=block_size-1;j>0;j--){
      int interval = ts_block.get(j).get(2);
      int value = ts_block.get(j).get(1);

      int delta_interval = ts_block.get(j).get(2) - ts_block.get(j-1).get(2);
      int deviation = (ts_block.get(j).get(0) - ts_block.get(j-1).get(0))-delta_interval*td;
      deviation = zigzag(deviation);
      deviation_list.add(deviation);
      if(deviation > deviation_max){
        deviation_max = deviation;
      }

      ArrayList<Integer> tmp = new ArrayList<>();
      tmp.add(interval);
      tmp.add(value);
      ts_block.set(j,tmp);
    }
    tmp0 = new ArrayList<>();
    tmp0.add(ts_block.get(0).get(2));
    tmp0.add(ts_block.get(0).get(1));
    ts_block.set(0,tmp0);


    max_bit_width_deviation = getBitWith(deviation_max);
    result.add(max_bit_width_deviation);
    result.add(r0);
    result.add(d0);
  }
  public static ArrayList<ArrayList<Integer>> getEncodeBitsRegression(ArrayList<ArrayList<Integer>> ts_block, int block_size,
                                                                 ArrayList<Integer> result, ArrayList<Integer> i_star,
                                                                      ArrayList<Double> theta){
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


    for(int i=1;i<block_size;i++){
      sum_X_r += ts_block.get(i-1).get(0);
      sum_X_v += ts_block.get(i-1).get(1);
      sum_Y_r += ts_block.get(i).get(0);
      sum_Y_v += ts_block.get(i).get(1);
      sum_squ_X_r += ((long) ts_block.get(i - 1).get(0) *ts_block.get(i-1).get(0));
      sum_squ_X_v += ((long) ts_block.get(i - 1).get(1) *ts_block.get(i-1).get(1));
      sum_squ_XY_r += ((long) ts_block.get(i - 1).get(0) *ts_block.get(i).get(0));
      sum_squ_XY_v += ((long) ts_block.get(i - 1).get(1) *ts_block.get(i).get(1));
    }

    int m_reg = block_size -1;
    double theta0_r = 0.0;
    double theta1_r = 1.0;
    if((double)(m_reg*sum_squ_X_r) != (double)(sum_X_r*sum_X_r) ){
      theta0_r = (double) (sum_squ_X_r*sum_Y_r - sum_X_r*sum_squ_XY_r) / (double) (m_reg*sum_squ_X_r - sum_X_r*sum_X_r);
      theta1_r = (double) (m_reg*sum_squ_XY_r - sum_X_r*sum_Y_r) / (double) (m_reg*sum_squ_X_r - sum_X_r*sum_X_r);
    }

    double theta0_v = 0.0;
    double theta1_v = 1.0;
    if((double)(m_reg*sum_squ_X_v) != (double)(sum_X_v*sum_X_v) ){
      theta0_v = (double) (sum_squ_X_v*sum_Y_v - sum_X_v*sum_squ_XY_v) / (double) (m_reg*sum_squ_X_v - sum_X_v*sum_X_v);
      theta1_v = (double) (m_reg*sum_squ_XY_v - sum_X_v*sum_Y_v) / (double) (m_reg*sum_squ_X_v - sum_X_v*sum_X_v);
    }


    ArrayList<Integer> tmp0 = new ArrayList<>();
    tmp0.add(ts_block.get(0).get(0));
    tmp0.add(ts_block.get(0).get(1));
    ts_block_delta.add(tmp0);

    // delta to Regression
    for(int j=1;j<block_size;j++) {
//      int epsilon_r = (int) ((double)ts_block.get(j).get(0) - theta0_r - theta1_r * (double)ts_block.get(j-1).get(0));
//      int epsilon_v = (int) ((double)ts_block.get(j).get(1) - theta0_v - theta1_v * (double)ts_block.get(j-1).get(1));
      int epsilon_r = ts_block.get(j).get(0) - (int) ( theta0_r + theta1_r * (double)ts_block.get(j-1).get(0));
      int epsilon_v = ts_block.get(j).get(1) - (int) ( theta0_v + theta1_v * (double)ts_block.get(j-1).get(1));

      if(epsilon_r<timestamp_delta_min){
        timestamp_delta_min = epsilon_r;
      }
      if(epsilon_v<value_delta_min){
        value_delta_min = epsilon_v;
      }
      ArrayList<Integer> tmp = new ArrayList<>();
      tmp.add(epsilon_r);
      tmp.add(epsilon_v);
      ts_block_delta.add(tmp);
    }

    int max_interval = Integer.MIN_VALUE;
    int max_interval_i = -1;
    int max_value = Integer.MIN_VALUE;
    int max_value_i = -1;
    for(int j=block_size-1;j>0;j--) {
      int epsilon_r = ts_block_delta.get(j).get(0) - timestamp_delta_min;
      int epsilon_v = ts_block_delta.get(j).get(1) - value_delta_min;
      if(epsilon_r>max_interval){
        max_interval = epsilon_r;
        max_interval_i = j;
      }
      if(epsilon_v>max_value){
        max_value = epsilon_v;
        max_value_i = j;
      }
      ArrayList<Integer> tmp = new ArrayList<>();
      tmp.add(epsilon_r);
      tmp.add(epsilon_v);
      ts_block_delta.set(j,tmp);
    }
    int max_bit_width_interval = getBitWith(max_interval);
    int max_bit_width_value = getBitWith(max_value);


    // calculate error
    int  length = (max_bit_width_interval+max_bit_width_value)*(block_size-1);
    result.clear();

    result.add(length);
    result.add(max_bit_width_interval);
    result.add(max_bit_width_value);

    result.add(timestamp_delta_min);
    result.add(value_delta_min);

    theta0_r += timestamp_delta_min;
    theta0_v += value_delta_min;
    theta.add(theta0_r);
    theta.add(theta1_r);
    theta.add(theta0_v);
    theta.add(theta1_v);
//    System.out.println(theta);

    i_star.add(max_interval_i);
    i_star.add(max_value_i);

    return ts_block_delta;
  }
  public static int getJStar(ArrayList<ArrayList<Integer>> ts_block, int i_star, int block_size,
                                    ArrayList<Integer> raw_length, int index, ArrayList<Double> theta){
    int j_star_bit_width = 33;
    int j_star = 0;
    double theta0_r = theta.get(0);
    double theta1_r = theta.get(1);
    double theta0_v = theta.get(2);
    double theta1_v = theta.get(3);
    if(i_star == block_size - 1 || i_star == 0)
      return 0;
    int epsilon_r_i_star_plus_1 = (int) ((double)ts_block.get(i_star+1).get(0) - theta0_r -
            theta1_r * (double) ts_block.get(i_star-1).get(0));
    int epsilon_v_i_star_plus_1 = (int) ((double)ts_block.get(i_star+1).get(1) - theta0_v -
            theta1_v * (double)ts_block.get(i_star-1).get(1));

    if(epsilon_r_i_star_plus_1 > raw_length.get(1) || epsilon_v_i_star_plus_1 > raw_length.get(2))
      return 0;


    for(int j = 1;j<block_size;j++){
      if(j!=i_star){
        int epsilon_r_j = (int) ((double) ts_block.get(j).get(0) - theta0_r -theta1_r * (double) ts_block.get(i_star).get(0));
        int epsilon_v_j = (int) ((double) ts_block.get(j).get(1) - theta0_v - theta1_v * (double) ts_block.get(i_star).get(1));
        int epsilon_r_i_star = (int) ((double) ts_block.get(i_star).get(0) - theta0_r -theta1_r * (double) ts_block.get(j-1).get(0));
        int epsilon_v_i_star = (int) ((double) ts_block.get(i_star).get(1) - theta0_r -theta1_r * (double) ts_block.get(j-1).get(1));
        if(epsilon_r_j >raw_length.get(1) || epsilon_v_j >raw_length.get(2) ||
                epsilon_r_i_star > raw_length.get(1) || epsilon_v_i_star> raw_length.get(2))
          return 0;
        int max_r = getBitWith(max3(epsilon_r_i_star_plus_1,epsilon_r_j,epsilon_r_i_star));
        int max_v = getBitWith(max3(epsilon_v_i_star_plus_1,epsilon_v_j,epsilon_v_i_star));
        if(index == 1){
          if(max_v<=raw_length.get(2) && max_r < j_star_bit_width && max_r < raw_length.get(1)){
            j_star_bit_width = max_r;
            j_star = j;
          }
        }else{
          if(max_v<raw_length.get(2) && max_v < j_star_bit_width && max_r <= raw_length.get(1)){
            j_star_bit_width = max_v;
            j_star = j;
          }
        }
      }
    }
    return j_star;
  }
  public static int getIStar(ArrayList<ArrayList<Integer>> ts_block, int block_size,
                             ArrayList<Integer> raw_length, int index, ArrayList<Double> theta){
    int i_star_bit_width = 33;
    int i_star = 0;
    double theta0_r = theta.get(0);
    double theta1_r = theta.get(1);
    double theta0_v = theta.get(2);
    double theta1_v = theta.get(3);

    for(int j = 1;j<block_size;j++){
        int epsilon_r_j = getBitWith((int) ((double) ts_block.get(j).get(0) - theta0_r -theta1_r * (double) ts_block.get(j-1).get(0)));
        int epsilon_v_j = getBitWith ((int) ((double) ts_block.get(j).get(1) - theta0_v - theta1_v * (double) ts_block.get(j-1).get(1)));
      if(index == 1){
        if(epsilon_v_j<=raw_length.get(2) && epsilon_r_j < i_star_bit_width && epsilon_r_j < raw_length.get(1)){
          i_star_bit_width = epsilon_r_j;
          i_star = j;
        }
      }else{
        if(epsilon_v_j<raw_length.get(2) && epsilon_v_j < i_star_bit_width && epsilon_r_j <= raw_length.get(1)){
          i_star_bit_width = epsilon_v_j;
          i_star = j;
        }
      }
    }
    return i_star;
  }

  public static ArrayList<Byte> encode2Bytes(ArrayList<ArrayList<Integer>> ts_block,ArrayList<Integer> deviation_list,
                                             ArrayList<Integer> raw_length,ArrayList<Double> theta){
    ArrayList<Byte> encoded_result = new ArrayList<>();
//    // encode block size (Integer)
//    byte[] block_size_byte = int2Bytes(ts_block.size());
//    for (byte b : block_size_byte) encoded_result.add(b);

    // r0 of a block (Integer)
    byte[] r0_byte = int2Bytes(raw_length.get(6));
    for (byte b : r0_byte) encoded_result.add(b);
    byte[] d0_byte = int2Bytes(raw_length.get(7));
    for (byte b : d0_byte) encoded_result.add(b);

    // encode interval0 and value0
    byte[] interval0_byte = int2Bytes(ts_block.get(0).get(0));
    for (byte b : interval0_byte) encoded_result.add(b);
    byte[] value0_byte = int2Bytes(ts_block.get(0).get(1));
    for (byte b : value0_byte) encoded_result.add(b);

//    // encode min_delta_interval and min_delta_value
//    byte[] min_delta_interval_byte = int2Bytes(raw_length.get(3));
//    for (byte b : min_delta_interval_byte) encoded_result.add(b);
//    byte[] min_delta_value_byte = int2Bytes(raw_length.get(4));
//    for (byte b : min_delta_value_byte) encoded_result.add(b);

    // encode theta
    byte[] theta0_r_byte = double2Bytes(theta.get(0));
    for (byte b : theta0_r_byte) encoded_result.add(b);
    byte[] theta1_r_byte = double2Bytes(theta.get(1));
    for (byte b : theta1_r_byte) encoded_result.add(b);
    byte[] theta0_v_byte = double2Bytes(theta.get(2));
    for (byte b : theta0_v_byte) encoded_result.add(b);
    byte[] theta1_v_byte = double2Bytes(theta.get(3));
    for (byte b : theta1_v_byte) encoded_result.add(b);

    // encode interval
    byte[] max_bit_width_interval_byte = int2Bytes(raw_length.get(1));
    for (byte b : max_bit_width_interval_byte) encoded_result.add(b);
    byte[] timestamp_bytes = bitPacking(ts_block,0,raw_length.get(1));
    for (byte b : timestamp_bytes) encoded_result.add(b);

    // encode value
    byte[] max_bit_width_value_byte = int2Bytes(raw_length.get(2));
    for (byte b : max_bit_width_value_byte) encoded_result.add(b);
    byte[] value_bytes = bitPacking(ts_block,1,raw_length.get(2));
    for (byte b : value_bytes) encoded_result.add(b);

    // encode deviation
    byte[] max_bit_width_deviation_byte = int2Bytes(raw_length.get(5));
    for (byte b: max_bit_width_deviation_byte) encoded_result.add(b);
    byte[] deviation_list_bytes = bitPacking(deviation_list,raw_length.get(5));
    for (byte b: deviation_list_bytes) encoded_result.add(b);

    return encoded_result;
  }
  public static ArrayList<Byte> ReorderingRegressionEncoder(ArrayList<ArrayList<Integer>> data,int block_size, int td){
    block_size ++;
    int length_all = data.size();
    int block_num = length_all/block_size;
    ArrayList<Byte> encoded_result=new ArrayList<Byte>();
    // encode block size (Integer)
    byte[] block_size_byte = int2Bytes(block_size);
    for (byte b : block_size_byte) encoded_result.add(b);

    int count_raw = 0;
    int count_reorder = 0;
//    for(int i=0;i<1;i++){
    for(int i=0;i<block_num;i++){
      ArrayList<ArrayList<Integer>> ts_block = new ArrayList<>();
      ArrayList<ArrayList<Integer>> ts_block_reorder = new ArrayList<>();
      for(int j=0;j<block_size;j++){
        ts_block.add(data.get(j+i*block_size));
        ts_block_reorder.add(data.get(j+i*block_size));
      }

      ArrayList<Integer> deviation_list = new ArrayList<>();
      ArrayList<Integer> result = new ArrayList<>();
      splitTimeStamp(ts_block,block_size,td,deviation_list,result);
      quickSort(ts_block,0,0,block_size-1);

      //ts_block order by interval

      // time-order
      ArrayList<Integer> raw_length = new ArrayList<>(); // length,max_bit_width_interval,max_bit_width_value,max_bit_width_deviation
      ArrayList<Integer> i_star_ready = new ArrayList<>();
      ArrayList<Double> theta = new ArrayList<>();
      ArrayList<ArrayList<Integer>> ts_block_delta = getEncodeBitsRegression( ts_block,  block_size, raw_length,
              i_star_ready,theta);


      // value-order
      quickSort(ts_block,1,0,block_size-1);
      ArrayList<Integer> reorder_length = new ArrayList<>();
      ArrayList<Integer> i_star_ready_reorder = new ArrayList<>();
      ArrayList<Double> theta_reorder = new ArrayList<>();
      ArrayList<ArrayList<Integer>> ts_block_delta_reorder = getEncodeBitsRegression( ts_block,  block_size, reorder_length,
              i_star_ready_reorder,theta_reorder);

      if(raw_length.get(0)<=reorder_length.get(0)){
        quickSort(ts_block,0,0,block_size-1);
//        System.out.println(ts_block);
        int i_star = i_star_ready.get(1);
        int j_star = 0;
        count_raw ++;
//        i_star =getIStar(ts_block,block_size,raw_length,0,theta);
        j_star =getJStar(ts_block,i_star,block_size,raw_length,0,theta);

        while(j_star!=0){
          ArrayList<Integer> tmp_tv = ts_block_reorder.get(i_star);
          if(j_star<i_star){
            for(int u=i_star-1;u>=j_star;u--){
              ArrayList<Integer> tmp_tv_cur = new ArrayList<>();
              tmp_tv_cur.add(ts_block_reorder.get(u).get(0));
              tmp_tv_cur.add(ts_block_reorder.get(u).get(1));
              ts_block.set(u+1,tmp_tv_cur);
            }
          }else{
            for(int u=i_star+1;u<=j_star;u++){
              ArrayList<Integer> tmp_tv_cur = new ArrayList<>();
              tmp_tv_cur.add(ts_block_reorder.get(u).get(0));
              tmp_tv_cur.add(ts_block_reorder.get(u).get(1));
              ts_block.set(u-1,tmp_tv_cur);
            }
          }
          ts_block.set(j_star,tmp_tv);
          i_star =getIStar(ts_block,block_size,raw_length,0,theta);
          j_star =getJStar(ts_block,i_star,block_size,raw_length,0,theta);
          System.out.println("adjust");
        }

        ts_block_delta = getEncodeBitsRegression( ts_block,  block_size,raw_length,
                i_star_ready_reorder,theta);
        raw_length.add(result.get(0)); // max_bit_width_deviation
        raw_length.add(result.get(1)); // r0
        raw_length.add(result.get(2)); // d0
        ArrayList<Byte> cur_encoded_result = encode2Bytes(ts_block_delta,deviation_list,raw_length,theta);
        encoded_result.addAll(cur_encoded_result);

      }
      else{
        // adjust to reduce max_bit_width_r
//        System.out.println(ts_block);
        int i_star = i_star_ready_reorder.get(0);
        int j_star = 0;
        ArrayList<Integer> j_star_list =new ArrayList<>();
        count_reorder ++;
        i_star =getIStar(ts_block,block_size,raw_length,0,theta);
        j_star =getJStar(ts_block,i_star,block_size,raw_length,0,theta);
        while(j_star != 0){
          ArrayList<Integer> tmp_tv = ts_block_reorder.get(i_star);
          if(j_star<i_star){
            for(int u=i_star-1;u>=j_star;u--){
              ArrayList<Integer> tmp_tv_cur = new ArrayList<>();
              tmp_tv_cur.add(ts_block_reorder.get(u).get(0));
              tmp_tv_cur.add(ts_block_reorder.get(u).get(1));
              ts_block.set(u+1,tmp_tv_cur);
            }
          }else{
            for(int u=i_star+1;u<=j_star;u++){
              ArrayList<Integer> tmp_tv_cur = new ArrayList<>();
              tmp_tv_cur.add(ts_block_reorder.get(u).get(0));
              tmp_tv_cur.add(ts_block_reorder.get(u).get(1));
              ts_block.set(u-1,tmp_tv_cur);
            }
          }
          System.out.println("adjust");
          ts_block.set(j_star,tmp_tv);
          i_star =getIStar(ts_block,block_size,reorder_length,0,theta);
          j_star =getJStar(ts_block,i_star,block_size,reorder_length,0,theta);
        }

        ts_block_delta_reorder = getEncodeBitsRegression( ts_block,  block_size,reorder_length,
                i_star_ready_reorder,theta_reorder);
        reorder_length.add(result.get(0)); // max_bit_width_deviation
        reorder_length.add(result.get(1)); // r0
        reorder_length.add(result.get(2)); // d0
        ArrayList<Byte> cur_encoded_result = encode2Bytes(ts_block_delta_reorder,deviation_list,reorder_length,theta_reorder);
        encoded_result.addAll(cur_encoded_result);
      }
    }
//    System.out.println(count_raw);
//    System.out.println(count_reorder);
    return encoded_result;
  }

  public static void quickSort22(ArrayList<ArrayList<Integer>> ts_block, int low, int high) {
    if(low>=high)
      return;
    ArrayList<Integer> pivot = ts_block.get(low);
    int l = low;
    int r = high;
    ArrayList<Integer> temp;
    while(l<r){
      while (l < r && (ts_block.get(r).get(0) > pivot.get(0)||
              (Objects.equals(ts_block.get(r).get(0), pivot.get(0)) &&ts_block.get(r).get(1) >= pivot.get(1)))) {
        r--;
      }
      while (l < r && ts_block.get(l).get(0) < pivot.get(0)||
              (Objects.equals(ts_block.get(l).get(0), pivot.get(0)) &&ts_block.get(l).get(1) < pivot.get(1))) {
        l++;
      }
      if (l < r) {
        temp = ts_block.get(l);
        ts_block.set(l, ts_block.get(r));
        ts_block.set(r, temp);
      }
    }
    ts_block.set(low, ts_block.get(l));
    ts_block.set(l, pivot);
    if (low < l) {
      quickSort22(ts_block, low, l - 1);
    }
    if (r < high) {
      quickSort22(ts_block,r + 1, high);
    }
  }

  public static double bytes2Double(ArrayList<Byte> encoded, int start, int num) {
    if(num > 8){
      System.out.println("bytes2Doubleerror");
      return 0;
    }
    long value = 0;
    for (int i = 0; i < 8; i++) {
      value |= ((long) (encoded.get(i+start) & 0xff)) << (8 * i);
    }
    return Double.longBitsToDouble(value);
  }

  public static int bytes2Integer(ArrayList<Byte> encoded, int start, int num) {
    int value = 0;
    if(num > 4){
      System.out.println("bytes2Integer error");
      return 0;
    }
    for (int i = 0; i < num; i++) {
      value <<= 8;
      int b = encoded.get(i+start) & 0xFF;
      value |= b;
    }
    return value;
  }

  public static ArrayList<ArrayList<Integer>> ReorderingRegressionDecoder(ArrayList<Byte> encoded,int td){
    ArrayList<ArrayList<Integer>> data = new ArrayList<>();
    int decode_pos = 0;
    int block_size = bytes2Integer(encoded, decode_pos, 4);
    decode_pos += 4;

    while(decode_pos < encoded.size()) {
      //ArrayList<Integer> interval_list = new ArrayList<>();
      ArrayList<Integer> time_list = new ArrayList<>();
      ArrayList<Integer> value_list = new ArrayList<>();
      //ArrayList<Integer> deviation_list = new ArrayList<>();

      ArrayList<ArrayList<Integer>> ts_block = new ArrayList<>();

      int r0 = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;
//      int d0 = bytes2Integer(encoded, decode_pos, 4);
//      decode_pos += 4;
//
//      if (d0 % 2 == 0) {
//        d0 = d0 / 2;
//      } else {
//        d0 = -(d0 + 1) / 2;
//      }

//      int interval0 = bytes2Integer(encoded, decode_pos, 4);
//      decode_pos += 4;
      int time0 = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;
      int value0 = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;

//      int min_delta_interval = bytes2Integer(encoded, decode_pos, 4);
//      decode_pos += 4;
//      int min_delta_value = bytes2Integer(encoded, decode_pos, 4);
//      decode_pos += 4;

      double theta0_r = bytes2Double(encoded, decode_pos, 8);
      decode_pos += 8;
      double theta1_r = bytes2Double(encoded, decode_pos, 8);
      decode_pos += 8;
      double theta0_v = bytes2Double(encoded, decode_pos, 8);
      decode_pos += 8;
      double theta1_v = bytes2Double(encoded, decode_pos, 8);
      decode_pos += 8;

//      int max_bit_width_interval = bytes2Integer(encoded, decode_pos, 4);
//      decode_pos += 4;
//      interval_list = decodebitPacking(encoded,decode_pos,max_bit_width_interval,0,block_size);
//      decode_pos += max_bit_width_interval * (block_size - 1) / 8;

      int max_bit_width_time = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;
      time_list = decodebitPacking(encoded,decode_pos,max_bit_width_time,0,block_size);
      decode_pos += max_bit_width_time * (block_size - 1) / 8;

      int max_bit_width_value = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;
      value_list = decodebitPacking(encoded,decode_pos,max_bit_width_value,0,block_size);
      decode_pos += max_bit_width_value * (block_size - 1) / 8;

//      int max_bit_width_deviation = bytes2Integer(encoded, decode_pos, 4);
//      decode_pos += 4;
//      deviation_list = decodebitPacking(encoded,decode_pos,max_bit_width_deviation,0,block_size);
//      decode_pos += max_bit_width_deviation * (block_size - 1) / 8;

//      for (int i = 0; i < block_size-1; i++) {
//        ArrayList<Integer> ts_block_tmp = new ArrayList<>();
//        ts_block_tmp.add(interval_list.get(i));
//        ts_block_tmp.add(value_list.get(i));
//        ts_block.add(ts_block_tmp);
//      }

//      ArrayList<Integer> tmp_data = new ArrayList<>();
//      int timestamp = r0 * td + d0;
//      tmp_data.add(timestamp);
//      tmp_data.add(value0);
//      data.add(tmp_data);

      int ti_pre = time0;
      int vi_pre = value0;
      ArrayList<Integer> ts_block_tmp0 = new ArrayList<>();
      ts_block_tmp0.add(time0);
      ts_block_tmp0.add(value0);
      ts_block.add(ts_block_tmp0);
      for (int i = 0; i < block_size-1; i++) {
        int ti = (int) (theta1_r * ti_pre + theta0_r + time_list.get(i));
        time_list.set(i,ti);
        ti_pre = ti;

        int vi = (int) (theta1_v * vi_pre + theta0_v + value_list.get(i));
        value_list.set(i,vi);
        vi_pre = vi;

        ArrayList<Integer> ts_block_tmp = new ArrayList<>();
        ts_block_tmp.add(time_list.get(i));
        ts_block_tmp.add(value_list.get(i));
        ts_block.add(ts_block_tmp);
      }

//      int ri_pre = interval0;
//      int vi_pre = value0;
//      ArrayList<Integer> ts_block_tmp0 = new ArrayList<>();
//      ts_block_tmp0.add(interval0);
//      ts_block_tmp0.add(value0);
//      ts_block.add(ts_block_tmp0);
//      for (int i = 0; i < block_size-1; i++) {
//        int ri = (int) (theta1_r * ri_pre + theta0_r + interval_list.get(i));
//        interval_list.set(i,ri);
//        ri_pre = ri;
//
//        int vi = (int) (theta1_v * vi_pre + theta0_v + value_list.get(i));
//        value_list.set(i,vi);
//        vi_pre = vi;
//
//        int dev; //zigzag
//        if (deviation_list.get(block_size-1 - i - 1) % 2 == 0) {
//          dev = deviation_list.get(block_size-1 - i - 1) / 2;
//        } else {
//          dev = -(deviation_list.get(block_size-1 - i - 1) + 1) / 2;
//        }
//        deviation_list.set(block_size-1 - i - 1,dev);
//
//        ArrayList<Integer> ts_block_tmp = new ArrayList<>();
//        ts_block_tmp.add(interval_list.get(i));
//        ts_block_tmp.add(value_list.get(i));
//        ts_block.add(ts_block_tmp);
//      }

//      for(int i=0;i<block_size-1;i++){
//        for(int j=0;j<block_size-1 -i -1;j++){
//          if(interval_list.get(j)>interval_list.get(j+1)){
//            int tmp_1 = interval_list.get(j);
//            interval_list.set(j,interval_list.get(j+1));
//            interval_list.set(j,tmp_1);
//            int tmp_2 = value_list.get(j);
//            value_list.set(j,value_list.get(j+1));
//            value_list.set(j,tmp_2);
//          }
//        }
//      }

      //quickSort(ts_block, 0, 0, block_size-2);
      quickSort22(ts_block, 0, block_size-1);

      ArrayList<Integer> tmp_data0 = new ArrayList<>();
      tmp_data0.add(ts_block.get(0).get(0) * td + r0 * td);
      tmp_data0.add(ts_block.get(0).get(1));
      ts_block.set(0,tmp_data0);

      for (int i = 1; i < block_size; i++) {
        ArrayList<Integer> tmp_datai = new ArrayList<>();
        tmp_datai.add(ts_block.get(i).get(0) * td + r0 * td);
        tmp_datai.add(ts_block.get(i).get(1));
        ts_block.set(i,tmp_datai);
      }

      quickSort(ts_block, 0, 0, block_size-1);

      data.addAll(ts_block);

//      for (int i = 0; i < block_size; i++) {
//        ArrayList<Integer> tmp_datai = new ArrayList<>();
//        tmp_datai.add(interval_list.get(i) * td + deviation_list.get(i-1) + r0 * td);
//        tmp_datai.add(value_list.get(i));
//        data.add(tmp_datai);
//      }

//      for (int i = 0; i < block_size-1; i++) {
//        //int vi = vi_pre + value_list.get(i);
//        int vi = vi_pre + ts_block.get(i).get(1);
//        vi_pre = vi;
//
//        int ri = r0 * td + ts_block.get(i).get(0) * td;
//
//        int dev; //zigzag
//        if (deviation_list.get(block_size-1 - i - 1) % 2 == 0) {
//          dev = deviation_list.get(block_size-1 - i - 1) / 2;
//        } else {
//          dev = -(deviation_list.get(block_size-1 - i - 1) + 1) / 2;
//        }
//        int di = di_pre + dev;
//        di_pre = di;
//
//        int timestampi = ri + (di + d0);
//
//        ArrayList<Integer> tmp_datai = new ArrayList<>();
//        tmp_datai.add(timestampi);
//        tmp_datai.add(vi);
//        data.add(tmp_datai);
//      }
    }
    return data;
  }

  public static ArrayList<Integer> decodebitPacking(ArrayList<Byte> encoded,int decode_pos,int bit_width,int min_delta,int block_size){
    ArrayList<Integer> result_list = new ArrayList<>();
    for (int i = 0; i < (block_size-1) / 8; i++) { //bitpacking  纵向8个，bit width是多少列
      int[] val8 = new int[8];
      for (int j = 0; j < 8; j++) {
        val8[j] = 0;
      }
      for (int j = 0; j < bit_width; j++) {
        byte tmp_byte = encoded.get(decode_pos + bit_width - 1 - j);
        byte[] bit8 = new byte[8];
        for (int k = 0; k <8 ; k++) {
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



  public static void main(@org.jetbrains.annotations.NotNull String[] args) throws IOException {
    ArrayList<String> input_path_list = new ArrayList<>();
    ArrayList<String> output_path_list = new ArrayList<>();
    ArrayList<Integer> dataset_map_td = new ArrayList<>();
    input_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test\\Metro-Traffic");
    output_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\result_evaluation" +
            "\\compression_ratio\\decoding_time_rr\\Metro-Traffic_ratio.csv");
    dataset_map_td.add(3600);
    input_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test\\Nifty-Stocks");
    output_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\result_evaluation" +
            "\\compression_ratio\\decoding_time_rr\\Nifty-Stocks_ratio.csv");
    dataset_map_td.add(86400);
    input_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test\\USGS-Earthquakes");
    output_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\result_evaluation" +
            "\\compression_ratio\\decoding_time_rr\\USGS-Earthquakes_ratio.csv");
    dataset_map_td.add(50);
    input_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test\\Cyber-Vehicle");
    output_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\result_evaluation" +
            "\\compression_ratio\\decoding_time_rr\\Cyber-Vehicle_ratio.csv");
    dataset_map_td.add(10);
    input_path_list.add( "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test\\TH-Climate");
    output_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\result_evaluation" +
            "\\compression_ratio\\decoding_time_rr\\TH-Climate_ratio.csv");
    dataset_map_td.add(4);
    input_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test\\TY-Transport");
    output_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\result_evaluation" +
            "\\compression_ratio\\decoding_time_rr\\TY-Transport_ratio.csv");
    dataset_map_td.add(6);
    input_path_list.add( "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test\\TY-Fuel");
    output_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\result_evaluation" +
            "\\compression_ratio\\decoding_time_rr\\TY-Fuel_ratio.csv");
    dataset_map_td.add(60);
    input_path_list.add( "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test\\GW-Magnetic");
    output_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\result_evaluation" +
            "\\compression_ratio\\decoding_time_rr\\GW-Magnetic_ratio.csv");
    dataset_map_td.add(100);

//    input_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\mycode-encoding-reorder\\reorder\\iotdb_test\\Metro-Traffic");
//    output_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\mycode-encoding-reorder\\reorder\\result_evaluation" +
//            "\\compression_ratio\\rd_ratio\\Metro-Traffic_ratio.csv");
//    dataset_map_td.add(3600);
//    input_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\mycode-encoding-reorder\\reorder\\iotdb_test\\Nifty-Stocks");
//    output_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\mycode-encoding-reorder\\reorder\\result_evaluation" +
//            "\\compression_ratio\\rd_ratio\\Nifty-Stocks_ratio.csv");
//    dataset_map_td.add(86400);
//    input_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\mycode-encoding-reorder\\reorder\\iotdb_test\\USGS-Earthquakes");
//    output_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\mycode-encoding-reorder\\reorder\\result_evaluation" +
//            "\\compression_ratio\\rd_ratio\\USGS-Earthquakes_ratio.csv");
//    dataset_map_td.add(50);
//    input_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\mycode-encoding-reorder\\reorder\\iotdb_test\\Cyber-Vehicle");
//    output_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\mycode-encoding-reorder\\reorder\\result_evaluation" +
//            "\\compression_ratio\\rd_ratio\\Cyber-Vehicle_ratio.csv");
//    dataset_map_td.add(10);
//    input_path_list.add( "E:\\thu\\Lab\\Group\\31编码论文\\mycode-encoding-reorder\\reorder\\iotdb_test\\TH-Climate");
//    output_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\mycode-encoding-reorder\\reorder\\result_evaluation" +
//            "\\compression_ratio\\rd_ratio\\TH-Climate_ratio.csv");
//    dataset_map_td.add(3);
//    input_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\mycode-encoding-reorder\\reorder\\iotdb_test\\TY-Transport");
//    output_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\mycode-encoding-reorder\\reorder\\result_evaluation" +
//            "\\compression_ratio\\rd_ratio\\TY-Transport_ratio.csv");
//    dataset_map_td.add(5);
//    input_path_list.add( "E:\\thu\\Lab\\Group\\31编码论文\\mycode-encoding-reorder\\reorder\\iotdb_test\\TY-Fuel");
//    output_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\mycode-encoding-reorder\\reorder\\result_evaluation" +
//            "\\compression_ratio\\rd_ratio\\TY-Fuel_ratio.csv");
//    dataset_map_td.add(60);

//    for(int file_i=0;file_i<1;file_i++){
    for(int file_i=0;file_i<input_path_list.size();file_i++){

      String inputPath = input_path_list.get(file_i);
      String Output =output_path_list.get(file_i);


  //    String Output =
  //            "C:\\Users\\xiaoj\\Desktop\\test_ratio.csv"; // the direction of output compression ratio and

      // speed
      int repeatTime = 1; // set repeat time

      File file = new File(inputPath);
      File[] tempList = file.listFiles();

      CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

      String[] head = {
              "Input Direction",
              "Encoding Algorithm",
  //      "Compress Algorithm",
              "Encoding Time",
              "Decoding Time",
  //      "Compress Time",
  //      "Uncompress Time",
              "Points",
              "Compressed Size",
              "Compression Ratio"
      };
      writer.writeRecord(head); // write header to output file

      assert tempList != null;

      for (File f : tempList) {
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
        long encodeTime = 0;
        long decodeTime = 0;
        double ratio = 0;
        double compressed_size = 0;
        for (int i = 0; i < repeatTime; i++) {
          long s = System.nanoTime();
          ArrayList<Byte> buffer = ReorderingRegressionEncoder(data, 256, dataset_map_td.get(file_i));
          long e = System.nanoTime();
          encodeTime += (e - s);
          compressed_size += buffer.size();
          double ratioTmp =
                  (double) buffer.size() / (double) (data.size() * Integer.BYTES*2);
          ratio += ratioTmp;
          s = System.nanoTime();

          data_decoded = ReorderingRegressionDecoder(buffer,dataset_map_td.get(file_i));

//          for(int j=0;j<256;j++){
//            if(!data.get(j).get(0).equals(data_decoded.get(j).get(0))){
//              System.out.println("Wrong Time!");
//              System.out.print(j);
//              System.out.print(" ");
//              System.out.print(data.get(j).get(0));
//              System.out.print(" ");
//              System.out.println(data_decoded.get(j).get(0));
//            }
//            else{
//              System.out.println("Correct Time!");
//              System.out.print(j);
//              System.out.print(" ");
//              System.out.print(data.get(j).get(0));
//              System.out.print(" ");
//              System.out.println(data_decoded.get(j).get(0));
//            }
//            if(!data.get(j).get(1).equals(data_decoded.get(j).get(1))){
//              System.out.println("Wrong Value!");
//              System.out.print(j);
//              System.out.print(" ");
//              System.out.print(data.get(j).get(1));
//              System.out.print(" ");
//              System.out.println(data_decoded.get(j).get(1));
//            }
//            else{
//              System.out.println("Correct Value!");
//              System.out.print(j);
//              System.out.print(" ");
//              System.out.print(data.get(j).get(1));
//              System.out.print(" ");
//              System.out.println(data_decoded.get(j).get(1));
//            }
//          }

          e = System.nanoTime();
          decodeTime += (e-s);
        }


        ratio /= repeatTime;
        compressed_size /= repeatTime;
        encodeTime /= repeatTime;
        decodeTime /= repeatTime;

        String[] record = {
                f.toString(),
                "RR",
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
