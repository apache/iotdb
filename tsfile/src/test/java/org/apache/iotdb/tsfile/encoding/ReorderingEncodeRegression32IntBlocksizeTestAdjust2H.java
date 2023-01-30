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

import static java.lang.Math.abs;

public class ReorderingEncodeRegression32IntBlocksizeTestAdjust2H {

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
  public static byte[] float2Bytes(float f){
    int value = Float.floatToIntBits(f);
    byte[] bytes= new byte[4];
    for(int i=0;i<4;i++){
      bytes[i] = (byte) ((value >>8*i)& 0xff);
    }
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

//  public static void splitTimeStamp(ArrayList<ArrayList<Integer>> ts_block, int block_size, int td,
//                                    ArrayList<Integer> deviation_list,ArrayList<Integer> result){
//    int deviation_max = Integer.MIN_VALUE;
//    int max_bit_width_deviation;
//    int r0;
//    int d0;
//
//    // split timestamp into intervals and deviations
//
//    //address other timestamps and values
//    for(int j=block_size-1;j>0;j--) {
//      int delta_interval = (ts_block.get(j).get(0) - ts_block.get(j-1).get(0))/td;
//      ArrayList<Integer> tmp = ts_block.get(j);
//      tmp.add(delta_interval);
//      ts_block.set(j,tmp);
//    }
//
//
//    // address timestamp0
//    r0 = ts_block.get(0).get(0) /td;
//    d0 = ts_block.get(0).get(0) %td;
//    if(d0 >= (td/2)){
//      d0 -= td;
//      r0 ++;
//    }
//    d0 = zigzag(d0);
////    deviation_list.add(d0);
//    if(d0 > deviation_max){
//      deviation_max = d0;
//    }
//    ArrayList<Integer> tmp0 = ts_block.get(0);
//    tmp0.add(0);
////    System.out.println(tmp0);
//    ts_block.set(0,tmp0);
//
//    for(int j=1;j<block_size;j++){
//      int interval = ts_block.get(j).get(2) + ts_block.get(j-1).get(2);
//      ArrayList<Integer> tmp;
//      tmp = ts_block.get(j);
//      tmp.set(2,interval);
//      ts_block.set(j,tmp);
//    }
////    System.out.println(ts_block);
//    quickSort2(ts_block,0,block_size-1);
//
//    for(int j=block_size-1;j>0;j--){
//      int interval = ts_block.get(j).get(2);
//      int value = ts_block.get(j).get(1);
//
//      int delta_interval = ts_block.get(j).get(2) - ts_block.get(j-1).get(2);
//      int deviation = (ts_block.get(j).get(0) - ts_block.get(j-1).get(0))-delta_interval*td;
//      deviation = zigzag(deviation);
//      deviation_list.add(deviation);
//      if(deviation > deviation_max){
//        deviation_max = deviation;
//      }
//
//      ArrayList<Integer> tmp = new ArrayList<>();
//      tmp.add(interval);
//      tmp.add(value);
//      ts_block.set(j,tmp);
//    }
//    tmp0 = new ArrayList<>();
//    tmp0.add(ts_block.get(0).get(2));
//    tmp0.add(ts_block.get(0).get(1));
//    ts_block.set(0,tmp0);
//
//
//    max_bit_width_deviation = getBitWith(deviation_max);
//    result.add(max_bit_width_deviation);
//    result.add(r0);
//    result.add(d0);
//  }

  public static void splitTimeStamp2(ArrayList<ArrayList<Integer>> ts_block, int td,
                                     ArrayList<Integer> deviation_list,ArrayList<Integer> result){

    int max_deviation = Integer.MIN_VALUE;

    int t0 = ts_block.get(0).get(0);
    for(int i=0;i<ts_block.size();i++){
      ArrayList<Integer> tmp = new ArrayList<>();
      int interval_i = (ts_block.get(i).get(0) - t0) / td;
      int deviation_i = ts_block.get(i).get(0) - t0 - interval_i * td;
      tmp.add(t0 + interval_i);
      tmp.add(ts_block.get(i).get(1));
      ts_block.set(i,tmp);

      deviation_list.add(zigzag(deviation_i));
      if(zigzag(deviation_i)>max_deviation){
        max_deviation = zigzag(deviation_i);
      }
    }

    int max_bit_width_deviation = getBitWith(max_deviation);
    result.add(max_bit_width_deviation);
  }

  public static int getCommon(int m,int n){
    int z;
    while (m%n!=0){
      z = m%n;
      m = n;
      n = z;
    }
    return n;
  }

  public static void splitTimeStamp3(ArrayList<ArrayList<Integer>> ts_block, int td,ArrayList<Integer> result){
    //int max_deviation = Integer.MIN_VALUE;

    int td_common = 0;
    for(int i=1;i<ts_block.size();i++){
      int time_diffi = ts_block.get(i).get(0) - ts_block.get(i-1).get(0);
      if(td_common == 0){
        if(time_diffi != 0){
          td_common = time_diffi;
          continue;
        }
        else{
          continue;
        }
      }
      if(time_diffi != 0){
        td_common = getCommon(time_diffi, td_common);
        if (td_common == 1) {
          break;
        }
      }
    }
    if(td_common==0){
      td_common = 1;
    }

    td = td_common;

    int t0 = ts_block.get(0).get(0);
    for(int i=0;i<ts_block.size();i++){
      ArrayList<Integer> tmp = new ArrayList<>();
      int interval_i = (ts_block.get(i).get(0) - t0) / td;
      //int deviation_i = ts_block.get(i).get(0) - t0 - interval_i * td;
      tmp.add(t0 + interval_i);
      tmp.add(ts_block.get(i).get(1));
      ts_block.set(i,tmp);

      //deviation_list.add(zigzag(deviation_i));
      //if(zigzag(deviation_i)>max_deviation){
      //  max_deviation = zigzag(deviation_i);
      //}
    }

    //int max_bit_width_deviation = getBitWith(max_deviation);
    //result.add(max_bit_width_deviation);
    result.add(td_common);
  }

  public static ArrayList<ArrayList<Integer>> getEncodeBitsRegression(ArrayList<ArrayList<Integer>> ts_block, int block_size,
                                                                      ArrayList<Integer> result, ArrayList<Integer> i_star, ArrayList<Integer> theta){
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    ArrayList<ArrayList<Integer>> ts_block_delta = new ArrayList<>();
    theta.clear();

//    for(int i=0;i<ts_block.size();i++){
//      System.out.println(ts_block.get(i).get(0));
//    }

//    int ti0 = ts_block.get(0).get(0);
//    for(int i=0;i<ts_block.size();i++){
//      ArrayList<Integer> tmp = new ArrayList<>();
//      tmp.add(ts_block.get(i).get(0) - ti0);
//      tmp.add(ts_block.get(i).get(1));
//      ts_block.set(i,tmp);
//    }

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
    int theta0_r = 0;
    int theta1_r = 1;
    if(m_reg*sum_squ_X_r != sum_X_r*sum_X_r ){
      theta0_r = (int) (sum_squ_X_r*sum_Y_r - sum_X_r*sum_squ_XY_r) / (int) (m_reg*sum_squ_X_r - sum_X_r*sum_X_r);
      theta1_r = (int) (m_reg*sum_squ_XY_r - sum_X_r*sum_Y_r) / (int) (m_reg*sum_squ_X_r - sum_X_r*sum_X_r);
    }

    int theta0_v = 0;
    int theta1_v = 1;
    if(m_reg*sum_squ_X_v != sum_X_v*sum_X_v ){
      theta0_v = (int) (sum_squ_X_v*sum_Y_v - sum_X_v*sum_squ_XY_v) / (int) (m_reg*sum_squ_X_v - sum_X_v*sum_X_v);
      theta1_v = (int) (m_reg*sum_squ_XY_v - sum_X_v*sum_Y_v) / (int) (m_reg*sum_squ_X_v - sum_X_v*sum_X_v);
    }

    //theta0_r = 0;
    //theta1_r = 1;


    ArrayList<Integer> tmp0 = new ArrayList<>();
    tmp0.add(ts_block.get(0).get(0));
    tmp0.add(ts_block.get(0).get(1));
    ts_block_delta.add(tmp0);

    // delta to Regression
    for(int j=1;j<block_size;j++) {
//      int epsilon_r = (int) ((double)ts_block.get(j).get(0) - theta0_r - theta1_r * (double)ts_block.get(j-1).get(0));
//      int epsilon_v = (int) ((double)ts_block.get(j).get(1) - theta0_v - theta1_v * (double)ts_block.get(j-1).get(1));
      int epsilon_r = ts_block.get(j).get(0) - ( theta0_r + theta1_r * ts_block.get(j-1).get(0));
      int epsilon_v = ts_block.get(j).get(1) - ( theta0_v + theta1_v * ts_block.get(j-1).get(1));

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

//    result.add(timestamp_delta_min);
//    result.add(value_delta_min);

//    theta0_r += timestamp_delta_min;
//    theta0_v += value_delta_min;
    theta.add(theta0_r);
    theta.add(theta1_r);
    theta.add(theta0_v);
    theta.add(theta1_v);
//    System.out.println(theta);

    i_star.add(max_interval_i);
    i_star.add(max_value_i);

    return ts_block_delta;
  }
  public static int getJStar(ArrayList<ArrayList<Integer>> ts_block, int alpha, int block_size,
                             ArrayList<Integer> raw_length, int index, ArrayList<Integer> theta){
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    int raw_timestamp_delta_max = Integer.MIN_VALUE;
    int raw_value_delta_max = Integer.MIN_VALUE;
    int raw_timestamp_delta_max_index = -1;
    int raw_value_delta_max_index = -1;
    int raw_bit_width_timestamp = 0;
    int raw_bit_width_value = 0;

    int theta0_t = theta.get(0);
    int theta1_t = theta.get(1);
    int theta0_v = theta.get(2);
    int theta1_v = theta.get(3);

    ArrayList<Integer> j_star_list = new ArrayList<>(); // beta list of min b phi alpha to j
    int j_star = -1;

    if(alpha == -1){
      return j_star;
    }
//    System.out.println(theta);
//    System.out.println(ts_block);
    for(int i = 1;i<block_size;i++){
      int delta_t_i =  ts_block.get(i).get(0) - ( theta0_t + theta1_t * ts_block.get(i-1).get(0));
      int delta_v_i =  ts_block.get(i).get(1) - ( theta0_v + theta1_v * ts_block.get(i-1).get(1));
      if(delta_t_i < timestamp_delta_min){
        timestamp_delta_min = delta_t_i;
      }
      if(delta_v_i < value_delta_min){
        value_delta_min = delta_v_i;
      }
      if(delta_t_i > raw_timestamp_delta_max){
        raw_timestamp_delta_max = delta_t_i;
        raw_timestamp_delta_max_index = i;
      }
      if(delta_v_i > raw_value_delta_max){
        raw_value_delta_max = delta_v_i;
        raw_value_delta_max_index = i;
      }
    }
    raw_bit_width_timestamp = getBitWith(raw_timestamp_delta_max-timestamp_delta_min);
    raw_bit_width_value = getBitWith(raw_value_delta_max-value_delta_min);
//    System.out.println(raw_length);
//    System.out.println(raw_bit_width_timestamp);
//    System.out.println(raw_bit_width_value);
    // alpha == 1
    if(alpha==0){
//      System.out.println("alpha == 1");
      for(int j = 2;j<block_size;j++){
        ArrayList<Integer> b = adjust0(ts_block,alpha,j,theta);
        if((b.get(0) + b.get(1)) < (raw_bit_width_timestamp+raw_bit_width_value) ){
          raw_bit_width_timestamp = b.get(0);
          raw_bit_width_value = b.get(1);
          j_star_list.clear();
          j_star_list.add(j);
//          System.out.println("j_star_list adjust0");
        }
        else if ((b.get(0) + b.get(1)) == (raw_bit_width_timestamp+raw_bit_width_value)){
          j_star_list.add(j);
        }
      }
      ArrayList<Integer> b = adjust0n1(ts_block,theta);
      if((b.get(0) + b.get(1)) < (raw_bit_width_timestamp+raw_bit_width_value) ){
        raw_bit_width_timestamp = b.get(0);
        raw_bit_width_value = b.get(1);
        j_star_list.clear();
        j_star_list.add(block_size);
//        System.out.println("j_star_list adjust0n1");
      }
      else if ((b.get(0) + b.get(1)) == (raw_bit_width_timestamp+raw_bit_width_value)){
        j_star_list.add(block_size);
      }

    } // alpha == n
    else if(alpha == block_size-1){
//      System.out.println("alpha == n");
      for(int j = 1;j<block_size-1;j++){
        ArrayList<Integer> b = adjustn(ts_block,alpha,j,theta);
        if((b.get(0) + b.get(1)) < (raw_bit_width_timestamp+raw_bit_width_value) ){
          raw_bit_width_timestamp = b.get(0);
          raw_bit_width_value = b.get(1);
          j_star_list.clear();
          j_star_list.add(j);
//          System.out.println("j_star_list adjustn");
        }
        else if ((b.get(0) + b.get(1)) == (raw_bit_width_timestamp+raw_bit_width_value)){
          j_star_list.add(j);
        }
      }
      ArrayList<Integer> b = adjustn0(ts_block,theta);
      if((b.get(0) + b.get(1)) < (raw_bit_width_timestamp+raw_bit_width_value)){
        raw_bit_width_timestamp = b.get(0);
        raw_bit_width_value = b.get(1);
        j_star_list.clear();
        j_star_list.add(0);
//        System.out.println("j_star_list adjustn0");
      }
      else if ((b.get(0) + b.get(1)) == (raw_bit_width_timestamp+raw_bit_width_value)){
        j_star_list.add(0);
//        System.out.println("j_star_list adjustn0 ==");
      }
    } // alpha != 1 and alpha != n
    else {
//      System.out.println("alpha == else");
      for(int j = 1;j<block_size;j++){
        if(alpha != j && (alpha+1) !=j){
          ArrayList<Integer> b = adjustAlphaToJ(ts_block,alpha,j,theta);
          if((b.get(0) + b.get(1)) < (raw_bit_width_timestamp+raw_bit_width_value) ){

            raw_bit_width_timestamp = b.get(0);
            raw_bit_width_value = b.get(1);
            j_star_list.clear();
            j_star_list.add(j);
//            System.out.println("j_star_list adjustAlphaToJ");
          }else if ((b.get(0) + b.get(1)) == (raw_bit_width_timestamp+raw_bit_width_value)){
            j_star_list.add(j);
//            System.out.println(b.get(0));
//            System.out.println(b.get(1));
//            System.out.println("j_star_list adjustAlphaToJ ==");
          }
        }
      }
      ArrayList<Integer> b = adjustTo0(ts_block,alpha,theta);
      if((b.get(0) + b.get(1)) < (raw_bit_width_timestamp+raw_bit_width_value) ){
        raw_bit_width_timestamp = b.get(0);
        raw_bit_width_value = b.get(1);
        j_star_list.clear();
        j_star_list.add(0);
//        System.out.println("j_star_list adjustTo0");
      }
      else if ((b.get(0) + b.get(1)) == (raw_bit_width_timestamp+raw_bit_width_value)){
//        System.out.println("j_star_list adjustTo0 ==");
        j_star_list.add(0);

      }
      b = adjustTon(ts_block,alpha,theta);
      if((b.get(0) + b.get(1)) < (raw_bit_width_timestamp+raw_bit_width_value) ){
        raw_bit_width_timestamp = b.get(0);
        raw_bit_width_value = b.get(1);
        j_star_list.clear();
        j_star_list.add(block_size);
//        System.out.println("j_star_list adjustTon");
      }
      else if ((b.get(0) + b.get(1)) == (raw_bit_width_timestamp+raw_bit_width_value)){
//        System.out.println("j_star_list adjustTon ==");

        j_star_list.add(block_size);
      }
    }
//    System.out.println(j_star_list);
    if(j_star_list.size() == 0){
    }else {
      j_star = getIstarClose(alpha,j_star_list);
    }
    return j_star;
  }

  private static ArrayList<Integer> adjustTo0(ArrayList<ArrayList<Integer>> ts_block, int alpha, ArrayList<Integer> theta) {
    int block_size = ts_block.size();
    assert alpha != block_size-1;
    assert alpha != 0;
    ArrayList<Integer> b = new ArrayList<>();
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    int timestamp_delta_max = Integer.MIN_VALUE;
    int value_delta_max = Integer.MIN_VALUE;

    int theta0_t = theta.get(0);
    int theta1_t = theta.get(1);
    int theta0_v = theta.get(2);
    int theta1_v = theta.get(3);

    for(int i=1;i<block_size;i++){
      int timestamp_delta_i;
      int value_delta_i;
      if( i == (alpha+1)){
        timestamp_delta_i =  ts_block.get(alpha+1).get(0) - (theta0_t + theta1_t * ts_block.get(alpha-1).get(0));
        value_delta_i = ts_block.get(alpha+1).get(1) - (theta0_v - theta1_v * ts_block.get(alpha-1).get(1));
      } else if (i == alpha){
        timestamp_delta_i =  ts_block.get(0).get(0) - (theta0_t + theta1_t * ts_block.get(alpha).get(0));
        value_delta_i = ts_block.get(0).get(1) - (theta0_v + theta1_v * ts_block.get(alpha).get(1));
      }
      else{
        timestamp_delta_i = ts_block.get(i).get(0) - (theta0_t + theta1_t * ts_block.get(i - 1).get(0));
        value_delta_i =  ts_block.get(i).get(1) - (theta0_v - theta1_v * ts_block.get(i - 1).get(1));
      }
      if(timestamp_delta_i>timestamp_delta_max){
        timestamp_delta_max = timestamp_delta_i;
      }
      if(timestamp_delta_i<timestamp_delta_min){
        timestamp_delta_min = timestamp_delta_i;
      }
      if(value_delta_i > value_delta_max){
        value_delta_max = value_delta_i;
      }
      if(value_delta_i <value_delta_min){
        value_delta_min = value_delta_i;
      }

    }
    b.add(getBitWith(timestamp_delta_max-timestamp_delta_min));
    b.add(getBitWith(value_delta_max-value_delta_min));
    return b;
  }
  private static ArrayList<Integer> adjustTon(ArrayList<ArrayList<Integer>> ts_block, int alpha, ArrayList<Integer> theta) {
    int block_size = ts_block.size();
    assert alpha != block_size-1;
    assert alpha != 0;
    ArrayList<Integer> b = new ArrayList<>();
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    int timestamp_delta_max = Integer.MIN_VALUE;
    int value_delta_max = Integer.MIN_VALUE;

    int theta0_t = theta.get(0);
    int theta1_t = theta.get(1);
    int theta0_v = theta.get(2);
    int theta1_v = theta.get(3);

    for(int i=1;i<block_size;i++){
      int timestamp_delta_i;
      int value_delta_i;
      if( i == (alpha+1)){
        timestamp_delta_i = ts_block.get(alpha+1).get(0) - ( theta0_t + theta1_t * ts_block.get(alpha-1).get(0));
        value_delta_i =  ts_block.get(alpha+1).get(1) - (theta0_v + theta1_v * ts_block.get(alpha-1).get(1));
      }
      else if (i == alpha){
        timestamp_delta_i = ts_block.get(alpha).get(0) - (theta0_t + theta1_t * ts_block.get(block_size-1).get(0));
        value_delta_i =  ts_block.get(alpha).get(1) - (theta0_v + theta1_v * ts_block.get(block_size-1).get(1));
      }
      else{
        timestamp_delta_i =  ts_block.get(i).get(0) - (theta0_t + theta1_t * ts_block.get(i - 1).get(0));
        value_delta_i = ts_block.get(i).get(1) - (theta0_v + theta1_v * ts_block.get(i - 1).get(1));
      }
      if(timestamp_delta_i>timestamp_delta_max){
        timestamp_delta_max = timestamp_delta_i;
      }
      if(timestamp_delta_i<timestamp_delta_min){
        timestamp_delta_min = timestamp_delta_i;
      }
      if(value_delta_i > value_delta_max){
        value_delta_max = value_delta_i;
      }
      if(value_delta_i <value_delta_min){
        value_delta_min = value_delta_i;
      }

    }

    b.add(getBitWith(timestamp_delta_max-timestamp_delta_min));
    b.add(getBitWith(value_delta_max-value_delta_min));
    return b;
  }

  private static ArrayList<Integer> adjustAlphaToJ(ArrayList<ArrayList<Integer>> ts_block, int alpha, int j, ArrayList<Integer> theta) {

    int block_size = ts_block.size();
    assert alpha != block_size-1;
    assert alpha != 0;
    assert j != 0;
    assert j != block_size;
    ArrayList<Integer> b = new ArrayList<>();
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    int timestamp_delta_max = Integer.MIN_VALUE;
    int value_delta_max = Integer.MIN_VALUE;

    int theta0_t = theta.get(0);
    int theta1_t = theta.get(1);
    int theta0_v = theta.get(2);
    int theta1_v = theta.get(3);

    for(int i=1;i<block_size;i++){
      int timestamp_delta_i;
      int value_delta_i;
      if(i==j){
        timestamp_delta_i =  ts_block.get(j).get(0) - (theta0_t + theta1_t * ts_block.get(alpha).get(0));
        value_delta_i = ts_block.get(j).get(1) - (theta0_v + theta1_v * ts_block.get(alpha).get(1));
      }
      else if (i == alpha){
        timestamp_delta_i =  ts_block.get(alpha).get(0) - (theta0_t + theta1_t * ts_block.get(j-1).get(0));
        value_delta_i =  ts_block.get(alpha).get(1) - (theta0_v + theta1_v * ts_block.get(j-1).get(1));
      }
      else if (i == alpha+1) {
        timestamp_delta_i =  ts_block.get(alpha+1).get(0) - (theta0_t + theta1_t * ts_block.get(alpha-1).get(0));
        value_delta_i =  ts_block.get(alpha+1).get(1) - (theta0_v + theta1_v * ts_block.get(alpha-1).get(1));
      }
      else {
        timestamp_delta_i =  ts_block.get(i).get(0) - (theta0_t + theta1_t * ts_block.get(i-1).get(0));
        value_delta_i = ts_block.get(i).get(1) - (theta0_v + theta1_v * ts_block.get(i-1).get(1));
      }
      if(timestamp_delta_i>timestamp_delta_max){
        timestamp_delta_max = timestamp_delta_i;
      }
      if(timestamp_delta_i<timestamp_delta_min){
        timestamp_delta_min = timestamp_delta_i;
      }
      if(value_delta_i > value_delta_max){
        value_delta_max = value_delta_i;
      }
      if(value_delta_i <value_delta_min){
        value_delta_min = value_delta_i;
      }

    }
    b.add(getBitWith(timestamp_delta_max-timestamp_delta_min));
    b.add(getBitWith(value_delta_max-value_delta_min));
    return b;
  }

  // adjust n to 0
  private static ArrayList<Integer> adjustn0(ArrayList<ArrayList<Integer>> ts_block, ArrayList<Integer> theta) {
    int block_size = ts_block.size();
    ArrayList<Integer> b = new ArrayList<>();
    int timestamp_delta_max = Integer.MIN_VALUE;
    int value_delta_max = Integer.MIN_VALUE;
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    int theta0_t = theta.get(0);
    int theta1_t = theta.get(1);
    int theta0_v = theta.get(2);
    int theta1_v = theta.get(3);

    for(int i=1;i<block_size-1;i++){
      int timestamp_delta_i;
      int value_delta_i;
      timestamp_delta_i = ts_block.get(i).get(0) -   (theta0_t + theta1_t *  ts_block.get(i - 1).get(0));
      value_delta_i =   ts_block.get(i).get(1) - (theta0_v + theta1_v *  ts_block.get(i - 1).get(1));
      if(timestamp_delta_i>timestamp_delta_max){
        timestamp_delta_max = timestamp_delta_i;
      }
      if(timestamp_delta_i<timestamp_delta_min){
        timestamp_delta_min = timestamp_delta_i;
      }
      if(value_delta_i > value_delta_max){
        value_delta_max = value_delta_i;
      }
      if(value_delta_i <value_delta_min){
        value_delta_min = value_delta_i;
      }

    }
    int timestamp_delta_i;
    int value_delta_i;
    timestamp_delta_i =   ts_block.get(0).get(0) - (theta0_t + theta1_t * ts_block.get(block_size - 1).get(0));
    value_delta_i =  ts_block.get(0).get(1) - (theta0_v + theta1_v *  ts_block.get(block_size - 1).get(1));
    if(timestamp_delta_i>timestamp_delta_max){
      timestamp_delta_max = timestamp_delta_i;
    }
    if(timestamp_delta_i<timestamp_delta_min){
      timestamp_delta_min = timestamp_delta_i;
    }
    if(value_delta_i > value_delta_max){
      value_delta_max = value_delta_i;
    }
    if(value_delta_i <value_delta_min){
      value_delta_min = value_delta_i;
    }
    b.add(getBitWith(timestamp_delta_max-timestamp_delta_min));
    b.add(getBitWith(value_delta_max-value_delta_min));
    return b;
  }

  // adjust n to no 0
  private static ArrayList<Integer> adjustn(ArrayList<ArrayList<Integer>> ts_block, int alpha, int j, ArrayList<Integer> theta) {
    int block_size = ts_block.size();
    assert alpha == block_size-1;
    assert j != 0;
    ArrayList<Integer> b = new ArrayList<>();
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    int timestamp_delta_max = Integer.MIN_VALUE;
    int value_delta_max = Integer.MIN_VALUE;

    int theta0_t = theta.get(0);
    int theta1_t = theta.get(1);
    int theta0_v = theta.get(2);
    int theta1_v = theta.get(3);

    for(int i=1;i<block_size-1;i++){
      int timestamp_delta_i;
      int value_delta_i;
      if(i!=j){
        timestamp_delta_i =  ts_block.get(i).get(0) - (theta0_t + theta1_t *  ts_block.get(i - 1).get(0));
        value_delta_i =  ts_block.get(i).get(1) - (theta0_v + theta1_v * ts_block.get(i - 1).get(1));
      }
      else {
        timestamp_delta_i =  ts_block.get(j).get(0) -  (theta0_t + theta1_t * ts_block.get(alpha).get(0));
        value_delta_i =  ts_block.get(j).get(1) - (theta0_v + theta1_v * ts_block.get(alpha).get(1));
        if(timestamp_delta_i>timestamp_delta_max){
          timestamp_delta_max = timestamp_delta_i;
        }
        if(timestamp_delta_i<timestamp_delta_min){
          timestamp_delta_min = timestamp_delta_i;
        }
        if(value_delta_i > value_delta_max){
          value_delta_max = value_delta_i;
        }
        if(value_delta_i <value_delta_min){
          value_delta_min = value_delta_i;
        }
        timestamp_delta_i = ts_block.get(alpha).get(0) -  (theta0_t + theta1_t *  ts_block.get(j-1).get(0));
        value_delta_i =  ts_block.get(alpha).get(1) - (theta0_v + theta1_v *  ts_block.get(j-1).get(1));
      }
      if(timestamp_delta_i>timestamp_delta_max){
        timestamp_delta_max = timestamp_delta_i;
      }
      if(timestamp_delta_i<timestamp_delta_min){
        timestamp_delta_min = timestamp_delta_i;
      }
      if(value_delta_i > value_delta_max){
        value_delta_max = value_delta_i;
      }
      if(value_delta_i <value_delta_min){
        value_delta_min = value_delta_i;
      }

    }
    b.add(getBitWith(timestamp_delta_max-timestamp_delta_min));
    b.add(getBitWith(value_delta_max-value_delta_min));
    return b;
  }

  private static int getIstarClose(int alpha, ArrayList<Integer> j_star_list) {
    int min_i = 0;
    int min_dis = Integer.MAX_VALUE;
    for (int i:j_star_list) {
      if(abs(alpha-i)<min_dis){
        min_i = i;
        min_dis = abs(alpha - i);
      }
    }
    if(min_dis==0){
      System.out.println("get IstarClose error");
      return 0;
    }
    return min_i;
  }

  // adjust 0 to n
  private static ArrayList<Integer> adjust0n1(ArrayList<ArrayList<Integer>> ts_block, ArrayList<Integer> theta) {
    int block_size = ts_block.size();
    ArrayList<Integer> b = new ArrayList<>();
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    int timestamp_delta_max = Integer.MIN_VALUE;
    int value_delta_max = Integer.MIN_VALUE;

    int theta0_t = theta.get(0);
    int theta1_t = theta.get(1);
    int theta0_v = theta.get(2);
    int theta1_v = theta.get(3);

    for(int i=2;i<block_size;i++){
      int timestamp_delta_i;
      int value_delta_i;
      timestamp_delta_i = ts_block.get(i).get(0) -  (theta0_t + theta1_t *  ts_block.get(i - 1).get(0));
      value_delta_i =  ts_block.get(i).get(1) - (theta0_v + theta1_v * ts_block.get(i - 1).get(1));
      if(timestamp_delta_i>timestamp_delta_max){
        timestamp_delta_max = timestamp_delta_i;
      }
      if(timestamp_delta_i<timestamp_delta_min){
        timestamp_delta_min = timestamp_delta_i;
      }
      if(value_delta_i > value_delta_max){
        value_delta_max = value_delta_i;
      }
      if(value_delta_i <value_delta_min){
        value_delta_min = value_delta_i;
      }
    }
    int timestamp_delta_i;
    int value_delta_i;
    timestamp_delta_i =  ts_block.get(0).get(0) -  (theta0_t + theta1_t * ts_block.get(block_size - 1).get(0));
    value_delta_i = ts_block.get(0).get(1) -  (theta0_v + theta1_v *  ts_block.get(block_size - 1).get(1));
    if(timestamp_delta_i>timestamp_delta_max){
      timestamp_delta_max = timestamp_delta_i;
    }
    if(timestamp_delta_i<timestamp_delta_min){
      timestamp_delta_min = timestamp_delta_i;
    }
    if(value_delta_i > value_delta_max){
      value_delta_max = value_delta_i;
    }
    if(value_delta_i <value_delta_min){
      value_delta_min = value_delta_i;
    }
    b.add(getBitWith(timestamp_delta_max-timestamp_delta_min));
    b.add(getBitWith(value_delta_max-value_delta_min));
    return b;
  }

  // adjust 0 to no n
  private static ArrayList<Integer> adjust0(ArrayList<ArrayList<Integer>> ts_block, int alpha, int j, ArrayList<Integer> theta) {
    int block_size = ts_block.size();
    assert alpha == 0;
    assert j != block_size;

    ArrayList<Integer> b = new ArrayList<>();
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    int timestamp_delta_max = Integer.MIN_VALUE;
    int value_delta_max = Integer.MIN_VALUE;

    int theta0_t = theta.get(0);
    int theta1_t = theta.get(1);
    int theta0_v = theta.get(2);
    int theta1_v = theta.get(3);

    for(int i=2;i<block_size;i++){
      int timestamp_delta_i;
      int value_delta_i;
      if(i!=j){
        timestamp_delta_i =  ts_block.get(i).get(0) - (theta0_t + theta1_t *   ts_block.get(i - 1).get(0));
        value_delta_i =  ts_block.get(i).get(1) - (theta0_v + theta1_v *  ts_block.get(i - 1).get(1));
      } else {
        timestamp_delta_i =   ts_block.get(j).get(0) - (theta0_t + theta1_t *  ts_block.get(alpha).get(0));
        value_delta_i =  ts_block.get(j).get(1) -  (theta0_v + theta1_v * ts_block.get(alpha).get(1));
        if(timestamp_delta_i>timestamp_delta_max){
          timestamp_delta_max = timestamp_delta_i;
        }
        if(timestamp_delta_i<timestamp_delta_min){
          timestamp_delta_min = timestamp_delta_i;
        }
        if(value_delta_i > value_delta_max){
          value_delta_max = value_delta_i;
        }
        if(value_delta_i <value_delta_min){
          value_delta_min = value_delta_i;
        }
        timestamp_delta_i =  ts_block.get(alpha).get(0) -  (theta0_t + theta1_t * ts_block.get(j-1).get(0));
        value_delta_i =  ts_block.get(alpha).get(1) - (theta0_v + theta1_v * ts_block.get(j-1).get(1));
      }
      if(timestamp_delta_i>timestamp_delta_max){
        timestamp_delta_max = timestamp_delta_i;
      }
      if(timestamp_delta_i<timestamp_delta_min){
        timestamp_delta_min = timestamp_delta_i;
      }
      if(value_delta_i > value_delta_max){
        value_delta_max = value_delta_i;
      }
      if(value_delta_i <value_delta_min){
        value_delta_min = value_delta_i;
      }

    }
    b.add(getBitWith(timestamp_delta_max-timestamp_delta_min));
    b.add(getBitWith(value_delta_max-value_delta_min));
    return b;
  }

//  public static int getJStar(ArrayList<ArrayList<Integer>> ts_block, int i_star, int block_size,
//                             ArrayList<Integer> raw_length, int index, ArrayList<Integer> theta){
//    int j_star_bit_width = 33;
//    int j_star = 0;
//    double theta0_r = theta.get(0);
//    double theta1_r = theta.get(1);
//    double theta0_v = theta.get(2);
//    double theta1_v = theta.get(3);
//    if(i_star == block_size - 1 || i_star == 0)
//      return 0;
//    int epsilon_r_i_star_plus_1 = (int) ((double)ts_block.get(i_star+1).get(0) - theta0_r -
//            theta1_r * (double) ts_block.get(i_star-1).get(0));
//    int epsilon_v_i_star_plus_1 = (int) ((double)ts_block.get(i_star+1).get(1) - theta0_v -
//            theta1_v * (double)ts_block.get(i_star-1).get(1));
//
//    if(epsilon_r_i_star_plus_1 > raw_length.get(1) || epsilon_v_i_star_plus_1 > raw_length.get(2))
//      return 0;
//
//
//    for(int j = 1;j<block_size;j++){
//      if(j!=i_star){
//        int epsilon_r_j = (int) ((double) ts_block.get(j).get(0) - theta0_r -theta1_r * (double) ts_block.get(i_star).get(0));
//        int epsilon_v_j = (int) ((double) ts_block.get(j).get(1) - theta0_v - theta1_v * (double) ts_block.get(i_star).get(1));
//        int epsilon_r_i_star = (int) ((double) ts_block.get(i_star).get(0) - theta0_r -theta1_r * (double) ts_block.get(j-1).get(0));
//        int epsilon_v_i_star = (int) ((double) ts_block.get(i_star).get(1) - theta0_r -theta1_r * (double) ts_block.get(j-1).get(1));
//        if(epsilon_r_j >raw_length.get(1) || epsilon_v_j >raw_length.get(2) ||
//                epsilon_r_i_star > raw_length.get(1) || epsilon_v_i_star> raw_length.get(2))
//          return 0;
//        int max_r = getBitWith(max3(epsilon_r_i_star_plus_1,epsilon_r_j,epsilon_r_i_star));
//        int max_v = getBitWith(max3(epsilon_v_i_star_plus_1,epsilon_v_j,epsilon_v_i_star));
//        if(index == 1){
//          if(max_v<=raw_length.get(2) && max_r < j_star_bit_width && max_r < raw_length.get(1)){
//            j_star_bit_width = max_r;
//            j_star = j;
//          }
//        }else{
//          if(max_v<raw_length.get(2) && max_v < j_star_bit_width && max_r <= raw_length.get(1)){
//            j_star_bit_width = max_v;
//            j_star = j;
//          }
//        }
//      }
//    }
//    return j_star;
//  }
  public static int getIStar(ArrayList<ArrayList<Integer>> ts_block, int block_size,
                           int index,ArrayList<Integer> theta){
  int timestamp_delta_max = Integer.MIN_VALUE;
  int value_delta_max = Integer.MIN_VALUE;
  int timestamp_delta_max_index = -1;
  int value_delta_max_index = -1;

  int i_star = 0;

  int theta0_t = theta.get(0);
  int theta1_t = theta.get(1);
  int theta0_v = theta.get(2);
  int theta1_v = theta.get(3);

  if(index==0){
    for(int j = 1;j<block_size;j++){
      int epsilon_v_j =ts_block.get(j).get(1) - ( theta0_v + theta1_v * ts_block.get(j-1).get(1));
      if(epsilon_v_j > value_delta_max){
        value_delta_max = epsilon_v_j;
        value_delta_max_index =j;

      }
    }
//      System.out.println(value_delta_max_index);
    i_star = value_delta_max_index;
  } else if (index==1) {
    for(int j = 1;j<block_size;j++){
      int epsilon_r_j = ts_block.get(j).get(0) -  ( theta0_t + theta1_t * ts_block.get(j-1).get(0));
      if(epsilon_r_j > timestamp_delta_max){
        timestamp_delta_max = epsilon_r_j;
        timestamp_delta_max_index = j;
      }
    }
    i_star = timestamp_delta_max_index;
  }

  return i_star;
}
  public static int getIStar(ArrayList<ArrayList<Integer>> ts_block, int block_size,
                             ArrayList<Integer> raw_length, ArrayList<Integer> theta){
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    int timestamp_delta_max = Integer.MIN_VALUE;
    int value_delta_max = Integer.MIN_VALUE;
    int timestamp_delta_max_index = -1;
    int value_delta_max_index = -1;

    int theta0_t = theta.get(0);
    int theta1_t = theta.get(1);
    int theta0_v = theta.get(2);
    int theta1_v = theta.get(3);

    int i_star_bit_width = 33;
    int i_star = 0;

    for(int j = 1;j<block_size;j++){
      int epsilon_r_j =  ts_block.get(j).get(0) - ( theta0_t + theta1_t * ts_block.get(j-1).get(0));
      int epsilon_v_j =  ts_block.get(j).get(1) - ( theta0_v + theta1_v *  ts_block.get(j-1).get(1));

      if(epsilon_r_j>timestamp_delta_max){
        timestamp_delta_max = epsilon_r_j;
        timestamp_delta_max_index = j;
      }
      if(epsilon_r_j<timestamp_delta_min){
        timestamp_delta_min = epsilon_r_j;
      }
      if(epsilon_v_j > value_delta_max){
        value_delta_max = epsilon_v_j;
        value_delta_max_index = j;
      }
      if(epsilon_v_j <value_delta_min){
        value_delta_min = epsilon_v_j;
      }

    }
    timestamp_delta_max -= timestamp_delta_min;
    value_delta_max -= value_delta_min;
    if(value_delta_max<=timestamp_delta_max)
      i_star = timestamp_delta_max_index;
    else
      i_star = value_delta_max_index;
    return i_star;
  }

//  public static int getIStar(ArrayList<ArrayList<Integer>> ts_block, int block_size,
//                             ArrayList<Integer> raw_length, int index, ArrayList<Integer> theta){
//    int i_star_bit_width = 33;
//    int i_star = 0;
//    double theta0_r = theta.get(0);
//    double theta1_r = theta.get(1);
//    double theta0_v = theta.get(2);
//    double theta1_v = theta.get(3);
//
//    for(int j = 1;j<block_size;j++){
//      int epsilon_r_j = getBitWith((int) ((double) ts_block.get(j).get(0) - theta0_r -theta1_r * (double) ts_block.get(j-1).get(0)));
//      int epsilon_v_j = getBitWith ((int) ((double) ts_block.get(j).get(1) - theta0_v - theta1_v * (double) ts_block.get(j-1).get(1)));
//      if(index == 1){
//        if(epsilon_v_j<=raw_length.get(2) && epsilon_r_j < i_star_bit_width && epsilon_r_j < raw_length.get(1)){
//          i_star_bit_width = epsilon_r_j;
//          i_star = j;
//        }
//      }else{
//        if(epsilon_v_j<raw_length.get(2) && epsilon_v_j < i_star_bit_width && epsilon_r_j <= raw_length.get(1)){
//          i_star_bit_width = epsilon_v_j;
//          i_star = j;
//        }
//      }
//    }
//    return i_star;
//  }

  public static ArrayList<Byte> encode2Bytes(ArrayList<ArrayList<Integer>> ts_block,
                                             ArrayList<Integer> raw_length,ArrayList<Integer> theta,ArrayList<Integer> result2){
    ArrayList<Byte> encoded_result = new ArrayList<>();
//    // encode block size (Integer)
//    byte[] block_size_byte = int2Bytes(ts_block.size());
//    for (byte b : block_size_byte) encoded_result.add(b);

//    // r0 of a block (Integer)
//    byte[] r0_byte = int2Bytes(raw_length.get(6));
//    for (byte b : r0_byte) encoded_result.add(b);
//    byte[] d0_byte = int2Bytes(raw_length.get(7));
//    for (byte b : d0_byte) encoded_result.add(b);

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
    byte[] theta0_r_byte = int2Bytes(theta.get(0)+raw_length.get(3));
    for (byte b : theta0_r_byte) encoded_result.add(b);
    byte[] theta1_r_byte = int2Bytes(theta.get(1));
    for (byte b : theta1_r_byte) encoded_result.add(b);
    byte[] theta0_v_byte = int2Bytes(theta.get(2)+raw_length.get(4));
    for (byte b : theta0_v_byte) encoded_result.add(b);
    byte[] theta1_v_byte = int2Bytes(theta.get(3));
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
    //byte[] max_bit_width_deviation_byte = int2Bytes(result2.get(1));
    //for (byte b: max_bit_width_deviation_byte) encoded_result.add(b);
    //byte[] deviation_list_bytes = bitPacking(deviation_list,result2.get(1));
    //for (byte b: deviation_list_bytes) encoded_result.add(b);

    byte[] td_common_byte = int2Bytes(result2.get(0));
    for (byte b: td_common_byte) encoded_result.add(b);

    return encoded_result;
  }
  //public static ArrayList<Byte> ReorderingRegressionEncoder(ArrayList<ArrayList<Integer>> data,int block_size,int td, ArrayList<Integer> flag){
  public static ArrayList<Byte> ReorderingRegressionEncoder(ArrayList<ArrayList<Integer>> data,int block_size,int td){
    block_size ++;
    ArrayList<Byte> encoded_result=new ArrayList<Byte>();
    int length_all = data.size();
    byte[] length_all_bytes = int2Bytes(length_all);
    for(byte b : length_all_bytes) encoded_result.add(b);
    int block_num = length_all/block_size;

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

      ArrayList<Integer> result2 = new ArrayList<>();
      splitTimeStamp3(ts_block,td,result2);

      quickSort(ts_block,0,0,block_size-1);


      // time-order
      ArrayList<Integer> raw_length = new ArrayList<>(); // length,max_bit_width_interval,max_bit_width_value,max_bit_width_deviation
      ArrayList<Integer> i_star_ready = new ArrayList<>();
      ArrayList<Integer> theta = new ArrayList<>();
      ArrayList<ArrayList<Integer>> ts_block_delta = getEncodeBitsRegression( ts_block,  block_size, raw_length,
              i_star_ready,theta);

      // value-order
      quickSort(ts_block,1,0,block_size-1);
      ArrayList<Integer> reorder_length = new ArrayList<>();
      ArrayList<Integer> i_star_ready_reorder = new ArrayList<>();
      ArrayList<Integer> theta_reorder = new ArrayList<>();
      ArrayList<ArrayList<Integer>> ts_block_delta_reorder = getEncodeBitsRegression( ts_block,  block_size, reorder_length,
              i_star_ready_reorder,theta_reorder);
//      System.out.println(raw_length);
//      System.out.println(reorder_length);

      int i_star;
      int j_star;
      if(raw_length.get(0)<=reorder_length.get(0)){
        quickSort(ts_block,0,0,block_size-1);
        count_raw ++;
        i_star =getIStar(ts_block,block_size,0,theta);
//        System.out.println("count_raw");
//        System.out.println(theta);
//        System.out.println(ts_block);
      }
      else{
        raw_length = reorder_length;
        theta = theta_reorder;
        quickSort(ts_block,1,0,block_size-1);
        count_reorder ++;
        i_star =getIStar(ts_block,block_size,1,theta);
//        System.out.println("count_reorder");
//        System.out.println(theta_reorder);
//        System.out.println(ts_block);
      }
//      System.out.println("--------------------------------------------------------------");
      j_star =getJStar(ts_block,i_star,block_size,raw_length,0,theta);
//      System.out.println(raw_length);
//      System.out.println(i_star);
//      System.out.println(j_star);

      int adjust_count = 0;
      while(j_star!=-1 && i_star !=-1){
        if(adjust_count < block_size/2 && adjust_count <= 33){
          adjust_count ++;
        }else {
          break;
        }

        ArrayList<ArrayList<Integer>> old_ts_block = (ArrayList<ArrayList<Integer>>) ts_block.clone();
        ArrayList<Integer> old_length = (ArrayList<Integer>) raw_length.clone();

        ArrayList<Integer> tmp_tv = ts_block.get(i_star);
        if(j_star<i_star){
          for(int u=i_star-1;u>=j_star;u--){
            ArrayList<Integer> tmp_tv_cur = new ArrayList<>();
            tmp_tv_cur.add(ts_block.get(u).get(0));
            tmp_tv_cur.add(ts_block.get(u).get(1));
            ts_block.set(u+1,tmp_tv_cur);
          }
        }else{
          for(int u=i_star+1;u<j_star;u++){
            ArrayList<Integer> tmp_tv_cur = new ArrayList<>();
            tmp_tv_cur.add(ts_block.get(u).get(0));
            tmp_tv_cur.add(ts_block.get(u).get(1));
            ts_block.set(u-1,tmp_tv_cur);
          }
          j_star --;
        }
        ts_block.set(j_star,tmp_tv);

        getEncodeBitsRegression(ts_block,  block_size, raw_length, i_star_ready_reorder,theta);

        if(old_length.get(1)+old_length.get(2) < raw_length.get(1)+raw_length.get(2)){
          ts_block = old_ts_block;
          break;
        }

//        System.out.println(raw_length);
//        System.out.println("--------------------------------------------------------------");
        i_star =getIStar(ts_block,block_size,raw_length,theta);
        if(i_star == j_star) break;
        j_star =getJStar(ts_block,i_star,block_size,raw_length,0,theta);
//        System.out.println(i_star);
//        System.out.println(j_star);
        //flag.set(2,flag.get(2)+1);
      }

      ts_block_delta = getEncodeBitsRegression(ts_block, block_size, raw_length, i_star_ready_reorder,theta);
      ArrayList<Byte> cur_encoded_result = encode2Bytes(ts_block_delta,raw_length,theta,result2);
      encoded_result.addAll(cur_encoded_result);
    }

    int remaining_length = length_all - block_num*block_size;
    if(remaining_length==1){
      byte[] timestamp_end_bytes = int2Bytes(data.get(data.size()-1).get(0));
      for(byte b : timestamp_end_bytes) encoded_result.add(b);
      byte[] value_end_bytes = int2Bytes(data.get(data.size()-1).get(1));
      for(byte b : value_end_bytes) encoded_result.add(b);
    }
    if(remaining_length!=0 && remaining_length!=1){
      ArrayList<ArrayList<Integer>> ts_block = new ArrayList<>();
      ArrayList<ArrayList<Integer>> ts_block_reorder = new ArrayList<>();

      for(int j=block_num*block_size;j<length_all;j++){
        ts_block.add(data.get(j));
        ts_block_reorder.add(data.get(j));
      }
      ArrayList<Integer> result2 = new ArrayList<>();
      splitTimeStamp3(ts_block,td,result2);

      quickSort(ts_block,0,0,remaining_length-1);


      // time-order
      ArrayList<Integer> raw_length = new ArrayList<>(); // length,max_bit_width_interval,max_bit_width_value,max_bit_width_deviation
      ArrayList<Integer> i_star_ready = new ArrayList<>();
      ArrayList<Integer> theta = new ArrayList<>();
      ArrayList<ArrayList<Integer>> ts_block_delta = getEncodeBitsRegression( ts_block,  remaining_length, raw_length,
              i_star_ready,theta);

      // value-order
      quickSort(ts_block,1,0,remaining_length-1);
      ArrayList<Integer> reorder_length = new ArrayList<>();
      ArrayList<Integer> i_star_ready_reorder = new ArrayList<>();
      ArrayList<Integer> theta_reorder = new ArrayList<>();
      ArrayList<ArrayList<Integer>> ts_block_delta_reorder = getEncodeBitsRegression( ts_block,  remaining_length, reorder_length,
              i_star_ready_reorder,theta_reorder);

      if(raw_length.get(0)<=reorder_length.get(0)){
        quickSort(ts_block,0,0,remaining_length-1);
        count_raw ++;
      }
      else{
        raw_length = reorder_length;
        theta = theta_reorder;
        quickSort(ts_block,1,0,remaining_length-1);
        count_reorder ++;
      }
      ts_block_delta = getEncodeBitsRegression(ts_block, remaining_length, raw_length, i_star_ready_reorder,theta);

      int supple_length;
      if(remaining_length % 8 == 0){
        supple_length = 1;
      }
      else if (remaining_length % 8 == 1){
        supple_length = 0;
      }
      else{
        supple_length = 9 - remaining_length % 8;
      }
      for(int s = 0;s<supple_length;s++) {
        ArrayList<Integer> tmp = new ArrayList<>();
        tmp.add(0);
        tmp.add(0);
        ts_block_delta.add(tmp);
      }
//      byte[] remaining_length_bytes = int2Bytes(remaining_length);
//      for(byte b:remaining_length_bytes)   encoded_result.add(b);
      ArrayList<Byte> cur_encoded_result = encode2Bytes(ts_block_delta,raw_length,theta,result2);
      encoded_result.addAll(cur_encoded_result);
    }
    return encoded_result;
  }


//  public static ArrayList<Byte> ReorderingRegressionEncoder(ArrayList<ArrayList<Integer>> data,int block_size,int td){
//    block_size ++;
//    int length_all = data.size();
//    int block_num = length_all/block_size;
//    ArrayList<Byte> encoded_result=new ArrayList<Byte>();
//    // encode block size (Integer)
//    byte[] block_size_byte = int2Bytes(block_size);
//    for (byte b : block_size_byte) encoded_result.add(b);
//
//    int count_raw = 0;
//    int count_reorder = 0;
////    for(int i=0;i<1;i++){
//    for(int i=0;i<block_num;i++){
//      ArrayList<ArrayList<Integer>> ts_block = new ArrayList<>();
//      ArrayList<ArrayList<Integer>> ts_block_reorder = new ArrayList<>();
//      for(int j=0;j<block_size;j++){
//        ts_block.add(data.get(j+i*block_size));
//        ts_block_reorder.add(data.get(j+i*block_size));
//      }
//
//      //ArrayList<Integer> deviation_list = new ArrayList<>();
//      ArrayList<Integer> result2 = new ArrayList<>();
//      splitTimeStamp3(ts_block,td,result2);
//
//      quickSort(ts_block,0,0,block_size-1);
//
//      //ts_block order by interval
//
//      // time-order
//      ArrayList<Integer> raw_length = new ArrayList<>(); // length,max_bit_width_interval,max_bit_width_value,max_bit_width_deviation
//      ArrayList<Integer> i_star_ready = new ArrayList<>();
//      ArrayList<Integer> theta = new ArrayList<>();
//      ArrayList<ArrayList<Integer>> ts_block_delta = getEncodeBitsRegression( ts_block,  block_size, raw_length,
//              i_star_ready,theta);
//
//
//      // value-order
//      quickSort(ts_block,1,0,block_size-1);
//      ArrayList<Integer> reorder_length = new ArrayList<>();
//      ArrayList<Integer> i_star_ready_reorder = new ArrayList<>();
//      ArrayList<Integer> theta_reorder = new ArrayList<>();
//      ArrayList<ArrayList<Integer>> ts_block_delta_reorder = getEncodeBitsRegression( ts_block,  block_size, reorder_length,
//              i_star_ready_reorder,theta_reorder);
//
//      if(raw_length.get(0)<=reorder_length.get(0)){
//        //flag.set(0,flag.get(0)+1);
//        quickSort(ts_block,0,0,block_size-1);
////        System.out.println(ts_block);
//        int i_star = i_star_ready.get(1);
//        int j_star = 0;
//        count_raw ++;
////        i_star =getIStar(ts_block,block_size,raw_length,0,theta);
//        j_star =getJStar(ts_block,i_star,block_size,raw_length,0,theta);
//
//        while(j_star!=0){
//          ArrayList<Integer> tmp_tv = ts_block_reorder.get(i_star);
//          if(j_star<i_star){
//            for(int u=i_star-1;u>=j_star;u--){
//              ArrayList<Integer> tmp_tv_cur = new ArrayList<>();
//              tmp_tv_cur.add(ts_block_reorder.get(u).get(0));
//              tmp_tv_cur.add(ts_block_reorder.get(u).get(1));
//              ts_block.set(u+1,tmp_tv_cur);
//            }
//          }else{
//            for(int u=i_star+1;u<=j_star;u++){
//              ArrayList<Integer> tmp_tv_cur = new ArrayList<>();
//              tmp_tv_cur.add(ts_block_reorder.get(u).get(0));
//              tmp_tv_cur.add(ts_block_reorder.get(u).get(1));
//              ts_block.set(u-1,tmp_tv_cur);
//            }
//          }
//          ts_block.set(j_star,tmp_tv);
//          i_star =getIStar(ts_block,block_size,raw_length,0,theta);
//          j_star =getJStar(ts_block,i_star,block_size,raw_length,0,theta);
//          System.out.println("adjust");
//          //flag.set(2,flag.get(2)+1);
//        }
//
//        ts_block_delta = getEncodeBitsRegression( ts_block,  block_size,raw_length,
//                i_star_ready_reorder,theta);
//        ArrayList<Byte> cur_encoded_result = encode2Bytes(ts_block_delta,raw_length,theta,result2);
//        encoded_result.addAll(cur_encoded_result);
//
//      }
//      else{
//        //flag.set(1,flag.get(1)+1);
//        // adjust to reduce max_bit_width_r
////        System.out.println(ts_block);
//        int i_star = i_star_ready_reorder.get(0);
//        int j_star = 0;
//        ArrayList<Integer> j_star_list =new ArrayList<>();
//        count_reorder ++;
//        i_star =getIStar(ts_block,block_size,raw_length,0,theta);
//        j_star =getJStar(ts_block,i_star,block_size,raw_length,0,theta);
//        while(j_star != 0){
//          ArrayList<Integer> tmp_tv = ts_block_reorder.get(i_star);
//          if(j_star<i_star){
//            for(int u=i_star-1;u>=j_star;u--){
//              ArrayList<Integer> tmp_tv_cur = new ArrayList<>();
//              tmp_tv_cur.add(ts_block_reorder.get(u).get(0));
//              tmp_tv_cur.add(ts_block_reorder.get(u).get(1));
//              ts_block.set(u+1,tmp_tv_cur);
//            }
//          }else{
//            for(int u=i_star+1;u<=j_star;u++){
//              ArrayList<Integer> tmp_tv_cur = new ArrayList<>();
//              tmp_tv_cur.add(ts_block_reorder.get(u).get(0));
//              tmp_tv_cur.add(ts_block_reorder.get(u).get(1));
//              ts_block.set(u-1,tmp_tv_cur);
//            }
//          }
//          System.out.println("adjust");
//          //flag.set(2,flag.get(2)+1);
//          ts_block.set(j_star,tmp_tv);
//          i_star =getIStar(ts_block,block_size,reorder_length,0,theta);
//          j_star =getJStar(ts_block,i_star,block_size,reorder_length,0,theta);
//        }
//
//        ts_block_delta_reorder = getEncodeBitsRegression( ts_block,  block_size,reorder_length,
//                i_star_ready_reorder,theta_reorder);
//        ArrayList<Byte> cur_encoded_result = encode2Bytes(ts_block_delta_reorder,reorder_length,theta_reorder, result2);
//        encoded_result.addAll(cur_encoded_result);
//      }
//    }
////    System.out.println(count_raw);
////    System.out.println(count_reorder);
//    return encoded_result;
//  }

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

  public static byte[] float2byte2(float f) {
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

  public static float byte2float2(ArrayList<Byte> b, int index) {
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

  public static int reverseZigzag(int num) {
    int value;
    if(num%2==0){
      value = num / 2;
    }
    else{
      value = -(num + 1) / 2;
    }
    return value;
  }

  public static ArrayList<ArrayList<Integer>> ReorderingRegressionDecoder(ArrayList<Byte> encoded,int td){
    ArrayList<ArrayList<Integer>> data = new ArrayList<>();
    int decode_pos = 0;
    int length_all = bytes2Integer(encoded, decode_pos, 4);
    decode_pos += 4;
    int block_size = bytes2Integer(encoded, decode_pos, 4);
    decode_pos += 4;

    int block_num = length_all / block_size;
    int remain_length = length_all - block_num * block_size;
    int zero_number;
    if(remain_length % 8 == 0){
      zero_number = 1;
    }
    else if (remain_length % 8 == 1){
      zero_number = 0;
    }
    else{
      zero_number = 9 - remain_length % 8;
    }

    //while(decode_pos < encoded.size()) {
    for(int k = 0; k < block_num; k++){
      ArrayList<Integer> time_list = new ArrayList<>();
      ArrayList<Integer> value_list = new ArrayList<>();
      //ArrayList<Integer> deviation_list = new ArrayList<>();

      ArrayList<ArrayList<Integer>> ts_block = new ArrayList<>();

      int time0 = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;
      int value0 = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;

      int theta0_r = bytes2Integer(encoded, decode_pos,4);
      decode_pos += 4;
      int theta1_r = bytes2Integer(encoded, decode_pos,4);
      decode_pos += 4;
      int theta0_v = bytes2Integer(encoded, decode_pos,4);
      decode_pos += 4;
      int theta1_v = bytes2Integer(encoded, decode_pos,4);
      decode_pos += 4;

      int max_bit_width_time = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;
      time_list = decodebitPacking(encoded,decode_pos,max_bit_width_time,0,block_size);
      decode_pos += max_bit_width_time * (block_size - 1) / 8;

      int max_bit_width_value = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;
      value_list = decodebitPacking(encoded,decode_pos,max_bit_width_value,0,block_size);
      decode_pos += max_bit_width_value * (block_size - 1) / 8;

      //int max_bit_width_deviation = bytes2Integer(encoded, decode_pos, 4);
      //decode_pos += 4;
      //deviation_list = decodebitPacking(encoded,decode_pos,max_bit_width_deviation,0,block_size);
      //decode_pos += max_bit_width_deviation * (block_size - 1) / 8;

      int td_common = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;

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
      for (int i = 0; i < block_size-1; i++) {
        int ti = (int) (theta1_r * ti_pre + theta0_r + time_list.get(i));
        time_list.set(i,ti);
        ti_pre = ti;

        int vi = (int) (theta1_v * vi_pre + theta0_v + value_list.get(i));
        value_list.set(i,vi);
        vi_pre = vi;

        //ArrayList<Integer> ts_block_tmp = new ArrayList<>();
        //ts_block_tmp.add(time_list.get(i));
        //ts_block_tmp.add(value_list.get(i));
        //ts_block.add(ts_block_tmp);
      }

      ArrayList<Integer> ts_block_tmp0 = new ArrayList<>();
      ts_block_tmp0.add(time0);
      ts_block_tmp0.add(value0);
      ts_block.add(ts_block_tmp0);
      for (int i=0;i<block_size-1;i++){
        //int ti = (time_list.get(i) - time0) * td_common  + time0 + reverseZigzag(deviation_list.get(i));
        int ti = (time_list.get(i) - time0) * td_common  + time0;
        ArrayList<Integer> ts_block_tmp = new ArrayList<>();
        ts_block_tmp.add(ti);
        ts_block_tmp.add(value_list.get(i));
        ts_block.add(ts_block_tmp);
      }

      quickSort(ts_block, 0, 0, block_size-1);
      data.addAll(ts_block);

    }

    if(remain_length == 1){
      int timestamp_end = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;
      int value_end = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;
      ArrayList<Integer> ts_block_end = new ArrayList<>();
      ts_block_end.add(timestamp_end);
      ts_block_end.add(value_end);
      data.add(ts_block_end);
    }
    if(remain_length != 0 && remain_length != 1){
      ArrayList<Integer> time_list = new ArrayList<>();
      ArrayList<Integer> value_list = new ArrayList<>();

      ArrayList<ArrayList<Integer>> ts_block = new ArrayList<>();

      int time0 = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;
      int value0 = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;

      int theta0_r = bytes2Integer(encoded, decode_pos,4);
      decode_pos += 4;
      int theta1_r = bytes2Integer(encoded, decode_pos,4);
      decode_pos += 4;
      int theta0_v = bytes2Integer(encoded, decode_pos,4);
      decode_pos += 4;
      int theta1_v = bytes2Integer(encoded, decode_pos,4);
      decode_pos += 4;

      int max_bit_width_time = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;
      time_list = decodebitPacking(encoded,decode_pos,max_bit_width_time,0,remain_length+zero_number);
      decode_pos += max_bit_width_time * (remain_length+zero_number - 1) / 8;

      int max_bit_width_value = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;
      value_list = decodebitPacking(encoded,decode_pos,max_bit_width_value,0,remain_length+zero_number);
      decode_pos += max_bit_width_value * (remain_length+zero_number - 1) / 8;

      int td_common = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;

      int ti_pre = time0;
      int vi_pre = value0;
      for (int i = 0; i < remain_length+zero_number-1; i++) {
        int ti = (int) (theta1_r * ti_pre + theta0_r + time_list.get(i));
        time_list.set(i,ti);
        ti_pre = ti;

        int vi = (int) (theta1_v * vi_pre + theta0_v + value_list.get(i));
        value_list.set(i,vi);
        vi_pre = vi;
      }

      ArrayList<Integer> ts_block_tmp0 = new ArrayList<>();
      ts_block_tmp0.add(time0);
      ts_block_tmp0.add(value0);
      ts_block.add(ts_block_tmp0);
      for (int i=0;i<remain_length+zero_number-1;i++){
        int ti = (time_list.get(i) - time0) * td_common  + time0;
        ArrayList<Integer> ts_block_tmp = new ArrayList<>();
        ts_block_tmp.add(ti);
        ts_block_tmp.add(value_list.get(i));
        ts_block.add(ts_block_tmp);
      }

      quickSort(ts_block, 0, 0, remain_length+zero_number-1);
      //data.addAll(ts_block);

      for(int i = zero_number; i < remain_length+zero_number; i++){
        data.add(ts_block.get(i));
      }
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
//    input_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test\\Metro-Traffic");
//    output_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\result_evaluation" +
//            "\\vary_parameter\\rr_int_ratio\\Metro-Traffic_ratio.csv");
//    dataset_map_td.add(3600);
//    input_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test\\Nifty-Stocks");
//    output_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\result_evaluation" +
//            "\\vary_parameter\\rr_int_ratio\\Nifty-Stocks_ratio.csv");
//    dataset_map_td.add(86400);
//    input_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test\\USGS-Earthquakes");
//    output_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\result_evaluation" +
//            "\\vary_parameter\\rr_int_ratio\\USGS-Earthquakes_ratio.csv");
//    dataset_map_td.add(50);
//    input_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test\\Cyber-Vehicle");
//    output_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\result_evaluation" +
//            "\\vary_parameter\\rr_int_ratio\\Cyber-Vehicle_ratio.csv");
//    dataset_map_td.add(10);
//    input_path_list.add( "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test\\TH-Climate");
//    output_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\result_evaluation" +
//            "\\vary_parameter\\rr_int_ratio\\TH-Climate_ratio.csv");
//    dataset_map_td.add(3);
//    input_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test\\TY-Transport");
//    output_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\result_evaluation" +
//            "\\vary_parameter\\rr_int_ratio\\TY-Transport_ratio.csv");
//    dataset_map_td.add(5);
//    input_path_list.add( "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test\\TY-Fuel");
//    output_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\result_evaluation" +
//            "\\vary_parameter\\rr_int_ratio\\TY-Fuel_ratio.csv");
//    dataset_map_td.add(60);
//    input_path_list.add( "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test\\GW-Magnetic");
//    output_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\result_evaluation" +
//            "\\vary_parameter\\rr_int_ratio\\GW-Magnetic_ratio.csv");
//    dataset_map_td.add(100);

    input_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\iotdb_test\\Metro-Traffic");
    output_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\result_evaluation" +
            "\\vary_parameter\\rr_int_ratio\\Metro-Traffic_ratio.csv");
    dataset_map_td.add(3600);
    input_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\iotdb_test\\Nifty-Stocks");
    output_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\result_evaluation" +
            "\\vary_parameter\\rr_int_ratio\\Nifty-Stocks_ratio.csv");
    dataset_map_td.add(86400);
    input_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\iotdb_test\\USGS-Earthquakes");
    output_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\result_evaluation" +
            "\\vary_parameter\\rr_int_ratio\\USGS-Earthquakes_ratio.csv");
    dataset_map_td.add(50);
    input_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\iotdb_test\\Cyber-Vehicle");
    output_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\result_evaluation" +
            "\\vary_parameter\\rr_int_ratio\\Cyber-Vehicle_ratio.csv");
    dataset_map_td.add(10);
    input_path_list.add( "E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\iotdb_test\\TH-Climate");
    output_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\result_evaluation" +
            "\\vary_parameter\\rr_int_ratio\\TH-Climate_ratio.csv");
    dataset_map_td.add(3);
    input_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\iotdb_test\\TY-Transport");
    output_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\result_evaluation" +
            "\\vary_parameter\\rr_int_ratio\\TY-Transport_ratio.csv");
    dataset_map_td.add(5);
    input_path_list.add( "E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\iotdb_test\\TY-Fuel");
    output_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\result_evaluation" +
            "\\vary_parameter\\rr_int_ratio\\TY-Fuel_ratio.csv");
    dataset_map_td.add(60);
    input_path_list.add( "E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\iotdb_test\\GW-Magnetic");
    output_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\result_evaluation" +
            "\\vary_parameter\\rr_int_ratio\\GW-Magnetic_ratio.csv");
    dataset_map_td.add(100);

    for(int file_i=0;file_i<input_path_list.size();file_i++){

      String inputPath = input_path_list.get(file_i);
      String Output =output_path_list.get(file_i);
      System.out.println(inputPath);

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
              "Block Size",
              "Compression Ratio"
      };
      writer.writeRecord(head); // write header to output file

      assert tempList != null;
      for(int block_size_exp=4;block_size_exp<12;block_size_exp++) {
        int block_size = (int) Math.pow(2, block_size_exp);
        System.out.println(block_size);
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
            ArrayList<Byte> buffer = ReorderingRegressionEncoder(data, block_size,dataset_map_td.get(file_i));
            long e = System.nanoTime();
            encodeTime += (e - s);
            compressed_size += buffer.size();
            double ratioTmp =
                    (double) buffer.size() / (double) (data.size() * Integer.BYTES * 2);
            ratio += ratioTmp;
            s = System.nanoTime();
            data_decoded = ReorderingRegressionDecoder(buffer,dataset_map_td.get(file_i));
            e = System.nanoTime();
            decodeTime += (e - s);
          }


          ratio /= repeatTime;
          compressed_size /= repeatTime;
          encodeTime /= repeatTime;
          decodeTime /= repeatTime;

          String[] record = {
                  f.toString(),
                  "REGER-32-INT",
                  String.valueOf(encodeTime),
                  String.valueOf(decodeTime),
                  String.valueOf(data.size()),
                  String.valueOf(compressed_size),
                  String.valueOf(block_size_exp),
                  String.valueOf(ratio)
          };
          writer.writeRecord(record);
        }
      }
      writer.close();

    }
  }
}
