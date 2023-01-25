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
import static org.codehaus.groovy.runtime.DefaultGroovyMethods.sort;

public class ReorderingEncodeTestS3Adjust2H {

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
  public static byte[] bitPacking(ArrayList<Integer> numbers,int bit_width){
    int block_num = numbers.size()/8;
//    System.out.println(bit_width);
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
      quickSort22(ts_block, low, l - 1);
    }
    if (r < high) {
      quickSort22(ts_block,r + 1, high);
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

  public static void splitTimeStamp3(ArrayList<ArrayList<Integer>> ts_block, int td,
                                     ArrayList<Integer> result){
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

    td = td_common;

    int t0 = ts_block.get(0).get(0);
    for(int i=0;i<ts_block.size();i++){
      ArrayList<Integer> tmp = new ArrayList<>();
      int interval_i = (ts_block.get(i).get(0) - t0) / td;
      //int deviation_i = ts_block.get(i).get(0) - t0 - interval_i * td;
      tmp.add(t0+interval_i);
      tmp.add(ts_block.get(i).get(1));
      ts_block.set(i,tmp);

      //deviation_list.add(zigzag(deviation_i));
      //if(zigzag(deviation_i)>max_deviation){
      //  max_deviation = zigzag(deviation_i);
      //}
    }
//    ArrayList<Integer> tmp = new ArrayList<>();
//    //int deviation_i = ts_block.get(i).get(0) - t0 - interval_i * td;
//    tmp.add(0);
//    tmp.add(ts_block.get(0).get(1));
//    ts_block.set(0,tmp);

//    System.out.println(ts_block);
    //int max_bit_width_deviation = getBitWith(max_deviation);
    //result.add(max_bit_width_deviation);
    result.add(td_common);
  }

  public static ArrayList<ArrayList<Integer>> getEncodeBitsDelta(ArrayList<ArrayList<Integer>> ts_block, int block_size,
                                                      ArrayList<Integer> result, ArrayList<Integer> i_star){
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    ArrayList<ArrayList<Integer>> ts_block_delta = new ArrayList<>();

    ArrayList<Integer> tmp0 = new ArrayList<>();
    tmp0.add(ts_block.get(0).get(0));
    tmp0.add(ts_block.get(0).get(1));
    ts_block_delta.add(tmp0);

    // delta to Delta
    for(int j=1;j<block_size;j++) {
      int delta_r = ts_block.get(j).get(0) - ts_block.get(j-1).get(0);
      int delta_v = ts_block.get(j).get(1) - ts_block.get(j-1).get(1);
      ArrayList<Integer> tmp = new ArrayList<>();
      tmp.add(delta_r);
      tmp.add(delta_v);
      ts_block_delta.add(tmp);
//      if(j==179){
//        System.out.println(ts_block.get(j).get(0));
//        System.out.println(ts_block.get(j-1).get(0));
//      }
      if(delta_r <timestamp_delta_min){
        timestamp_delta_min = delta_r;
      }
      if(delta_v < value_delta_min){
        value_delta_min = delta_v;
      }
    }
    int max_timestamp = Integer.MIN_VALUE;
    int max_timestamp_i = -1;
    int max_value = Integer.MIN_VALUE;
    int max_value_i = -1;
    for(int j=block_size-1;j>0;j--) {
      int ts_2diff_ts = ts_block_delta.get(j).get(0) - timestamp_delta_min;
      int ts_2diff_value = ts_block_delta.get(j).get(1) - value_delta_min;
      ArrayList<Integer> tmp = new ArrayList<>();
      tmp.add(ts_2diff_ts);
      tmp.add(ts_2diff_value);
      ts_block_delta.set(j,tmp);
      if(ts_2diff_ts>max_timestamp){
        max_timestamp = ts_2diff_ts;
        max_timestamp_i = j;
      }
      if(ts_2diff_value>max_value){
        max_value = ts_2diff_value;
        max_value_i = j;
      }
    }
    int max_bit_width_interval = getBitWith(max_timestamp);
    int max_bit_width_value = getBitWith(max_value);
    //System.out.println(max_timestamp);
    //System.out.println(max_bit_width_interval);
//    System.out.println(max_value);
//    System.out.println(max_bit_width_value);

    // calculate error
    int  length = (max_bit_width_interval+max_bit_width_value)*(block_size-1);
    result.clear();
    result.add(length);
    result.add(max_bit_width_interval);
    result.add(max_bit_width_value);

    result.add(timestamp_delta_min);
    result.add(value_delta_min);

    i_star.add(max_timestamp_i);
    i_star.add(max_value_i);

//    System.out.println(ts_block_delta);
    return ts_block_delta;
  }
  public static int getJStar(ArrayList<ArrayList<Integer>> ts_block, int alpha, int block_size,
                             ArrayList<Integer> raw_length, int index){
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    int raw_timestamp_delta_max = Integer.MIN_VALUE;
    int raw_value_delta_max = Integer.MIN_VALUE;
    int raw_timestamp_delta_max_index = -1;
    int raw_value_delta_max_index = -1;
    int raw_bit_width_timestamp = 0;
    int raw_bit_width_value = 0;
    int j_star_bit_width = 33;
    ArrayList<Integer> j_star_list = new ArrayList<>(); // beta list of min b phi alpha to j
    int j_star = -1;

    for(int i = 1;i<block_size;i++){
      int delta_t_i = ts_block.get(i).get(0) - ts_block.get(i-1).get(0);
      int delta_v_i = ts_block.get(i).get(1) - ts_block.get(i-1).get(1);
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

    // alpha == 1
    if(alpha==0){
      for(int j = 2;j<block_size;j++){
        ArrayList<Integer> b = adjust0(ts_block,alpha,j);
        if((b.get(0) + b.get(1)) < (raw_bit_width_timestamp+raw_bit_width_value) ){
          raw_bit_width_timestamp = b.get(0);
          raw_bit_width_value = b.get(1);
          j_star_list.clear();
          j_star_list.add(j);
//          System.out.println("j_star_list");
        }else if ((b.get(0) + b.get(1)) == (raw_bit_width_timestamp+raw_bit_width_value)){
          j_star_list.add(j);
        }
      }
      ArrayList<Integer> b = adjust0n1(ts_block);
      if((b.get(0) + b.get(1)) < (raw_bit_width_timestamp+raw_bit_width_value) ){
        raw_bit_width_timestamp = b.get(0);
        raw_bit_width_value = b.get(1);
        j_star_list.clear();
        j_star_list.add(block_size);
//        System.out.println("j_star_list");
      }
      else if ((b.get(0) + b.get(1)) == (raw_bit_width_timestamp+raw_bit_width_value)){
        j_star_list.add(block_size);
      }

    } // alpha == n
    else if(alpha == block_size-1){
      for(int j = 1;j<block_size-1;j++){
        ArrayList<Integer> b = adjustn(ts_block,alpha,j);
        if((b.get(0) + b.get(1)) < (raw_bit_width_timestamp+raw_bit_width_value) ){
          raw_bit_width_timestamp = b.get(0);
          raw_bit_width_value = b.get(1);
          j_star_list.clear();
          j_star_list.add(j);
//          System.out.println("j_star_list");
        }
        else if ((b.get(0) + b.get(1)) == (raw_bit_width_timestamp+raw_bit_width_value)){
          j_star_list.add(j);
        }
      }
      ArrayList<Integer> b = adjustn0(ts_block);
      if((b.get(0) + b.get(1)) < (raw_bit_width_timestamp+raw_bit_width_value) ){
        raw_bit_width_timestamp = b.get(0);
        raw_bit_width_value = b.get(1);
        j_star_list.clear();
        j_star_list.add(0);
//        System.out.println("j_star_list");
      }
      else if ((b.get(0) + b.get(1)) == (raw_bit_width_timestamp+raw_bit_width_value)){
        j_star_list.add(0);
      }
    } // alpha != 1 and alpha != n
    else {
      for(int j = 1;j<block_size;j++){
        if(alpha != j && (alpha+1) !=j){
          ArrayList<Integer> b = adjustAlphaToJ(ts_block,alpha,j);
          if((b.get(0) + b.get(1)) < (raw_bit_width_timestamp+raw_bit_width_value) ){
//            System.out.println(raw_bit_width_timestamp);
//            System.out.println(b.get(0));
//            System.out.println(raw_bit_width_value);
//            System.out.println(b.get(1));
            raw_bit_width_timestamp = b.get(0);
            raw_bit_width_value = b.get(1);
            j_star_list.clear();
            j_star_list.add(j);
//            System.out.println("j_star_list");
          }else if ((b.get(0) + b.get(1)) == (raw_bit_width_timestamp+raw_bit_width_value)){
            j_star_list.add(j);
          }
        }
      }
      ArrayList<Integer> b = adjustTo0(ts_block,alpha);
      if((b.get(0) + b.get(1)) < (raw_bit_width_timestamp+raw_bit_width_value) ){
        raw_bit_width_timestamp = b.get(0);
        raw_bit_width_value = b.get(1);
        j_star_list.clear();
        j_star_list.add(0);
//        System.out.println("j_star_list");
      }
      else if ((b.get(0) + b.get(1)) == (raw_bit_width_timestamp+raw_bit_width_value)){
        j_star_list.add(0);
      }
      b = adjustTon(ts_block,alpha);
      if((b.get(0) + b.get(1)) < (raw_bit_width_timestamp+raw_bit_width_value) ){
        raw_bit_width_timestamp = b.get(0);
        raw_bit_width_value = b.get(1);
        j_star_list.clear();
        j_star_list.add(0);
//        System.out.println("j_star_list");
      }
      else if ((b.get(0) + b.get(1)) == (raw_bit_width_timestamp+raw_bit_width_value)){
        j_star_list.add(0);
      }
    }

    if(j_star_list.size() == 0){
    }else if(j_star_list.size() == 1){
      j_star = j_star_list.get(0);
    }else{
      j_star = getIstarClose(alpha,j_star_list);
    }
//    System.out.println(j_star);
    return j_star;
  }

  private static ArrayList<Integer> adjustTo0(ArrayList<ArrayList<Integer>> ts_block, int alpha) {
    int block_size = ts_block.size();
    assert alpha != block_size-1;
    assert alpha != 0;
    ArrayList<Integer> b = new ArrayList<>();
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    int timestamp_delta_max = Integer.MIN_VALUE;
    int value_delta_max = Integer.MIN_VALUE;
    for(int i=1;i<block_size;i++){
      int timestamp_delta_i;
      int value_delta_i;
      if( i == (alpha+1)){
        timestamp_delta_i = ts_block.get(alpha+1).get(0) - ts_block.get(alpha-1).get(0);
        value_delta_i = ts_block.get(alpha+1).get(1) - ts_block.get(alpha-1).get(1);
      } else if (i == alpha){
        timestamp_delta_i = ts_block.get(0).get(0) - ts_block.get(alpha).get(0);
        value_delta_i = ts_block.get(0).get(1) - ts_block.get(alpha).get(1);
      }
      else{
        timestamp_delta_i = ts_block.get(i).get(0) - ts_block.get(i - 1).get(0);
        value_delta_i = ts_block.get(i).get(1) - ts_block.get(i - 1).get(1);
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
  private static ArrayList<Integer> adjustTon(ArrayList<ArrayList<Integer>> ts_block, int alpha) {
    int block_size = ts_block.size();
    assert alpha != block_size-1;
    assert alpha != 0;
    ArrayList<Integer> b = new ArrayList<>();
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    int timestamp_delta_max = Integer.MIN_VALUE;
    int value_delta_max = Integer.MIN_VALUE;
    for(int i=1;i<block_size;i++){
      int timestamp_delta_i;
      int value_delta_i;
      if( i == (alpha+1)){
        timestamp_delta_i = ts_block.get(alpha+1).get(0) - ts_block.get(alpha-1).get(0);
        value_delta_i = ts_block.get(alpha+1).get(1) - ts_block.get(alpha-1).get(1);
      } else if (i == alpha){
        timestamp_delta_i = ts_block.get(alpha).get(0) - ts_block.get(block_size-1).get(0);
        value_delta_i = ts_block.get(alpha).get(1) - ts_block.get(block_size-1).get(1);
      }
      else{
        timestamp_delta_i = ts_block.get(i).get(0) - ts_block.get(i - 1).get(0);
        value_delta_i = ts_block.get(i).get(1) - ts_block.get(i - 1).get(1);
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

  private static ArrayList<Integer> adjustAlphaToJ(ArrayList<ArrayList<Integer>> ts_block, int alpha, int j) {

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
    for(int i=1;i<block_size;i++){
      int timestamp_delta_i;
      int value_delta_i;
      if(i!=j){
        timestamp_delta_i = ts_block.get(i).get(0) - ts_block.get(i - 1).get(0);
        value_delta_i = ts_block.get(i).get(1) - ts_block.get(i - 1).get(1);
      } else {
        timestamp_delta_i = ts_block.get(j).get(0) - ts_block.get(alpha).get(0);
        value_delta_i = ts_block.get(j).get(1) - ts_block.get(alpha).get(1);
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
        timestamp_delta_i = ts_block.get(alpha).get(0) - ts_block.get(j-1).get(0);
        value_delta_i = ts_block.get(alpha).get(1) - ts_block.get(j-1).get(1);
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
  private static ArrayList<Integer> adjustn0(ArrayList<ArrayList<Integer>> ts_block) {
    int block_size = ts_block.size();
    ArrayList<Integer> b = new ArrayList<>();
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    int timestamp_delta_max = Integer.MIN_VALUE;
    int value_delta_max = Integer.MIN_VALUE;
    for(int i=1;i<block_size-1;i++){
      int timestamp_delta_i;
      int value_delta_i;
      timestamp_delta_i = ts_block.get(i).get(0) - ts_block.get(i - 1).get(0);
      value_delta_i = ts_block.get(i).get(1) - ts_block.get(i - 1).get(1);
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
    timestamp_delta_i = ts_block.get(0).get(0) - ts_block.get(block_size - 1).get(0);
    value_delta_i = ts_block.get(0).get(1) - ts_block.get(block_size - 1).get(1);
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
  private static ArrayList<Integer> adjustn(ArrayList<ArrayList<Integer>> ts_block, int alpha, int j) {
    int block_size = ts_block.size();
    assert alpha == block_size-1;
    assert j != 0;
    ArrayList<Integer> b = new ArrayList<>();
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    int timestamp_delta_max = Integer.MIN_VALUE;
    int value_delta_max = Integer.MIN_VALUE;
    for(int i=1;i<block_size-1;i++){
      int timestamp_delta_i;
      int value_delta_i;
      if(i!=j){
        timestamp_delta_i = ts_block.get(i).get(0) - ts_block.get(i - 1).get(0);
        value_delta_i = ts_block.get(i).get(1) - ts_block.get(i - 1).get(1);
      } else {
        timestamp_delta_i = ts_block.get(j).get(0) - ts_block.get(alpha).get(0);
        value_delta_i = ts_block.get(j).get(1) - ts_block.get(alpha).get(1);
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
        timestamp_delta_i = ts_block.get(alpha).get(0) - ts_block.get(j-1).get(0);
        value_delta_i = ts_block.get(alpha).get(1) - ts_block.get(j-1).get(1);
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
  private static ArrayList<Integer> adjust0n1(ArrayList<ArrayList<Integer>> ts_block) {
    int block_size = ts_block.size();
    ArrayList<Integer> b = new ArrayList<>();
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    int timestamp_delta_max = Integer.MIN_VALUE;
    int value_delta_max = Integer.MIN_VALUE;
    for(int i=2;i<block_size;i++){
      int timestamp_delta_i;
      int value_delta_i;
      timestamp_delta_i = ts_block.get(i).get(0) - ts_block.get(i - 1).get(0);
      value_delta_i = ts_block.get(i).get(1) - ts_block.get(i - 1).get(1);
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
    timestamp_delta_i = ts_block.get(0).get(0) - ts_block.get(block_size - 1).get(0);
    value_delta_i = ts_block.get(0).get(1) - ts_block.get(block_size - 1).get(1);
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
  private static ArrayList<Integer> adjust0(ArrayList<ArrayList<Integer>> ts_block, int alpha, int j) {
    int block_size = ts_block.size();
    assert alpha == 0;
    assert j != block_size;

    ArrayList<Integer> b = new ArrayList<>();
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    int timestamp_delta_max = Integer.MIN_VALUE;
    int value_delta_max = Integer.MIN_VALUE;
    for(int i=2;i<block_size;i++){
      int timestamp_delta_i;
      int value_delta_i;
      if(i!=j){
        timestamp_delta_i = ts_block.get(i).get(0) - ts_block.get(i - 1).get(0);
        value_delta_i = ts_block.get(i).get(1) - ts_block.get(i - 1).get(1);
      } else {
        timestamp_delta_i = ts_block.get(j).get(0) - ts_block.get(alpha).get(0);
        value_delta_i = ts_block.get(j).get(1) - ts_block.get(alpha).get(1);
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
        timestamp_delta_i = ts_block.get(alpha).get(0) - ts_block.get(j-1).get(0);
        value_delta_i = ts_block.get(alpha).get(1) - ts_block.get(j-1).get(1);
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

  public static int getIStar(ArrayList<ArrayList<Integer>> ts_block, int block_size,
                             int index){
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    int timestamp_delta_max = Integer.MIN_VALUE;
    int value_delta_max = Integer.MIN_VALUE;
    int timestamp_delta_max_index = -1;
    int value_delta_max_index = -1;

    int i_star = 0;

    if(index==0){
      for(int j = 1;j<block_size;j++){
        int epsilon_v_j = ts_block.get(j).get(1) - ts_block.get(j-1).get(1);
        if(epsilon_v_j < value_delta_min){
          value_delta_min = epsilon_v_j;
        }
        if(epsilon_v_j > value_delta_max){
          value_delta_max = epsilon_v_j;
          value_delta_max_index =j;
        }
      }
      i_star = value_delta_max_index;
    } else if (index==1) {
      for(int j = 1;j<block_size;j++){
        int epsilon_r_j = ts_block.get(j).get(0) - ts_block.get(j-1).get(0);
        if(epsilon_r_j < timestamp_delta_min){
          timestamp_delta_min = epsilon_r_j;
        }
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
                             ArrayList<Integer> raw_length){
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    int timestamp_delta_max = Integer.MIN_VALUE;
    int value_delta_max = Integer.MIN_VALUE;
    int timestamp_delta_max_index = -1;
    int value_delta_max_index = -1;

    int i_star_bit_width = 33;
    int i_star = 0;

    for(int j = 1;j<block_size;j++){
      int epsilon_r_j = ts_block.get(j).get(0) - ts_block.get(j-1).get(0);
      int epsilon_v_j = ts_block.get(j).get(1) - ts_block.get(j-1).get(1);
      if(epsilon_r_j < timestamp_delta_min){
        timestamp_delta_min = epsilon_r_j;
      }
      if(epsilon_v_j < value_delta_min){
        value_delta_min = epsilon_v_j;
      }
      if(epsilon_r_j > timestamp_delta_max){
        timestamp_delta_max = epsilon_r_j;
        timestamp_delta_max_index = j;
      }
      if(epsilon_v_j > value_delta_max){
        value_delta_max = epsilon_v_j;
        value_delta_max_index =j;
      }
    }
    int timestamp_delta_max_value = ts_block.get(timestamp_delta_max_index).get(0) - ts_block.get(timestamp_delta_max_index-1).get(0)
            -  timestamp_delta_min;
    int value_delta_max_value = ts_block.get(value_delta_max_index).get(0) - ts_block.get(value_delta_max_index-1).get(0)
            -  value_delta_min;
    if(timestamp_delta_max_value<=value_delta_max_value)
      i_star = timestamp_delta_max_index;
    else
      i_star = value_delta_max_index;
    return i_star;
  }
  public static ArrayList<Byte> encode2Bytes(ArrayList<ArrayList<Integer>> ts_block,ArrayList<Integer> raw_length,
                                            ArrayList<Integer> result2){
    ArrayList<Byte> encoded_result = new ArrayList<>();
//    // encode block size (Integer)
//    byte[] block_size_byte = int2Bytes(ts_block.size());
//    for (byte b : block_size_byte) encoded_result.add(b);

//    // encode r0 and d0 of a block (Integer)
//    byte[] r0_byte = int2Bytes(raw_length.get(6));
//    for (byte b : r0_byte) encoded_result.add(b);
//    byte[] d0_byte = int2Bytes(raw_length.get(7));
//    for (byte b : d0_byte) encoded_result.add(b);

    // encode min_delta_interval and min_delta_value
    byte[] min_delta_interval_byte = int2Bytes(raw_length.get(3));
    for (byte b : min_delta_interval_byte) encoded_result.add(b);
    byte[] min_delta_value_byte = int2Bytes(raw_length.get(4));
    for (byte b : min_delta_value_byte) encoded_result.add(b);


    // encode timestamp0 and value0
    byte[] interval0_byte = int2Bytes(ts_block.get(0).get(0));
    for (byte b : interval0_byte) encoded_result.add(b);
    byte[] value0_byte = int2Bytes(ts_block.get(0).get(1));
    for (byte b : value0_byte) encoded_result.add(b);

    // encode timestamp
    byte[] max_bit_width_interval_byte = int2Bytes(raw_length.get(1));
//    System.out.println(raw_length.get(1));
    for (byte b : max_bit_width_interval_byte) encoded_result.add(b);
    byte[] timestamp_bytes = bitPacking(ts_block,0,raw_length.get(1));
    for (byte b : timestamp_bytes) encoded_result.add(b);

    // encode value
    byte[] max_bit_width_value_byte = int2Bytes(raw_length.get(2));
//    System.out.println(raw_length.get(2));
    for (byte b : max_bit_width_value_byte) encoded_result.add(b);
    byte[] value_bytes = bitPacking(ts_block,1,raw_length.get(2));
    for (byte b : value_bytes) encoded_result.add(b);

    // encode deviation
    //byte[] max_bit_width_deviation_byte = int2Bytes(result2.get(0));
    //for (byte b: max_bit_width_deviation_byte) encoded_result.add(b);
    //byte[] deviation_list_bytes = bitPacking(deviation_list,result2.get(0));
    //for (byte b: deviation_list_bytes) encoded_result.add(b);

    byte[] td_common_byte = int2Bytes(result2.get(0));
    for (byte b: td_common_byte) encoded_result.add(b);

    return encoded_result;
  }
  //public static ArrayList<Byte> ReorderingDeltaEncoder(ArrayList<ArrayList<Integer>> data,int block_size,int td, ArrayList<Integer> flag){
  public static ArrayList<Byte> ReorderingDeltaEncoder(ArrayList<ArrayList<Integer>> data,int block_size,int td){
    block_size ++;
    int length_all = data.size();
    int block_num = length_all/block_size;
    ArrayList<Byte> encoded_result=new ArrayList<Byte>();
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

      //ArrayList<Integer> deviation_list = new ArrayList<>();
      ArrayList<Integer> result2 = new ArrayList<>();
      splitTimeStamp3(ts_block,td,result2);

//      ArrayList<Integer> deviation_list = new ArrayList<>();
      ArrayList<Integer> result = new ArrayList<>();
//      splitTimeStamp(ts_block,block_size,td,deviation_list,result);
      quickSort(ts_block,0,0,block_size-1);

      //ts_block order by interval

      // time-order
      ArrayList<Integer> raw_length = new ArrayList<>(); // length,max_bit_width_interval,max_bit_width_value,max_bit_width_deviation
      ArrayList<Integer> i_star_ready = new ArrayList<>();
      ArrayList<ArrayList<Integer>> ts_block_delta = getEncodeBitsDelta( ts_block,  block_size, raw_length,i_star_ready);


      // value-order
      quickSort(ts_block,1,0,block_size-1);
      ArrayList<Integer> reorder_length = new ArrayList<>();
      ArrayList<Integer> i_star_ready_reorder = new ArrayList<>();
      ArrayList<ArrayList<Integer>> ts_block_delta_reorder = getEncodeBitsDelta( ts_block,  block_size, reorder_length,i_star_ready_reorder);


      if(raw_length.get(0)<=reorder_length.get(0)){
        quickSort(ts_block,0,0,block_size-1);
        count_raw ++;
      }
      else{
        raw_length = reorder_length;
        quickSort(ts_block,1,0,block_size-1);
        count_reorder ++;
      }

      int i_star;
      int j_star;
      i_star =getIStar(ts_block,block_size,0);
      j_star =getJStar(ts_block,i_star,block_size,raw_length,0);
      int adjust_count = 0;
      while(j_star!=-1){
        if(adjust_count < block_size/2){
          adjust_count ++;
        }else {
          break;
        }
        ArrayList<Integer> tmp_tv = ts_block.get(i_star);

        if(j_star<i_star){
          for(int u=i_star-1;u>=j_star;u--){
            ArrayList<Integer> tmp_tv_cur = new ArrayList<>();
            tmp_tv_cur.add(ts_block.get(u).get(0));
            tmp_tv_cur.add(ts_block.get(u).get(1));
            ts_block.set(u+1,tmp_tv_cur);
          }
        }else{
          for(int u=i_star+1;u<=j_star;u++){
            ArrayList<Integer> tmp_tv_cur = new ArrayList<>();
            tmp_tv_cur.add(ts_block.get(u).get(0));
            tmp_tv_cur.add(ts_block.get(u).get(1));
            ts_block.set(u-1,tmp_tv_cur);
          }
        }

        ts_block.set(j_star,tmp_tv);
        i_star =getIStar(ts_block,block_size,raw_length);
        if(i_star == j_star) break;

        j_star =getJStar(ts_block,i_star,block_size,raw_length,0);
      }

      ts_block_delta = getEncodeBitsDelta( ts_block,  block_size,raw_length,
              i_star_ready_reorder);
      ArrayList<Byte> cur_encoded_result = encode2Bytes(ts_block_delta,raw_length,result2);
      encoded_result.addAll(cur_encoded_result);

    }
    return encoded_result;
  }

  public static int bytes2Integer(ArrayList<Byte> encoded, int start, int num) {
    int value = 0;
    if(num > 4){
      System.out.println("bytes2Integer error");
      return 0;
    }
    for (int i = start; i < start + num; i++) {
      value <<= 8;
      int b = encoded.get(i) & 0xFF;
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

  public static ArrayList<ArrayList<Integer>> ReorderingDeltaDecoder(ArrayList<Byte> encoded,int td){
    ArrayList<ArrayList<Integer>> data = new ArrayList<>();
    int decode_pos = 0;
    int block_size = bytes2Integer(encoded, decode_pos, 4);
    decode_pos += 4;

    while(decode_pos < encoded.size()) {
      ArrayList<Integer> time_list = new ArrayList<>();
      ArrayList<Integer> value_list = new ArrayList<>();
      //ArrayList<Integer> deviation_list = new ArrayList<>();

      ArrayList<ArrayList<Integer>> ts_block = new ArrayList<>();

      int min_delta_interval = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;
      int min_delta_value = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;

      int time0 = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;
      int value0 = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;

      int max_bit_width_time = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;
      time_list = decodebitPacking(encoded,decode_pos,max_bit_width_time,min_delta_interval,block_size);
      decode_pos += max_bit_width_time * (block_size - 1) / 8;

      int max_bit_width_value = bytes2Integer(encoded, decode_pos, 4);
      decode_pos += 4;
      value_list = decodebitPacking(encoded,decode_pos,max_bit_width_value,min_delta_value,block_size);
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
        int ri = ti_pre + time_list.get(i);
        time_list.set(i,ri);
        ti_pre = ri;

        int vi = vi_pre + value_list.get(i);
        value_list.set(i,vi);
        vi_pre = vi;

//        ArrayList<Integer> ts_block_tmp = new ArrayList<>();
//        ts_block_tmp.add(time_list.get(i));
//        ts_block_tmp.add(value_list.get(i));
//        ts_block.add(ts_block_tmp);
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

      //quickSort(ts_block, 0, 0, block_size-1);
      //quickSort22(ts_block, 0, block_size-1);

//      for (int i = 0; i < block_size; i++) {
//        ArrayList<Integer> tmp_datai = new ArrayList<>();
//        tmp_datai.add(ts_block.get(i).get(0));
//        tmp_datai.add(ts_block.get(i).get(1));
//        ts_block.set(i,tmp_datai);
//      }

//      ArrayList<Integer> tmp_data0 = new ArrayList<>();
//      tmp_data0.add(ts_block.get(0).get(0) * td + d0 + r0 * td);
//      tmp_data0.add(ts_block.get(0).get(1));
//      data.add(tmp_data0);
//
//      for (int i = 1; i < block_size; i++) {
//        ArrayList<Integer> tmp_datai = new ArrayList<>();
//        tmp_datai.add(ts_block.get(i).get(0) * td + deviation_list.get(i-1) + r0 * td);
//        tmp_datai.add(ts_block.get(i).get(1));
//        data.add(tmp_datai);
//      }

//      for (int i = 0; i < block_size-1; i++) {
//        ArrayList<Integer> tmp_datai = new ArrayList<>();
//        tmp_datai.add(interval_list.get(i) * td + deviation_list.get(i) + r0 * td + d0);
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
            "\\compression_ratio\\rd_ratio\\Metro-Traffic_ratio.csv");
    dataset_map_td.add(3600);
    input_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test\\Nifty-Stocks");
    output_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\result_evaluation" +
            "\\compression_ratio\\rd_ratio\\Nifty-Stocks_ratio.csv");
    dataset_map_td.add(86400);
    input_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test\\USGS-Earthquakes");
    output_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\result_evaluation" +
            "\\compression_ratio\\rd_ratio\\USGS-Earthquakes_ratio.csv");
    dataset_map_td.add(50);
    input_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test\\Cyber-Vehicle");
    output_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\result_evaluation" +
            "\\compression_ratio\\rd_ratio\\Cyber-Vehicle_ratio.csv");
    dataset_map_td.add(10);
    input_path_list.add( "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test\\TH-Climate");
    output_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\result_evaluation" +
            "\\compression_ratio\\rd_ratio\\TH-Climate_ratio.csv");
    dataset_map_td.add(4);
    input_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test\\TY-Transport");
    output_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\result_evaluation" +
            "\\compression_ratio\\rd_ratio\\TY-Transport_ratio.csv");
    dataset_map_td.add(6);
    input_path_list.add( "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test\\TY-Fuel");
    output_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\result_evaluation" +
            "\\compression_ratio\\rd_ratio\\TY-Fuel_ratio.csv");
    dataset_map_td.add(60);
    input_path_list.add( "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test\\GW-Magnetic");
    output_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\result_evaluation" +
            "\\compression_ratio\\rd_ratio\\GW-Magnetic_ratio.csv");
    dataset_map_td.add(100);

//    input_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\iotdb_test\\Metro-Traffic");
//    output_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\result_evaluation" +
//            "\\compression_ratio\\rd_ratio\\Metro-Traffic_ratio.csv");
//    dataset_map_td.add(3600);
//    input_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\iotdb_test\\Nifty-Stocks");
//    output_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\result_evaluation" +
//            "\\compression_ratio\\rd_ratio\\Nifty-Stocks_ratio.csv");
//    dataset_map_td.add(86400);
//    input_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\iotdb_test\\USGS-Earthquakes");
//    output_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\result_evaluation" +
//            "\\compression_ratio\\rd_ratio\\USGS-Earthquakes_ratio.csv");
//    dataset_map_td.add(50);
//    input_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\iotdb_test\\Cyber-Vehicle");
//    output_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\result_evaluation" +
//            "\\compression_ratio\\rd_ratio\\Cyber-Vehicle_ratio.csv");
//    dataset_map_td.add(10);
//    input_path_list.add( "E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\iotdb_test\\TH-Climate");
//    output_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\result_evaluation" +
//            "\\compression_ratio\\rd_ratio\\TH-Climate_ratio.csv");
//    dataset_map_td.add(3);
//    input_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\iotdb_test\\TY-Transport");
//    output_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\result_evaluation" +
//            "\\compression_ratio\\rd_ratio\\TY-Transport_ratio.csv");
//    dataset_map_td.add(5);
//    input_path_list.add( "E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\iotdb_test\\TY-Fuel");
//    output_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\result_evaluation" +
//            "\\compression_ratio\\rd_ratio\\TY-Fuel_ratio.csv");
//    dataset_map_td.add(60);
//    input_path_list.add( "E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\iotdb_test\\GW-Magnetic");
//    output_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\result_evaluation" +
//            "\\compression_ratio\\rd_ratio\\GW-Magnetic_ratio.csv");
//    dataset_map_td.add(100);

//    for(int file_i=0;file_i<input_path_list.size();file_i++){
    for(int file_i=0;file_i<1;file_i++){
      String inputPath = input_path_list.get(file_i);
//      String Output =output_path_list.get(file_i);
//      System.out.println(inputPath);

          String Output = "C:\\Users\\xiaoj\\Desktop\\test_ratio.csv";
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
//        ArrayList<Integer> flag = new ArrayList<>();
//        flag.add(0);
//        flag.add(0);
//        flag.add(0);
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
          //ArrayList<Byte> buffer = ReorderingDeltaEncoder(data, 256);
          ArrayList<Byte> buffer = ReorderingDeltaEncoder(data, 256, dataset_map_td.get(file_i));
          //ArrayList<Byte> buffer = ReorderingDeltaEncoder(data, 256, dataset_map_td.get(file_i),flag);
//          System.out.print(flag.get(0));
//          System.out.print(" ");
//          System.out.print(flag.get(1));
//          System.out.print(" ");
//          System.out.println(flag.get(2));
          long e = System.nanoTime();
          encodeTime += (e - s);
          compressed_size += buffer.size();
          double ratioTmp =
                  (double) buffer.size() / (double) (data.size() * Integer.BYTES*2);
          ratio += ratioTmp;
          s = System.nanoTime();

//          data_decoded = ReorderingDeltaDecoder(buffer,dataset_map_td.get(file_i));

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
                "RD",
                String.valueOf(encodeTime),
                String.valueOf(decodeTime),
                String.valueOf(data.size()),
                String.valueOf(compressed_size),
                String.valueOf(ratio)
        };
        writer.writeRecord(record);
        //System.out.println(ratio);
      }
      writer.close();

    }
  }
}
