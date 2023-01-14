package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;

public class ReorderingEncodeRegressionBlocksizeTest {

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
  public static byte[] int2Bytes(int integer) {
    byte[] bytes=new byte[4];
    bytes[3]= (byte) ((byte) integer>>24);
    bytes[2]= (byte) ((byte) integer>>16);
    bytes[1]= (byte) ((byte) integer>>8);
    bytes[0]=(byte) integer;
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
          tmp_int += (((numbers.get(i*8+k).get(index) >>j) %2) << k);
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

  public static void splitTimeStamp(ArrayList<ArrayList<Integer>> ts_block, int block_size, int td,
                                    ArrayList<Integer> deviation_list,ArrayList<Integer> result){
    int deviation_max = Integer.MIN_VALUE;
    int max_bit_width_deviation=0;
    int r0 = 0;
    int d0 = 0;

    // split timestamp into intervals and deviations

    //address other timestamps and values

    for(int j=block_size-1;j>0;j--) {
      int delta_interval = (ts_block.get(j).get(0) - ts_block.get(j-1).get(0))/td;
      int deviation = (ts_block.get(j).get(0) - ts_block.get(j-1).get(0))%td;
      if(deviation >= (td/2)){
        deviation -= td;
        delta_interval ++;
      }
      deviation = zigzag(deviation);
      deviation_list.add(deviation);
      if(deviation > deviation_max){
        deviation_max = deviation;
      }

      int value = ts_block.get(j).get(1);
      ArrayList<Integer> tmp = new ArrayList<>();
      tmp.add(delta_interval);
      tmp.add(value);
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
    if(d0 > deviation_max){
      deviation_max = d0;
    }

    int value0 = ts_block.get(0).get(1);
    ArrayList<Integer> tmp0 = new ArrayList<>();
    tmp0.add(0);
    tmp0.add(value0);
    ts_block.set(0,tmp0);

    for(int j=1;j<block_size-1;j++){
      int interval = ts_block.get(j).get(0) + ts_block.get(j-1).get(0);
      int value = ts_block.get(j).get(1);
      ArrayList<Integer> tmp = new ArrayList<>();
      tmp.add(interval);
      tmp.add(value);
      ts_block.set(j,tmp);
    }
    max_bit_width_deviation = getBitWith(deviation_max);
    result.add(max_bit_width_deviation);
    result.add(r0);
    result.add(d0);
  }
  public static ArrayList<ArrayList<Integer>> getEncodeBitsRegression(ArrayList<ArrayList<Integer>> ts_block, int block_size,
                                                                 ArrayList<Integer> result, ArrayList<Integer> i_star,
                                                                      ArrayList<Double> theta){
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
      int epsilon_r = (int) ((double)ts_block.get(j).get(0) - theta0_r - theta1_r * (double)ts_block.get(j-1).get(0));
      int epsilon_v = (int) ((double)ts_block.get(j).get(1) - theta0_v - theta1_v * (double)ts_block.get(j-1).get(1));
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
      int epsilon_r = ts_block_delta.get(j).get(0);
      int epsilon_v = ts_block_delta.get(j).get(1) ;
      if(epsilon_r>max_interval){
        max_interval = epsilon_r;
        max_interval_i = j;
      }
      if(epsilon_v>max_value){
        max_value = epsilon_v;
        max_value_i = j;
      }
    }
    int max_bit_width_interval = getBitWith(max_interval);
    int max_bit_width_value = getBitWith(max_value);


    // calculate error
    int  length = (max_bit_width_interval+max_bit_width_value)*(block_size-1);
    result.clear();

    result.add(length);
    result.add(max_bit_width_interval);
    result.add(max_bit_width_value);

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
    byte[] r0_byte = int2Bytes(raw_length.get(4));
    for (byte b : r0_byte) encoded_result.add(b);
    byte[] d0_byte = int2Bytes(raw_length.get(5));
    for (byte b : d0_byte) encoded_result.add(b);

    // encode interval0 and value0
    byte[] interval0_byte = int2Bytes(ts_block.get(0).get(0));
    for (byte b : interval0_byte) encoded_result.add(b);
    byte[] value0_byte = int2Bytes(ts_block.get(0).get(1));
    for (byte b : value0_byte) encoded_result.add(b);

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
    byte[] max_bit_width_deviation_byte = int2Bytes(raw_length.get(3));
    for (byte b: max_bit_width_deviation_byte) encoded_result.add(b);
    byte[] deviation_list_bytes = bitPacking(deviation_list,raw_length.get(3));
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

  public static int bytes2Integer(ArrayList<Byte> encoded, int start, int num) {
    if(num > 4){
      System.out.println("bytes2Integer error");
      return 0;
    }
    return 0;
  }
  public static ArrayList<ArrayList<Integer>> ReorderingRegressionDecoder(ArrayList<Byte> encoded){

    int max_bit_width_interval   = bytes2Integer(encoded,0,4);
    return null;
  }


  public static void main(@org.jetbrains.annotations.NotNull String[] args) throws IOException {
    ArrayList<String> input_path_list = new ArrayList<>();
    ArrayList<String> output_path_list = new ArrayList<>();
    ArrayList<Integer> dataset_map_td = new ArrayList<>();
    input_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test\\Metro-Traffic");
    output_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\result_evaluation" +
            "\\vary_parameter\\rr_ratio\\Metro-Traffic_ratio.csv");
    dataset_map_td.add(3600);
    input_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test\\Nifty-Stocks");
    output_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\result_evaluation" +
            "\\vary_parameter\\rr_ratio\\Nifty-Stocks_ratio.csv");
    dataset_map_td.add(86400);
    input_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test\\USGS-Earthquakes");
    output_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\result_evaluation" +
            "\\vary_parameter\\rr_ratio\\USGS-Earthquakes_ratio.csv");
    dataset_map_td.add(50);
    input_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test\\Cyber-Vehicle");
    output_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\result_evaluation" +
            "\\vary_parameter\\rr_ratio\\Cyber-Vehicle_ratio.csv");
    dataset_map_td.add(10);
    input_path_list.add( "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test\\TH-Climate");
    output_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\result_evaluation" +
            "\\vary_parameter\\rr_ratio\\TH-Climate_ratio.csv");
    dataset_map_td.add(3);
    input_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test\\TY-Transport");
    output_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\result_evaluation" +
            "\\vary_parameter\\rr_ratio\\TY-Transport_ratio.csv");
    dataset_map_td.add(5);
    input_path_list.add( "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test\\TY-Fuel");
    output_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\result_evaluation" +
            "\\vary_parameter\\rr_ratio\\TY-Fuel_ratio.csv");
    dataset_map_td.add(60);
    input_path_list.add( "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test\\GW-Magnetic");
    output_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\result_evaluation" +
            "\\vary_parameter\\rr_ratio\\GW-Magnetic_ratio.csv");
    dataset_map_td.add(100);

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
              "Block Size",
              "Compression Ratio"
      };
      writer.writeRecord(head); // write header to output file

      assert tempList != null;
      for(int block_size_exp=4;block_size_exp<9;block_size_exp++) {
        int block_size = (int) Math.pow(2, block_size_exp);
        for (File f : tempList) {
          InputStream inputStream = Files.newInputStream(f.toPath());
          CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
          ArrayList<ArrayList<Integer>> data = new ArrayList<>();

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
            ArrayList<Byte> buffer = ReorderingRegressionEncoder(data, block_size, dataset_map_td.get(file_i));
            long e = System.nanoTime();
            encodeTime += (e - s);
            compressed_size += buffer.size();
            double ratioTmp =
                    (double) buffer.size() / (double) (data.size() * Integer.BYTES * 2);
            ratio += ratioTmp;
            s = System.nanoTime();
//          ReorderingRegressionDecoder(buffer);
            e = System.nanoTime();
            decodeTime += (e - s);
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
