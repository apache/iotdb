package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;

public class ReorderingEncodeRegressionTest {

  static int OutlierThreshold = 0;
  public static int zigzag(int num){
    if(num<0){
      return 2*(-num)-1;
    }else{
      return 2*num;
    }
  }
  public static int getBitWith(int num){
    return 32 - Integer.numberOfLeadingZeros(num);
  }
  public static byte[] int2Bytes(int integer)
  {
    byte[] bytes=new byte[4];
    bytes[3]= (byte) ((byte) integer>>24);
    bytes[2]= (byte) ((byte) integer>>16);
    bytes[1]= (byte) ((byte) integer>>8);
    bytes[0]=(byte) integer;
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
                                    ArrayList<ArrayList<Integer>> deviation_result, int max_bit_width_deviation, int r0){
    int deviation_max = Integer.MIN_VALUE;
    ArrayList<Integer> deviation_list = new ArrayList<>();

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
    int deviation0 = ts_block.get(0).get(0) %td;
    if(deviation0 >= (td/2)){
      deviation0 -= td;
      r0 ++;
    }
    deviation0 = zigzag(deviation0);
    deviation_list.add(deviation0);
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

    // get outlier of deviation
    int raw_count_max_bit_deviation = 0;
    max_bit_width_deviation = getBitWith(deviation_max);
    ArrayList<Integer> outlier_deviation_index = new ArrayList<>();

    while(raw_count_max_bit_deviation < OutlierThreshold){
      int cur_max_bit_count = 0;
      ArrayList<Integer> cur_outlier_deviation_index = new ArrayList<>();
      for(int deviation_index=0;deviation_index<(block_size-1);deviation_index++){
        if(getBitWith(deviation_list.get(deviation_index))==max_bit_width_deviation){
          cur_max_bit_count ++;
          cur_outlier_deviation_index.add(deviation_index);
        }
      }
      if(raw_count_max_bit_deviation+cur_max_bit_count>OutlierThreshold){
        break;
      }
      max_bit_width_deviation --;
      for(int cur_outlier : cur_outlier_deviation_index){
        if( !outlier_deviation_index.contains(cur_outlier)){
          outlier_deviation_index.add(cur_outlier);
        }
        int tmp_outlier = (deviation_list.get(cur_outlier))%(1<<max_bit_width_deviation);
        deviation_list.set(cur_outlier,tmp_outlier);
      }
      raw_count_max_bit_deviation= outlier_deviation_index.size();
    }
    deviation_result.add(deviation_list);
    deviation_result.add(outlier_deviation_index);
  }
  public static ArrayList<Integer> getEncodeBitsRegression(ArrayList<ArrayList<Integer>> ts_block, int block_size, ArrayList<Integer> outlier_value_index){
    int timestamp_delta_min = Integer.MAX_VALUE;
    int value_delta_min = Integer.MAX_VALUE;
    ArrayList<Integer> result = new ArrayList<Integer>(); // length,max_bit_width_timestamp,max_bit_width_value,max_bit_width_deviation

    System.out.println(ts_block);
    // delta to regression
    for(int j=block_size-1;j>0;j--) {
      int delta_r = ts_block.get(j).get(0) - ts_block.get(j-1).get(0);
      int delta_v = ts_block.get(j).get(1) - ts_block.get(j-1).get(1);
      ArrayList<Integer> tmp = new ArrayList<>();
      tmp.add(delta_r);
      tmp.add(delta_v);
      ts_block.set(j,tmp);

      if(delta_r <timestamp_delta_min){
        timestamp_delta_min = delta_r;
      }
      if(delta_v < value_delta_min){
        value_delta_min = delta_v;
      }
    }

    int max_timestamp = Integer.MIN_VALUE;
    int max_value = Integer.MIN_VALUE;
    for(int j=block_size-1;j>0;j--) {
      int ts_2diff_ts = ts_block.get(j).get(0) - timestamp_delta_min;
      int ts_2diff_value = ts_block.get(j).get(1) - value_delta_min;
      ArrayList<Integer> tmp = new ArrayList<>();
      tmp.add(ts_2diff_ts);
      tmp.add(ts_2diff_value);
      ts_block.set(j,tmp);
      if(ts_2diff_ts>max_timestamp){
        max_timestamp = ts_2diff_ts;
      }
      if(ts_2diff_value>max_value){
        max_value = ts_2diff_value;
      }
    }
    int max_bit_width_timestamp = getBitWith(max_timestamp);
    int max_bit_width_value = getBitWith(max_value);

    // get outlier of value
    int raw_count_max_bit_value = 0;
    while(raw_count_max_bit_value < OutlierThreshold){
      int cur_max_bit_count = 0;
      ArrayList<Integer> cur_outlier_value_index = new ArrayList<>();
      for(int value_index=0;value_index<(block_size-1);value_index++){
        if(getBitWith(ts_block.get(value_index).get(1))==max_bit_width_value){
          cur_max_bit_count ++;
          cur_outlier_value_index.add(value_index);
        }
      }
      if(raw_count_max_bit_value+cur_max_bit_count>OutlierThreshold){
        break;
      }
      max_bit_width_value --;
      for(int cur_outlier : cur_outlier_value_index){
        if(!outlier_value_index.contains(cur_outlier)){
          outlier_value_index.add(cur_outlier);
        }
        int tmp_outlier = (ts_block.get(cur_outlier).get(1))%(1<<max_bit_width_value);
        int tmp_ts = ts_block.get(cur_outlier).get(0);
        ArrayList<Integer> tmp_tv = new ArrayList<>();
        tmp_tv.add(tmp_ts);
        tmp_tv.add(tmp_outlier);
        ts_block.set(cur_outlier,tmp_tv);
      }
      raw_count_max_bit_value = outlier_value_index.size();
    }
//    int raw_count_max_bit_deviation = deviation_result.get(1).size();
    int raw_count_outlier_value = outlier_value_index.size();

    // calculate error
    int  length = (max_bit_width_timestamp+max_bit_width_value)*(block_size-1) +
            (raw_count_outlier_value)*8;
    result.add(length);
    result.add(max_bit_width_timestamp);
    result.add(max_bit_width_value);
//    result.add(max_bit_width_deviation);

//    other_result.add(deviation_list);
//    other_result.add(outlier_deviation_index);
//    other_result.add(outlier_value_index);
    return result;
  }

  public static ArrayList<Byte> encode2Bytes(ArrayList<ArrayList<Integer>> ts_block,ArrayList<ArrayList<Integer>> other_result,ArrayList<Integer> raw_length){
    ArrayList<Byte> encoded_result = new ArrayList<>();

    // encode timestamp
    byte[] max_bit_width_timestamp_byte = int2Bytes(raw_length.get(1));
    for (byte b : max_bit_width_timestamp_byte) encoded_result.add(b);
    byte[] timestamp_bytes = bitPacking(ts_block,0,raw_length.get(1));
    for (byte b : timestamp_bytes) encoded_result.add(b);

    // encode value
    byte[] max_bit_width_value_byte = int2Bytes(raw_length.get(2));
    for (byte b : max_bit_width_value_byte) encoded_result.add(b);
    byte[] value_bytes = bitPacking(ts_block,1,raw_length.get(1));
    for (byte b : value_bytes) encoded_result.add(b);

    // encode outlier of value
    byte[] outlier_value_index_length_bytes = int2Bytes(other_result.get(2).size());
    for (byte b : outlier_value_index_length_bytes) encoded_result.add(b);
    byte[] outlier_value_index_bytes = bitPacking(other_result.get(2),8);
    for (byte b : outlier_value_index_bytes) encoded_result.add(b);

    // encode deviation
    byte[] max_bit_width_deviation_byte = int2Bytes(raw_length.get(3));
    for (byte b: max_bit_width_deviation_byte) encoded_result.add(b);
    byte[] deviation_list_bytes = bitPacking(other_result.get(0),raw_length.get(3));
    for (byte b: deviation_list_bytes) encoded_result.add(b);

    // encode outlier of deviation
    byte[] outlier_deviation_index_length_bytes = int2Bytes(other_result.get(1).size());
    for (byte b: outlier_deviation_index_length_bytes) encoded_result.add(b);
    byte[] outlier_deviation_index_bytes = bitPacking(other_result.get(1),8);
    for (byte b: outlier_deviation_index_bytes) encoded_result.add(b);
    return encoded_result;
  }
  public static ArrayList<Byte> ReorderingRegressionEncoder(ArrayList<ArrayList<Integer>> data,int block_size, int td){
    block_size ++;
    int length_all = data.size();
    int block_num = length_all/block_size;
    ArrayList<Byte> encoded_result=new ArrayList<Byte>();

    for(int i=0;i<1;i++){
//    for(int i=0;i<block_num;i++){
      ArrayList<ArrayList<Integer>> ts_block = new ArrayList<>();
      ArrayList<ArrayList<Integer>> ts_block_reorder = new ArrayList<>();
      for(int j=0;j<block_size;j++){
        ts_block.add(data.get(j+i*block_size));
        ts_block_reorder.add(data.get(j+i*block_size));
      }

      int max_bit_width_deviation = 0;
      int r0 = 0;
      ArrayList<ArrayList<Integer>> outlier_result = new ArrayList<>();
      splitTimeStamp(ts_block,block_size,td,outlier_result,max_bit_width_deviation,r0);
      quickSort(ts_block,0,0,block_size-1);
      // time-order
      ArrayList<ArrayList<Integer>> other_result = new ArrayList<>();
      ArrayList<Integer> outlier_value_index = new ArrayList<>();
      ArrayList<Integer> raw_length; // length,max_bit_width_timestamp,max_bit_width_value,max_bit_width_deviation
      raw_length = getEncodeBitsRegression( ts_block,  block_size, outlier_value_index);
//      System.out.println(ts_block);

      // value-order
      ArrayList<ArrayList<Integer>> outlier_result_resort = new ArrayList<>();
      int max_bit_width_deviation_resort = 0;
      int r0_reorder= 0;
      splitTimeStamp(ts_block_reorder,block_size,td,outlier_result_resort,max_bit_width_deviation_resort,r0_reorder);
      quickSort(ts_block_reorder,1,0,block_size-1);
      ArrayList<Integer> outlier_value_index_reorder = new ArrayList<>();
      ArrayList<Integer> reorder_length;
      reorder_length = getEncodeBitsRegression( ts_block_reorder,  block_size, outlier_value_index_reorder);
//      System.out.println(ts_block_reorder);

      if(raw_length.get(0)<=reorder_length.get(0)){

        outlier_result.add(outlier_value_index);
        System.out.println(outlier_result.size());
        raw_length.add(max_bit_width_deviation);
        encoded_result = encode2Bytes(ts_block,outlier_result,raw_length);
      }else{

        outlier_result_resort.add(outlier_value_index_reorder);
        reorder_length.add(max_bit_width_deviation);
        encoded_result = encode2Bytes(ts_block_reorder,outlier_result_resort,reorder_length);
      }
    }
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

    int max_bit_width_timestamp   = bytes2Integer(encoded,0,4);
    return null;
  }



  public static void main(@org.jetbrains.annotations.NotNull String[] args) throws IOException {
//    ArrayList<Integer> test = new ArrayList<>();
//    for(int i=0;i<8;i++){
//      test.add(i);
//    }
//    System.out.println(bitPacking(test,3)[2]);
//    System.out.println((byte)15);

    String inputPath =
        "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test\\Metro-Traffic"; // the direction of input compressed data
//    String Output =
//        "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\result_evaluation\\compression_ratio\\Metro-Traffic_ratio.csv"; // the direction of output compression ratio and

//    String inputPath =
//            "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test\\Nifty-Stocks"; // the direction of input compressed data
//    String Output =
//            "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\result_evaluation\\compression_ratio\\Nifty-Stocks_ratio.csv"; // the direction of output compression ratio and

//    String inputPath =
//            "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test\\USGS-Earthquakes"; // the direction of input compressed data
//    String Output =
//            "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\result_evaluation\\compression_ratio\\USGS-Earthquakes_ratio.csv"; // the direction of output compression ratio and

//    String inputPath =
//            "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test\\Cyber-Vehicle"; // the direction of input compressed data
//    String Output =
//            "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\result_evaluation\\compression_ratio\\Cyber-Vehicle_ratio.csv"; // the direction of output compression ratio and

//    String inputPath =
//            "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test\\TH-Climate"; // the direction of input compressed data
//    String Output =
//            "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\result_evaluation\\compression_ratio\\TH-Climate_ratio.csv"; // the direction of output compression ratio and


//    String inputPath =
//            "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test\\TY-Transport"; // the direction of input compressed data
//    String Output =
//            "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\result_evaluation\\compression_ratio\\TY-Transport_ratio.csv"; // the direction of output compression ratio and

//        String inputPath =
//            "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test\\TY-Fuel"; // the direction of input compressed data
//    String Output =
//            "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\result_evaluation\\compression_ratio\\TY-Fuel_ratio.csv"; // the direction of output compression ratio and


    String Output =
            "C:\\Users\\xiaoj\\Desktop\\test_ratio.csv"; // the direction of output compression ratio and

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
      "Compressed Size",
      "Compression Ratio"
    };
    writer.writeRecord(head); // write header to output file

    assert tempList != null;

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
        ArrayList<Byte> buffer = ReorderingRegressionEncoder(data, 8, 3600);
        long e = System.nanoTime();
        encodeTime += (e - s);
        compressed_size += buffer.size();
        double ratioTmp =
                (double) buffer.size() / (double) (data.size() * Integer.BYTES);
        ratio += ratioTmp;
        s = System.nanoTime();
        ReorderingRegressionDecoder(buffer);
        e = System.nanoTime();
        decodeTime += (e-s);
      }


      ratio /= repeatTime;
      compressed_size /= repeatTime;
      encodeTime /= repeatTime;
      decodeTime /= repeatTime;

      String[] record = {
        f.toString(),
        "Reordering",
        String.valueOf(encodeTime),
        String.valueOf(decodeTime),
        String.valueOf(compressed_size),
        String.valueOf(ratio)
      };
      writer.writeRecord(record);
    }
    writer.close();
  }
}
