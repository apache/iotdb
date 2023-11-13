package org.apache.iotdb.tsfile.encoding;

import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;

public class EncodeTest {

  public static void main(@NotNull String[] args) throws IOException {
//    String parent_dir =
//            "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\vldb\\compression_ratio\\sota_ratio";
//
    String parent_dir = "C:\\Users\\Jinnsjao Shawl\\Documents\\GitHub\\encoding-reorder\\";
    String output_parent_dir = parent_dir + "vldb\\compression_ratio\\sota_ratio";
    String input_parent_dir = parent_dir + "reorder\\iotdb_test_small\\";
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


    for (int i = 0; i < dataset_name.size(); i++) {
      input_path_list.add(input_parent_dir + dataset_name.get(i));
    }

    output_path_list.add(output_parent_dir + "\\CS-Sensors_ratio.csv"); // 0
    dataset_block_size.add(1024);
//    dataset_k.add(5);
    output_path_list.add(output_parent_dir + "\\Metro-Traffic_ratio.csv");// 1
    dataset_block_size.add(512);
//    dataset_k.add(7);
    output_path_list.add(output_parent_dir + "\\USGS-Earthquakes_ratio.csv");// 2
    dataset_block_size.add(512);
//    dataset_k.add(7);
    output_path_list.add(output_parent_dir + "\\YZ-Electricity_ratio.csv"); // 3
    dataset_block_size.add(512);
//    dataset_k.add(1);
    output_path_list.add(output_parent_dir + "\\GW-Magnetic_ratio.csv"); //4
    dataset_block_size.add(128);
//    dataset_k.add(6);
    output_path_list.add(output_parent_dir + "\\TY-Fuel_ratio.csv");//5
    dataset_block_size.add(64);
//    dataset_k.add(5);
    output_path_list.add(output_parent_dir + "\\Cyber-Vehicle_ratio.csv"); //6
    dataset_block_size.add(128);
//    dataset_k.add(4);
    output_path_list.add(output_parent_dir + "\\Vehicle-Charge_ratio.csv");//7
    dataset_block_size.add(512);
//    dataset_k.add(8);
    output_path_list.add(output_parent_dir + "\\Nifty-Stocks_ratio.csv");//8
    dataset_block_size.add(256);
//    dataset_k.add(1);
    output_path_list.add(output_parent_dir + "\\TH-Climate_ratio.csv");//9
    dataset_block_size.add(512);
//    dataset_k.add(2);
    output_path_list.add(output_parent_dir + "\\TY-Transport_ratio.csv");//10
    dataset_block_size.add(512);
//    dataset_k.add(9);
    output_path_list.add(output_parent_dir + "\\EPM-Education_ratio.csv");//11
    dataset_block_size.add(512);

//        for(int file_i=3;file_i<4;file_i++){
    for (int file_i = 0; file_i < input_path_list.size(); file_i++) {
      String inputPath = input_path_list.get(file_i);
      String Output = output_path_list.get(file_i);
      //      String Output = "C:\\Users\\xiaoj\\Desktop\\test_ratio_ts_2diff.csv";

      // speed
      int repeatTime = 1; // set repeat time
      String dataTypeName = "int"; // set dataType
      //    if (args.length >= 2) inputPath = args[1];
      //    if (args.length >= 3) Output = args[2];

      File file = new File(inputPath);
      File[] tempList = file.listFiles();

      // select encoding algorithms
      TSEncoding[] encodingList = {
              TSEncoding.PLAIN ,
              TSEncoding.TS_2DIFF,
              TSEncoding.RLE,
              TSEncoding.SPRINTZ,
              TSEncoding.GORILLA,
              TSEncoding.RLBE,
//              TSEncoding.RAKE,
              TSEncoding.CHIMP,
//              TSEncoding.BUFF
      };
      // select compression algorithms
      CompressionType[] compressList = {
              CompressionType.UNCOMPRESSED,
              //            CompressionType.LZ4,
              //            CompressionType.GZIP,
              //            CompressionType.SNAPPY
      };
      CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

      String[] head = {
              "Input Direction",
              "Column Index",
              "Encoding Algorithm",
              "Compress Algorithm",
              "Encoding Time",
              "Decoding Time",
//        "Compress Time",
//        "Uncompress Time",
              "Points",
              "Compressed Size",
              "Compression Ratio"
      };
      writer.writeRecord(head); // write header to output file

      assert tempList != null;
      int fileRepeat = 0;
      ArrayList<Integer> columnIndexes = new ArrayList<>(); // set the column indexes of compressed
      for (int i = 0; i < 2; i++) {
        columnIndexes.add(i, i);
      }
      for (File f : tempList) {
        System.out.println(f);
        fileRepeat += 1;
        InputStream inputStream = Files.newInputStream(f.toPath());
        CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
        String fileName = f.getAbsolutePath();
        ArrayList<String> data = new ArrayList<>();

        for (int index : columnIndexes) {
          // add a column to "data"
          //        System.out.println(index);
          int max_precision = 0;
          loader.readHeaders();
          data.clear();
          while (loader.readRecord()) {
            String v = loader.getValues()[index];
            //          int ind = v.indexOf(".");
            //          if (ind > -1) {
            //            int len = v.substring(ind + 1).length();
            //            if (len > max_precision) {
            //              max_precision = len;
            //            }
            //          }
            data.add(v);
          }
          //        System.out.println(max_precision);
          inputStream.close();

          switch (dataTypeName) {
            case "int":
            {
              TSDataType dataType = TSDataType.INT32; // set TSDataType
              ArrayList<Integer> tmp = new ArrayList<>();
              for (String value : data) {
                tmp.add(Integer.valueOf(value));
              }
              // Iterate over each encoding algorithm
              for (TSEncoding encoding : encodingList) {
                Encoder encoder =
                        TSEncodingBuilder.getEncodingBuilder(encoding).getEncoder(dataType);
                Decoder decoder = Decoder.getDecoderByType(encoding, dataType);
                long encodeTime = 0;
                long decodeTime = 0;

                // Iterate over each compression algorithm
                for (CompressionType comp : compressList) {
                  ICompressor compressor = ICompressor.getCompressor(comp);
                  IUnCompressor unCompressor = IUnCompressor.getUnCompressor(comp);

                  double ratio = 0;
                  double compressed_size = 0;

                  long compressTime = 0;
                  long uncompressTime = 0;

                  // repeat many times to test time
                  for (int i = 0; i < repeatTime; i++) {
                    ByteArrayOutputStream buffer = new ByteArrayOutputStream();

                    // test encode time
                    long s = System.nanoTime();
                    for (int val : tmp) {
                      encoder.encode(val, buffer);
                    }

                    //                    byte[] elems = buffer.toByteArray();
                    encoder.flush(buffer);
                    long e = System.nanoTime();
                    encodeTime += (e - s);

                    // test compress time
                    byte[] elems = buffer.toByteArray();
                    s = System.nanoTime();
                    byte[] compressed = compressor.compress(elems);
                    e = System.nanoTime();
                    compressTime += (e - s);

                    // test compression ratio and compressed size
                    compressed_size += compressed.length;
                    double ratioTmp =
                            (double) compressed.length / (double) (tmp.size() * Integer.BYTES);
                    ratio += ratioTmp;

                    // test uncompress time
                    s = System.nanoTime();
                    byte[] x = unCompressor.uncompress(compressed);
                    e = System.nanoTime();
                    uncompressTime += (e - s);

                    // test decode time
                    ByteBuffer ebuffer = ByteBuffer.wrap(buffer.toByteArray());
                    s = System.nanoTime();
                    int i_tmp = 0;
                    while (decoder.hasNext(ebuffer)) {
                      //                      decoder.readInt(ebuffer);
                      decoder.readInt(ebuffer);
                      //                      if(tmp.get(i_tmp) == tmp_tmp)
                      //                        System.out.println("equal");
                      //                      i_tmp += 1;
                    }
                    e = System.nanoTime();
                    decodeTime += (e - s);

                    buffer.close();
                  }

                  ratio /= repeatTime;
                  compressed_size /= repeatTime;
                  encodeTime /= repeatTime;
                  decodeTime /= repeatTime;
                  compressTime /= repeatTime;
                  uncompressTime /= repeatTime;

                  String[] record = {
                          f.toString(),
                          String.valueOf(index),
                          encoding.toString(),
                          comp.toString(),
                          String.valueOf(encodeTime),
                          String.valueOf(decodeTime),
//                      String.valueOf(compressTime),
//                      String.valueOf(uncompressTime),
                          String.valueOf(data.size()),
                          String.valueOf(compressed_size),
                          String.valueOf(ratio)
                  };
                  System.out.println(ratio);
                  writer.writeRecord(record);
                }
              }
              tmp.clear();
              break;
            }
            case "long":
            {
              TSDataType dataType = TSDataType.INT64;
              ArrayList<Long> tmp = new ArrayList<>();
              for (String value : data) {
                tmp.add(Long.valueOf(value));
              }
              // Iterate over each encoding algorithm
              for (TSEncoding encoding : encodingList) {
                Encoder encoder =
                        TSEncodingBuilder.getEncodingBuilder(encoding).getEncoder(dataType);
                Decoder decoder = Decoder.getDecoderByType(encoding, dataType);
                long encodeTime = 0;
                long decodeTime = 0;

                // Iterate over each compression algorithm
                for (CompressionType comp : compressList) {
                  ICompressor compressor = ICompressor.getCompressor(comp);
                  IUnCompressor unCompressor = IUnCompressor.getUnCompressor(comp);
                  double ratio = 0;
                  double compressed_size = 0;

                  long compressTime = 0;
                  long uncompressTime = 0;
                  for (int i = 0; i < repeatTime; i++) {
                    ByteArrayOutputStream buffer = new ByteArrayOutputStream();

                    // test encode time
                    long s = System.nanoTime();
                    for (long val : tmp) encoder.encode(val, buffer);
                    encoder.flush(buffer);
                    long e = System.nanoTime();
                    encodeTime += (e - s);

                    // test compress time
                    byte[] elems = buffer.toByteArray();
                    s = System.nanoTime();
                    byte[] compressed = compressor.compress(elems);
                    e = System.nanoTime();
                    compressTime += (e - s);

                    // test compression ratio and compressed size
                    compressed_size = compressed.length;
                    double ratioTmp =
                            (double) compressed.length / (double) (tmp.size() * Long.BYTES);
                    ratio += ratioTmp;

                    // test uncompress time
                    s = System.nanoTime();
                    byte[] x = unCompressor.uncompress(compressed);
                    e = System.nanoTime();
                    uncompressTime += (e - s);

                    // test decode time
                    ByteBuffer ebuffer = ByteBuffer.wrap(buffer.toByteArray());
                    s = System.nanoTime();
                    while (decoder.hasNext(ebuffer)) {
                      decoder.readInt(ebuffer);
                    }
                    e = System.nanoTime();
                    decodeTime += (e - s);

                    buffer.close();
                  }

                  ratio /= repeatTime;
                  compressed_size /= repeatTime;
                  encodeTime /= repeatTime;
                  decodeTime /= repeatTime;
                  compressTime /= repeatTime;
                  uncompressTime /= repeatTime;

                  // write info to file
                  String[] record = {
                          f.toString(),
                          String.valueOf(index),
                          encoding.toString(),
                          comp.toString(),
                          String.valueOf(encodeTime),
                          String.valueOf(decodeTime),
                          String.valueOf(compressTime),
                          String.valueOf(uncompressTime),
                          String.valueOf(compressed_size),
                          String.valueOf(ratio)
                  };
                  writer.writeRecord(record);
                }
              }
              break;
            }
            case "double":
            {
              TSDataType dataType = TSDataType.DOUBLE;
              ArrayList<Double> tmp = new ArrayList<>();
              data.removeIf(String::isEmpty);
              for (String value : data) {
                tmp.add(Double.valueOf(value));
              }
              // Iterate over each encoding algorithm
              for (TSEncoding encoding : encodingList) {
                Encoder encoder =
                        TSEncodingBuilder.getEncodingBuilder(encoding).getEncoder(dataType);
                Decoder decoder = Decoder.getDecoderByType(encoding, dataType);
                long encodeTime = 0;
                long decodeTime = 0;

                // Iterate over each compression algorithm
                for (CompressionType comp : compressList) {
                  ICompressor compressor = ICompressor.getCompressor(comp);
                  IUnCompressor unCompressor = IUnCompressor.getUnCompressor(comp);
                  long compressTime = 0;
                  long uncompressTime = 0;
                  double ratio = 0;
                  double compressed_size = 0;

                  // repeat many times to test time
                  for (int i = 0; i < repeatTime; i++) {
                    ByteArrayOutputStream buffer = new ByteArrayOutputStream();

                    // test encode time
                    long s = System.nanoTime();
                    for (double val : tmp) encoder.encode(val, buffer);
                    encoder.flush(buffer);
                    long e = System.nanoTime();
                    encodeTime += (e - s);

                    // test compress time
                    byte[] elems = buffer.toByteArray();
                    s = System.nanoTime();
                    byte[] compressed = compressor.compress(elems);
                    e = System.nanoTime();
                    compressTime += (e - s);

                    // test compression ratio and compressed size
                    compressed_size = compressed.length;
                    double ratioTmp =
                            (double) compressed.length / (double) (tmp.size() * Double.BYTES);
                    ratio += ratioTmp;

                    // test uncompress time
                    s = System.nanoTime();
                    byte[] x = unCompressor.uncompress(compressed);
                    e = System.nanoTime();
                    uncompressTime += (e - s);

                    // test decode time
                    ByteBuffer ebuffer = ByteBuffer.wrap(buffer.toByteArray());
                    s = System.nanoTime();
                    int i_de = 0;
                    while (decoder.hasNext(ebuffer)) {
                      double v = decoder.readDouble(ebuffer);
                      //                      if(v!=Double.parseDouble(data.get(i_de))){
                      //                        System.out.println(v);
                      //                        System.out.println(data.get(i_de));
                      //                        System.out.println("noequal");
                      //                        System.out.println(encoding);
                      //                      };
                      //                      i_de++;
                    }
                    e = System.nanoTime();
                    decodeTime += (e - s);

                    buffer.close();
                  }

                  ratio /= repeatTime;
                  compressed_size /= repeatTime;
                  encodeTime /= repeatTime;
                  decodeTime /= repeatTime;
                  compressTime /= repeatTime;
                  uncompressTime /= repeatTime;

                  // write info to file
                  String[] record = {
                          f.toString(),
                          String.valueOf(index),
                          encoding.toString(),
                          comp.toString(),
                          String.valueOf(encodeTime),
                          String.valueOf(decodeTime),
                          String.valueOf(compressTime),
                          String.valueOf(uncompressTime),
                          String.valueOf(compressed_size),
                          String.valueOf(ratio)
                  };
                  writer.writeRecord(record);
                  System.out.println(ratio);
                }
              }
              break;
            }
            case "float":
            {
              TSDataType dataType = TSDataType.FLOAT;
              ArrayList<Float> tmp = new ArrayList<>();
              data.removeIf(String::isEmpty);
              for (int i = 0; i < data.size(); i++) {
                tmp.add(Float.valueOf(data.get(i)));
              }

              // Iterate over each encoding algorithm
              for (TSEncoding encoding : encodingList) {
                Encoder encoder;
                //                if(encoding == TSEncoding.TS_2DIFF){
                //                  Map<String, String> props = null;
                //                  props.put(Encoder.MAX_POINT_NUMBER,
                // String.valueOf(max_precision));
                //                  TSEncodingBuilder.Ts2Diff.setMaxPointNumber(max_precision);
                //                }
                encoder = TSEncodingBuilder.getEncodingBuilder(encoding).getEncoder(dataType);

                Decoder decoder = Decoder.getDecoderByType(encoding, dataType);

                long encodeTime = 0;
                long decodeTime = 0;
                // Iterate over each compression algorithm
                for (CompressionType comp : compressList) {
                  ICompressor compressor = ICompressor.getCompressor(comp);
                  IUnCompressor unCompressor = IUnCompressor.getUnCompressor(comp);
                  long compressTime = 0;
                  long uncompressTime = 0;
                  double ratio = 0;
                  double compressed_size = 0;

                  // repeat many times to test time
                  for (int i = 0; i < repeatTime; i++) {
                    ByteArrayOutputStream buffer = new ByteArrayOutputStream();

                    // test encode time
                    long s = System.nanoTime();
                    for (float val : tmp) {
                      encoder.encode(val, buffer);
                    }
                    encoder.flush(buffer);
                    long e = System.nanoTime();
                    encodeTime += (e - s);

                    // test compress time
                    byte[] elems = buffer.toByteArray();
                    s = System.nanoTime();
                    byte[] compressed = compressor.compress(elems);
                    e = System.nanoTime();
                    compressTime += (e - s);

                    // test compression ratio and compressed size
                    compressed_size += compressed.length;
                    double ratioTmp =
                            (double) compressed.length / (double) (tmp.size() * Float.BYTES);
                    ratio += ratioTmp;

                    // test uncompress time
                    s = System.nanoTime();
                    byte[] x = unCompressor.uncompress(compressed);
                    e = System.nanoTime();
                    uncompressTime += (e - s);

                    // test decode time
                    ByteBuffer ebuffer = ByteBuffer.wrap(buffer.toByteArray());
                    while (decoder.hasNext(ebuffer)) {
                      decoder.readFloat(ebuffer);
                    }
                    e = System.nanoTime();
                    decodeTime += (e - s);

                    buffer.close();
                  }
                  ratio /= repeatTime;
                  compressed_size /= repeatTime;
                  encodeTime /= repeatTime;
                  decodeTime /= repeatTime;
                  compressTime /= repeatTime;
                  uncompressTime /= repeatTime;

                  // write info to file
                  String[] record = {
                          f.toString(),
                          String.valueOf(index),
                          encoding.toString(),
                          comp.toString(),
                          String.valueOf(encodeTime),
                          String.valueOf(decodeTime),
                          String.valueOf(compressTime),
                          String.valueOf(uncompressTime),
                          String.valueOf(compressed_size),
                          String.valueOf(ratio)
                  };
                  writer.writeRecord(record);
                }
              }
              break;
            }
          }
          inputStream = Files.newInputStream(f.toPath());
          loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
        }
        //        if (fileRepeat > repeatTime) break;
        //      break;
      }
      writer.close();
    }
  }
}