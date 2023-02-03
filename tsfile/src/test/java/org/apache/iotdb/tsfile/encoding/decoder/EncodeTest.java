package org.apache.iotdb.tsfile.encoding.decoder;

import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.encoding.encoder.*;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;

public class EncodeTest {

  public static void main(@org.jetbrains.annotations.NotNull String[] args) throws IOException {
    ArrayList<String> input_path_list = new ArrayList<>();
    ArrayList<String> output_path_list = new ArrayList<>();

    input_path_list.add("reorder\\iotdb_test\\Metro-Traffic");
    output_path_list.add(
        "reorder\\result_evaluation\\vary_parameter\\rr_int_ratio\\Metro-Traffic_ratio.csv");
    input_path_list.add("reorder\\iotdb_test\\Nifty-Stocks");
    output_path_list.add(
        "reorder\\result_evaluation\\vary_parameter\\rr_int_ratio\\Nifty-Stocks_ratio.csv");
    input_path_list.add("reorder\\iotdb_test\\USGS-Earthquakes");
    output_path_list.add(
        "reorder\\result_evaluation\\vary_parameter\\rr_int_ratio\\USGS-Earthquakes_ratio.csv");
    input_path_list.add("reorder\\iotdb_test\\Cyber-Vehicle");
    output_path_list.add(
        "reorder\\result_evaluation\\vary_parameter\\rr_int_ratio\\Cyber-Vehicle_ratio.csv");
    input_path_list.add("reorder\\iotdb_test\\TH-Climate");
    output_path_list.add(
        "reorder\\result_evaluation\\vary_parameter\\rr_int_ratio\\TH-Climate_ratio.csv");
    input_path_list.add("reorder\\iotdb_test\\TY-Transport");
    output_path_list.add(
        "reorder\\result_evaluation\\vary_parameter\\rr_int_ratio\\TY-Transport_ratio.csv");
    input_path_list.add("reorder\\iotdb_test\\TY-Fuel");
    output_path_list.add(
        "reorder\\result_evaluation\\vary_parameter\\rr_int_ratio\\TY-Fuel_ratio.csv");
    input_path_list.add("reorder\\iotdb_test\\GW-Magnetic");
    output_path_list.add(
        "reorder\\result_evaluation\\vary_parameter\\rr_int_ratio\\GW-Magnetic_ratio.csv");

    for (int file_i = 0; file_i < input_path_list.size(); file_i++) {
      String inputPath = input_path_list.get(file_i);
      String Output = output_path_list.get(file_i);

      // speed
      int repeatTime = 10; // set repeat time
      String dataTypeName = "int"; // set dataType

      File file = new File(inputPath);
      File[] tempList = file.listFiles();

      // select encoding algorithms
      TSEncoding[] encodingList = {
        TSEncoding.TS_2DIFF,
        TSEncoding.RLE,
        TSEncoding.SPRINTZ,
        TSEncoding.GORILLA,
        TSEncoding.RLBE,
        TSEncoding.RAKE
      };
      // select compression algorithms
      CompressionType[] compressList = {
        CompressionType.UNCOMPRESSED,
      };
      CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

      String[] head = {
        "Input Direction",
        "Column Index",
        "Encoding Algorithm",
        "Compress Algorithm",
        "Encoding Time",
        "Decoding Time",
        "Compress Time",
        "Uncompress Time",
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
        fileRepeat += 1;
        InputStream inputStream = Files.newInputStream(f.toPath());
        CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
        String fileName = f.getAbsolutePath();
        ArrayList<String> data = new ArrayList<>();

        for (int index : columnIndexes) {
          // add a column to "data"
          loader.readHeaders();
          data.clear();
          //        int sb = 0;&&sb<256
          while (loader.readRecord()) {
            data.add(loader.getValues()[index]);
            //          sb++;
          }
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
                      while (decoder.hasNext(ebuffer)) {
                        int tmp_tmp = decoder.readInt(ebuffer);
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
                      String.valueOf(compressTime),
                      String.valueOf(uncompressTime),
                      String.valueOf(data.size()),
                      String.valueOf(compressed_size),
                      String.valueOf(ratio)
                    };
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
                      while (decoder.hasNext(ebuffer)) {
                        decoder.readDouble(ebuffer);
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
            case "float":
              {
                TSDataType dataType = TSDataType.FLOAT;
                ArrayList<Float> tmp = new ArrayList<>();
                for (String value : data) {
                  tmp.add(Float.valueOf(value));
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
      }
      writer.close();
    }
  }
}
