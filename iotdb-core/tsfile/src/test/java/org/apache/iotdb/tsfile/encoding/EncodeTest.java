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

    String parent_dir = "/Users/xiaojinzhao/Documents/GitHub/iotdb/iotdb-core/tsfile/src/test/resources/";
    String output_parent_dir = "/Users/xiaojinzhao/Documents/GitHub/encoding-reorder/compression_ratio/sota_ratio";
    String input_parent_dir = parent_dir + "trans_data/";


    ArrayList<String> input_path_list = new ArrayList<>();
    ArrayList<String> output_path_list = new ArrayList<>();
    ArrayList<String> dataset_name = new ArrayList<>();

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

    for (String item : dataset_name) {
      input_path_list.add(input_parent_dir + item);
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

      int repeatTime = 1; // set repeat time

      File file = new File(inputPath);
      File[] tempList = file.listFiles();

      // select encoding algorithms
      TSEncoding[] encodingList = {
        TSEncoding.PLAIN,
        TSEncoding.TS_2DIFF,
        TSEncoding.RLE,
        TSEncoding.SPRINTZ,
        TSEncoding.GORILLA,
        TSEncoding.RLBE,
        //              TSEncoding.RAKE,
        TSEncoding.CHIMP,
        TSEncoding.BUFF
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
      ArrayList<Integer> columnIndexes = new ArrayList<>(); // set the column indexes of compressed
      for (int i = 0; i < 2; i++) {
        columnIndexes.add(i, i);
      }
      int count_csv =0;
      for (File f : tempList) {
        System.out.println(count_csv);
        count_csv ++;
        System.out.println(f);
        InputStream inputStream = Files.newInputStream(f.toPath());
        CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
        ArrayList<String> data = new ArrayList<>();

        for (int index : columnIndexes) {

          loader.readHeaders();
          data.clear();
          while (loader.readRecord()) {
            String v = loader.getValues()[index];
            data.add(v);
          }
          inputStream.close();

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
                byte[] compressed = compressor.compress(elems);

                // test compression ratio and compressed size
                compressed_size += compressed.length;
                double ratioTmp =
                        (double) compressed.length / (double) (tmp.size() * Integer.BYTES);
                ratio += ratioTmp;

                // test uncompress time
                unCompressor.uncompress(compressed);

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

              String[] record = {
                      f.toString(),
                      String.valueOf(index),
                      encoding.toString(),
                      comp.toString(),
                      String.valueOf(encodeTime),
                      String.valueOf(decodeTime),
                      String.valueOf(data.size()),
                      String.valueOf(compressed_size),
                      String.valueOf(ratio)
              };
              System.out.println(ratio);
              writer.writeRecord(record);
            }
          }
          tmp.clear();
          inputStream = Files.newInputStream(f.toPath());
          loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
        }
      }
      writer.close();
    }
  }
}
