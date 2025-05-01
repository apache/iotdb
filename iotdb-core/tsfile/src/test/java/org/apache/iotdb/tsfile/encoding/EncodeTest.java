package org.apache.iotdb.tsfile.encoding;

import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.encoding.encoder.FloatEncoder;
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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.stream.Stream;

public class EncodeTest {

  public static void main(@NotNull String[] args) throws IOException {

    String parent_dir = "D:/github/xjz17/subcolumn/";

    String output_parent_dir = "D:/encoding-subcolumn/trans_data_result/";
    // String output_parent_dir = parent_dir + "trans_data_result/";

    String input_parent_dir = parent_dir + "trans_data/";

    ArrayList<String> input_path_list = new ArrayList<>();
    ArrayList<String> output_path_list = new ArrayList<>();
    ArrayList<String> dataset_name = new ArrayList<>();

    try (Stream<Path> paths = Files.walk(Paths.get(input_parent_dir))) {
      paths.filter(Files::isDirectory)
          .filter(path -> !path.equals(Paths.get(input_parent_dir)))
          .forEach(dir -> {
            String name = dir.getFileName().toString();
            dataset_name.add(name);
            input_path_list.add(input_parent_dir + name);
            output_path_list.add(output_parent_dir + name + "_ratio.csv");
          });
    }

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
          TSEncoding.PLAIN,
          TSEncoding.TS_2DIFF,
          TSEncoding.RLE,
          TSEncoding.SPRINTZ,
          TSEncoding.GORILLA,
          TSEncoding.RLBE,
          TSEncoding.CHIMP,
      };

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
      ArrayList<Integer> columnIndexes = new ArrayList<>(); // set the column indexes of compressed
      for (int i = 0; i < 2; i++) {
        columnIndexes.add(i, i);
      }
      for (File f : tempList) {
        System.out.println(f);
        InputStream inputStream = Files.newInputStream(f.toPath());
        CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
        ArrayList<String> data = new ArrayList<>();

        for (int index : columnIndexes) {
          if (index == 0) {
            continue;
          }
          int max_precision = 2;
          if (file_i == 3) {
            max_precision = 4;
          } else if (file_i == 4 || file_i == 5) {
            max_precision = 3;
          }
          loader.readHeaders();
          data.clear();
          while (loader.readRecord()) {
            String v = loader.getValues()[index];

            data.add(v);
            if (!v.matches("-?\\d+")) {
              dataTypeName = "float";
            }
          }
          inputStream.close();

          switch (dataTypeName) {
            case "int": {
              TSDataType dataType = TSDataType.INT32; // set TSDataType
              ArrayList<Integer> tmp = new ArrayList<>();
              for (String value : data) {
                tmp.add(Integer.valueOf(value));
              }
              // Iterate over each encoding algorithm
              for (TSEncoding encoding : encodingList) {
                Encoder encoder = TSEncodingBuilder.getEncodingBuilder(encoding).getEncoder(dataType);
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
                    double ratioTmp = (double) compressed.length / (double) (tmp.size() * Integer.BYTES);
                    ratio += ratioTmp;

                    // test uncompress time
                    s = System.nanoTime();
                    unCompressor.uncompress(compressed);
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
                  System.out.println(ratio);
                  writer.writeRecord(record);
                }
              }
              tmp.clear();
              break;
            }

            // write info to file
            case "float": {
              System.out.println("get float");
              TSDataType dataType = TSDataType.FLOAT;
              ArrayList<Float> tmp = new ArrayList<>();
              data.removeIf(String::isEmpty);
              for (String datum : data) {
                tmp.add(Float.valueOf(datum));
              }

              // Iterate over each encoding algorithm
              for (TSEncoding encoding : encodingList) {
                Encoder encoder;
                encoder = TSEncodingBuilder.getEncodingBuilder(encoding).getEncoder(dataType);
                if (encoding == TSEncoding.TS_2DIFF) {
                  encoder = new FloatEncoder(TSEncoding.TS_2DIFF, dataType, max_precision);
                } else if (encoding == TSEncoding.RLE) {
                  encoder = new FloatEncoder(TSEncoding.RLE, dataType, max_precision);
                }

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
                    double ratioTmp = (double) compressed.length / (double) (tmp.size() * Float.BYTES);
                    ratio += ratioTmp;

                    // test uncompress time
                    s = System.nanoTime();
                    unCompressor.uncompress(compressed);
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
                      String.valueOf(data.size()),
                      String.valueOf(compressed_size),
                      String.valueOf(ratio)
                  };
                  System.out.println(ratio);
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
