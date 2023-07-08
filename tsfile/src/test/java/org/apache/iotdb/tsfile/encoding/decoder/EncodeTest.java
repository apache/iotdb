package org.apache.iotdb.tsfile.encoding.decoder;

import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class EncodeTest {

  @Test
  public void test() throws IOException {
    String[] Inputs = {
      "/home/ubuntu/Datasets/Real-World/Numerical", "/home/ubuntu/Datasets/Synthetic/Numerical"
    };
    String[] Outputs = {
      "/home/ubuntu/Real_Numerical_result.csv", "/home/ubuntu/Synthetic_Numerical_result.csv"
    };

    for (int idx = 0; idx < 2; idx++) {

      String Input = Inputs[idx];
      String Output = Outputs[idx];
      int repeatTime = 5; // set repeat time

      String[] dataTypeNames = {"INT32", "INT64", "FLOAT", "DOUBLE"};
      // select encoding algorithms
      TSEncoding[] encodingList = {
        TSEncoding.PLAIN,
        TSEncoding.TS_2DIFF,
        TSEncoding.RLE,
        TSEncoding.SPRINTZ,
        TSEncoding.GORILLA,
        TSEncoding.RLBE,
        TSEncoding.RAKE,
        TSEncoding.BUFF,
        TSEncoding.CHIMP
      };
      // select compression algorithms
      CompressionType[] compressList = {
        CompressionType.UNCOMPRESSED,
        CompressionType.LZ4,
        CompressionType.GZIP,
        CompressionType.SNAPPY
      };
      String[] head = {
        "Input Direction",
        "Data Type",
        "Encoding Algorithm",
        "Compress Algorithm",
        "Encoding Time",
        "Decoding Time",
        "Compress Time",
        "Uncompress Time",
        "Compressed Size",
        "Compression Ratio"
      };

      CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);
      writer.writeRecord(head); // write header to output file

      for (String dataTypeName : dataTypeNames) {
        String inputPath = Input + "/" + dataTypeName; // the direction of input compressed data
        File file = new File(inputPath);
        File[] tempList = file.listFiles();

        for (File dataset : tempList) {
          File[] temp2List = dataset.listFiles();
          for (File f : temp2List) {
            // fileRepeat += 1;
            InputStream inputStream = new FileInputStream(f);
            CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
            String fileName = f.getAbsolutePath();
            // ArrayList<String> dataIndex = new ArrayList<>();
            ArrayList<String> data = new ArrayList<>();

            loader.readHeaders();
            while (loader.readRecord()) {
              String[] temp = loader.getValues();
              // dataIndex.add(temp[0]);
              data.add(temp[1]);
            }
            loader.close();
            inputStream.close();

            // for (int index : columnIndexes) {
            // // add a column to "data"
            // System.out.println(index);
            // loader.readHeaders();
            // while (loader.readRecord()) {
            // data.add(loader.getValues()[index]);
            // }
            // // loader.close();
            // inputStream.close();

            switch (dataTypeName) {
              case "INT32":
                {
                  TSDataType dataType = TSDataType.INT32; // set TSDataType
                  // ArrayList<Long> tmpIndex = new ArrayList<>();
                  ArrayList<Integer> tmp = new ArrayList<>();
                  // for (String valueIndex : dataIndex) {
                  // tmpIndex.add(Long.valueOf(valueIndex));
                  // }
                  for (String value : data) {
                    tmp.add(Integer.valueOf(value));
                  }
                  // Iterate over each encoding algorithm
                  for (TSEncoding encoding : encodingList) {

                    // Iterate over each compression algorithm
                    for (CompressionType comp : compressList) {
                      long encodeTime = 0;
                      long decodeTime = 0;

                      double ratio = 0;
                      double compressed_size = 0;

                      long compressTime = 0;
                      long uncompressTime = 0;

                      // repeat many times to test time
                      for (int i = 0; i < repeatTime; i++) {
                        // TSEncodingBuilder.getEncodingBuilder(encoding).initFromProps(props);
                        // Encoder encoderIndex =
                        // TSEncodingBuilder.getEncodingBuilder(encoding)
                        // .getEncoder(TSDataType.INT64);
                        // Decoder decoderIndex = Decoder.getDecoderByType(encoding,
                        // TSDataType.INT64);
                        Encoder encoder =
                            TSEncodingBuilder.getEncodingBuilder(encoding).getEncoder(dataType);
                        Decoder decoder = Decoder.getDecoderByType(encoding, dataType);

                        // ICompressor compressorIndex = ICompressor.getCompressor(comp);
                        // IUnCompressor unCompressorIndex = IUnCompressor.getUnCompressor(comp);
                        ICompressor compressor = ICompressor.getCompressor(comp);
                        IUnCompressor unCompressor = IUnCompressor.getUnCompressor(comp);

                        // ByteArrayOutputStream bufferIndex = new ByteArrayOutputStream();
                        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

                        // test encode time
                        long s = System.nanoTime();
                        // for (long valIndex : tmpIndex) encoderIndex.encode(valIndex,
                        // bufferIndex);
                        // encoderIndex.flush(bufferIndex);
                        for (int val : tmp) encoder.encode(val, buffer);
                        encoder.flush(buffer);
                        long e = System.nanoTime();
                        encodeTime += (e - s);

                        // test compress time
                        // byte[] elemsIndex = bufferIndex.toByteArray();
                        byte[] elems = buffer.toByteArray();
                        s = System.nanoTime();
                        // byte[] compressedIndex = compressorIndex.compress(elemsIndex);
                        byte[] compressed = compressor.compress(elems);
                        e = System.nanoTime();
                        compressTime += (e - s);

                        // test compression ratio and compressed size
                        // compressed_size += compressedIndex.length;
                        compressed_size += compressed.length;
                        double ratioTmp =
                            (double) (/* compressedIndex.length + */ compressed.length)
                                / (double)
                                    (
                                    /* tmpIndex.size() * Long.BYTES + */ tmp.size()
                                        * Integer.BYTES);
                        ratio += ratioTmp;

                        // test uncompress time
                        s = System.nanoTime();
                        // byte[] xIndex = unCompressorIndex.uncompress(compressedIndex);
                        byte[] x = unCompressor.uncompress(compressed);
                        e = System.nanoTime();
                        uncompressTime += (e - s);

                        // test decode time
                        // ByteBuffer ebufferIndex = ByteBuffer.wrap(bufferIndex.toByteArray());
                        ByteBuffer ebuffer = ByteBuffer.wrap(buffer.toByteArray());
                        s = System.nanoTime();
                        // while (decoderIndex.hasNext(ebufferIndex)) {
                        // decoderIndex.readLong(ebufferIndex);
                        // }
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

                      String[] record = {
                        f.toString().replaceAll("^/home/ubuntu/", ""),
                        dataTypeName,
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
              case "INT64":
                {
                  TSDataType dataType = TSDataType.INT64; // set TSDataType
                  // ArrayList<Long> tmpIndex = new ArrayList<>();
                  ArrayList<Long> tmp = new ArrayList<>();
                  // for (String valueIndex : dataIndex) {
                  // tmpIndex.add(Long.valueOf(valueIndex));
                  // }
                  for (String value : data) {
                    tmp.add(Long.valueOf(value));
                  }
                  // Iterate over each encoding algorithm
                  for (TSEncoding encoding : encodingList) {

                    // Iterate over each compression algorithm
                    for (CompressionType comp : compressList) {
                      long encodeTime = 0;
                      long decodeTime = 0;

                      double ratio = 0;
                      double compressed_size = 0;

                      long compressTime = 0;
                      long uncompressTime = 0;

                      // repeat many times to test time
                      for (int i = 0; i < repeatTime; i++) {
                        // TSEncodingBuilder.getEncodingBuilder(encoding).initFromProps(props);
                        // Encoder encoderIndex =
                        // TSEncodingBuilder.getEncodingBuilder(encoding)
                        // .getEncoder(TSDataType.INT64);
                        // Decoder decoderIndex = Decoder.getDecoderByType(encoding,
                        // TSDataType.INT64);
                        Encoder encoder =
                            TSEncodingBuilder.getEncodingBuilder(encoding).getEncoder(dataType);
                        Decoder decoder = Decoder.getDecoderByType(encoding, dataType);

                        // ICompressor compressorIndex = ICompressor.getCompressor(comp);
                        // IUnCompressor unCompressorIndex = IUnCompressor.getUnCompressor(comp);
                        ICompressor compressor = ICompressor.getCompressor(comp);
                        IUnCompressor unCompressor = IUnCompressor.getUnCompressor(comp);

                        // ByteArrayOutputStream bufferIndex = new ByteArrayOutputStream();
                        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

                        // test encode time
                        long s = System.nanoTime();
                        // for (long valIndex : tmpIndex) encoderIndex.encode(valIndex,
                        // bufferIndex);
                        // encoderIndex.flush(bufferIndex);
                        for (long val : tmp) encoder.encode(val, buffer);
                        encoder.flush(buffer);
                        long e = System.nanoTime();
                        encodeTime += (e - s);

                        // test compress time
                        // byte[] elemsIndex = bufferIndex.toByteArray();
                        byte[] elems = buffer.toByteArray();
                        s = System.nanoTime();
                        // byte[] compressedIndex = compressorIndex.compress(elemsIndex);
                        byte[] compressed = compressor.compress(elems);
                        e = System.nanoTime();
                        compressTime += (e - s);

                        // test compression ratio and compressed size
                        // compressed_size += compressedIndex.length;
                        compressed_size += compressed.length;
                        double ratioTmp =
                            (double) (/* compressedIndex.length + */ compressed.length)
                                / (double)
                                    (
                                    /* tmpIndex.size() * Long.BYTES + */ tmp.size() * Long.BYTES);
                        ratio += ratioTmp;

                        // test uncompress time
                        s = System.nanoTime();
                        // byte[] xIndex = unCompressorIndex.uncompress(compressedIndex);
                        byte[] x = unCompressor.uncompress(compressed);
                        e = System.nanoTime();
                        uncompressTime += (e - s);

                        // test decode time
                        // ByteBuffer ebufferIndex = ByteBuffer.wrap(bufferIndex.toByteArray());
                        ByteBuffer ebuffer = ByteBuffer.wrap(buffer.toByteArray());
                        s = System.nanoTime();
                        // while (decoderIndex.hasNext(ebufferIndex)) {
                        // decoderIndex.readLong(ebufferIndex);
                        // }
                        while (decoder.hasNext(ebuffer)) {
                          decoder.readLong(ebuffer);
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
                        f.toString().replaceAll("^/home/ubuntu/", ""),
                        dataTypeName,
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
              case "DOUBLE":
                {
                  TSDataType dataType = TSDataType.DOUBLE; // set TSDataType
                  // ArrayList<Long> tmpIndex = new ArrayList<>();
                  ArrayList<Double> tmp = new ArrayList<>();
                  // for (String valueIndex : dataIndex) {
                  // tmpIndex.add(Long.valueOf(valueIndex));
                  // }
                  for (String value : data) {
                    tmp.add(Double.valueOf(value));
                  }
                  // Iterate over each encoding algorithm
                  for (TSEncoding encoding : encodingList) {

                    // Iterate over each compression algorithm
                    for (CompressionType comp : compressList) {
                      long encodeTime = 0;
                      long decodeTime = 0;

                      double ratio = 0;
                      double compressed_size = 0;

                      long compressTime = 0;
                      long uncompressTime = 0;

                      // repeat many times to test time
                      for (int i = 0; i < repeatTime; i++) {
                        // TSEncodingBuilder.getEncodingBuilder(encoding).initFromProps(props);
                        // Encoder encoderIndex =
                        // TSEncodingBuilder.getEncodingBuilder(encoding)
                        // .getEncoder(TSDataType.INT64);
                        // Decoder decoderIndex = Decoder.getDecoderByType(encoding,
                        // TSDataType.INT64);
                        Encoder encoder =
                            TSEncodingBuilder.getEncodingBuilder(encoding).getEncoder(dataType);
                        Decoder decoder = Decoder.getDecoderByType(encoding, dataType);

                        // ICompressor compressorIndex = ICompressor.getCompressor(comp);
                        // IUnCompressor unCompressorIndex = IUnCompressor.getUnCompressor(comp);
                        ICompressor compressor = ICompressor.getCompressor(comp);
                        IUnCompressor unCompressor = IUnCompressor.getUnCompressor(comp);

                        // ByteArrayOutputStream bufferIndex = new ByteArrayOutputStream();
                        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

                        // test encode time
                        long s = System.nanoTime();
                        // for (long valIndex : tmpIndex) encoderIndex.encode(valIndex,
                        // bufferIndex);
                        // encoderIndex.flush(bufferIndex);
                        for (double val : tmp) encoder.encode(val, buffer);
                        encoder.flush(buffer);
                        long e = System.nanoTime();
                        encodeTime += (e - s);

                        // test compress time
                        // byte[] elemsIndex = bufferIndex.toByteArray();
                        byte[] elems = buffer.toByteArray();
                        s = System.nanoTime();
                        // byte[] compressedIndex = compressorIndex.compress(elemsIndex);
                        byte[] compressed = compressor.compress(elems);
                        e = System.nanoTime();
                        compressTime += (e - s);

                        // test compression ratio and compressed size
                        // compressed_size += compressedIndex.length;
                        compressed_size += compressed.length;
                        double ratioTmp =
                            (double) (/* compressedIndex.length + */ compressed.length)
                                / (double)
                                    (
                                    /* tmpIndex.size() * Long.BYTES + */ tmp.size() * Double.BYTES);
                        ratio += ratioTmp;

                        // test uncompress time
                        s = System.nanoTime();
                        // byte[] xIndex = unCompressorIndex.uncompress(compressedIndex);
                        byte[] x = unCompressor.uncompress(compressed);
                        e = System.nanoTime();
                        uncompressTime += (e - s);

                        // test decode time
                        // ByteBuffer ebufferIndex = ByteBuffer.wrap(bufferIndex.toByteArray());
                        ByteBuffer ebuffer = ByteBuffer.wrap(buffer.toByteArray());
                        s = System.nanoTime();
                        // while (decoderIndex.hasNext(ebufferIndex)) {
                        // decoderIndex.readLong(ebufferIndex);
                        // }
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

                      String[] record = {
                        f.toString().replaceAll("^/home/ubuntu/", ""),
                        dataTypeName,
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
              case "FLOAT":
                {
                  TSDataType dataType = TSDataType.FLOAT; // set TSDataType
                  // ArrayList<Long> tmpIndex = new ArrayList<>();
                  ArrayList<Float> tmp = new ArrayList<>();
                  // for (String valueIndex : dataIndex) {
                  // tmpIndex.add(Long.valueOf(valueIndex));
                  // }
                  for (String value : data) {
                    tmp.add(Float.valueOf(value));
                  }
                  // Iterate over each encoding algorithm
                  for (TSEncoding encoding : encodingList) {

                    // Iterate over each compression algorithm
                    for (CompressionType comp : compressList) {
                      long encodeTime = 0;
                      long decodeTime = 0;

                      double ratio = 0;
                      double compressed_size = 0;

                      long compressTime = 0;
                      long uncompressTime = 0;

                      // repeat many times to test time
                      for (int i = 0; i < repeatTime; i++) {
                        // TSEncodingBuilder.getEncodingBuilder(encoding).initFromProps(props);
                        // Encoder encoderIndex =
                        // TSEncodingBuilder.getEncodingBuilder(encoding)
                        // .getEncoder(TSDataType.INT64);
                        // Decoder decoderIndex = Decoder.getDecoderByType(encoding,
                        // TSDataType.INT64);
                        Encoder encoder =
                            TSEncodingBuilder.getEncodingBuilder(encoding).getEncoder(dataType);
                        Decoder decoder = Decoder.getDecoderByType(encoding, dataType);

                        // ICompressor compressorIndex = ICompressor.getCompressor(comp);
                        // IUnCompressor unCompressorIndex = IUnCompressor.getUnCompressor(comp);
                        ICompressor compressor = ICompressor.getCompressor(comp);
                        IUnCompressor unCompressor = IUnCompressor.getUnCompressor(comp);

                        // ByteArrayOutputStream bufferIndex = new ByteArrayOutputStream();
                        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

                        // test encode time
                        long s = System.nanoTime();
                        // for (long valIndex : tmpIndex) encoderIndex.encode(valIndex,
                        // bufferIndex);
                        // encoderIndex.flush(bufferIndex);
                        for (float val : tmp) encoder.encode(val, buffer);
                        encoder.flush(buffer);
                        long e = System.nanoTime();
                        encodeTime += (e - s);

                        // test compress time
                        // byte[] elemsIndex = bufferIndex.toByteArray();
                        byte[] elems = buffer.toByteArray();
                        s = System.nanoTime();
                        // byte[] compressedIndex = compressorIndex.compress(elemsIndex);
                        byte[] compressed = compressor.compress(elems);
                        e = System.nanoTime();
                        compressTime += (e - s);

                        // test compression ratio and compressed size
                        // compressed_size += compressedIndex.length;
                        compressed_size += compressed.length;
                        double ratioTmp =
                            (double) (/* compressedIndex.length + */ compressed.length)
                                / (double)
                                    (
                                    /* tmpIndex.size() * Long.BYTES + */ tmp.size() * Float.BYTES);
                        ratio += ratioTmp;

                        // test uncompress time
                        s = System.nanoTime();
                        // byte[] xIndex = unCompressorIndex.uncompress(compressedIndex);
                        byte[] x = unCompressor.uncompress(compressed);
                        e = System.nanoTime();
                        uncompressTime += (e - s);

                        // test decode time
                        // ByteBuffer ebufferIndex = ByteBuffer.wrap(bufferIndex.toByteArray());
                        ByteBuffer ebuffer = ByteBuffer.wrap(buffer.toByteArray());
                        s = System.nanoTime();
                        // while (decoderIndex.hasNext(ebufferIndex)) {
                        // decoderIndex.readLong(ebufferIndex);
                        // }
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

                      String[] record = {
                        f.toString().replaceAll("^/home/ubuntu/", ""),
                        dataTypeName,
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
          }

          // if (fileRepeat > repeatTime) break;
        }
      }
      writer.close();
    }
  }
}
