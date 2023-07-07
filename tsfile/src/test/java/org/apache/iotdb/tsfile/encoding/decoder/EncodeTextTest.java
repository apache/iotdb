package org.apache.iotdb.tsfile.encoding.decoder;

import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;

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
import java.util.Arrays;

public class EncodeTextTest {

  @Test
  public void test() throws IOException {
    String[] Inputs = {
      "/home/ubuntu/Datasets/Real-world/Text", "/home/ubuntu/Datasets/Synthetic/Text"
    };
    String[] Outputs = {
      "/home/ubuntu/Real_Text_result.csv", "/home/ubuntu/Synthetic_Text_result.csv"
    };

    for (int idx = 0; idx < 2; idx++) {

      String Input = Inputs[idx];
      String Output = Outputs[idx];
      int repeatTime = 3; // set repeat time

      // select encoding algorithms
      TSEncoding[] encodingList = {
        TSEncoding.HUFFMAN,
        TSEncoding.MTF,
        TSEncoding.BW,
        TSEncoding.DICTIONARY,
        TSEncoding.RLE,
        TSEncoding.AC,
        TSEncoding.PLAIN
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

      String inputPath = Input; // the direction of input compressed data
      File file = new File(inputPath);
      File[] tempList = file.listFiles();

      for (File dataset : tempList) {
        File[] temp2List = dataset.listFiles();
        ArrayList<File> temp3List = new ArrayList<>();
        if (idx == 0) temp3List = new ArrayList<>(Arrays.asList(temp2List));
        else {
          for (File seed : temp2List) temp3List.addAll(Arrays.asList(seed.listFiles()));
        }
        for (File f : temp3List) {
          System.out.println(f.toString());
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
            if (temp.length < 2) break;
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
          TSDataType dataType = TSDataType.TEXT; // set TSDataType
          // ArrayList<Long> tmpIndex = new ArrayList<>();
          ArrayList<Binary> tmp = new ArrayList<>();
          // for (String valueIndex : dataIndex) {
          // tmpIndex.add(Long.valueOf(valueIndex));
          // }
          for (String value : data) {
            tmp.add(Binary.valueOf(value));
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
                // for (long valIndex : tmpIndex) encoderIndex.encode(valIndex, bufferIndex);
                // encoderIndex.flush(bufferIndex);
                int ori_size = 0;
                for (Binary val : tmp) {
                  ori_size += val.getLength();
                  encoder.encode(val, buffer);
                }
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
                        / (double) (/* tmpIndex.size() * Long.BYTES + */ ori_size);
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
                  decoder.readBinary(ebuffer);
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
                "TEXT",
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
        }

        // if (fileRepeat > repeatTime) break;
      }
      writer.close();
    }
  }
}
