package org.apache.iotdb.tsfile.encoding.decoder;

//import cn.edu.thu.diq.compression.encoderbuff.BuffEncoder;
import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.encoding.encoderbuff.BuffEncoder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class EncodeTestBuff {

  public static void main(@org.jetbrains.annotations.NotNull String[] args) throws IOException {
    BuffEncoder buffEncoder = new BuffEncoder(18);
    String inputPath = "C:\\Users\\xiaoj\\Desktop\\bufftest";
    String Output = "C:\\Users\\xiaoj\\Desktop\\compressedResultBUFF.csv"; // the direction of output compression ratio and speed


    CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);
    String[] head = {"compressed size","encoded size"};
    writer.writeRecord(head); // write header to output file


    File file = new File(inputPath);
    File[] tempList = file.listFiles();
    for(int columnIndex=0;columnIndex<71;columnIndex++){
      assert tempList != null;
      for (File f : tempList) {
          InputStream inputStream = new FileInputStream(f);
          CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
          String fileName = f.getAbsolutePath();
          ArrayList<String> data = new ArrayList<>();
          loader.readHeaders();
          while (loader.readRecord()) {
            data.add(loader.getValues()[columnIndex]);
          }
          loader.close();
          inputStream.close();
          int len = data.size();
          double[] tmp = new double[len];
          for (int i=0;i<len;i++) {
            tmp[i] = Double.parseDouble(data.get(i));
          }
          System.out.println(len);
  //      System.out.println(tmp[0]);
  //      System.out.println(tmp[1]);
  //      System.out.println(tmp[2]);
          byte[] encodedBytes = buffEncoder.encode(tmp);
          ICompressor.GZIPCompressor compressor = new ICompressor.GZIPCompressor();
          System.out.println(encodedBytes.length);
          byte[] compressed = compressor.compress(encodedBytes);
          System.out.println(compressed.length);

  //      double[] decoded = buffEncoder.decode(encodedBytes);
  //      for (int i=0;i<len;i++) {
  //        String a_str = String.format("%.5f", decoded[i]);
  //        if(tmp[i] != Double.parseDouble(a_str)) System.out.println(i);
  //      }
          String[] record = {String.valueOf(compressed.length),String.valueOf(encodedBytes.length)};
          writer.writeRecord(record);
        }

    }

      writer.close();
  }
}
