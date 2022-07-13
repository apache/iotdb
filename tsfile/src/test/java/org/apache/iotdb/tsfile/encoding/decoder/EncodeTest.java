package org.apache.iotdb.tsfile.encoding.decoder;

import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.encoding.encoder.*;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.encoding.DST.DST;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.apache.iotdb.tsfile.utils.Binary;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class EncodeTest {

    public static void main(java.lang.String[] args) throws IOException {

        java.lang.String inputPath = "D:\\本科\\大一\\SRT\\data\\CSSC\\floattest";
        java.lang.String Output = "D:\\本科\\大一\\SRT\\data\\CSSC\\results.csv";
        if (args.length >= 2) inputPath = args[1];
        if (args.length >= 3) Output = args[2];

        File file = new File(inputPath);
        File[] tempList = file.listFiles();
        TSEncoding[] schemeList = {
                TSEncoding.BUCKET
                //TSEncoding.BUCKET
//      TSEncoding.SPRINTZ,  TSEncoding.GORILLA, TSEncoding.RLBE, TSEncoding.RAKE
        };
        //        TSEncoding[] schemeList = { TSEncoding.RLBE,TSEncoding.PLAIN};
        //        CompressionType[] compressList = {CompressionType.LZ4, CompressionType.GZIP,
        // CompressionType.SNAPPY};
        CompressionType[] compressList = {CompressionType.GZIP};//, CompressionType.LZ4, CompressionType.GZIP, CompressionType.SNAPPY}; //
        CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

        String[] head = {
                "Filename",
                "Encoding",
                "Compress",
//      "Encoding Time",
//      "Compress Time",
//      "Uncompress Time",
//      "Decoding Time",
                "Compression Ratio"
        };
        writer.writeRecord(head);
        int repeatTime = 1;

        assert tempList != null;
        int fileRepeat = 0;

        int column = -1;
        long originalSize = 0;
        long compressedSize = 0;
        for (File f : tempList) {
            column++;
            //System.out.println(f);
            fileRepeat += 1;
            InputStream inputStream = new FileInputStream(f);
            CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
            java.lang.String fileName = f.getAbsolutePath();
            ArrayList<java.lang.String> data = new ArrayList<>();
            TSDataType dataType = TSDataType.FLOAT;

            loader.readHeaders();
            int firstline = 0;
            int count = 0;
            while (loader.readRecord()) {
                if (0 == firstline) {
                    firstline = 1;
                    continue;
                }
                count += 1;
//        if(count > 100000)
//          break;
                data.add(loader.getValues()[0]);
            }
            loader.close();
            inputStream.close();

            dataType = TSDataType.FLOAT;
            ArrayList<Float> tmp = new ArrayList<>();
            for (java.lang.String value : data) {
                tmp.add(Float.valueOf(value));
            }

            for (int kk = 32; kk <= 32; kk*=2) {
//                System.out.print("kk: ");
//                System.out.println(kk);
                int col = 0;
                for (TSEncoding scheme : schemeList) {
                    Encoder encoder = TSEncodingBuilder.getEncodingBuilder(scheme).getEncoder(dataType);

                    // Decoder decoder = Decoder.getDecoderByType(scheme, dataType);
                    long encodeTime = 0;
                    long decodeTime = 0;
                    for (CompressionType comp : compressList) {
                        ICompressor compressor = ICompressor.getCompressor(comp);
                        IUnCompressor unCompressor = IUnCompressor.getUnCompressor(comp);
                        long compressTime = 0;
                        long uncompressTime = 0;
                        double ratio = 0;
                        for (int i = 0; i < repeatTime; i++) {
                            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                            long s = System.nanoTime();
                            for (Float val : tmp) {
                                encoder.encode(val, buffer);
                            }
                            long e = System.nanoTime();
                            encodeTime += (e - s);
                            //System.out.println("encodeTime:");
                            //System.out.println(e - s);
                            encoder.flush(buffer);
                            byte[] elems = buffer.toByteArray();
                            s = System.nanoTime();
                            byte[] compressed = compressor.compress(elems);
                            e = System.nanoTime();
                            compressTime += (e - s);
//                            System.out.println(scheme);
//                            System.out.println("compressed length:");
                            System.out.println(compressed.length);

                            compressedSize += compressed.length;
                            originalSize += f.length();
                            double ratioTmp = (double) compressed.length / f.length();//(tmp.size() * 4);
                            ratio += ratioTmp;
//                            System.out.println(ratio);
//              s = System.nanoTime();
//              byte[] x = unCompressor.uncompress(compressed);
//              e = System.nanoTime();
//              uncompressTime += (e - s);
//
//              ByteBuffer ebuffer = ByteBuffer.wrap(buffer.toByteArray());
//              s = System.nanoTime();
//              while (decoder.hasNext(ebuffer)) {
//                decoder.readInt(ebuffer);
//              }
//              e = System.nanoTime();
//              decodeTime += (e - s);
                            buffer.close();
                        }
                        ratio /= repeatTime;
                        encodeTime = encodeTime / ((long) repeatTime);
                        decodeTime = decodeTime / ((long) repeatTime);
                        //                        compressTime = compressTime / ((long) repeatTime);
                        //                        uncompressTime = uncompressTime / ((long) repeatTime);
                        // System.out.println(scheme.toString());
                        //                        System.out.println(String.valueOf(encodeTime));
//          String[] record = {
//                 // String.valueOf(column),
//                  scheme.toString(),
//                  comp.toString(),
////              String.valueOf(encodeTime),
////              "",
////              "",
////              String.valueOf(decodeTime),
//                 // String.valueOf(ratio)
//          };

                        //                        String[] record = {scheme.toString(), comp.toString(),
                        // String.valueOf(encodeTime), String.valueOf(compressTime),
                        //                                String.valueOf(uncompressTime),
                        // String.valueOf(decodeTime), String.valueOf(ratio)};
                        //  writer.writeRecord(record);
                    }
                }
            }
        }
        System.out.println("total compressed size:");
        System.out.println(compressedSize);
        System.out.println("total original size:");
        System.out.println(originalSize);
        System.out.println((double) compressedSize / originalSize);
    }
}


//------------------

//      if (fileName.contains("int")) {
//        dataType = TSDataType.INT32;
//        ArrayList<Integer> tmp = new ArrayList<>();
//        for (String value : data) {
//          tmp.add(Integer.valueOf(value));
//        }
//        for (TSEncoding scheme : schemeList) {
//          Encoder encoder = TSEncodingBuilder.getEncodingBuilder(scheme).getEncoder(dataType);
//          Decoder decoder = Decoder.getDecoderByType(scheme, dataType);
//          long encodeTime = 0;
//          long decodeTime = 0;
//          for (CompressionType comp : compressList) {
//            ICompressor compressor = ICompressor.getCompressor(comp);
//            IUnCompressor unCompressor = IUnCompressor.getUnCompressor(comp);
//            long compressTime = 0;
//            long uncompressTime = 0;
//            double ratio = 0;
//            for (int i = 0; i < repeatTime; i++) {
//              ByteArrayOutputStream buffer = new ByteArrayOutputStream();
//              long s = System.nanoTime();
//              for (int val : tmp) encoder.encode(val, buffer);
//              long e = System.nanoTime();
//              encodeTime += (e - s);
//              //System.out.println("encodeTime:");
//              //System.out.println(e - s);
//              encoder.flush(buffer);
//
//              byte[] elems = buffer.toByteArray();
//              s = System.nanoTime();
//              byte[] compressed = compressor.compress(elems);
//              e = System.nanoTime();
//              compressTime += (e - s);
//
//              double ratioTmp = (double) compressed.length / f.length();//(tmp.size() * 4);
//              ratio += ratioTmp;
//              System.out.println(ratio);
////              s = System.nanoTime();
////              byte[] x = unCompressor.uncompress(compressed);
////              e = System.nanoTime();
////              uncompressTime += (e - s);
////
////              ByteBuffer ebuffer = ByteBuffer.wrap(buffer.toByteArray());
////              s = System.nanoTime();
////              while (decoder.hasNext(ebuffer)) {
////                decoder.readInt(ebuffer);
////              }
////              e = System.nanoTime();
////              decodeTime += (e - s);
//              buffer.close();
//            }
//            ratio /= repeatTime;
//            encodeTime = encodeTime / ((long) repeatTime);
//            decodeTime = decodeTime / ((long) repeatTime);
//            //                        compressTime = compressTime / ((long) repeatTime);
//            //                        uncompressTime = uncompressTime / ((long) repeatTime);
//            // System.out.println(scheme.toString());
//            //                        System.out.println(String.valueOf(encodeTime));
//            String[] record = {
//              String.valueOf(column),
//              scheme.toString(),
//              comp.toString(),
////              String.valueOf(encodeTime),
////              "",
////              "",
////              String.valueOf(decodeTime),
//              String.valueOf(ratio)
//            };
//
//            //                        String[] record = {scheme.toString(), comp.toString(),
//            // String.valueOf(encodeTime), String.valueOf(compressTime),
//            //                                String.valueOf(uncompressTime),
//            // String.valueOf(decodeTime), String.valueOf(ratio)};
//            writer.writeRecord(record);
//          }
//        }
//      }
//      else if (fileName.contains("long")) {
//        dataType = TSDataType.INT64;
//        ArrayList<Long> tmp = new ArrayList<>();
//        for (String value : data) {
//          tmp.add(Long.valueOf(value));
//        }
//        for (TSEncoding scheme : schemeList) {
//          Encoder encoder = TSEncodingBuilder.getEncodingBuilder(scheme).getEncoder(dataType);
//          Decoder decoder = Decoder.getDecoderByType(scheme, dataType);
//          long encodeTime = 0;
//          long decodeTime = 0;
//          for (CompressionType comp : compressList) {
//            ICompressor compressor = ICompressor.getCompressor(comp);
//            IUnCompressor unCompressor = IUnCompressor.getUnCompressor(comp);
//            long compressTime = 0;
//            long uncompressTime = 0;
//            double ratio = 0;
//            for (int i = 0; i < repeatTime; i++) {
//              ByteArrayOutputStream buffer = new ByteArrayOutputStream();
////              long s = System.nanoTime();
//              for (long val : tmp) encoder.encode(val, buffer);
////              long e = System.nanoTime();
////              encodeTime += (e - s);
////              System.out.println("encodeTime:");
////              System.out.println(e - s);
//              encoder.flush(buffer);
//
//              byte[] elems = buffer.toByteArray();
////              s = System.nanoTime();
//              byte[] compressed = compressor.compress(elems);
////              e = System.nanoTime();
////              compressTime += (e - s);
//
//              double ratioTmp = (double)compressed.length / (tmp.size() * Long.BYTES);
//              //System.out.println(tmp.size());
//              ratio += ratioTmp;
//
////              s = System.nanoTime();
//              byte[] x = unCompressor.uncompress(compressed);
////              e = System.nanoTime();
////              uncompressTime += (e - s);
//
//              ByteBuffer ebuffer = ByteBuffer.wrap(buffer.toByteArray());
////              s = System.nanoTime();
//              while (decoder.hasNext(ebuffer)) {
//                decoder.readLong(ebuffer);
//              }
////              e = System.nanoTime();
////              decodeTime += (e - s);
//              buffer.close();
//            }
//            ratio /= repeatTime;
////            encodeTime = encodeTime / ((long) repeatTime);
////            decodeTime = decodeTime / ((long) repeatTime);
//            //                        compressTime = compressTime / ((long) repeatTime);
//            //                        uncompressTime = uncompressTime / ((long) repeatTime);
//            System.out.println(scheme.toString());
//            System.out.println(String.valueOf(ratio));
//            String[] record = {
//              scheme.toString(),
//              comp.toString(),
////              String.valueOf(encodeTime),
////              "",
////              "",
////              String.valueOf(decodeTime),
//              String.valueOf(ratio)
//            };
//
//            //                        String[] record = {scheme.toString(), comp.toString(),
//            // String.valueOf(encodeTime), String.valueOf(compressTime),
//            //                                String.valueOf(uncompressTime),
//            // String.valueOf(decodeTime), String.valueOf(ratio)};
//            writer.writeRecord(record);
//          }
//        }
//      }
//      else if (fileName.contains("double")) {
//        dataType = TSDataType.DOUBLE;
//        ArrayList<Double> tmp = new ArrayList<>();
//        for (String value : data) {
//          tmp.add(Double.valueOf(value));
//        }
//        for (TSEncoding scheme : schemeList) {
//          Encoder encoder = TSEncodingBuilder.getEncodingBuilder(scheme).getEncoder(dataType);
//          Decoder decoder = Decoder.getDecoderByType(scheme, dataType);
//          long encodeTime = 0;
//          long decodeTime = 0;
//          for (CompressionType comp : compressList) {
//            ICompressor compressor = ICompressor.getCompressor(comp);
//            IUnCompressor unCompressor = IUnCompressor.getUnCompressor(comp);
//            long compressTime = 0;
//            long uncompressTime = 0;
//            double ratio = 0;
//            for (int i = 0; i < repeatTime; i++) {
//              ByteArrayOutputStream buffer = new ByteArrayOutputStream();
//              long s = System.nanoTime();
//              for (double val : tmp) encoder.encode(val, buffer);
//              long e = System.nanoTime();
//              encodeTime += (e - s);
//              System.out.println("encodeTime:");
//              System.out.println(e - s);
//              encoder.flush(buffer);
//
//              byte[] elems = buffer.toByteArray();
//              s = System.nanoTime();
//              byte[] compressed = compressor.compress(elems);
//              e = System.nanoTime();
//              compressTime += (e - s);
//
//              int ratioTmp = compressed.length / (tmp.size() * Long.BYTES);
//              ratio += ratioTmp;
//
//              s = System.nanoTime();
//              byte[] x = unCompressor.uncompress(compressed);
//              e = System.nanoTime();
//              uncompressTime += (e - s);
//
//              ByteBuffer ebuffer = ByteBuffer.wrap(buffer.toByteArray());
//              s = System.nanoTime();
//              while (decoder.hasNext(ebuffer)) {
//                decoder.readDouble(ebuffer);
//              }
//              e = System.nanoTime();
//              decodeTime += (e - s);
//              buffer.close();
//            }
//            ratio /= repeatTime;
//            encodeTime = encodeTime / ((long) repeatTime);
//            decodeTime = decodeTime / ((long) repeatTime);
//            //                        compressTime = compressTime / ((long) repeatTime);
//            //                        uncompressTime = uncompressTime / ((long) repeatTime);
//            System.out.println(scheme.toString());
//            //                        System.out.println(String.valueOf(encodeTime));
//            String[] record = {
//              scheme.toString(),
//              comp.toString(),
//              String.valueOf(encodeTime),
//              "",
//              "",
//              String.valueOf(decodeTime),
//              String.valueOf(ratio)
//            };
//
//            //                        String[] record = {scheme.toString(), comp.toString(),
//            // String.valueOf(encodeTime), String.valueOf(compressTime),
//            //                                String.valueOf(uncompressTime),
//            // String.valueOf(decodeTime), String.valueOf(ratio)};
//            writer.writeRecord(record);
//          }
//        }
//      }
//      else if (fileName.contains("float")) {
//        dataType = TSDataType.FLOAT;
//        ArrayList<Float> tmp = new ArrayList<>();
//        for (String value : data) {
//          tmp.add(Float.valueOf(value));
//        }
//        //System.out.println(tmp);
//        for (TSEncoding scheme : schemeList) {
//          Encoder encoder = TSEncodingBuilder.getEncodingBuilder(scheme).getEncoder(dataType);
//          Decoder decoder = Decoder.getDecoderByType(scheme, dataType);
////          long encodeTime = 0;
////          long decodeTime = 0;
//          for (CompressionType comp : compressList) {
//            ICompressor compressor = ICompressor.getCompressor(comp);
//            IUnCompressor unCompressor = IUnCompressor.getUnCompressor(comp);
////            long compressTime = 0;
////            long uncompressTime = 0;
//            double ratio = 0;
//            for (int i = 0; i < repeatTime; i++) {
//              ByteArrayOutputStream buffer = new ByteArrayOutputStream();
////              long s = System.nanoTime();
//              for (float val : tmp) encoder.encode(val, buffer);
////              long e = System.nanoTime();
////              encodeTime += (e - s);
////              System.out.println("encodeTime:");
////              System.out.println(e - s);
//              encoder.flush(buffer);
//
//              byte[] elems = buffer.toByteArray();
////              s = System.nanoTime();
//              byte[] compressed = compressor.compress(elems);
////              e = System.nanoTime();
////              compressTime += (e - s);
////              System.out.println( compressed.length);
//
//
//              float ratioTmp = (float) compressed.length / (float)f.length();// (tmp.size() * Integer.BYTES);
//              ratio += ratioTmp;
//
////              s = System.nanoTime();
////              byte[] x = unCompressor.uncompress(compressed);
//////              e = System.nanoTime();
//////              uncompressTime += (e - s);
////
////              ByteBuffer ebuffer = ByteBuffer.wrap(buffer.toByteArray());
//////              s = System.nanoTime();
////              while (decoder.hasNext(ebuffer)) {
////                decoder.readFloat(ebuffer);
////              }
////              e = System.nanoTime();
////              decodeTime += (e - s);
//              buffer.close();
//            }
//            //System.out.println(ratio);
//            ratio /= repeatTime;
////            encodeTime = encodeTime / ((long) repeatTime);
////            decodeTime = decodeTime / ((long) repeatTime);
//            //                        compressTime = compressTime / ((long) repeatTime);
//            //                        uncompressTime = uncompressTime / ((long) repeatTime);
//            //System.out.println(scheme.toString());
//            //                        System.out.println(String.valueOf(encodeTime));
//            String[] record = {
//              String.valueOf(column),
//              scheme.toString(),
//              comp.toString(),
////              String.valueOf(encodeTime),
////              "",
////              "",
////              String.valueOf(decodeTime),
//              String.valueOf(ratio)
//            };
//
//            //                        String[] record = {scheme.toString(), comp.toString(),
//            // String.valueOf(encodeTime), String.valueOf(compressTime),
//            //                                String.valueOf(uncompressTime),
//            // String.valueOf(decodeTime), String.valueOf(ratio)};
//            writer.writeRecord(record);
//          }
//        }
//      }
////      else if (fileName.contains("text")) {
////        dataType = TSDataType.FLOAT;
////      }
//
//      //if (fileRepeat > 2) break;
//    }
//    writer.close();
//  }
//}

//-------------------------------------------------------------
//                for (String value : data) {
//                    tmp.add(Integer.valueOf(value));
//                }
//                for (TSEncoding scheme : schemeList) {
//                    Encoder encoder =
// TSEncodingBuilder.getEncodingBuilder(scheme).getEncoder(dataType);
//                    Decoder decoder = Decoder.getDecoderByType(scheme, dataType);
//                    long encodeTime = 0;
//                    long decodeTime = 0;
//                    for (CompressionType comp : compressList) {
//                        ICompressor compressor = ICompressor.getCompressor(comp);
//                        IUnCompressor unCompressor = IUnCompressor.getUnCompressor(comp);
//                        long compressTime = 0;
//                        long uncompressTime = 0;
//                        double ratio = tmp.size() * Integer.BYTES;
//
//                        for(int i=0;i<repeatTime;i++){
//
//                            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
//
//                            long s = System.nanoTime();
//                            for (int val : tmp) encoder.encode(val, buffer);
//                            long e = System.nanoTime();
//                            System.out.println("e-s:");
//                            System.out.println(e-s);
//                            encodeTime += (e - s);
//                            encoder.flush(buffer);
//
//                            byte[] elems = buffer.toByteArray();
//                            s = System.nanoTime();
//                            byte[] compressed = compressor.compress(elems);
//                            e = System.nanoTime();
//                            compressTime += (e - s);
//
//                            int ratioTmp = compressed.length / (tmp.size() * Long.BYTES);
//                            ratio += ratioTmp;
//
//                            s = System.nanoTime();
//                            byte[] x = unCompressor.uncompress(compressed);
//                            e = System.nanoTime();
//                            uncompressTime += (e - s);
//
//                            ByteBuffer ebuffer = ByteBuffer.wrap(x);
//                            s = System.nanoTime();
//                            while(decoder.hasNext(ebuffer)){
//                                decoder.readInt(ebuffer);
//                            }
//                            e = System.nanoTime();
//                            decodeTime += (e - s);
//                            buffer.close();
//                        }
////                        System.out.println(encodeTime);
//                        ratio /= repeatTime;
//                        encodeTime = encodeTime * 2 / ((long) repeatTime);
//                        decodeTime = decodeTime * 2 / ((long)  repeatTime);
//                        compressTime = compressTime * 2 / ((long) repeatTime);
//                        uncompressTime = uncompressTime * 2 / ((long) repeatTime);
//                        System.out.println(scheme.toString());
////                        System.out.println(String.valueOf(encodeTime));
////                        String[] record = {scheme.toString(), comp.toString(),
// String.valueOf(encodeTime), "",
////                                "", String.valueOf(decodeTime), String.valueOf(ratio)};
//
//                        String[] record = {scheme.toString(), comp.toString(),
// String.valueOf(encodeTime), String.valueOf(compressTime),
//                                String.valueOf(uncompressTime), String.valueOf(decodeTime),
// String.valueOf(ratio)};
//                        writer.writeRecord(record);
//                    }
//                }
//            else if (fileName.contains("double")) {
//                ArrayList<Double> tmp = new ArrayList<>();
//                dataType = TSDataType.DOUBLE;
//                for (String value : data) {
//                    tmp.add(Double.valueOf(value));
//                }
//                for (TSEncoding scheme : schemeList) {
//                    Encoder encoder =
// TSEncodingBuilder.getEncodingBuilder(scheme).getEncoder(dataType);
//                    Decoder decoder = Decoder.getDecoderByType(scheme, dataType);
//                    long encodeTime = 0;
//                    long decodeTime = 0;
//                    for (int i = 0; i < repeatTime; i++) {
//                        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
//                        long s = System.nanoTime();
//                        for (double val : tmp) encoder.encode(val, buffer);
//                        long e = System.nanoTime();
//                        encodeTime += (e - s);
//
//                        ByteBuffer ebuffer = ByteBuffer.wrap(buffer.toByteArray());
//                        s = System.nanoTime();
//                        for (int c = 0; c < tmp.size(); c++) decoder.readDouble(ebuffer);
//                        e = System.nanoTime();
//                        decodeTime += (e - s);
//                    }
//
//                    encodeTime /= tmp.size();
//                    decodeTime /= tmp.size();
//                    String[] record = {scheme.toString(), "DOUBLE", String.valueOf(encodeTime),
// String.valueOf(decodeTime)};
//                    writer.writeRecord(record);
//                }
//            }
//            else if (fileName.contains("float")) {
//                ArrayList<Float> tmp = new ArrayList<>();
//                dataType = TSDataType.FLOAT;
//                for (String value : data) {
//                    tmp.add(Float.valueOf(value));
//                }
//                System.out.println(tmp);
//                for (TSEncoding scheme : schemeList) {
//                    Encoder encoder =
// TSEncodingBuilder.getEncodingBuilder(scheme).getEncoder(dataType);
//                    Decoder decoder = Decoder.getDecoderByType(scheme, dataType);
//                    long encodeTime = 0;
//                    long decodeTime = 0;
//                    for (int i = 0; i < repeatTime; i++) {
//                        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
//                        long s = System.nanoTime();
//                        for (float val : tmp) encoder.encode(val, buffer);
//                        long e = System.nanoTime();
//                        encodeTime += (e - s);
//
//                        ByteBuffer ebuffer = ByteBuffer.wrap(buffer.toByteArray());
//                        s = System.nanoTime();
//                        for (int c = 0; c < tmp.size(); c++) decoder.readFloat(ebuffer);
//                        e = System.nanoTime();
//                        decodeTime += (e - s);
//                    }
//
//                    encodeTime /= tmp.size();
//                    decodeTime /= tmp.size();
//                    String[] record = {scheme.toString(), "FLOAT", String.valueOf(encodeTime),
// String.valueOf(decodeTime)};
//                    writer.writeRecord(record);
//                }
//            }
//            else if (fileName.contains("text")) {
//                ArrayList<Byte> tmp = new ArrayList<>();
//                dataType = TSDataType.FLOAT;
//                for (String value : data) {
//                    tmp.add(Byte.valueOf(value));
//                }
//                for (TSEncoding scheme : schemeList) {
//                    Encoder encoder =
// TSEncodingBuilder.getEncodingBuilder(scheme).getEncoder(dataType);
//                    Decoder decoder = Decoder.getDecoderByType(scheme, dataType);
//                    long encodeTime = 0;
//                    long decodeTime = 0;
//                    for (int i = 0; i < repeatTime; i++) {
//                        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
//                        long s = System.nanoTime();
//                        for (byte val : tmp) encoder.encode(val, buffer);
//                        long e = System.nanoTime();
//                        encodeTime += (e - s);
//
//                        ByteBuffer ebuffer = ByteBuffer.wrap(buffer.toByteArray());
//                        s = System.nanoTime();
//                        for (int c = 0; c < tmp.size(); c++) decoder.readBinary(ebuffer);
//                        e = System.nanoTime();
//                        decodeTime += (e - s);
//                    }
//
//                    encodeTime /= tmp.size();
//                    decodeTime /= tmp.size();
//                    String[] record = {scheme.toString(), "TEXT", String.valueOf(encodeTime),
// String.valueOf(decodeTime)};
//                    writer.writeRecord(record);
//                }
//            } //else throw new NotImplementedException();
//            //System.gc();
