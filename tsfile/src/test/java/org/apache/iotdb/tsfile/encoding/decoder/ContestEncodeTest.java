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

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class ContestEncodeTest {

    public static void main(@org.jetbrains.annotations.NotNull String[] args) throws IOException {
        String inputPath = "C:\\Users\\xiaoj\\Desktop\\float",
                Output = "C:\\Users\\xiaoj\\Desktop\\compressedResult.csv";
        if (args.length >= 2) inputPath = args[1];
        if (args.length >= 3) Output = args[2];
        long s = System.nanoTime();
        File file = new File(inputPath);
        File[] tempList = file.listFiles();
        TSEncoding[] schemeList = {
//                TSEncoding.PLAIN,
                TSEncoding.TS_2DIFF
//                TSEncoding.RLE,
//                TSEncoding.SPRINTZ,  TSEncoding.GORILLA, TSEncoding.RLBE, TSEncoding.RAKE
        };
        //        CompressionType[] compressList = {CompressionType.LZ4, CompressionType.GZIP,
        // CompressionType.SNAPPY};
        CompressionType[] compressList = {CompressionType.UNCOMPRESSED}; //
        CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

        String[] head = {
                "Encoding",
                "Compress",
                "Compression Ratio"
        };
        writer.writeRecord(head);

        assert tempList != null;
        double ratio = 0;
        for (File f : tempList) {
//      System.out.println(f);
            InputStream inputStream = new FileInputStream(f);
            CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
            String fileName = f.getAbsolutePath();
            ArrayList<String> data = new ArrayList<>();
            TSDataType dataType = TSDataType.INT32;

            loader.readHeaders();
            while (loader.readRecord()) {
                data.add(loader.getValues()[1]);
            }
            loader.close();
            inputStream.close();
            if (fileName.contains("float")) {
                dataType = TSDataType.FLOAT;
                ArrayList<Float> tmp = new ArrayList<>();
                for (String value : data) {
                    tmp.add(Float.valueOf(value));
                }
                for (TSEncoding scheme : schemeList) {
                    Encoder encoder = TSEncodingBuilder.getEncodingBuilder(scheme).getEncoder(dataType);
                    Decoder decoder = Decoder.getDecoderByType(scheme, dataType);
                    for (CompressionType comp : compressList) {
                        ICompressor compressor = ICompressor.getCompressor(comp);
                        IUnCompressor unCompressor = IUnCompressor.getUnCompressor(comp);

                        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                        for (float val : tmp) encoder.encode(val, buffer);
                        encoder.flush(buffer);
                        byte[] elems = buffer.toByteArray();
                        byte[] compressed = compressor.compress(elems);
                        float ratioTmp = (float) compressed.length / (float) (tmp.size() * Long.BYTES);
                        ratio += ratioTmp;
                        buffer.close();
                        System.out.println(scheme);

                    }
                }

            }
        }
        String[] record = {
                "TS_2DIFF",
                "GZIP",
                String.valueOf(ratio/71)
        };
        writer.writeRecord(record);
        writer.close();
        long e = System.nanoTime();
        long encodeTime = (e - s);
        System.out.println("encodeTime:");
        System.out.println(encodeTime);
    }
}
//            if (fileName.contains("int")) {
//                dataType = TSDataType.INT32;
//                ArrayList<Integer> tmp = new ArrayList<>();
//                for (String value : data) {
//                    tmp.add(Integer.valueOf(value));
//                }
//                for (TSEncoding scheme : schemeList) {
//                    Encoder encoder = TSEncodingBuilder.getEncodingBuilder(scheme).getEncoder(dataType);
//                    Decoder decoder = Decoder.getDecoderByType(scheme, dataType);
//                    long encodeTime = 0;
//                    long decodeTime = 0;
//                    for (CompressionType comp : compressList) {
//                        ICompressor compressor = ICompressor.getCompressor(comp);
//                        IUnCompressor unCompressor = IUnCompressor.getUnCompressor(comp);
//                        long compressTime = 0;
//                        long uncompressTime = 0;
//                        double ratio = 0;
//                        for (int i = 0; i < repeatTime; i++) {
//                            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
//                            long s = System.nanoTime();
//                            for (int val : tmp) encoder.encode(val, buffer);
//                            long e = System.nanoTime();
//                            encodeTime += (e - s);
//                            System.out.println("encodeTime:");
//                            System.out.println(e - s);
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
//                            ByteBuffer ebuffer = ByteBuffer.wrap(buffer.toByteArray());
//                            s = System.nanoTime();
//                            while (decoder.hasNext(ebuffer)) {
//                                decoder.readInt(ebuffer);
//                            }
//                            e = System.nanoTime();
//                            decodeTime += (e - s);
//                            buffer.close();
//                        }
//                        ratio /= repeatTime;
//                        encodeTime = encodeTime / ((long) repeatTime);
//                        decodeTime = decodeTime / ((long) repeatTime);
//                        //                        compressTime = compressTime / ((long) repeatTime);
//                        //                        uncompressTime = uncompressTime / ((long) repeatTime);
//                        System.out.println(scheme.toString());
//                        //                        System.out.println(String.valueOf(encodeTime));
//                        String[] record = {
//                                scheme.toString(),
//                                comp.toString(),
//                                String.valueOf(encodeTime),
//                                "",
//                                "",
//                                String.valueOf(decodeTime),
//                                String.valueOf(ratio)
//                        };
//
//                        //                        String[] record = {scheme.toString(), comp.toString(),
//                        // String.valueOf(encodeTime), String.valueOf(compressTime),
//                        //                                String.valueOf(uncompressTime),
//                        // String.valueOf(decodeTime), String.valueOf(ratio)};
//                        writer.writeRecord(record);
//                    }
//                }
//            }
//            else if (fileName.contains("long")) {
//                dataType = TSDataType.INT64;
//                ArrayList<Long> tmp = new ArrayList<>();
//                for (String value : data) {
//                    tmp.add(Long.valueOf(value));
//                }
//                for (TSEncoding scheme : schemeList) {
//                    Encoder encoder = TSEncodingBuilder.getEncodingBuilder(scheme).getEncoder(dataType);
//                    Decoder decoder = Decoder.getDecoderByType(scheme, dataType);
//                    long encodeTime = 0;
//                    long decodeTime = 0;
//                    for (CompressionType comp : compressList) {
//                        ICompressor compressor = ICompressor.getCompressor(comp);
//                        IUnCompressor unCompressor = IUnCompressor.getUnCompressor(comp);
//                        long compressTime = 0;
//                        long uncompressTime = 0;
//                        double ratio = 0;
//                        for (int i = 0; i < repeatTime; i++) {
//                            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
//                            long s = System.nanoTime();
//                            for (long val : tmp) encoder.encode(val, buffer);
//                            long e = System.nanoTime();
//                            encodeTime += (e - s);
//                            System.out.println("encodeTime:");
//                            System.out.println(e - s);
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
//                            ByteBuffer ebuffer = ByteBuffer.wrap(buffer.toByteArray());
//                            s = System.nanoTime();
//                            while (decoder.hasNext(ebuffer)) {
//                                decoder.readLong(ebuffer);
//                            }
//                            e = System.nanoTime();
//                            decodeTime += (e - s);
//                            buffer.close();
//                        }
//                        ratio /= repeatTime;
//                        encodeTime = encodeTime / ((long) repeatTime);
//                        decodeTime = decodeTime / ((long) repeatTime);
//                        //                        compressTime = compressTime / ((long) repeatTime);
//                        //                        uncompressTime = uncompressTime / ((long) repeatTime);
//                        System.out.println(scheme.toString());
//                        //                        System.out.println(String.valueOf(encodeTime));
//                        String[] record = {
//                                scheme.toString(),
//                                comp.toString(),
//                                String.valueOf(encodeTime),
//                                "",
//                                "",
//                                String.valueOf(decodeTime),
//                                String.valueOf(ratio)
//                        };
//
//                        //                        String[] record = {scheme.toString(), comp.toString(),
//                        // String.valueOf(encodeTime), String.valueOf(compressTime),
//                        //                                String.valueOf(uncompressTime),
//                        // String.valueOf(decodeTime), String.valueOf(ratio)};
//                        writer.writeRecord(record);
//                    }
//                }
//            }
//            else if (fileName.contains("double")) {
//                dataType = TSDataType.DOUBLE;
//                ArrayList<Double> tmp = new ArrayList<>();
//                for (String value : data) {
//                    tmp.add(Double.valueOf(value));
//                }
//                for (TSEncoding scheme : schemeList) {
//                    Encoder encoder = TSEncodingBuilder.getEncodingBuilder(scheme).getEncoder(dataType);
//                    Decoder decoder = Decoder.getDecoderByType(scheme, dataType);
//                    long encodeTime = 0;
//                    long decodeTime = 0;
//                    for (CompressionType comp : compressList) {
//                        ICompressor compressor = ICompressor.getCompressor(comp);
//                        IUnCompressor unCompressor = IUnCompressor.getUnCompressor(comp);
//                        long compressTime = 0;
//                        long uncompressTime = 0;
//                        double ratio = 0;
//                        for (int i = 0; i < repeatTime; i++) {
//                            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
//                            long s = System.nanoTime();
//                            for (double val : tmp) encoder.encode(val, buffer);
//                            long e = System.nanoTime();
//                            encodeTime += (e - s);
//                            System.out.println("encodeTime:");
//                            System.out.println(e - s);
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
//                            ByteBuffer ebuffer = ByteBuffer.wrap(buffer.toByteArray());
//                            s = System.nanoTime();
//                            while (decoder.hasNext(ebuffer)) {
//                                decoder.readDouble(ebuffer);
//                            }
//                            e = System.nanoTime();
//                            decodeTime += (e - s);
//                            buffer.close();
//                        }
//                        ratio /= repeatTime;
//                        encodeTime = encodeTime / ((long) repeatTime);
//                        decodeTime = decodeTime / ((long) repeatTime);
//                        //                        compressTime = compressTime / ((long) repeatTime);
//                        //                        uncompressTime = uncompressTime / ((long) repeatTime);
//                        System.out.println(scheme.toString());
//                        //                        System.out.println(String.valueOf(encodeTime));
//                        String[] record = {
//                                scheme.toString(),
//                                comp.toString(),
//                                String.valueOf(encodeTime),
//                                "",
//                                "",
//                                String.valueOf(decodeTime),
//                                String.valueOf(ratio)
//                        };
//
//                        //                        String[] record = {scheme.toString(), comp.toString(),
//                        // String.valueOf(encodeTime), String.valueOf(compressTime),
//                        //                                String.valueOf(uncompressTime),
//                        // String.valueOf(decodeTime), String.valueOf(ratio)};
//                        writer.writeRecord(record);
//                    }
//                }
//            }