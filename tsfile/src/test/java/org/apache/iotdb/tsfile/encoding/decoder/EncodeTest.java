package org.apache.iotdb.tsfile.encoding.decoder;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class EncodeTest {

    public static void main(@org.jetbrains.annotations.NotNull String[] args) throws IOException {
        String inputPath = "C:\\Users\\xiaoj\\Desktop\\long", Output = "C:\\Users\\xiaoj\\Desktop\\UncompressedSpeedResult_long.csv";
        if (args.length >= 2) inputPath = args[1];
        if (args.length >= 3) Output = args[2];

        File file = new File(inputPath);
        File[] tempList = file.listFiles();
        TSEncoding[] schemeList = {TSEncoding.PLAIN, TSEncoding.TS_2DIFF, TSEncoding.RLE,
                TSEncoding.SPRINTZ, TSEncoding.RLBE, TSEncoding.GORILLA,TSEncoding.RAKE};
//        TSEncoding[] schemeList = { TSEncoding.RLBE,TSEncoding.PLAIN};
//        CompressionType[] compressList = {CompressionType.LZ4, CompressionType.GZIP, CompressionType.SNAPPY};
        CompressionType[] compressList = {CompressionType.UNCOMPRESSED};//
        CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

        String[] head = {"Encoding", "Compress", "Encoding Time", "Compress Time", "Uncompress Time", "Decoding Time",
                "Compression Ratio"};
        writer.writeRecord(head);
        int repeatTime = 50;

        assert tempList != null;
        int fileRepeat = 0;
        for (File f : tempList) {
            System.out.println(f);
            fileRepeat += 1;
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

            if (fileName.contains("int")) {
                dataType = TSDataType.INT32;
            } else if (fileName.contains("long")) {
                dataType = TSDataType.INT64;
            } else if (fileName.contains("double")) {
                dataType = TSDataType.DOUBLE;
            } else if (fileName.contains("float")) {
                dataType = TSDataType.FLOAT;
            } else if (fileName.contains("text")) {
                dataType = TSDataType.FLOAT;
            }
            ArrayList<Long> tmp = new ArrayList<>();
            for (String value : data) {
                tmp.add(Long.valueOf(value));
            }
            for (TSEncoding scheme : schemeList) {
                Encoder encoder = TSEncodingBuilder.getEncodingBuilder(scheme).getEncoder(dataType);
                Decoder decoder = Decoder.getDecoderByType(scheme, dataType);
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
                        for (long val : tmp) encoder.encode(val, buffer);
                        long e = System.nanoTime();
                        encodeTime += (e - s);
                        System.out.println("encodeTime:");
                        System.out.println(e - s);
                        encoder.flush(buffer);

                        byte[] elems = buffer.toByteArray();
                        s = System.nanoTime();
                        byte[] compressed = compressor.compress(elems);
                        e = System.nanoTime();
                        compressTime += (e - s);

                        int ratioTmp = compressed.length / (tmp.size() * Long.BYTES);
                        ratio += ratioTmp;

                        s = System.nanoTime();
                        byte[] x = unCompressor.uncompress(compressed);
                        e = System.nanoTime();
                        uncompressTime += (e - s);

                        ByteBuffer ebuffer = ByteBuffer.wrap(buffer.toByteArray());
                        s = System.nanoTime();
                        for (int c = 0; c < tmp.size(); c++) decoder.readLong(ebuffer);
                        e = System.nanoTime();
                        decodeTime += (e - s);
                        buffer.close();
                    }
                    ratio /= repeatTime;
                    encodeTime = encodeTime / ((long) repeatTime);
                    decodeTime = decodeTime / ((long)  repeatTime);
//                        compressTime = compressTime * 1000 / ((long) tmp.size() *repeatTime);
//                        uncompressTime = uncompressTime * 1000 / ((long) tmp.size() *repeatTime);
                    System.out.println(scheme.toString());
//                        System.out.println(String.valueOf(encodeTime));
                    String[] record = {scheme.toString(), comp.toString(), String.valueOf(encodeTime), "",
                            "", String.valueOf(decodeTime), String.valueOf(ratio)};

//                        String[] record = {scheme.toString(), comp.toString(), String.valueOf(encodeTime), String.valueOf(compressTime),
//                                String.valueOf(uncompressTime), String.valueOf(decodeTime), String.valueOf(ratio)};
                    writer.writeRecord(record);
                }
            }
            if(fileRepeat > 2)
                break;
        }
        writer.close();
    }
}
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
////                        String[] record = {scheme.toString(), comp.toString(), String.valueOf(encodeTime), "",
////                                "", String.valueOf(decodeTime), String.valueOf(ratio)};
//
//                        String[] record = {scheme.toString(), comp.toString(), String.valueOf(encodeTime), String.valueOf(compressTime),
//                                String.valueOf(uncompressTime), String.valueOf(decodeTime), String.valueOf(ratio)};
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
//                    Encoder encoder = TSEncodingBuilder.getEncodingBuilder(scheme).getEncoder(dataType);
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
//                    String[] record = {scheme.toString(), "DOUBLE", String.valueOf(encodeTime), String.valueOf(decodeTime)};
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
//                    Encoder encoder = TSEncodingBuilder.getEncodingBuilder(scheme).getEncoder(dataType);
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
//                    String[] record = {scheme.toString(), "FLOAT", String.valueOf(encodeTime), String.valueOf(decodeTime)};
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
//                    Encoder encoder = TSEncodingBuilder.getEncodingBuilder(scheme).getEncoder(dataType);
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
//                    String[] record = {scheme.toString(), "TEXT", String.valueOf(encodeTime), String.valueOf(decodeTime)};
//                    writer.writeRecord(record);
//                }
//            } //else throw new NotImplementedException();
//            //System.gc();