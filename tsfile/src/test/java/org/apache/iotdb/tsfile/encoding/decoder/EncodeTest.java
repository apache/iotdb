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
        String inputPath = "C:\\Users\\xiaoj\\Desktop\\int", Output = "C:\\Users\\xiaoj\\Desktop\\SpeedResult.csv";
        if (args.length >= 2) inputPath = args[1];
        if (args.length >= 3) Output = args[2];

        File file = new File(inputPath);
        File[] tempList = file.listFiles();
        TSEncoding[] schemeList = {TSEncoding.PLAIN, TSEncoding.TS_2DIFF, TSEncoding.RLE,
                TSEncoding.SPRINTZ, TSEncoding.RLBE, TSEncoding.GORILLA,TSEncoding.RAKE};
        CompressionType[] compressList = {CompressionType.LZ4, CompressionType.GZIP, CompressionType.SNAPPY};
//        CompressionType[] compressList = {CompressionType.UNCOMPRESSED};//
        CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

        String[] head = {"Encoding", "Compress", "Encoding Time", "Compress Time", "Uncompress Time", "Decoding Time",
                "Compression Ratio"};
        writer.writeRecord(head);
        int repeatTime = 1;

        assert tempList != null;
        for (File f : tempList) {
            System.out.println(f);
            InputStream inputStream = new FileInputStream(f);
            CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
            String fileName = f.getAbsolutePath();
            ArrayList<String> data = new ArrayList<>();
            TSDataType dataType;

            loader.readHeaders();
            while (loader.readRecord()) {
                data.add(loader.getValues()[1]);
            }
            loader.close();
            inputStream.close();

            if (fileName.contains("int")) {
                ArrayList<Integer> tmp = new ArrayList<>();
                dataType = TSDataType.INT32;
                for (String value : data) {
                    tmp.add(Integer.valueOf(value));
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
                        double ratio = tmp.size() * Integer.BYTES;

                        for(int i=0;i<10;i++){

                            ByteArrayOutputStream buffer = new ByteArrayOutputStream();

                            long s = System.nanoTime();
                            for (int val : tmp) encoder.encode(val, buffer);
                            encoder.flush(buffer);
                            long e = System.nanoTime();
                            encodeTime += (e - s);

                            byte[] elems = buffer.toByteArray();
                            s = System.nanoTime();
                            byte[] compressed = compressor.compress(elems);
                            e = System.nanoTime();
                            compressTime += (e - s);

                            ratio /= compressed.length;

                            s = System.nanoTime();
                            byte[] x = unCompressor.uncompress(compressed);
                            e = System.nanoTime();
                            uncompressTime += (e - s);

                            ByteBuffer ebuffer = ByteBuffer.wrap(x);
                            s = System.nanoTime();
//                        int y=0;
                            while(decoder.hasNext(ebuffer)){
                                decoder.readInt(ebuffer);
                            }
                            e = System.nanoTime();
                            decodeTime += (e - s);
                            buffer.close();
                        }
//                        System.out.println(encodeTime);
                        encodeTime = encodeTime * 100 / tmp.size();
                        decodeTime = decodeTime * 100 / tmp.size();
                        compressTime = compressTime * 100 / tmp.size();
                        uncompressTime = uncompressTime * 100 / tmp.size();

                        String[] record = {scheme.toString(), comp.toString(), String.valueOf(encodeTime), String.valueOf(compressTime),
                                String.valueOf(uncompressTime), String.valueOf(decodeTime), String.valueOf(ratio)};
                        writer.writeRecord(record);
                    }
                }
            } else if (fileName.contains("long")) {

                ArrayList<Long> tmp = new ArrayList<>();
                dataType = TSDataType.INT64;
                for (String value : data) {
                    tmp.add(Long.valueOf(value));
                }
                for (TSEncoding scheme : schemeList) {
                    Encoder encoder = TSEncodingBuilder.getEncodingBuilder(scheme).getEncoder(dataType);
                    Decoder decoder = Decoder.getDecoderByType(scheme, dataType);
                    long encodeTime = 0;
                    long decodeTime = 0;
                    for (int i = 0; i < repeatTime; i++) {
                        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                        long s = System.nanoTime();
                        for (long val : tmp) encoder.encode(val, buffer);
                        long e = System.nanoTime();
                        encodeTime += (e - s);

                        ByteBuffer ebuffer = ByteBuffer.wrap(buffer.toByteArray());
                        s = System.nanoTime();
                        for (int c = 0; c < tmp.size(); c++) decoder.readLong(ebuffer);
                        e = System.nanoTime();
                        decodeTime += (e - s);
                    }

                    encodeTime /= tmp.size();
                    decodeTime /= tmp.size();
                    String[] record = {scheme.toString(), "LONG", String.valueOf(encodeTime), String.valueOf(decodeTime)};
                    writer.writeRecord(record);
                }
            } else if (fileName.contains("double")) {
                ArrayList<Double> tmp = new ArrayList<>();
                dataType = TSDataType.DOUBLE;
                for (String value : data) {
                    tmp.add(Double.valueOf(value));
                }
                for (TSEncoding scheme : schemeList) {
                    Encoder encoder = TSEncodingBuilder.getEncodingBuilder(scheme).getEncoder(dataType);
                    Decoder decoder = Decoder.getDecoderByType(scheme, dataType);
                    long encodeTime = 0;
                    long decodeTime = 0;
                    for (int i = 0; i < repeatTime; i++) {
                        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                        long s = System.nanoTime();
                        for (double val : tmp) encoder.encode(val, buffer);
                        long e = System.nanoTime();
                        encodeTime += (e - s);

                        ByteBuffer ebuffer = ByteBuffer.wrap(buffer.toByteArray());
                        s = System.nanoTime();
                        for (int c = 0; c < tmp.size(); c++) decoder.readDouble(ebuffer);
                        e = System.nanoTime();
                        decodeTime += (e - s);
                    }

                    encodeTime /= tmp.size();
                    decodeTime /= tmp.size();
                    String[] record = {scheme.toString(), "DOUBLE", String.valueOf(encodeTime), String.valueOf(decodeTime)};
                    writer.writeRecord(record);
                }
            } else if (fileName.contains("float")) {
                ArrayList<Float> tmp = new ArrayList<>();
                dataType = TSDataType.FLOAT;
                for (String value : data) {
                    tmp.add(Float.valueOf(value));
                }
                System.out.println(tmp);
                for (TSEncoding scheme : schemeList) {
                    Encoder encoder = TSEncodingBuilder.getEncodingBuilder(scheme).getEncoder(dataType);
                    Decoder decoder = Decoder.getDecoderByType(scheme, dataType);
                    long encodeTime = 0;
                    long decodeTime = 0;
                    for (int i = 0; i < repeatTime; i++) {
                        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                        long s = System.nanoTime();
                        for (float val : tmp) encoder.encode(val, buffer);
                        long e = System.nanoTime();
                        encodeTime += (e - s);

                        ByteBuffer ebuffer = ByteBuffer.wrap(buffer.toByteArray());
                        s = System.nanoTime();
                        for (int c = 0; c < tmp.size(); c++) decoder.readFloat(ebuffer);
                        e = System.nanoTime();
                        decodeTime += (e - s);
                    }

                    encodeTime /= tmp.size();
                    decodeTime /= tmp.size();
                    String[] record = {scheme.toString(), "FLOAT", String.valueOf(encodeTime), String.valueOf(decodeTime)};
                    writer.writeRecord(record);
                }
            } else if (fileName.contains("text")) {
                ArrayList<Byte> tmp = new ArrayList<>();
                dataType = TSDataType.FLOAT;
                for (String value : data) {
                    tmp.add(Byte.valueOf(value));
                }
                for (TSEncoding scheme : schemeList) {
                    Encoder encoder = TSEncodingBuilder.getEncodingBuilder(scheme).getEncoder(dataType);
                    Decoder decoder = Decoder.getDecoderByType(scheme, dataType);
                    long encodeTime = 0;
                    long decodeTime = 0;
                    for (int i = 0; i < repeatTime; i++) {
                        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                        long s = System.nanoTime();
                        for (byte val : tmp) encoder.encode(val, buffer);
                        long e = System.nanoTime();
                        encodeTime += (e - s);

                        ByteBuffer ebuffer = ByteBuffer.wrap(buffer.toByteArray());
                        s = System.nanoTime();
                        for (int c = 0; c < tmp.size(); c++) decoder.readBinary(ebuffer);
                        e = System.nanoTime();
                        decodeTime += (e - s);
                    }

                    encodeTime /= tmp.size();
                    decodeTime /= tmp.size();
                    String[] record = {scheme.toString(), "TEXT", String.valueOf(encodeTime), String.valueOf(decodeTime)};
                    writer.writeRecord(record);
                }
            } //else throw new NotImplementedException();
            //System.gc();
        }

        writer.close();
    }
}