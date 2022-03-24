package org.apache.iotdb.tsfile.timeTest;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.io.*;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class EncodeTest {

    public static void main(@org.jetbrains.annotations.NotNull String[] args) throws IOException {
        String inputPath = "/home/client-py/data", ouputPath = "/home/client-py/encodeSpeed.csv";
        if (args.length >= 2) inputPath = args[1];
        if (args.length >= 3) ouputPath = args[2];

        File file = new File(inputPath);
        File[] tempList = file.listFiles();
        TSEncoding[] schemeList = {TSEncoding.PLAIN, TSEncoding.GORILLA, TSEncoding.TS_2DIFF, TSEncoding.RLE,
                TSEncoding.SPRINTZ, TSEncoding.RLBE, TSEncoding.RLBE};
        CsvWriter writer = new CsvWriter(ouputPath, ',', StandardCharsets.UTF_8);
        Encoder encoder;
        Decoder decoder;

        String[] head = {"Encoding", "DataType", "Encoding Time", "Decoding Time"};
        writer.writeRecord(head);
        int repeatTime = 10;

        assert tempList != null;
        for (File f : tempList) {
            InputStream inputStream = new FileInputStream(f);
            CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
            String fileName = f.getName();
            ArrayList<String> data = new ArrayList<>();
            TSDataType dataType;

            loader.readHeaders();
            loader.readHeaders();
            while (loader.readRecord()) {
                data.add(loader.getValues()[1]);
            }
            loader.close();

            if (fileName.contains("int")) {
                ArrayList<Integer> tmp = new ArrayList<>();
                dataType = TSDataType.INT32;
                for (String value : data) {
                    tmp.add(Integer.valueOf(value));
                }
                for (TSEncoding scheme : schemeList) {
                    encoder = TSEncodingBuilder.getEncodingBuilder(scheme).getEncoder(dataType);
                    decoder = Decoder.getDecoderByType(scheme, dataType);
                    long startTime = System.nanoTime();
                    ByteArrayOutputStream tmpBuffer = new ByteArrayOutputStream();
                    for (int i = 0; i < repeatTime; i++) {
                        for (int val : tmp) {
                            encoder.encode(val, tmpBuffer);
                        }
                        encoder.flush(tmpBuffer);
                    }
                    long endTime = System.nanoTime();
                    long encodeTime = (endTime - startTime) / repeatTime;

                    ByteBuffer buffer = ByteBuffer.wrap(tmpBuffer.toByteArray());
                    startTime = System.nanoTime();
                    for (int i = 0; i < repeatTime; i++) {
                        while (decoder.hasNext(buffer)) {
                            decoder.readInt(buffer);
                        }
                        decoder.reset();
                    }
                    endTime = System.nanoTime();
                    long decodeTime = (endTime - startTime) / repeatTime;
                    String[] record = {scheme.toString(), "INT", String.valueOf(encodeTime), String.valueOf(decodeTime)};
                    writer.writeRecord(record);
                }
            } else if (fileName.contains("long")) {
                ArrayList<Long> tmp = new ArrayList<>();
                dataType = TSDataType.INT64;
                for (String value : data) {
                    tmp.add(Long.valueOf(value));
                }
                for (TSEncoding scheme : schemeList) {
                    encoder = TSEncodingBuilder.getEncodingBuilder(scheme).getEncoder(dataType);
                    decoder = Decoder.getDecoderByType(scheme, dataType);
                    long startTime = System.nanoTime();
                    ByteArrayOutputStream tmpBuffer = new ByteArrayOutputStream();
                    for (int i = 0; i < repeatTime; i++) {
                        for (long val : tmp) {
                            encoder.encode(val, tmpBuffer);
                        }
                        encoder.flush(tmpBuffer);
                    }
                    long endTime = System.nanoTime();
                    long encodeTime = (endTime - startTime) / repeatTime;

                    ByteBuffer buffer = ByteBuffer.wrap(tmpBuffer.toByteArray());
                    startTime = System.nanoTime();
                    for (int i = 0; i < repeatTime; i++) {
                        while (decoder.hasNext(buffer)) {
                            decoder.readLong(buffer);
                        }
                        decoder.reset();
                    }
                    endTime = System.nanoTime();
                    long decodeTime = (endTime - startTime) / repeatTime;
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
                    encoder = TSEncodingBuilder.getEncodingBuilder(scheme).getEncoder(dataType);
                    decoder = Decoder.getDecoderByType(scheme, dataType);
                    long startTime = System.nanoTime();
                    ByteArrayOutputStream tmpBuffer = new ByteArrayOutputStream();
                    for (int i = 0; i < repeatTime; i++) {
                        for (double val : tmp) {
                            encoder.encode(val, tmpBuffer);
                        }
                        encoder.flush(tmpBuffer);
                    }
                    long endTime = System.nanoTime();
                    long encodeTime = (endTime - startTime) / repeatTime;

                    ByteBuffer buffer = ByteBuffer.wrap(tmpBuffer.toByteArray());
                    startTime = System.nanoTime();
                    for (int i = 0; i < repeatTime; i++) {
                        while (decoder.hasNext(buffer)) {
                            decoder.readDouble(buffer);
                        }
                        decoder.reset();
                    }
                    endTime = System.nanoTime();
                    long decodeTime = (endTime - startTime) / repeatTime;
                    String[] record = {scheme.toString(), "DOUBLE", String.valueOf(encodeTime), String.valueOf(decodeTime)};
                    writer.writeRecord(record);
                }
            } else if (fileName.contains("float")) {
                ArrayList<Float> tmp = new ArrayList<>();
                dataType = TSDataType.FLOAT;
                for (String value : data) {
                    tmp.add(Float.valueOf(value));
                }
                for (TSEncoding scheme : schemeList) {
                    encoder = TSEncodingBuilder.getEncodingBuilder(scheme).getEncoder(dataType);
                    decoder = Decoder.getDecoderByType(scheme, dataType);
                    long startTime = System.nanoTime();
                    ByteArrayOutputStream tmpBuffer = new ByteArrayOutputStream();
                    for (int i = 0; i < repeatTime; i++) {
                        for (float val : tmp) {
                            encoder.encode(val, tmpBuffer);
                        }
                        encoder.flush(tmpBuffer);
                    }
                    long endTime = System.nanoTime();
                    long encodeTime = (endTime - startTime) / repeatTime;

                    ByteBuffer buffer = ByteBuffer.wrap(tmpBuffer.toByteArray());
                    startTime = System.nanoTime();
                    for (int i = 0; i < repeatTime; i++) {
                        while (decoder.hasNext(buffer)) {
                            decoder.readFloat(buffer);
                        }
                        decoder.reset();
                    }
                    endTime = System.nanoTime();
                    long decodeTime = (endTime - startTime) / repeatTime;
                    String[] record = {scheme.toString(), "FLOAT", String.valueOf(encodeTime), String.valueOf(decodeTime)};
                    writer.writeRecord(record);
                }
            } else if (fileName.contains("text")) {
                ArrayList<Byte> tmp = new ArrayList<>();
                dataType = TSDataType.TEXT;
                for (String value : data) {
                    tmp.add(Byte.valueOf(value));
                }
                for (TSEncoding scheme : schemeList) {
                    encoder = TSEncodingBuilder.getEncodingBuilder(scheme).getEncoder(dataType);
                    decoder = Decoder.getDecoderByType(scheme, dataType);
                    long startTime = System.nanoTime();
                    ByteArrayOutputStream tmpBuffer = new ByteArrayOutputStream();
                    for (int i = 0; i < repeatTime; i++) {
                        for (int val : tmp) {
                            encoder.encode(val, tmpBuffer);
                        }
                        encoder.flush(tmpBuffer);
                    }
                    long endTime = System.nanoTime();
                    long encodeTime = (endTime - startTime) / repeatTime;

                    ByteBuffer buffer = ByteBuffer.wrap(tmpBuffer.toByteArray());
                    startTime = System.nanoTime();
                    for (int i = 0; i < repeatTime; i++) {
                        while (decoder.hasNext(buffer)) {
                            decoder.readBinary(buffer);
                        }
                        decoder.reset();
                    }
                    endTime = System.nanoTime();
                    long decodeTime = (endTime - startTime) / repeatTime;
                    String[] record = {scheme.toString(), "TEXT", String.valueOf(encodeTime), String.valueOf(decodeTime)};
                    writer.writeRecord(record);
                }
            } else throw new NotImplementedException();
        }

        writer.close();
    }
}