package org.apache.iotdb.tsfile.timeTest;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import com.sun.tools.javac.util.ByteBuffer;
import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class CompressTest {

    public static void main(@org.jetbrains.annotations.NotNull String[] args) throws IOException {
        String inputPath = "D:\\HCY\\其他\\挑战杯\\client-py\\data\\learn\\float", Output = "D:\\HCY\\compressSpeed.csv";
        if (args.length >= 2) inputPath = args[1];
        if (args.length >= 3) Output = args[2];

        File file = new File(inputPath);
        File[] tempList = file.listFiles();
        CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);
        CompressionType[] schemes = {CompressionType.SNAPPY,CompressionType.SNAPPY,CompressionType.GZIP};
        ICompressor compressor;
        IUnCompressor unCompressor;

        String[] head = {"Encoding", "DataType", "Encoding Time", "Decoding Time"};
        writer.writeRecord(head);
        int repeatTime = 1;

        assert tempList != null;
        for (File f : tempList) {
            InputStream inputStream = new FileInputStream(f);
            CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
            String fileName = f.getAbsolutePath();
            ArrayList<String> data = new ArrayList<>();

            loader.readHeaders();
            while (loader.readRecord()) {
                data.add(loader.getValues()[1]);
            }
            loader.close();

            if (fileName.contains("int")) {
                ByteBuffer out = new ByteBuffer();
                for (String value : data) {
                    out.appendInt(Integer.parseInt(value));
                }
                for (CompressionType scheme :schemes) {
                    compressor = ICompressor.getCompressor(scheme);
                    unCompressor = IUnCompressor.getUnCompressor(scheme);
                    byte[] compressed = new byte[compressor.getMaxBytesForCompression(out.length)];
                    long compressTime = 0;
                    long uncompressTime = 0;
                    for (int i = 0; i < repeatTime; i++) {
                        long s = System.nanoTime();
                        compressor.compress(out.elems, 0, out.length, compressed);
                        long e = System.nanoTime();
                        compressTime += (e - s);

                        s = System.nanoTime();
                        byte[] uncompressed = unCompressor.uncompress(compressed);
                        e = System.nanoTime();
                        uncompressTime += (e - s);
                    }

                    compressTime /= 10;
                    uncompressTime /= 10;
                    String[] record = {scheme.toString(), "INT", String.valueOf(compressTime), String.valueOf(uncompressTime)};
                    writer.writeRecord(record);
                }
            } else if (fileName.contains("long")) {
                ByteBuffer out = new ByteBuffer();
                for (String value : data) {
                    out.appendLong(Long.parseLong(value));
                }
                for (CompressionType scheme : CompressionType.values()) {
                    compressor = ICompressor.getCompressor(scheme);
                    unCompressor = IUnCompressor.getUnCompressor(scheme);
                    byte[] compressed = new byte[compressor.getMaxBytesForCompression(out.length)];
                    long compressTime = 0;
                    long uncompressTime = 0;
                    for (int i = 0; i < repeatTime; i++) {
                        long s = System.nanoTime();
                        compressor.compress(out.elems, 0, out.length, compressed);
                        long e = System.nanoTime();
                        compressTime += (e - s);

                        s = System.nanoTime();
                        unCompressor.uncompress(compressed);
                        e = System.nanoTime();
                        uncompressTime += (e - s);
                    }

                    compressTime /= 10;
                    uncompressTime /= 10;
                    String[] record = {scheme.toString(), "LONG", String.valueOf(compressTime), String.valueOf(uncompressTime)};
                    writer.writeRecord(record);
                }
            } else if (fileName.contains("double")) {
                ByteBuffer out = new ByteBuffer();
                for (String value : data) {
                    out.appendDouble(Double.parseDouble(value));
                }
                for (CompressionType scheme : CompressionType.values()) {
                    compressor = ICompressor.getCompressor(scheme);
                    unCompressor = IUnCompressor.getUnCompressor(scheme);
                    byte[] compressed = new byte[compressor.getMaxBytesForCompression(out.length)];
                    long compressTime = 0;
                    long uncompressTime = 0;
                    for (int i = 0; i < repeatTime; i++) {
                        long s = System.nanoTime();
                        compressor.compress(out.elems, 0, out.length, compressed);
                        long e = System.nanoTime();
                        compressTime += (e - s);

                        s = System.nanoTime();
                        unCompressor.uncompress(compressed);
                        e = System.nanoTime();
                        uncompressTime += (e - s);
                    }

                    compressTime /= 10;
                    uncompressTime /= 10;
                    String[] record = {scheme.toString(), "DOUBLE", String.valueOf(compressTime), String.valueOf(uncompressTime)};
                    writer.writeRecord(record);
                }
            } else if (fileName.contains("float")) {
                ByteBuffer out = new ByteBuffer();
                for (String value : data) {
                    out.appendFloat(Float.parseFloat(value));
                }
                for (CompressionType scheme : CompressionType.values()) {
                    compressor = ICompressor.getCompressor(scheme);
                    unCompressor = IUnCompressor.getUnCompressor(scheme);
                    byte[] compressed = new byte[compressor.getMaxBytesForCompression(out.length)];
                    long compressTime = 0;
                    long uncompressTime = 0;
                    for (int i = 0; i < repeatTime; i++) {
                        long s = System.nanoTime();
                        compressor.compress(out.elems, 0, out.length, compressed);
                        long e = System.nanoTime();
                        compressTime += (e - s);

                        s = System.nanoTime();
                        unCompressor.uncompress(compressed);
                        e = System.nanoTime();
                        uncompressTime += (e - s);
                    }

                    compressTime /= 10;
                    uncompressTime /= 10;
                    String[] record = {scheme.toString(), "FLOAT", String.valueOf(compressTime), String.valueOf(uncompressTime)};
                    writer.writeRecord(record);
                }
            } else if (fileName.contains("text")) {
                ByteBuffer out = new ByteBuffer();
                for (String value : data) {
                    out.appendInt(Byte.parseByte(value));
                }
                for (CompressionType scheme : CompressionType.values()) {
                    compressor = ICompressor.getCompressor(scheme);
                    unCompressor = IUnCompressor.getUnCompressor(scheme);
                    byte[] compressed = new byte[compressor.getMaxBytesForCompression(out.length)];
                    long compressTime = 0;
                    long uncompressTime = 0;
                    for (int i = 0; i < repeatTime; i++) {
                        long s = System.nanoTime();
                        compressor.compress(out.elems, 0, out.length, compressed);
                        long e = System.nanoTime();
                        compressTime += (e - s);

                        s = System.nanoTime();
                        unCompressor.uncompress(compressed);
                        e = System.nanoTime();
                        uncompressTime += (e - s);
                    }

                    compressTime /= 10;
                    uncompressTime /= 10;
                    String[] record = {scheme.toString(), "TEXT", String.valueOf(compressTime), String.valueOf(uncompressTime)};
                    writer.writeRecord(record);
                }
            } else throw new NotImplementedException();
        }

        writer.close();
    }
}