package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;

public class SubcolumnPruneNewBlockSizeTest {

    @Test
    public void testBlockSizeBenchmark() throws IOException {
        String parentDir = "/Users/xiaojinzhao/Documents/GitHub/subcolumn/";

        String inputParentDir = parentDir + "dataset/";
        String outputParentDir = parentDir + "result/compression_vs_block_prune_fast/";

        File outputDir = new File(outputParentDir);
        if (!outputDir.exists() && !outputDir.mkdirs()) {
            throw new IOException("Cannot create " + outputParentDir);
        }

        int[] blockSizeList = {32, 64, 128, 256, 512, 1024, 2048, 4096, 8192};
        int repeatTime = 400;
        int decodeRepeatTime = 400;

        String[] datasets = {
            "Bird-migration",
            "Bitcoin-price",
            "City-temp",
            "Dewpoint-temp",
            "EPM-Education",
            "Gov10",
            "IR-bio-temp",
            "PM10-dust",
            "Stocks-DE",
            "Stocks-UK",
            "Stocks-USA",
            "Wind-Speed",
            "Wine-Tasting"
        };

        final int firstBlockSize = blockSizeList[0];

        for (int blockSize : blockSizeList) {
            final boolean warmupBeforeTiming = blockSize == firstBlockSize;
            String outputPath = outputParentDir + "subcolumn_block_" + blockSize + ".csv";
            CsvWriter writer = new CsvWriter(outputPath, ',', StandardCharsets.UTF_8);
            writer.setRecordDelimiter('\n');
            writer.writeRecord(
                    new String[] {
                        "Dataset",
                        "Encoding Algorithm",
                        "Encoding Time",
                        "Decoding Time",
                        "Points",
                        "Compressed Size",
                        "Compression Ratio"
                    });

            for (String datasetName : datasets) {
                File file = new File(inputParentDir + datasetName + ".csv");
                if (!file.exists()) {
                    System.out.println("Skip missing: " + file);
                    continue;
                }

                InputStream inputStream = Files.newInputStream(file.toPath());
                CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                ArrayList<Float> data1 = new ArrayList<>();
                int maxDecimal = 0;
                while (loader.readRecord()) {
                    String fStr = loader.getValues()[0];
                    if (fStr.isEmpty()) {
                        continue;
                    }
                    maxDecimal = Math.max(
                            maxDecimal, SubcolumnPruneNewTest.getDecimalPrecision(fStr));
                    data1.add(Float.valueOf(fStr));
                }
                loader.close();
                inputStream.close();

                if (maxDecimal > 8) {
                    maxDecimal = 8;
                }

                int[] data2Arr = new int[data1.size()];
                int maxMul = (int) Math.pow(10, maxDecimal);
                for (int i = 0; i < data1.size(); i++) {
                    data2Arr[i] = (int) (data1.get(i) * maxMul);
                }

                byte[] encoded = new byte[Math.max(16, data2Arr.length * 8)];

                if (warmupBeforeTiming) {
                    for (int r = 0; r < repeatTime; r++) {
                        SubcolumnPruneNewTest.Encoder(data2Arr, blockSize, encoded);
                    }
                }

                long s = System.nanoTime();
                int encodedLen = 0;
                for (int r = 0; r < repeatTime; r++) {
                    encodedLen = SubcolumnPruneNewTest.Encoder(data2Arr, blockSize, encoded);
                }
                long encodeTime = (System.nanoTime() - s) / repeatTime;

                if (warmupBeforeTiming) {
                    for (int r = 0; r < decodeRepeatTime; r++) {
                        SubcolumnPruneNewTest.Decoder(encoded);
                    }
                }

                s = System.nanoTime();
                for (int r = 0; r < decodeRepeatTime; r++) {
                    SubcolumnPruneNewTest.Decoder(encoded);
                }
                long decodeTime = (System.nanoTime() - s) / decodeRepeatTime;

                double ratio = encodedLen / (double) (data1.size() * Long.BYTES);
                writer.writeRecord(
                        new String[] {
                            datasetName,
                            "Sub-columns",
                            String.valueOf(encodeTime),
                            String.valueOf(decodeTime),
                            String.valueOf(data1.size()),
                            String.valueOf(encodedLen),
                            String.valueOf(ratio)
                        });
                System.out.println(
                        datasetName
                                + " block="
                                + blockSize
                                + " encode_ns="
                                + encodeTime
                                + " ratio="
                                + ratio);
            }
            writer.close();
        }
    }
}
