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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SubcolumnFullEncodingTypeRatioTest {

    private static class EncodingTypeStats {
        private int totalBlockCount;
        private int totalSubcolumnCount;
        private final int[] encodingTypeCounts = new int[3];

        private void setTotalBlockCount(int totalBlockCount) {
            this.totalBlockCount = totalBlockCount;
        }

        private void recordEncodingType(int[] encodingType, int length) {
            if (length <= 0) {
                return;
            }
            totalSubcolumnCount += length;
            for (int i = 0; i < length; i++) {
                int currentType = encodingType[i];
                if (currentType >= 0 && currentType < encodingTypeCounts.length) {
                    encodingTypeCounts[currentType]++;
                }
            }
        }

        private int getTotalBlockCount() {
            return totalBlockCount;
        }

        private int getTotalSubcolumnCount() {
            return totalSubcolumnCount;
        }

        private int getBitPackingSubcolumnCount() {
            return encodingTypeCounts[0];
        }

        private int getRleSubcolumnCount() {
            return encodingTypeCounts[1];
        }

        private int getDictionarySubcolumnCount() {
            return encodingTypeCounts[2];
        }

        private double getBitPackingRatio() {
            return getEncodingTypeRatio(0);
        }

        private double getRleRatio() {
            return getEncodingTypeRatio(1);
        }

        private double getDictionaryRatio() {
            return getEncodingTypeRatio(2);
        }

        private double getEncodingTypeRatio(int type) {
            if (totalSubcolumnCount == 0) {
                return 0;
            }
            return encodingTypeCounts[type] / (double) totalSubcolumnCount;
        }
    }

    public static int SubcolumnEncoder(
            int[] list,
            int encodePos,
            byte[] encodedResult,
            int[] beta,
            int blockSize,
            EncodingTypeStats stats) {
        int listLength = list.length;
        int maxValue = 0;
        for (int value : list) {
            if (value > maxValue) {
                maxValue = value;
            }
        }

        int m = SubcolumnFullTest.bitWidth(maxValue);

        SubcolumnFullTest.intByte2Bytes(m, encodePos, encodedResult);
        encodePos += 1;

        if (m == 0) {
            return encodePos;
        }

        int l = (m + beta[0] - 1) / beta[0];
        int[] bitWidthList = new int[l];
        int[][] subcolumnList = new int[l][listLength];

        SubcolumnFullTest.intByte2Bytes(beta[0], encodePos, encodedResult);
        encodePos += 1;

        int bw = SubcolumnFullTest.bitWidth(blockSize);
        int mask = (1 << beta[0]) - 1;

        for (int i = 0; i < l; i++) {
            int maxValuePart = 0;
            int shiftAmount = i * beta[0];
            for (int j = 0; j < listLength; j++) {
                subcolumnList[i][j] = (list[j] >> shiftAmount) & mask;
                if (subcolumnList[i][j] > maxValuePart) {
                    maxValuePart = subcolumnList[i][j];
                }
            }
            bitWidthList[i] = SubcolumnFullTest.bitWidth(maxValuePart);
        }

        encodePos = SubcolumnFullTest.bitPacking(bitWidthList, 8, encodePos, encodedResult, l);

        int[] encodingType = new int[l];
        int preTypePos = encodePos;
        encodePos += (l + 3) / 4;

        for (int i = l - 1; i >= 0; i--) {
            int bpCost = bitWidthList[i] * listLength;
            int rleCost = 0;

            int previous = subcolumnList[i][0];
            int index = 0;
            for (int j = 1; j < listLength; j++) {
                int currentNumber = subcolumnList[i][j];
                if (currentNumber != previous) {
                    index++;
                    previous = currentNumber;
                }
                if (bw * index + bitWidthList[i] * index >= bpCost) {
                    break;
                }
            }

            Set<Integer> uniqueValues = new HashSet<>();
            for (int j = 0; j < listLength; j++) {
                uniqueValues.add(subcolumnList[i][j]);
            }
            int cardinality = uniqueValues.size();

            index++;
            rleCost = bw * index + bitWidthList[i] * index;

            if (cardinality < Math.pow(2, bitWidthList[i] - 1)) {
                int dictBitWidth = SubcolumnFullTest.bitWidth(cardinality);
                int dictCost =
                        dictBitWidth * listLength + cardinality * (bitWidthList[i] + dictBitWidth);
                if (dictCost < rleCost && dictCost < bpCost) {
                    encodingType[i] = 2;

                    List<Integer> sortedUnique = new ArrayList<>(uniqueValues);
                    Collections.sort(sortedUnique);
                    Map<Integer, Integer> valueToCode = new HashMap<>();
                    int[] dictKeyList = new int[cardinality];
                    for (int j = 0; j < cardinality; j++) {
                        valueToCode.put(sortedUnique.get(j), j);
                        dictKeyList[j] = sortedUnique.get(j);
                    }
                    for (int j = 0; j < listLength; j++) {
                        subcolumnList[i][j] = valueToCode.get(subcolumnList[i][j]);
                    }

                    encodedResult[encodePos] = (byte) (cardinality >> 8);
                    encodePos += 1;
                    encodedResult[encodePos] = (byte) (cardinality & 0xFF);
                    encodePos += 1;

                    encodePos =
                            SubcolumnFullTest.bitPacking(
                                    dictKeyList, bitWidthList[i], encodePos, encodedResult, cardinality);
                    encodePos =
                            SubcolumnFullTest.bitPacking(
                                    subcolumnList[i], dictBitWidth, encodePos, encodedResult, listLength);
                    continue;
                }
            }

            if (bpCost <= rleCost) {
                encodingType[i] = 0;
                encodePos =
                        SubcolumnFullTest.bitPacking(
                                subcolumnList[i], bitWidthList[i], encodePos, encodedResult, listLength);
            } else {
                encodingType[i] = 1;

                encodedResult[encodePos] = (byte) (index >> 8);
                encodePos += 1;
                encodedResult[encodePos] = (byte) (index & 0xFF);
                encodePos += 1;

                index = 0;
                int[] runLength = new int[listLength];
                int[] rleValues = new int[listLength];
                previous = subcolumnList[i][0];

                for (int j = 1; j < listLength; j++) {
                    int currentNumber = subcolumnList[i][j];
                    if (currentNumber != previous) {
                        runLength[index] = j;
                        rleValues[index] = previous;
                        index++;
                        previous = currentNumber;
                    }
                }

                runLength[index] = listLength;
                rleValues[index] = previous;
                index++;

                encodePos = SubcolumnFullTest.bitPacking(runLength, bw, encodePos, encodedResult, index);
                encodePos =
                        SubcolumnFullTest.bitPacking(
                                rleValues, bitWidthList[i], encodePos, encodedResult, index);
            }
        }

        if (stats != null) {
            stats.recordEncodingType(encodingType, l);
        }

        SubcolumnFullTest.bitPacking(encodingType, 2, preTypePos, encodedResult, l);
        return encodePos;
    }

    public static int BlockEncoder(
            int[] data,
            int blockIndex,
            int blockSize,
            int remainder,
            int encodePos,
            byte[] encodedResult,
            int[] beta,
            EncodingTypeStats stats) {
        int[] minDelta = new int[3];
        int[] dataDelta =
                SubcolumnFullTest.getAbsDeltaTsBlock(data, blockIndex, blockSize, remainder, minDelta);

        SubcolumnFullTest.int2Bytes(minDelta[0], encodePos, encodedResult);
        encodePos += 4;

        // Keep SubcolumnFull behavior: only pick beta on the first block.
        if (blockIndex == 0) {
            int maxValue = 0;
            for (int j = 0; j < remainder; j++) {
                if (dataDelta[j] > maxValue) {
                    maxValue = dataDelta[j];
                }
            }
            int m = SubcolumnFullTest.bitWidth(maxValue);
            beta[0] = SubcolumnFullTest.Subcolumn(dataDelta, remainder, m, blockSize);
        }

        return SubcolumnEncoder(dataDelta, encodePos, encodedResult, beta, blockSize, stats);
    }

    public static int Encoder(
            int[] data, int blockSize, byte[] encodedResult, EncodingTypeStats stats) {
        int dataLength = data.length;
        int encodePos = 0;

        SubcolumnFullTest.int2Bytes(dataLength, encodePos, encodedResult);
        encodePos += 4;

        SubcolumnFullTest.int2Bytes(blockSize, encodePos, encodedResult);
        encodePos += 4;

        int numBlocks = dataLength / blockSize;
        int remainder = dataLength % blockSize;
        int[] beta = new int[] {2};

        if (stats != null) {
            stats.setTotalBlockCount(numBlocks + (remainder > 0 ? 1 : 0));
        }

        for (int i = 0; i < numBlocks; i++) {
            encodePos =
                    BlockEncoder(data, i, blockSize, blockSize, encodePos, encodedResult, beta, stats);
        }

        if (remainder <= 3) {
            int base = numBlocks * blockSize;
            for (int i = 0; i < remainder; i++) {
                SubcolumnFullTest.int2Bytes(data[base + i], encodePos, encodedResult);
                encodePos += 4;
            }
        } else {
            encodePos =
                    BlockEncoder(
                            data, numBlocks, blockSize, remainder, encodePos, encodedResult, beta, stats);
        }

        return encodePos;
    }

    public static int SubcolumnDecoder(
            byte[] encodedResult, int encodePos, int[] list, int blockSize) {
        int listLength = list.length;
        int m = SubcolumnFullTest.bytes2Integer(encodedResult, encodePos, 1);
        encodePos += 1;

        if (m == 0) {
            return encodePos;
        }

        int bw = SubcolumnFullTest.bitWidth(blockSize);
        int beta = SubcolumnFullTest.bytes2Integer(encodedResult, encodePos, 1);
        encodePos += 1;

        int l = (m + beta - 1) / beta;
        int[] bitWidthList = new int[l];
        encodePos = SubcolumnFullTest.decodeBitPacking(encodedResult, encodePos, 8, l, bitWidthList);

        int[][] subcolumnList = new int[l][listLength];
        int[] encodingType = new int[l];
        encodePos = SubcolumnFullTest.decodeBitPacking(encodedResult, encodePos, 2, l, encodingType);

        for (int i = l - 1; i >= 0; i--) {
            int type = encodingType[i];
            int bitWidth = bitWidthList[i];
            if (type == 0) {
                encodePos =
                        SubcolumnFullTest.decodeBitPacking(
                                encodedResult, encodePos, bitWidth, listLength, subcolumnList[i]);
            } else if (type == 1) {
                int index =
                        ((encodedResult[encodePos] & 0xFF) << 8) | (encodedResult[encodePos + 1] & 0xFF);
                encodePos += 2;

                int[] runLength = new int[index];
                int[] rleValues = new int[index];

                encodePos = SubcolumnFullTest.decodeBitPacking(encodedResult, encodePos, bw, index, runLength);
                encodePos =
                        SubcolumnFullTest.decodeBitPacking(
                                encodedResult, encodePos, bitWidth, index, rleValues);

                int currentIndex = 0;
                for (int j = 0; j < index; j++) {
                    int endPos = runLength[j];
                    int value = rleValues[j];
                    while (currentIndex < endPos) {
                        subcolumnList[i][currentIndex] = value;
                        currentIndex++;
                    }
                }
            } else {
                int cardinality =
                        ((encodedResult[encodePos] & 0xFF) << 8) | (encodedResult[encodePos + 1] & 0xFF);
                encodePos += 2;

                int dictBitWidth = SubcolumnFullTest.bitWidth(cardinality);
                int[] dictKeyList = new int[cardinality];
                int[] dictValueList = new int[cardinality];
                for (int j = 0; j < cardinality; j++) {
                    dictValueList[j] = j;
                }

                encodePos =
                        SubcolumnFullTest.decodeBitPacking(
                                encodedResult, encodePos, bitWidthList[i], cardinality, dictKeyList);
                encodePos =
                        SubcolumnFullTest.decodeBitPacking(
                                encodedResult, encodePos, dictBitWidth, listLength, subcolumnList[i]);

                Map<Integer, Integer> valueToCode = new HashMap<>();
                for (int j = 0; j < cardinality; j++) {
                    valueToCode.put(dictValueList[j], dictKeyList[j]);
                }

                for (int j = 0; j < listLength; j++) {
                    subcolumnList[i][j] = valueToCode.get(subcolumnList[i][j]);
                }
            }
        }

        for (int i = 0; i < l; i++) {
            int shiftAmount = i * beta;
            for (int j = 0; j < listLength; j++) {
                list[j] |= subcolumnList[i][j] << shiftAmount;
            }
        }

        return encodePos;
    }

    public static int BlockDecoder(
            byte[] encodedResult, int blockIndex, int blockSize, int remainder, int encodePos, int[] data) {
        int[] minDelta = new int[3];
        minDelta[0] = SubcolumnFullTest.bytes2Integer(encodedResult, encodePos, 4);
        encodePos += 4;

        int[] blockData = new int[remainder];
        encodePos = SubcolumnDecoder(encodedResult, encodePos, blockData, blockSize);

        for (int i = 0; i < remainder; i++) {
            data[blockIndex * blockSize + i] = blockData[i] + minDelta[0];
        }

        return encodePos;
    }

    public static int[] Decoder(byte[] encodedResult) {
        int encodePos = 0;
        int dataLength = SubcolumnFullTest.bytes2Integer(encodedResult, encodePos, 4);
        encodePos += 4;

        int blockSize = SubcolumnFullTest.bytes2Integer(encodedResult, encodePos, 4);
        encodePos += 4;

        int numBlocks = dataLength / blockSize;
        int[] data = new int[dataLength];

        for (int i = 0; i < numBlocks; i++) {
            encodePos = BlockDecoder(encodedResult, i, blockSize, blockSize, encodePos, data);
        }

        int remainder = dataLength % blockSize;
        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                data[numBlocks * blockSize + i] = SubcolumnFullTest.bytes2Integer(encodedResult, encodePos, 4);
                encodePos += 4;
            }
        } else {
            encodePos = BlockDecoder(encodedResult, numBlocks, blockSize, remainder, encodePos, data);
        }

        return data;
    }

    @Test
    public void test0() throws IOException {
        String parentDir = "D://github/xjz17/subcolumn/";
        String inputParentDir = parentDir + "dataset/";
        // String outputParentDir = parentDir + "result/";
        String outputParentDir = "D://encoding-subcolumn/result/";
        String outputPath = outputParentDir + "subcolumn_full_encoding_type_ratio_2048.csv";

        int blockSize = 512;
        blockSize = 2048;

        int repeatTime = 20;

        CsvWriter writer = new CsvWriter(outputPath, ',', StandardCharsets.UTF_8);
        writer.setRecordDelimiter('\n');

        String[] head = {
                "Dataset",
                "Encoding Algorithm",
                "Encoding Time",
                "Decoding Time",
                "Points",
                "Compressed Size",
                "Compression Ratio",
                "Block Count",
                "Subcolumn Count",
                "Bit Packing Subcolumn Count",
                "Bit Packing Ratio",
                "RLE Subcolumn Count",
                "RLE Ratio",
                "Dictionary Subcolumn Count",
                "Dictionary Ratio"
        };
        writer.writeRecord(head);

        File directory = new File(inputParentDir);
        File[] csvFiles = directory.listFiles((dir, name) -> name.endsWith(".csv"));
        if (csvFiles == null) {
            writer.close();
            return;
        }

        for (File file : csvFiles) {
            String datasetName = SubcolumnFullTest.extractFileName(file.toString());
            System.out.println(datasetName);

            InputStream inputStream = Files.newInputStream(file.toPath());
            CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
            ArrayList<Float> data1 = new ArrayList<>();

            int maxDecimal = 0;
            while (loader.readRecord()) {
                String fStr = loader.getValues()[0];
                if (fStr.isEmpty()) {
                    continue;
                }
                int currentDecimal = SubcolumnFullTest.getDecimalPrecision(fStr);
                if (currentDecimal > maxDecimal) {
                    maxDecimal = currentDecimal;
                }
                data1.add(Float.valueOf(fStr));
            }
            inputStream.close();

            if (maxDecimal > 8) {
                maxDecimal = 8;
            }

            int[] data2Arr = new int[data1.size()];
            int maxMul = (int) Math.pow(10, maxDecimal);
            for (int i = 0; i < data1.size(); i++) {
                data2Arr[i] = (int) (data1.get(i) * maxMul);
            }

            byte[] encodedResult = new byte[data2Arr.length * 13];

            long encodeTime = 0;
            long decodeTime = 0;
            double ratio = 0;
            double compressedSize = 0;
            int length = 0;
            EncodingTypeStats stats = new EncodingTypeStats();

            long start = System.nanoTime();
            for (int repeat = 0; repeat < repeatTime; repeat++) {
                stats = new EncodingTypeStats();
                length = Encoder(data2Arr, blockSize, encodedResult, stats);
            }
            long end = System.nanoTime();
            encodeTime += ((end - start) / repeatTime);
            compressedSize += length;

            double ratioTmp = compressedSize / (double) (data1.size() * Long.BYTES);
            ratio += ratioTmp;

            start = System.nanoTime();
            for (int repeat = 0; repeat < repeatTime; repeat++) {
                Decoder(encodedResult);
            }
            end = System.nanoTime();
            decodeTime += ((end - start) / repeatTime);

            String[] record = {
                    datasetName,
                    "Sub-columns (Full)",
                    String.valueOf(encodeTime),
                    String.valueOf(decodeTime),
                    String.valueOf(data1.size()),
                    String.valueOf(compressedSize),
                    String.valueOf(ratio),
                    String.valueOf(stats.getTotalBlockCount()),
                    String.valueOf(stats.getTotalSubcolumnCount()),
                    String.valueOf(stats.getBitPackingSubcolumnCount()),
                    String.valueOf(stats.getBitPackingRatio()),
                    String.valueOf(stats.getRleSubcolumnCount()),
                    String.valueOf(stats.getRleRatio()),
                    String.valueOf(stats.getDictionarySubcolumnCount()),
                    String.valueOf(stats.getDictionaryRatio())
            };
            writer.writeRecord(record);
        }

        writer.close();
    }
}
