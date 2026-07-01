package org.apache.iotdb.tsfile.encoding;

import org.junit.Test;

import java.io.IOException;

import org.apache.iotdb.tsfile.encoding.SubcolumnAblationPruneNewEngine.Mode;

public class SubcolumnFullTest {

    public static int bitWidth(int value) {
        return SubcolumnPruneNewTest.bitWidth(value);
    }

    public static void intByte2Bytes(int value, int encodePos, byte[] encodedResult) {
        SubcolumnPruneNewTest.intByte2Bytes(value, encodePos, encodedResult);
    }

    public static void int2Bytes(int value, int encodePos, byte[] encodedResult) {
        SubcolumnPruneNewTest.int2Bytes(value, encodePos, encodedResult);
    }

    public static int bytes2Integer(byte[] encodedResult, int encodePos, int byteNum) {
        return SubcolumnPruneNewTest.bytes2Integer(encodedResult, encodePos, byteNum);
    }

    public static int bitPacking(
            int[] numbers, int bitWidth, int encodePos, byte[] encodedResult, int length) {
        return SubcolumnPruneNewTest.bitPacking(numbers, bitWidth, encodePos, encodedResult, length);
    }

    public static int decodeBitPacking(
            byte[] encodedResult,
            int encodePos,
            int bitWidth,
            int length,
            int[] numbers) {
        return SubcolumnPruneNewTest.decodeBitPacking(
                encodedResult, encodePos, bitWidth, length, numbers);
    }

    public static int[] getAbsDeltaTsBlock(
            int[] tsBlock, int blockIndex, int blockSize, int remainder, int[] minDelta) {
        return SubcolumnPruneNewTest.getAbsDeltaTsBlock(
                tsBlock, blockIndex, blockSize, remainder, minDelta);
    }

    public static int Subcolumn(int[] x, int xLength, int m, int blockSize) {
        return SubcolumnPruneNewTest.Subcolumn(
                x, xLength, m, blockSize, SubcolumnPruneNewTest.borrowEncodingTypeBuffer());
    }

    public static int Encoder(int[] data, int blockSize, byte[] encodedResult) {
        return SubcolumnAblationPruneNewEngine.Encoder(data, blockSize, encodedResult, Mode.FULL);
    }

    public static int[] Decoder(byte[] encodedResult) {
        return SubcolumnAblationPruneNewEngine.Decoder(encodedResult);
    }

    public static String extractFileName(String filePath) {
        return SubcolumnPruneNewTest.extractFileName(filePath);
    }

    public static int getDecimalPrecision(String numberStr) {
        return SubcolumnPruneNewTest.getDecimalPrecision(numberStr);
    }

    @Test
    public void test0() throws IOException {
        SubcolumnAblationPruneNewEngine.runAblationBenchmark(
                "/Users/xiaojinzhao/Documents/GitHub/subcolumn/result/subcolumn_full.csv", Mode.FULL);
    }
}
