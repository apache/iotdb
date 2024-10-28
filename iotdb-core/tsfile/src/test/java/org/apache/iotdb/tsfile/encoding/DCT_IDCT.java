package org.apache.iotdb.tsfile.encoding;

import org.apache.commons.math3.transform.DctNormalization;
import org.apache.commons.math3.transform.FastCosineTransformer;
import org.apache.commons.math3.transform.TransformType;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.filter.operator.In;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

class ValueWithIndex {
    double value;
    int index;

    public ValueWithIndex(double value, int index) {
        this.value = value;
        this.index = index;
    }
}

public class DCT_IDCT {

    static int blockSize = 9;// 使用的blocksize，只能使用2的幂次+1
    static int n = 3;// 选取前几个分量，需要保证n < blockSize
    public static void getTopNMaxAbsoluteValues(double[] array, int n, double[] values, int[] indices) {
        if (array == null || array.length < n) {
            throw new IllegalArgumentException("Array must contain at least " + n + " elements.");
        }

        List<ValueWithIndex> list = new ArrayList<>();
        for (int i = 0; i < array.length; i++) {
            list.add(new ValueWithIndex(array[i], i));
        }

        list.sort((o1, o2) -> Double.compare(Math.abs(o2.value), Math.abs(o1.value)));

        for (int i = 0; i < n; i++) {
            values[i] = list.get(i).value;
            indices[i] = list.get(i).index;
        }
    }
    private static final FastCosineTransformer dctTransformer = new FastCosineTransformer(DctNormalization.ORTHOGONAL_DCT_I);

    public static ArrayList<Byte> encode(int[] timeSeries) throws IOException {
        int numberOfBlocks = (timeSeries.length + blockSize - 1) / blockSize;
        ArrayList<Byte> resList = new ArrayList<>();
        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        byteBuffer.putInt(numberOfBlocks);
        byte[] byteArray = byteBuffer.array();
        for (byte temp:byteArray){
            resList.add(temp);
        }

        for (int i = 0; i < numberOfBlocks; i++) {
            if (i != numberOfBlocks - 1) {
                int start = i * blockSize;
                int end = Math.min(start + blockSize, timeSeries.length);

                int[] block = new int[end - start];
                System.arraycopy(timeSeries, start, block, 0, end - start);

                double[] d_ts = new double[block.length];
                int index = 0;
                for (int value : block) {
                    d_ts[index] = value;
                    index++;
                }
                double[] res = dctTransformer.transform(d_ts, TransformType.FORWARD);
                double[] values = new double[n];
                int[] indices = new int[n];

                try {
                    getTopNMaxAbsoluteValues(res, n, values, indices);
                    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                    double[] new_res = new double[block.length];
                    for (int k = 0; k < n; k++){
                        new_res[indices[k]] = values[k];
                    }
                    double[] new_dts = dctTransformer.transform(new_res, TransformType.INVERSE);
                    Encoder encoder =
                            TSEncodingBuilder.getEncodingBuilder(TSEncoding.TS_2DIFF).getEncoder(TSDataType.INT32);
                    int[] new_its = new int[new_dts.length];
                    for (int k = 0; k < new_dts.length; k++) {
                         new_its[k] = (int) Math.round(new_dts[k]);
                    }
                    int[] err = new int[new_dts.length];
                    for (int k = 0; k < new_dts.length; k++) {
                        err[k] = new_its[k] - block[k];
                    }
                    for (int k = 0; k < new_dts.length; k++) {
                        encoder.encode(err[k],buffer);
                    }
                    encoder.flush(buffer);
                    byte[] elems = buffer.toByteArray();
                    for (int k = 0; k < n; k++){
                        byteBuffer = ByteBuffer.allocate(4);
                        byteBuffer.putInt(indices[k]);
                        byteArray = byteBuffer.array();
                        for (byte temp:byteArray){
                            resList.add(temp);
                        }
                        byteBuffer = ByteBuffer.allocate(8);
                        byteBuffer.putDouble(values[k]);
                        byteArray = byteBuffer.array();
                        for (byte temp:byteArray){
                            resList.add(temp);
                        }
                    }
                    byteBuffer = ByteBuffer.allocate(4);
                    byteBuffer.putInt(elems.length);
                    byteArray = byteBuffer.array();
                    for (byte temp:byteArray){
                        resList.add(temp);
                    }
                    for (byte temp:elems){
                        resList.add(temp);
                    }
                } catch (IllegalArgumentException e) {
                    System.out.println(e.getMessage());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                int start = i * blockSize;
                int end = Math.min(start + blockSize, timeSeries.length);

                int[] block = new int[end - start];
                System.arraycopy(timeSeries, start, block, 0, end - start);
                Encoder encoder =
                        TSEncodingBuilder.getEncodingBuilder(TSEncoding.TS_2DIFF).getEncoder(TSDataType.INT32);
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                for (int j : block) {
                    encoder.encode(j, buffer);
                }
                encoder.flush(buffer);
                byte[] elems = buffer.toByteArray();
                byteBuffer = ByteBuffer.allocate(4);
                byteBuffer.putInt(elems.length);
                byteArray = byteBuffer.array();
                for (byte temp:byteArray){
                    resList.add(temp);
                }
                for (byte temp:elems){
                    resList.add(temp);
                }
            }
        }
        return resList;
    }

    public static ArrayList<Integer> decode(ArrayList<Byte> encoded) throws IOException {
        ArrayList<Integer> res = new ArrayList<>();
        int cursor = 0;
        byte[] numByte = new byte[4];
        for (int i = 0; i < 4; i++){
            numByte[i] = encoded.get(cursor++);
        }
        ByteBuffer byteBuffer = ByteBuffer.wrap(numByte);
        int numberOfBlocks = byteBuffer.getInt(0);
        for (int i = 0; i < numberOfBlocks; i++) {
            if (i != numberOfBlocks - 1) {
                double[] new_res = new double[blockSize];
                for (int k = 0; k < n; k++){
                    numByte = new byte[4];
                    for (int j = 0; j < 4; j++){
                        numByte[j] = encoded.get(cursor++);
                    }
                    byteBuffer = ByteBuffer.wrap(numByte);
                    int index = byteBuffer.getInt(0);
                    numByte = new byte[8];
                    for (int j = 0; j < 8; j++){
                        numByte[j] = encoded.get(cursor++);
                    }
                    byteBuffer = ByteBuffer.wrap(numByte);
                    double value = byteBuffer.getDouble(0);
                    new_res[index] = value;
                }
                double[] new_dts = dctTransformer.transform(new_res, TransformType.INVERSE);
                Decoder decoder = Decoder.getDecoderByType(TSEncoding.TS_2DIFF, TSDataType.INT32);
                int[] new_its = new int[new_dts.length];
                for (int k = 0; k < new_dts.length; k++) {
                    new_its[k] = (int) Math.round(new_dts[k]);
                }
                numByte = new byte[4];
                for (int j = 0; j < 4; j++){
                    numByte[j] = encoded.get(cursor++);
                }
                byteBuffer = ByteBuffer.wrap(numByte);
                int length = byteBuffer.getInt(0);
                numByte = new byte[length];
                for (int j = 0; j < length; j++){
                    numByte[j] = encoded.get(cursor++);
                }
                byteBuffer = ByteBuffer.wrap(numByte);
                int j = 0;
                while (decoder.hasNext(byteBuffer)) {
                    int temp = decoder.readInt(byteBuffer);
                    res.add(new_its[j++] - temp);
                }
            } else {
                Decoder decoder = Decoder.getDecoderByType(TSEncoding.TS_2DIFF, TSDataType.INT32);
                numByte = new byte[4];
                for (int j = 0; j < 4; j++){
                    numByte[j] = encoded.get(cursor++);
                }
                byteBuffer = ByteBuffer.wrap(numByte);
                int length = byteBuffer.getInt(0);
                numByte = new byte[length];
                for (int j = 0; j < length; j++){
                    numByte[j] = encoded.get(cursor++);
                }
                byteBuffer = ByteBuffer.wrap(numByte);
                while (decoder.hasNext(byteBuffer)) {
                    int temp = decoder.readInt(byteBuffer);
                    res.add(temp);
                }
            }
        }
        return res;
    }

    //测试
    public static void main(String[] args) throws IOException {
        int[] timeSeries = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13,};

        ArrayList<Byte> encodeResult = encode(timeSeries);

        ArrayList<Integer> reconstructedTimeSeries = decode(encodeResult);
        for (int i:reconstructedTimeSeries){
            System.out.println(i);
        }
    }
}