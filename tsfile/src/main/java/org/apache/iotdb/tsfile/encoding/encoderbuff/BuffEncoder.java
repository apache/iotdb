/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.iotdb.tsfile.encoding.encoderbuff;

//import cn.edu.thu.diq.compression.BitConstructor;
//import cn.edu.thu.diq.compression.BitReader;
//import org.eclipse.collections.impl.list.mutable.primitive.DoubleArrayList;

import jodd.util.collection.DoubleArrayList;
import org.apache.iotdb.tsfile.encoding.BitConstructor;
import org.apache.iotdb.tsfile.encoding.BitReader;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Wang Haoyu
 */
public class BuffEncoder implements Encoder {

    private final double sparseThreshold = 0.9;
    private final int blocksize = 1024;
    private final int precision;
    private final double eps;

    public BuffEncoder(int precision) {
        this.precision = Math.max(0, precision);
        this.eps = Math.pow(2, -precision);
    }

    @Override
    public byte[] encode(double[] origin) {
        BitConstructor constructor = new BitConstructor();
        for (int i = 0; i < origin.length; i += blocksize) {
            int n = Math.min(origin.length, i + blocksize) - i;
            encodeBlock(constructor, origin, i, i + n);
        }
        return constructor.toByteArray();
    }

    @Override
    public double[] decode(byte[] bytes) {
        BitReader reader = new BitReader(bytes);
        DoubleArrayList decoded = new DoubleArrayList();
        while (reader.hasNext()) {
            decoded.addAll(decodeBlock(reader));
        }
        return decoded.toArray();
    }

    private void encodeBlock(BitConstructor constructor, double[] values, int from, int to) {
        //获取整数部分的范围
        int min = Integer.MAX_VALUE, max = Integer.MIN_VALUE;
        for (int i = from; i < to; i++) {
            min = Math.min(min, (int) Math.floor(values[i]));
            max = Math.max(max, (int) Math.floor(values[i]));
        }
        //保存元数据
        constructor.add(to - from, 32);
//        System.out.println(to - from);
        constructor.add(this.precision, 32);
        constructor.add(min, 32);
        constructor.add(max, 32);
        //浮点数转定点数
        long[] fixed = new long[to - from];
        for (int i = 0; i < fixed.length; i++) {
            fixed[i] = (long) (Math.round((values[i + from] - min) / eps));
        }
        //按照子列分别进行存储
        int[] masks = {0, 0x0001, 0x0003, 0x0007, 0x000f, 0x001f, 0x003f, 0x007f, 0x00ff};
        int totalWidth = getValueWidth(max - min) + precision;
        for (int i = totalWidth; i > 0; i -= 8) {
            int shift = Math.max(0, i - 8), len = Math.min(8, i);
            byte[] bytes = new byte[fixed.length];
            for (int j = 0; j < fixed.length; j++) {
                bytes[j] = (byte) ((fixed[j] >> shift) & masks[len]);
            }
            encodeSubColumn(constructor, bytes, len);
        }
        constructor.pad();
    }

    private double[] decodeBlock(BitReader reader) {
        //读取元数据
        int n = (int) reader.next(32);
//        System.out.println(n);
        int p = (int) reader.next(32);
        int min = (int) reader.next(32);
        int max = (int) reader.next(32);
        //读取子列
        long[] fixed = new long[n];
        int totalWidth = getValueWidth(max - min) + p;
        for (int i = totalWidth; i > 0; i -= 8) {
            int len = Math.min(8, i);
            byte[] bytes = decodeSubColumn(reader, n, len);
            for (int j = 0; j < n; j++) {
                fixed[j] = (fixed[j] << len) | (bytes[j] & 0xff);
            }
        }
        reader.skip();
        //定点数转换为浮点数        
        double[] values = new double[n];
        double eps1 = Math.pow(2, -p);
        for (int i = 0; i < n; i++) {
            values[i] = fixed[i] * eps1 + min;
        }
        return values;
    }

    private void encodeSubColumn(BitConstructor constructor, byte[] bytes, int len) {
        //统计各种数值出现的次数，判断是否采用稀疏表示
        Byte frequentValue = count(bytes, new HashMap<>());
        if (frequentValue == null) {
            constructor.add(0, 8);
            encodeDenseSubColumn(constructor, bytes, len);
        } else {
            constructor.add(1, 8);
            encodeSparseSubColumn(constructor, bytes, frequentValue, len);
        }
    }

    private byte[] decodeSubColumn(BitReader reader, int n, int len) {
        int sparseFlag = (int) reader.next(8);
        switch (sparseFlag) {
            case 0:
                return decodeDenseSubColumn(reader, n, len);
            case 1:
                return decodeSparseSubColumn(reader, n, len);
            default:
                throw new RuntimeException("It cannot be reached.");
        }
    }

    private void encodeDenseSubColumn(BitConstructor constructor, byte[] bytes, int len) {
        //存储数据
        for (byte b : bytes) {
            constructor.add(b, len);
        }
    }

    private byte[] decodeDenseSubColumn(BitReader reader, int n, int len) {
        byte[] bytes = new byte[n];
        for (int i = 0; i < n; i++) {
            bytes[i] = (byte) reader.next(len);
        }
        return bytes;
    }

    private void encodeSparseSubColumn(BitConstructor constructor, byte[] bytes, byte frequentValue, int len) {
        //存储众数
        constructor.add(frequentValue, len);
        //存储RLE压缩的比特向量
        BitConstructor rle = new BitConstructor();
        int cnt = encodeRLEVector(rle, bytes, frequentValue);
        byte[] rleBytes = rle.toByteArray();
        constructor.pad();
        constructor.add(rleBytes.length, 32);
        constructor.add(rleBytes);
        //存储离群点
        constructor.add(cnt, 32);
        for (byte b : bytes) {
            if (b != frequentValue) {
                constructor.add(b, len);
            }
        }
    }

    private int encodeRLEVector(BitConstructor constructor, byte[] bytes, byte frequentValue) {
        int width = getValueWidth(bytes.length);
        boolean outlier = false;
        int run = 0;
        int cnt = 0;
        for (byte b : bytes) {
            if ((b != frequentValue) == outlier) {
                run++;
            } else {
                outlier = !outlier;
                constructor.add(run, width);
                run = 1;
            }
            if (b != frequentValue) {
                cnt++;
            }
        }
        constructor.add(run, width);
        return cnt;
    }

    private boolean[] decodeRLEVector(byte[] bytes, int n) {
        BitReader reader = new BitReader(bytes);
        boolean[] vector = new boolean[n];
        int width = getValueWidth(n);
        int i = 0;
        boolean bit = false;
        while (i < n) {
            int run = (int) reader.next(width);
            int j = i + run;
            for (; i < j; i++) {
                vector[i] = bit;
            }
            bit = !bit;
        }
        return vector;
    }

    private byte[] decodeSparseSubColumn(BitReader reader, int n, int len) {
        //读取众数
        byte frequentValue = (byte) reader.next(len);
        //读取RLE比特向量
        reader.skip();
        int rleByteSize = (int) reader.next(32);
        boolean vector[] = decodeRLEVector(reader.nextBytes(rleByteSize), n);
        //读取离群点
        int cnt = (int) reader.next(32);
        byte[] bytes = new byte[n];
        for (int i = 0; i < n; i++) {
            if (vector[i]) {
                bytes[i] = (byte) reader.next(len);
            } else {
                bytes[i] = frequentValue;
            }
        }
        return bytes;
    }

    private Byte count(byte[] bytes, HashMap<Byte, Integer> map) {
        for (byte x : bytes) {
            map.put(x, map.getOrDefault(x, 0) + 1);
        }
        Byte maxByte = null;
        int maxTimes = 0;
        for (Map.Entry<Byte, Integer> entry : map.entrySet()) {
            Byte key = entry.getKey();
            Integer value = entry.getValue();
            if (value > maxTimes) {
                maxTimes = value;
                maxByte = key;
            }
        }
        if (maxTimes > sparseThreshold * bytes.length) {
            return maxByte;
        } else {
            return null;
        }
    }

    /**
     * 计算x的数据宽度
     *
     * @param x
     * @return 数据宽度
     */
    public int getValueWidth(long x) {
        return 64 - Long.numberOfLeadingZeros(x);
    }
}
