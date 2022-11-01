/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.iotdb.tsfile.encoding;

//import org.eclipse.collections.impl.list.mutable.primitive.ByteArrayList;

import com.carrotsearch.hppc.ByteArrayList;

public class BitConstructor {

//    public static long time = 0;
    private static final int BITS_IN_A_BYTE = 8;
    private static final long ALL_MASK = -1;
    private final ByteArrayList data;
    private byte cache = 0;
    private int cnt = 0;

    public BitConstructor() {
        this.data = new ByteArrayList();
    }

    public BitConstructor(int initialCapacity) {
        this.data = new ByteArrayList(initialCapacity);
    }

    public void add(long x, int len) {
//        long before = System.nanoTime();
        x = x & ~(ALL_MASK << len);//保证x除最低的len位之外都是0
        while (len > 0) {
            int m = len + cnt >= BITS_IN_A_BYTE ? BITS_IN_A_BYTE - cnt : len;//向cache中插入的bit数
            len -= m;
            cnt += m;
            byte y = (byte) (x >> len);
            y = (byte) (y << (BITS_IN_A_BYTE - cnt));
            cache = (byte) (cache | y);
            x = x & ~(ALL_MASK << len);
            if (cnt == BITS_IN_A_BYTE) {
                pad();
            }
        }
//        long after = System.nanoTime();
//        time += (after - before);
    }

    public byte[] toByteArray() {
        byte[] ret;
        if (cnt > 0) {
            data.add(cache);
            ret = data.toArray();
            data.remove(data.size() - 1);
        } else {
            ret = data.toArray();
        }
        return ret;
    }

    public void clear() {
        data.clear();
        cache = 0x00;
        cnt = 0;
    }

    /**
     * 如果当前字节存在剩余位，则全部填充0
     */
    public void pad() {
        if (cnt > 0) {
            data.add(cache);
            cache = 0x00;
            cnt = 0;
        }
    }

    public void add(byte[] bytes) {
        if (cnt == 0) {
            data.add(bytes);
        } else {
            for (int i = 0; i < bytes.length; i++) {
                add(bytes[i], 8);
            }
        }
    }

    public int length() {
        return data.size();
    }

    public int lengthInBits() {
        return data.size() * 8 + cnt;
    }
}
