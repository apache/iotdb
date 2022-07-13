/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.iotdb.tsfile.encoding;

import java.util.Arrays;

/**
 *
 * @author Wang Haoyu
 */
public class BitReader {

    private static final int BITS_IN_A_BYTE = 8;
    private static final byte MASKS[] = {(byte) 0xff, 0x7f, 0x3f, 0x1f, 0x0f, 0x07, 0x03, 0x01};
    private final byte[] data;
    private int byteCnt = 0, bitCnt = 0;

    public BitReader(byte[] data) {
        this.data = data;
    }

    public long next(int len) {
        long ret = 0;
        while (len > 0) {
            int m = len + bitCnt >= BITS_IN_A_BYTE ? BITS_IN_A_BYTE - bitCnt : len;//从当前byte中读取的bit数
            len -= m;
            ret = ret << m;
            byte y = (byte) (data[byteCnt] & MASKS[bitCnt]);//运算时byte自动转化为int，需要&截取低位
            y = (byte) ((y & 0xff) >>> (BITS_IN_A_BYTE - bitCnt - m));//逻辑右移，高位补0
            ret = ret | (y & 0xff);
            bitCnt += m;
            if (bitCnt == BITS_IN_A_BYTE) {
                skip();
            }
        }
        return ret;
    }

    public byte[] nextBytes(int len) {
        byte[] ret;
        if (bitCnt == 0) {
            ret = Arrays.copyOfRange(data, byteCnt, byteCnt + len);
            byteCnt += len;
        } else {
            ret = new byte[len];
            for (int i = 0; i < len; i++) {
                ret[i] = (byte) next(8);
            }
        }
        return ret;
    }

    public void skip() {
        if (bitCnt > 0) {
            bitCnt = 0;
            byteCnt++;
        }
    }

    public boolean hasNext() {
        return byteCnt < data.length;
    }

}
