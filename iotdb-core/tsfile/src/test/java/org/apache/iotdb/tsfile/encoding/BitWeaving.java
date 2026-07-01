package org.apache.iotdb.tsfile.encoding;

import java.util.BitSet;
import java.util.Random;
import java.util.Arrays;

public class BitWeaving {
    private final long[][] bitPlanes;
    private final int bitWidth;
    private final int rowCount;
    private final int wordsPerPlane;

    public BitWeaving(int[] values) {
        if (values == null) throw new IllegalArgumentException("values == null");
        this.rowCount = values.length;
        if (rowCount == 0) {
            this.bitWidth = 1;
            this.wordsPerPlane = 0;
            this.bitPlanes = new long[bitWidth][0];
            return;
        }
        int maxV = 0;
        for (int v : values) {
            if (v < 0) throw new IllegalArgumentException("This simple implementation expects non-negative integers. v=" + v);
            if (v > maxV) maxV = v;
        }
        this.bitWidth = Math.max(1, 32 - Integer.numberOfLeadingZeros(maxV));
        this.wordsPerPlane = (rowCount + 63) / 64;
        this.bitPlanes = new long[bitWidth][wordsPerPlane];

        for (int bit = 0; bit < bitWidth; ++bit) {
            long[] plane = bitPlanes[bit];
            for (int row = 0; row < rowCount; ++row) {
                if (((values[row] >> bit) & 1) != 0) {
                    int wordIdx = row >>> 6;
                    int bitInWord = row & 63;
                    plane[wordIdx] |= (1L << bitInWord);
                }
            }
        }
    }

    public int rowCount() { return rowCount; }

    private static void maskToBitSet(long mask, int baseIndex, int rowCount, BitSet out) {
        int limit = Math.min(64, Math.max(0, rowCount - baseIndex));
        for (int b = 0; b < limit; ++b) {
            if ((mask & (1L << b)) != 0L) out.set(baseIndex + b);
        }
    }

    public BitSet lessThanToConst(int K) {
        BitSet result = new BitSet(rowCount);
        if (rowCount == 0) return result;
        for (int w = 0; w < wordsPerPlane; ++w) {
            long eq = ~0L;
            long lt = 0L;
            long gt = 0L;
            for (int bit = bitWidth - 1; bit >= 0; --bit) {
                long planeWord = bitPlanes[bit][w];
                if (((K >> bit) & 1) == 1) {
                    lt |= (eq & ~planeWord);
                    eq &= planeWord;
                } else {
                    gt |= (eq & planeWord);
                    eq &= ~planeWord;
                }
            }
            int baseIndex = w << 6;
            int remain = Math.max(0, rowCount - baseIndex);
            long validMask = (remain >= 64) ? ~0L : ((1L << remain) - 1L);
            lt &= validMask;
            maskToBitSet(lt, baseIndex, rowCount, result);
        }
        return result;
    }

    public BitSet equalToConst(int K) {
        BitSet result = new BitSet(rowCount);
        if (rowCount == 0) return result;
        for (int w = 0; w < wordsPerPlane; ++w) {
            long eq = ~0L;
            for (int bit = bitWidth - 1; bit >= 0; --bit) {
                long planeWord = bitPlanes[bit][w];
                if (((K >> bit) & 1) == 1) {
                    eq &= planeWord;
                } else {
                    eq &= ~planeWord;
                }
            }
            int baseIndex = w << 6;
            int remain = Math.max(0, rowCount - baseIndex);
            long validMask = (remain >= 64) ? ~0L : ((1L << remain) - 1L);
            eq &= validMask;
            maskToBitSet(eq, baseIndex, rowCount, result);
        }
        return result;
    }

    public BitSet greaterThanConst(int K) {
        BitSet result = new BitSet(rowCount);
        if (rowCount == 0) return result;
        for (int w = 0; w < wordsPerPlane; ++w) {
            long eq = ~0L;
            long gt = 0L;
            for (int bit = bitWidth - 1; bit >= 0; --bit) {
                long planeWord = bitPlanes[bit][w];
                if (((K >> bit) & 1) == 1) {
                    eq &= planeWord;
                } else {
                    gt |= (eq & planeWord);
                    eq &= ~planeWord;
                }
            }
            int baseIndex = w << 6;
            int remain = Math.max(0, rowCount - baseIndex);
            long validMask = (remain >= 64) ? ~0L : ((1L << remain) - 1L);
            gt &= validMask;
            maskToBitSet(gt, baseIndex, rowCount, result);
        }
        return result;
    }

    public void dumpPlanes() {
        System.out.println("bitWidth=" + bitWidth + ", rowCount=" + rowCount + ", wordsPerPlane=" + wordsPerPlane);
        for (int bit = bitWidth - 1; bit >= 0; --bit) {
            System.out.print("bit " + bit + " : ");
            for (int r = 0; r < rowCount; ++r) {
                int wi = r >>> 6;
                int bi = r & 63;
                long word = bitPlanes[bit][wi];
                System.out.print(((word >>> bi) & 1L));
            }
            System.out.println();
        }
    }

    public static void main(String[] args) {
        final int N = 130;
        final int MAXV = 31;
        int[] values = new int[N];
        Random rnd = new Random(12345);
        for (int i = 0; i < N; ++i) values[i] = rnd.nextInt(MAXV + 1);

        BitWeaving bw = new BitWeaving(values);
        System.out.println("Generated " + N + " random values (max " + MAXV + "). bitWidth used = " + bw.bitWidth);
        for (int K = 0; K <= MAXV; ++K) {
            BitSet lt = bw.lessThanToConst(K);
            BitSet eq = bw.equalToConst(K);
            BitSet gt = bw.greaterThanConst(K);

            BitSet expLt = new BitSet(N);
            BitSet expEq = new BitSet(N);
            BitSet expGt = new BitSet(N);
            for (int i = 0; i < N; ++i) {
                if (values[i] < K) expLt.set(i);
                else if (values[i] == K) expEq.set(i);
                else expGt.set(i);
            }

            if (!lt.equals(expLt) || !eq.equals(expEq) || !gt.equals(expGt)) {
                System.err.println("Mismatch for K=" + K);
                System.err.println("values: " + Arrays.toString(values));
                System.err.println("lt result: " + lt);
                System.err.println("expected lt: " + expLt);
                System.err.println("eq result: " + eq);
                System.err.println("expected eq: " + expEq);
                System.err.println("gt result: " + gt);
                System.err.println("expected gt: " + expGt);
                System.exit(2);
            }
        }
        int Kdemo = 10;
        BitSet ltd = bw.lessThanToConst(Kdemo);
        BitSet eqd = bw.equalToConst(Kdemo);
        BitSet gtd = bw.greaterThanConst(Kdemo);
        for (int i = 0; i < Math.min(32, N); ++i) {
            System.out.printf("%3d: %2d  <%b  =%b  >%b\n", i, values[i], ltd.get(i), eqd.get(i), gtd.get(i));
        }
    }
}

