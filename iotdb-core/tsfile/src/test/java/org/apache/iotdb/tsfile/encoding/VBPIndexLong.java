package org.apache.iotdb.tsfile.encoding;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class VBPIndexLong {

    public static final int W = 64;

    public final int k;
    public final int n;
    public final int wordsPerPlane;
    public final long[][] planes;

    public enum Op {
        EQ, NE, LT, LE, GT, GE
    }

    public VBPIndexLong(int kBits, long[] codes) {

        this.k = kBits;
        this.n = codes.length;
        this.wordsPerPlane = (n + W - 1) / W;
        this.planes = new long[k][wordsPerPlane];
        pack(codes);
    }

    private void pack(long[] codes) {
        for (int row = 0; row < n; row++) {
            int wordIdx = row / W;
            int bitPos = row % W;
            long bitMask = 1L << bitPos;
            long code = codes[row];
            for (int t = 0; t < k; t++) {
                if (((code >>> t) & 1) != 0) {
                    planes[t][wordIdx] |= bitMask;
                }
            }
        }
    }

    public int[] select(Op op, long C) {
        long codeMask = (k == 64) ? ~0L : ((1L << k) - 1L);
        long Ck = (C & codeMask);
        // return selectInternal(op, Ck);

        // return selectInternal2(op, Ck);
        return selectInternal3(op, Ck);
    }

    private int[] selectInternal2(Op op, long Ck) {
        List<Integer> out = new ArrayList<>();
        for (int row = 0; row < n; row++) {
            long code = 0;
            for (int t = 0; t < k; t++) {
                int wordIdx = row / W;
                int bitPos = row % W;
                long bitVal = (planes[t][wordIdx] >> bitPos) & 1L;
                code |= (bitVal << t);
            }
            switch (op) {
                case EQ:
                    if (code == Ck)
                        out.add(row);
                    break;
                case NE:
                    if (code != Ck)
                        out.add(row);
                    break;
                case LT:
                    if (code < Ck)
                        out.add(row);
                    break;
                case LE:
                    if (code <= Ck)
                        out.add(row);
                    break;
                case GT:
                    if (code > Ck)
                        out.add(row);
                    break;
                case GE:
                    if (code >= Ck)
                        out.add(row);
                    break;
            }
        }
        return out.stream().mapToInt(i -> i).toArray();
    }

    private int[] selectInternal3(Op op, long Ck) {
        List<Integer> out = new ArrayList<>();
        for (int row = 0; row < n; row++) {
            int wordIdx = row / W;
            int bitPos = row % W;
            switch (op) {
                case EQ:
                    boolean match = true;
                    for (int t = 0; t < k; t++) {
                        long bitVal = (planes[t][wordIdx] >> bitPos) & 1L;
                        long cBit = (Ck >> t) & 1L;
                        if (bitVal != cBit) {
                            match = false;
                            break;
                        }
                    }
                    if (match) {
                        out.add(row);
                    }
                    break;
                case NE:
                    boolean notMatch = false;
                    for (int t = 0; t < k; t++) {
                        long bitVal = (planes[t][wordIdx] >> bitPos) & 1L;
                        long cBit = (Ck >> t) & 1L;
                        if (bitVal != cBit) {
                            notMatch = true;
                            break;
                        }
                    }
                    if (notMatch) {
                        out.add(row);
                    }
                    break;
                default:
                    long code = 0;
                    for (int t = 0; t < k; t++) {
                        long bitVal = (planes[t][wordIdx] >> bitPos) & 1L;
                        code |= (bitVal << t);
                    }
                    switch (op) {
                        case LT:
                            if (code < Ck)
                                out.add(row);
                            break;
                        case LE:
                            if (code <= Ck)
                                out.add(row);
                            break;
                        case GT:
                            if (code > Ck)
                                out.add(row);
                            break;
                        case GE:
                            if (code >= Ck)
                                out.add(row);
                            break;
                    }
            }
        }
        return out.stream().mapToInt(i -> i).toArray();
    }

    private int[] selectInternal(Op op, long Ck) {
        List<Integer> out = new ArrayList<>();

        for (int w = 0; w < wordsPerPlane; w++) {
            int bitsInThisWord = Math.min(W, n - w * W);
            long validMask = (bitsInThisWord == 64) ? ~0L : ((1L << bitsInThisWord) - 1L);

            long E = validMask;
            long L = 0L;
            long G = 0L;

            for (int t = k - 1; t >= 0; t--) {
                long B = planes[t][w] & validMask;
                long cb = (Ck >>> t) & 1;
                if (cb == 1) {
                    L |= (E & (~B));
                    E &= B;
                } else {
                    G |= (E & B);
                    E &= (~B);
                }
            }

            long res;
            switch (op) {
                case EQ:
                    res = E;
                    break;
                case NE:
                    res = (~E) & validMask;
                    break;
                case LT:
                    res = L;
                    break;
                case LE:
                    res = (L | E) & validMask;
                    break;
                case GT:
                    res = G;
                    break;
                case GE:
                    res = (G | E) & validMask;
                    break;
                default:
                    res = 0L;
            }

            int base = w * W;
            long tmp = res;
            while (tmp != 0L) {
                int t = Long.numberOfTrailingZeros(tmp);
                out.add(base + t);
                tmp &= (tmp - 1);
            }
        }

        return out.stream().mapToInt(i -> i).toArray();
    }

    public int count(Op op, long C) {
        int[] res = select(op, C);
        return res.length;
    }

    public int count() {
        return n;
    }

    public int size() {
        return n;
    }

    public long getCode(int row) {
        if (row < 0 || row >= n)
            throw new IndexOutOfBoundsException();
        int wordIdx = row / W;
        int bitPos = row % W;
        long code = 0;
        long mask = 1L << bitPos;
        for (int t = 0; t < k; t++) {
            long planeWord = planes[t][wordIdx];
            if ((planeWord & mask) != 0) {
                code |= (1L << t);
            }
        }
        return code;
    }

    public int findMaxIndex() {
        if (n == 0)
            return -1;

        boolean[] candidates = new boolean[n];
        for (int i = 0; i < n; i++) {
            candidates[i] = true;
        }

        for (int t = k - 1; t >= 0; t--) {
            boolean hasOnes = false;

            for (int row = 0; row < n; row++) {
                if (candidates[row]) {
                    if ((planes[t][row / W] & (1L << (row % W))) != 0) {
                        hasOnes = true;
                    } else {
                        candidates[row] = false;
                    }
                }
            }

            if (!hasOnes) {
                for (int row = 0; row < n; row++) {
                    if (candidates[row]) {
                        if ((planes[t][row / W] & (1L << (row % W))) != 0) {
                            candidates[row] = false;
                        }
                    }
                }
            }
        }

        for (int row = 0; row < n; row++) {
            if (candidates[row]) {
                return row;
            }
        }

        return -1;
    }

    public long sum() {
        if (n == 0)
            return 0L;

        long totalSum = 0L;

        for (int t = 0; t < k; t++) {
            long bitContribution = 0L;

            for (int i = 0; i < n; i++) {
                long planeWord = planes[t][i / W];
                bitContribution += (planeWord >> (i % W)) & 1;
            }

            totalSum += (bitContribution << t);
        }

        return totalSum;
    }

    public static void main(String[] args) {
        int k = 3;
        long[] codes = { 1, 5, 6, 1, 6, 4, 0, 7, 4, 3 };
        VBPIndexLong idx = new VBPIndexLong(k, codes);

        for (int i = 0; i < idx.wordsPerPlane; i++) {
            System.out.println("word " + i + ":");
            for (int t = 0; t < k; t++) {
                System.out.println(String.format("%64s", Long.toBinaryString(idx.planes[t][i])).replace(' ', '0'));
            }
            System.out.println();
        }

        System.out.println("n = " + idx.size());

        int[] lt4 = idx.select(Op.LT, 4);
        System.out.print("< 4 -> ");
        for (int i = 0; i < lt4.length; i++) {
            System.out.print(lt4[i] + " ");
        }
        System.out.println();

        int[] eq4 = idx.select(Op.EQ, 4);
        System.out.print("= 4 -> ");
        for (int i = 0; i < eq4.length; i++) {
            System.out.print(eq4[i] + " ");
        }
        System.out.println();

        int[] ge6 = idx.select(Op.GE, 6);
        System.out.print(">= 6 -> ");
        for (int i = 0; i < ge6.length; i++) {
            System.out.print(ge6[i] + " ");
        }
        System.out.println();

        for (int i = 0; i < idx.size(); i++) {
            System.out.print(idx.getCode(i) + " ");
        }
        System.out.println();

        System.out.println("count(<5) = " + idx.count(Op.LT, 5));
    }
}
