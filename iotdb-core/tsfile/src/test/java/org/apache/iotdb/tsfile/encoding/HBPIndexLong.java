package org.apache.iotdb.tsfile.encoding;

import java.util.*;

public class HBPIndexLong {

    public static final int W = 64;

    public final int k;

    public final int sectionBits;

    public final int sectionsPerWord;

    public final int codesPerSegment;

    public final int n;

    public final int segments;

    public final long[] words;

    public final long lowKOnesRepeat;
    public final long delimiterBitRepeat;
    public final long addOneEachSection;

    public enum Op {
        EQ, NE, LT, LE, GT, GE
    }

    public HBPIndexLong(int kBits, long[] codes) {

        this.k = kBits;
        this.sectionBits = k + 1;
        this.sectionsPerWord = W / sectionBits;
        if (sectionsPerWord <= 0)
            throw new IllegalArgumentException("k too large for 64-bit word");
        this.codesPerSegment = sectionsPerWord * (k + 1);
        this.n = codes.length;
        this.segments = (n + codesPerSegment - 1) / codesPerSegment;
        this.words = new long[segments * (k + 1)];

        this.lowKOnesRepeat = repeatInSections((1L << k) - 1L);
        this.delimiterBitRepeat = repeatInSections(1L << k);
        this.addOneEachSection = repeatInSections(1L);

        pack(codes);
    }

    public BitSet select(Op op, long C) {
        return selectInternal(op, C & ((1L << k) - 1));
    }

    public long count(Op op, long C) {
        BitSet bs = select(op, C);
        return bs.cardinality();
    }

    public int size() {
        return n;
    }

    public long getCode(int index) {
        if (index < 0 || index >= n)
            throw new IndexOutOfBoundsException();
        int s = index / codesPerSegment;
        int posInSeg = index - s * codesPerSegment;
        int i = posInSeg % (k + 1);
        int j = posInSeg / (k + 1);
        long word = words[s * (k + 1) + i];
        long code = 0L;
        for (int b = 0; b < k; b += 2) {
            int chunkBits = Math.min(2, k - b);
            long chunk = (word >>> (j * sectionBits + b)) & ((1L << chunkBits) - 1L);
            code |= chunk << b;
        }
        return code;
    }

    public void pack(long[] codes) {
        for (int s = 0; s < segments; s++) {
            int base = s * codesPerSegment;
            int upto = Math.min(n, base + codesPerSegment);
            for (int i = 0; i <= k; i++) {
                long w = 0L;
                for (int j = 0; j < sectionsPerWord; j++) {
                    int idx = base + i + j * (k + 1);
                    if (idx >= upto)
                        break;
                    long code = codes[idx] & ((1L << k) - 1L);
                    for (int b = 0; b < k; b += 2) {
                        int chunkBits = Math.min(2, k - b);
                        long chunk = (code >> b) & ((1L << chunkBits) - 1L);
                        w |= chunk << (j * sectionBits + b);
                    }
                }
                words[s * (k + 1) + i] = w;
            }
        }
    }

    public BitSet selectInternal(Op op, long Ck) {
        BitSet out = new BitSet(n);
        long yRepeat = repeatInSections(Ck & ((1L << k) - 1));
        for (int s = 0; s < segments; s++) {
            long segBits = 0L;
            for (int i = 0; i <= k; i++) {

                long X = words[s * (k + 1) + i];
                long Z = fOp(op, X, yRepeat);

                long shifted = Z >>> (k - i);
                segBits |= shifted;
            }

            int base = s * codesPerSegment;
            int valid = Math.min(codesPerSegment, n - base);
            long mask = valid == 64 ? ~0L : ((1L << valid) - 1L);
            segBits &= mask;

            int bitIndexBase = s * codesPerSegment;
            while (segBits != 0) {
                int t = Long.numberOfTrailingZeros(segBits);
                out.set(bitIndexBase + t);
                segBits &= (segBits - 1);
            }
        }
        return out;
    }

    public long fOp(Op op, long X, long Yrepeat) {

        switch (op) {
            case NE:
                return ((X ^ Yrepeat) + lowKOnesRepeat) & delimiterBitRepeat;
            case EQ:
                return (~((X ^ Yrepeat) + lowKOnesRepeat)) & delimiterBitRepeat;
            case LT:
                return (Yrepeat + (X ^ lowKOnesRepeat)) & delimiterBitRepeat;
            case LE:
                return (Yrepeat + (X ^ lowKOnesRepeat) + addOneEachSection) & delimiterBitRepeat;
            case GT:
                return (X + (Yrepeat ^ lowKOnesRepeat)) & delimiterBitRepeat;
            case GE:
                return (X + (Yrepeat ^ lowKOnesRepeat) + addOneEachSection) & delimiterBitRepeat;
        }

        return 0L;
    }

    public long repeatInSections(long payload) {
        long res = 0L;
        for (int j = 0; j < sectionsPerWord; j++) {
            res |= (payload & ((1L << sectionBits) - 1L)) << (j * sectionBits);
        }
        return res;
    }


    public static void main(String[] args) {
        int k = 3;
        long[] codes = {
                1, 5, 6, 1, 6, 4, 0, 7, 4, 3
        };
        HBPIndexLong idx = new HBPIndexLong(k, codes);

        for (int i = 0; i < idx.words.length; i++) {
            System.out.printf("word[%d] = %016X\n", i, idx.words[i]);
        }

        System.out.println("segments: " + idx.segments);

        System.out.println("lowKOnesRepeat: " + String.format("%64s", Long.toBinaryString(idx.lowKOnesRepeat)).replace(' ', '0'));
        System.out.println("delimiterBitRepeat: " + String.format("%64s", Long.toBinaryString(idx.delimiterBitRepeat)).replace(' ', '0'));
        System.out.println("addOneEachSection: " + String.format("%64s", Long.toBinaryString(idx.addOneEachSection)).replace(' ', '0'));

        System.out.println("n = " + idx.size());

        BitSet lt5 = idx.select(Op.LT, 4);
        System.out.println("< 4 -> " + lt5);

        BitSet eq4 = idx.select(Op.EQ, 4);
        System.out.println("= 4 -> " + eq4);

        BitSet ge6 = idx.select(Op.GE, 6);
        System.out.println(">= 6 -> " + ge6);

        for (int i = 0; i < idx.size(); i++) {
            System.out.print(idx.getCode(i) + (i + 1 == idx.size() ? "\n" : " "));
        }

        System.out.println("count(<5) = " + idx.count(Op.LT, 5));
    }
}
