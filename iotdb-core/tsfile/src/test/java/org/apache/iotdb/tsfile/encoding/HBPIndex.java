package org.apache.iotdb.tsfile.encoding;

import java.util.*;

public class HBPIndex {

    /** width of processor word */
    public static final int W = 64;
    // public static final int W = 8;

    /** number of bits per code (k) */
    public final int k;

    /** section width = k + 1 (delimiter bit + k code bits) */
    public final int sectionBits;

    /** how many sections per 64-bit word */
    public final int sectionsPerWord;

    /** number of codes per segment = (k+1) * sectionsPerWord (≤ W) */
    public final int codesPerSegment;

    /** total number of codes (rows) */
    public final int n;

    /** number of segments */
    public final int segments;

    /**
     * Packed storage: for each segment s (0..segments-1), we store (k+1) words
     * v[0..k].
     * Layout: words[(s * (k+1)) + i] corresponds to vi (i in 0..k).
     */
    public final long[] words;

    /** Masks repeated per section */
    public final long lowKOnesRepeat; // 01^k01^k...01^k mask
    public final long delimiterBitRepeat; // 10^k10^k...10^k mask
    public final long addOneEachSection; // ...0001 in each section (LSB 1)

    public enum Op {
        EQ, NE, LT, LE, GT, GE
    }

    /**
     * Build HBP storage from k-bit codes.
     * 
     * @param kBits number of bits per code (1..63)
     * @param codes encoded values in [0, 2^kBits)
     */
    public HBPIndex(int kBits, int[] codes) {
        // if (kBits <= 0 || kBits >= W) {
        //     throw new IllegalArgumentException("k must be in [1, 63]");
        // }

        this.k = kBits;
        this.sectionBits = k + 1;
        this.sectionsPerWord = W / sectionBits; // floor
        if (sectionsPerWord <= 0)
            throw new IllegalArgumentException("k too large for 64-bit word");
        this.codesPerSegment = sectionsPerWord * (k + 1);
        this.n = codes.length;
        this.segments = (n + codesPerSegment - 1) / codesPerSegment;
        this.words = new long[segments * (k + 1)];

        // build masks
        this.lowKOnesRepeat = repeatInSections((1L << k) - 1L); // 01^k
        this.delimiterBitRepeat = repeatInSections(1L << k); // 10^k
        this.addOneEachSection = repeatInSections(1L); // ...0001

        // pack codes
        pack(codes);
    }

    /* ---------- Public API ---------- */

    /** Return a BitSet with one bit per row; bit=1 means row selected by op C. */
    public BitSet select(Op op, int C) {
        return selectInternal(op, C & ((1 << k) - 1));
    }

    /** Count matches for op C. */
    public long count(Op op, int C) {
        BitSet bs = select(op, C);
        return bs.cardinality();
    }

    /** Total rows */
    public int size() {
        return n;
    }

    /** Debug: fetch original k-bit code back (slow path). */
    public int getCode(int index) {
        if (index < 0 || index >= n)
            throw new IndexOutOfBoundsException();
        int s = index / codesPerSegment;
        int posInSeg = index - s * codesPerSegment; // 0..(codesPerSegment-1)
        // Which word vi holds this code in the segment?
        int i = posInSeg % (k + 1); // word index (v_i)
        int j = posInSeg / (k + 1); // section index inside that word
        long word = words[s * (k + 1) + i];
        long section = (word >>> (j * sectionBits)) & ((1L << sectionBits) - 1L);
        // section: [delimiter bit (MSB)=0][k code bits]
        return (int) (section & ((1L << k) - 1L));
    }

    /* ---------- Core HBP packing & scan ---------- */

    public void pack(int[] codes) {
        long sectionMask = (1L << sectionBits) - 1L;
        for (int s = 0; s < segments; s++) {
            int base = s * codesPerSegment;
            int upto = Math.min(n, base + codesPerSegment);
            int count = upto - base;
            // prepare k+1 words for this segment
            for (int i = 0; i <= k; i++) {
                long w = 0L;
                for (int j = 0; j < sectionsPerWord; j++) {
                    int idx = base + i + j * (k + 1);
                    if (idx >= upto)
                        break;
                    int code = codes[idx] & ((1 << k) - 1);
                    long section = (long) code; // delimiter (MSB) left as 0
                    w |= (section & sectionMask) << (j * sectionBits);
                }
                words[s * (k + 1) + i] = w;
            }
        }
    }

    public BitSet selectInternal(Op op, int Ck) {
        BitSet out = new BitSet(n);
        long yRepeat = repeatInSections(Ck & ((1 << k) - 1));
        for (int s = 0; s < segments; s++) {
            long segBits = 0L;
            // compute ms := OR_{i=0..k} ( f◦(v_i, C) >>> i )
            for (int i = 0; i <= k; i++) {
                // System.out.println("current segment = " + s + ", word index = " + i);

                long X = words[s * (k + 1) + i];
                long Z = fOp(op, X, yRepeat);

                // System.out.println("Z = " + String.format("%64s", Long.toBinaryString(Z)).replace(' ', '0'));

                // long shifted = (i == 0) ? Z : (Z >>> i);
                long shifted = Z >>> (k - i);
                segBits |= shifted;
            }

            // System.out.println("segBits = " + String.format("%64s", Long.toBinaryString(segBits)).replace(' ', '0'));

            // mask off any high bits beyond rows in last (possibly partial) segment
            int base = s * codesPerSegment;
            int valid = Math.min(codesPerSegment, n - base);
            long mask = valid == 64 ? ~0L : ((1L << valid) - 1L);
            segBits &= mask;

            // System.out.println("masked segBits = " + String.format("%64s", Long.toBinaryString(segBits)).replace(' ', '0'));

            // write into BitSet
            int bitIndexBase = s * codesPerSegment;
            while (segBits != 0) {
                int t = Long.numberOfTrailingZeros(segBits);
                out.set(bitIndexBase + t);
                segBits &= (segBits - 1); // clear lowest set bit
            }
        }
        return out;
    }

    /**
     * f◦ on one 64-bit word of packed sections, returns delimiter-bit-only result
     * (10^k in true sections).
     */
    public long fOp(Op op, long X, long Yrepeat) {
        // return switch (op) {
        // // case NE -> ((X ^ Yrepeat) + lowKOnesRepeat) & delimiterBitRepeat;
        // // case EQ -> (~((X ^ Yrepeat) + lowKOnesRepeat)) & delimiterBitRepeat;
        // // case LT -> (Yrepeat + (X ^ lowKOnesRepeat)) & delimiterBitRepeat;
        // // case LE -> (Yrepeat + (X ^ lowKOnesRepeat) + addOneEachSection) &
        // delimiterBitRepeat;
        // // case GT -> (X + (Yrepeat ^ lowKOnesRepeat)) & delimiterBitRepeat;
        // // case GE -> (X + (Yrepeat ^ lowKOnesRepeat) + addOneEachSection) &
        // delimiterBitRepeat;
        //
        // };

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

    /**
     * Repeat a (k+1)-bit payload across all sections (payload must fit into
     * sectionBits).
     */
    public long repeatInSections(long payload) {
        long res = 0L;
        for (int j = 0; j < sectionsPerWord; j++) {
            res |= (payload & ((1L << sectionBits) - 1L)) << (j * sectionBits);
        }
        return res;
    }

    /* ---------- Demo ---------- */

    public static void main(String[] args) {
        // Running example from the paper (§3.1.1): k=3, 10 codes
        int k = 3;
        int[] codes = {
                1, 5, 6, 1, 6, 4, 0, 7, 4, 3
        };
        HBPIndex idx = new HBPIndex(k, codes);

        for (int i = 0; i < idx.words.length; i++) {
            System.out.printf("word[%d] = %016X\n", i, idx.words[i]);
        }

        System.out.println("segments: " + idx.segments);

        System.out.println(String.format("%64s", Long.toBinaryString(idx.lowKOnesRepeat)).replace(' ', '0'));
        System.out.println(String.format("%64s", Long.toBinaryString(idx.delimiterBitRepeat)).replace(' ', '0'));
        System.out.println(String.format("%64s", Long.toBinaryString(idx.addOneEachSection)).replace(' ', '0'));

        System.out.println("n = " + idx.size());

        // c < 4
        BitSet lt5 = idx.select(Op.LT, 4);
        System.out.println("< 4 -> " + lt5);

        // c == 4
        BitSet eq4 = idx.select(Op.EQ, 4);
        System.out.println("= 4 -> " + eq4);

        // c >= 6
        BitSet ge6 = idx.select(Op.GE, 6);
        System.out.println(">= 6 -> " + ge6);

        // show decoded codes back (debug)
        for (int i = 0; i < idx.size(); i++) {
            System.out.print(idx.getCode(i) + (i + 1 == idx.size() ? "\n" : " "));
        }

        // count example
        System.out.println("count(<5) = " + idx.count(Op.LT, 5));
    }
}
