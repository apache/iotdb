package org.apache.iotdb.tsfile.encoding;

/**
 * Java port of Simple8b&lt;MarkLength=true&gt; from FastPFOR ({@code headers/simple8b.h}). Reference:
 * Vo Ngoc Anh, Alistair Moffat, "Index compression using 64-bit words", Software: Practice and
 * Experience 40(2), 2010; Daniel Lemire's FastPFOR library.
 *
 * <p>Compressed layout matches the C++ codec: first uint32 is original length, followed by 64-bit
 * words stored as little-endian uint32 pairs.
 */
final class FastPForSimple8bCodec {

    private static final int SIMPLE8B_LOGDESC = 4;

    private FastPForSimple8bCodec() {}

    static int which(long w) {
        return (int) (w >>> (64 - SIMPLE8B_LOGDESC));
    }

    private static boolean tryMe(int[] in, int ip, int remaining, int num1, int log1) {
        int n = Math.min(remaining, num1);
        if (log1 >= 32) {
            return true;
        }
        long limit = 1L << log1;
        for (int i = 0; i < n; i++) {
            if ((in[ip + i] & 0xffffffffL) >= limit) {
                return false;
            }
        }
        return true;
    }

    private static boolean tryMeFull(int[] in, int ip, int num1, int log1) {
        if (log1 >= 32) {
            return true;
        }
        long limit = 1L << log1;
        for (int i = 0; i < num1; i++) {
            if ((in[ip + i] & 0xffffffffL) >= limit) {
                return false;
            }
        }
        return true;
    }

    private static long maskFor(int log1) {
        if (log1 >= 32) {
            return 0xffffffffL;
        }
        return (1L << log1) - 1;
    }

    private static long encodeWord(int[] in, int ip, int valuesRemaining, boolean useFull240Path) {
        long w;
        if (useFull240Path && valuesRemaining >= 240) {
            if (tryMeFull(in, ip, 120, 0) && tryMeFull(in, ip + 120, 120, 0)) {
                return 0L;
            }
            if (tryMeFull(in, ip, 120, 0)) {
                return 1L << (64 - SIMPLE8B_LOGDESC);
            }
        }
        if (tryMe(in, ip, valuesRemaining, 60, 1)) {
            w = 2;
            int coded = Math.min(valuesRemaining, 60);
            for (int i = 0; i < coded; i++) {
                w = (w << 1) | (in[ip + i] & 1L);
            }
            w <<= (64 - SIMPLE8B_LOGDESC - coded);
            return w;
        }
        if (tryMe(in, ip, valuesRemaining, 30, 2)) {
            w = 3;
            int coded = Math.min(valuesRemaining, 30);
            for (int i = 0; i < coded; i++) {
                w = (w << 2) | (in[ip + i] & 3L);
            }
            w <<= (64 - SIMPLE8B_LOGDESC - 2 * coded);
            return w;
        }
        if (tryMe(in, ip, valuesRemaining, 20, 3)) {
            w = 4;
            int coded = Math.min(valuesRemaining, 20);
            for (int i = 0; i < coded; i++) {
                w = (w << 3) | (in[ip + i] & 7L);
            }
            w <<= (64 - SIMPLE8B_LOGDESC - 3 * coded);
            return w;
        }
        if (tryMe(in, ip, valuesRemaining, 15, 4)) {
            w = 5;
            int coded = Math.min(valuesRemaining, 15);
            for (int i = 0; i < coded; i++) {
                w = (w << 4) | (in[ip + i] & 15L);
            }
            w <<= (64 - SIMPLE8B_LOGDESC - 4 * coded);
            return w;
        }
        if (tryMe(in, ip, valuesRemaining, 12, 5)) {
            w = 6;
            int coded = Math.min(valuesRemaining, 12);
            for (int i = 0; i < coded; i++) {
                w = (w << 5) | (in[ip + i] & 31L);
            }
            w <<= (64 - SIMPLE8B_LOGDESC - 5 * coded);
            return w;
        }
        if (tryMe(in, ip, valuesRemaining, 10, 6)) {
            w = 7;
            int coded = Math.min(valuesRemaining, 10);
            for (int i = 0; i < coded; i++) {
                w = (w << 6) | (in[ip + i] & 63L);
            }
            w <<= (64 - SIMPLE8B_LOGDESC - 6 * coded);
            return w;
        }
        if (tryMe(in, ip, valuesRemaining, 8, 7)) {
            w = 8;
            int coded = Math.min(valuesRemaining, 8);
            for (int i = 0; i < coded; i++) {
                w = (w << 7) | (in[ip + i] & 127L);
            }
            w <<= (64 - SIMPLE8B_LOGDESC - 7 * coded);
            return w;
        }
        if (tryMe(in, ip, valuesRemaining, 7, 8)) {
            w = 9;
            int coded = Math.min(valuesRemaining, 7);
            for (int i = 0; i < coded; i++) {
                w = (w << 8) | (in[ip + i] & 255L);
            }
            w <<= (64 - SIMPLE8B_LOGDESC - 8 * coded);
            return w;
        }
        if (tryMe(in, ip, valuesRemaining, 6, 10)) {
            w = 10;
            int coded = Math.min(valuesRemaining, 6);
            for (int i = 0; i < coded; i++) {
                w = (w << 10) | (in[ip + i] & 1023L);
            }
            w <<= (64 - SIMPLE8B_LOGDESC - 10 * coded);
            return w;
        }
        if (tryMe(in, ip, valuesRemaining, 5, 12)) {
            w = 11;
            int coded = Math.min(valuesRemaining, 5);
            for (int i = 0; i < coded; i++) {
                w = (w << 12) | (in[ip + i] & 4095L);
            }
            w <<= (64 - SIMPLE8B_LOGDESC - 12 * coded);
            return w;
        }
        if (tryMe(in, ip, valuesRemaining, 4, 15)) {
            w = 12;
            int coded = Math.min(valuesRemaining, 4);
            for (int i = 0; i < coded; i++) {
                w = (w << 15) | (in[ip + i] & 32767L);
            }
            w <<= (64 - SIMPLE8B_LOGDESC - 15 * coded);
            return w;
        }
        if (tryMe(in, ip, valuesRemaining, 3, 20)) {
            w = 13;
            int coded = Math.min(valuesRemaining, 3);
            for (int i = 0; i < coded; i++) {
                w = (w << 20) | (in[ip + i] & 1048575L);
            }
            w <<= (64 - SIMPLE8B_LOGDESC - 20 * coded);
            return w;
        }
        if (tryMe(in, ip, valuesRemaining, 2, 30)) {
            w = 14;
            int coded = Math.min(valuesRemaining, 2);
            for (int i = 0; i < coded; i++) {
                w = (w << 30) | (in[ip + i] & 1073741823L);
            }
            w <<= (64 - SIMPLE8B_LOGDESC - 30 * coded);
            return w;
        }
        if (tryMe(in, ip, valuesRemaining, 1, 60)) {
            w = 15;
            int coded = Math.min(valuesRemaining, 1);
            for (int i = 0; i < coded; i++) {
                w = (w << 60) | (in[ip + i] & ((1L << 60) - 1));
            }
            w <<= (64 - SIMPLE8B_LOGDESC - 60 * coded);
            return w;
        }
        throw new IllegalStateException("Simple8b: no case applies");
    }

    /** @return number of input values consumed for this word */
    private static int codedCountForWord(long word, int valuesRemaining) {
        switch (which(word)) {
            case 0:
                return Math.min(valuesRemaining, 240);
            case 1:
                return Math.min(valuesRemaining, 120);
            case 2:
                return Math.min(valuesRemaining, 60);
            case 3:
                return Math.min(valuesRemaining, 30);
            case 4:
                return Math.min(valuesRemaining, 20);
            case 5:
                return Math.min(valuesRemaining, 15);
            case 6:
                return Math.min(valuesRemaining, 12);
            case 7:
                return Math.min(valuesRemaining, 10);
            case 8:
                return Math.min(valuesRemaining, 8);
            case 9:
                return Math.min(valuesRemaining, 7);
            case 10:
                return Math.min(valuesRemaining, 6);
            case 11:
                return Math.min(valuesRemaining, 5);
            case 12:
                return Math.min(valuesRemaining, 4);
            case 13:
                return Math.min(valuesRemaining, 3);
            case 14:
                return Math.min(valuesRemaining, 2);
            case 15:
                return Math.min(valuesRemaining, 1);
            default:
                throw new IllegalStateException("bad selector");
        }
    }

    /** Encode with length prefix (MarkLength=true). Output: [length][u64 as LE pair]... */
    static int[] encode(int[] in) {
        int length = in.length;
        java.util.ArrayList<Long> words = new java.util.ArrayList<>(length / 60 + 8);
        int ip = 0;
        int valuesRemaining = length;
        while (valuesRemaining >= 240) {
            long word = encodeWord(in, ip, valuesRemaining, true);
            int coded = codedCountForWord(word, valuesRemaining);
            words.add(word);
            ip += coded;
            valuesRemaining -= coded;
        }
        while (valuesRemaining > 0) {
            long word = encodeWord(in, ip, valuesRemaining, false);
            int coded = codedCountForWord(word, valuesRemaining);
            words.add(word);
            ip += coded;
            valuesRemaining -= coded;
        }
        int[] out = new int[1 + 2 * words.size()];
        out[0] = length;
        int o = 1;
        for (long lw : words) {
            out[o++] = (int) (lw & 0xffffffffL);
            out[o++] = (int) (lw >>> 32);
        }
        return out;
    }

    private static void unpackFull(long w, int[] out, int op, int num1, int log1) {
        long mask = maskFor(log1);
        for (int k = 0; k < num1; k++) {
            int sh = 64 - SIMPLE8B_LOGDESC - log1 - k * log1;
            out[op + k] = (int) ((w >>> sh) & mask);
        }
    }

    private static void unpackCareful(long w, int[] out, int op, int num1, int log1) {
        long mask = maskFor(log1);
        for (int k = 0; k < num1; k++) {
            int sh = 64 - SIMPLE8B_LOGDESC - log1 - k * log1;
            out[op + k] = (int) ((w >>> sh) & mask);
        }
    }

    static int[] decode(int[] compressed) {
        int marked = compressed[0];
        int[] out = new int[marked];
        int wp = 0;
        int idx = 1;
        while (wp < marked) {
            long w = (compressed[idx] & 0xffffffffL) | ((long) compressed[idx + 1] << 32);
            idx += 2;
            int sel = which(w);
            int left = marked - wp;
            switch (sel) {
                case 0:
                    if (left > 240) {
                        unpackFull(w, out, wp, 240, 0);
                        wp += 240;
                    } else {
                        unpackCareful(w, out, wp, left, 0);
                        wp += left;
                    }
                    break;
                case 1:
                    if (left > 120) {
                        unpackFull(w, out, wp, 120, 0);
                        wp += 120;
                    } else {
                        unpackCareful(w, out, wp, left, 0);
                        wp += left;
                    }
                    break;
                case 2:
                    if (left > 60) {
                        unpackFull(w, out, wp, 60, 1);
                        wp += 60;
                    } else {
                        unpackCareful(w, out, wp, left, 1);
                        wp += left;
                    }
                    break;
                case 3:
                    if (left > 30) {
                        unpackFull(w, out, wp, 30, 2);
                        wp += 30;
                    } else {
                        unpackCareful(w, out, wp, left, 2);
                        wp += left;
                    }
                    break;
                case 4:
                    if (left > 20) {
                        unpackFull(w, out, wp, 20, 3);
                        wp += 20;
                    } else {
                        unpackCareful(w, out, wp, left, 3);
                        wp += left;
                    }
                    break;
                case 5:
                    if (left > 15) {
                        unpackFull(w, out, wp, 15, 4);
                        wp += 15;
                    } else {
                        unpackCareful(w, out, wp, left, 4);
                        wp += left;
                    }
                    break;
                case 6:
                    if (left > 12) {
                        unpackFull(w, out, wp, 12, 5);
                        wp += 12;
                    } else {
                        unpackCareful(w, out, wp, left, 5);
                        wp += left;
                    }
                    break;
                case 7:
                    if (left > 10) {
                        unpackFull(w, out, wp, 10, 6);
                        wp += 10;
                    } else {
                        unpackCareful(w, out, wp, left, 6);
                        wp += left;
                    }
                    break;
                case 8:
                    if (left > 8) {
                        unpackFull(w, out, wp, 8, 7);
                        wp += 8;
                    } else {
                        unpackCareful(w, out, wp, left, 7);
                        wp += left;
                    }
                    break;
                case 9:
                    if (left > 7) {
                        unpackFull(w, out, wp, 7, 8);
                        wp += 7;
                    } else {
                        unpackCareful(w, out, wp, left, 8);
                        wp += left;
                    }
                    break;
                case 10:
                    if (left > 6) {
                        unpackFull(w, out, wp, 6, 10);
                        wp += 6;
                    } else {
                        unpackCareful(w, out, wp, left, 10);
                        wp += left;
                    }
                    break;
                case 11:
                    if (left > 5) {
                        unpackFull(w, out, wp, 5, 12);
                        wp += 5;
                    } else {
                        unpackCareful(w, out, wp, left, 12);
                        wp += left;
                    }
                    break;
                case 12:
                    if (left > 4) {
                        unpackFull(w, out, wp, 4, 15);
                        wp += 4;
                    } else {
                        unpackCareful(w, out, wp, left, 15);
                        wp += left;
                    }
                    break;
                case 13:
                    if (left > 3) {
                        unpackFull(w, out, wp, 3, 20);
                        wp += 3;
                    } else {
                        unpackCareful(w, out, wp, left, 20);
                        wp += left;
                    }
                    break;
                case 14:
                    if (left > 2) {
                        unpackFull(w, out, wp, 2, 30);
                        wp += 2;
                    } else {
                        unpackCareful(w, out, wp, left, 30);
                        wp += left;
                    }
                    break;
                case 15:
                    unpackCareful(w, out, wp, 1, 60);
                    wp += 1;
                    break;
                default:
                    throw new IllegalStateException("Simple8b decode: bad selector");
            }
        }
        return out;
    }

    static int compressedSizeBytes(int[] compressed) {
        return compressed.length * Integer.BYTES;
    }
}
