package org.apache.iotdb.tsfile.encoding.elf.xordecompressor;

import org.apache.iotdb.tsfile.encoding.elf.chimp.ElfInputBitStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ElfXORDecompressor {
    private long storedVal = 0;
    private int storedLeadingZeros = Integer.MAX_VALUE;
    private int storedTrailingZeros = Integer.MAX_VALUE;
    private boolean first = true;
    private boolean endOfStream = false;

    private final ElfInputBitStream in;

    private final static long END_SIGN = Double.doubleToLongBits(Double.NaN);

    private final static short[] leadingRepresentation = {0, 8, 12, 16, 18, 20, 22, 24};

    public ElfXORDecompressor(byte[] bs) {
        in = new ElfInputBitStream(bs);
    }

    public List<Double> getValues() {
        List<Double> list = new ArrayList<>(1024);
        Double value = readValue();
        while (value != null) {
            list.add(value);
            value = readValue();
        }
        return list;
    }

    public ElfInputBitStream getInputStream() {
        return in;
    }

    /**
     * Returns the next pair in the time series, if available.
     *
     * @return Pair if there's next value, null if series is done.
     */
    public Double readValue() {
        try {
            next();
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
        if (endOfStream) {
            return null;
        }
        return Double.longBitsToDouble(storedVal);
    }

    private void next() throws IOException {
        if (first) {
            first = false;
            int trailingZeros = in.readInt(7);
            if (trailingZeros < 64) {
                storedVal = ((in.readLong(63 - trailingZeros) << 1) + 1) << trailingZeros;
            } else {
                storedVal = 0;
            }
            if (storedVal == END_SIGN) {
                endOfStream = true;
            }
        } else {
            nextValue();
        }
    }

    private void nextValue() throws IOException {
        long value;
        int centerBits, leadAndCenter;
        int flag = in.readInt(2);
        switch (flag) {
            case 3:
                // case 11
                leadAndCenter = in.readInt(9);
                storedLeadingZeros = leadingRepresentation[leadAndCenter >>> 6];
                centerBits = leadAndCenter & 0x3f;
                if(centerBits == 0) {
                    centerBits = 64;
                }
                storedTrailingZeros = 64 - storedLeadingZeros - centerBits;
                value = ((in.readLong(centerBits - 1) << 1) + 1) << storedTrailingZeros;
                value = storedVal ^ value;
                if (value == END_SIGN) {
                    endOfStream = true;
                } else {
                    storedVal = value;
                }
                break;
            case 2:
                // case 10
                leadAndCenter = in.readInt(7);
                storedLeadingZeros = leadingRepresentation[leadAndCenter >>> 4];
                centerBits = leadAndCenter & 0xf;
                if(centerBits == 0) {
                    centerBits = 16;
                }
                storedTrailingZeros = 64 - storedLeadingZeros - centerBits;
                value = ((in.readLong(centerBits - 1) << 1) + 1) << storedTrailingZeros;
                value = storedVal ^ value;
                if (value == END_SIGN) {
                    endOfStream = true;
                } else {
                    storedVal = value;
                }
                break;
            case 1:
                // case 01, we do nothing, the same value as before
                break;
            default:
                // case 00
                centerBits = 64 - storedLeadingZeros - storedTrailingZeros;
                value = in.readLong(centerBits) << storedTrailingZeros;
                value = storedVal ^ value;
                if (value == END_SIGN) {
                    endOfStream = true;
                } else {
                    storedVal = value;
                }
                break;
        }
    }
}
