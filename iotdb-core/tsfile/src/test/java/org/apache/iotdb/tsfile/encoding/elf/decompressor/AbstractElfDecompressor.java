package org.apache.iotdb.tsfile.encoding.elf.decompressor;

import org.apache.iotdb.tsfile.encoding.elf.utils.Elf64Utils;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractElfDecompressor implements IDecompressor {
    private int lastBetaStar = Integer.MAX_VALUE;

    public List<Double> decompress() {
        List<Double> values = new ArrayList<>(1024);
        Double value;
        while ((value = nextValue()) != null) {
            values.add(value);
        }
        return values;
    }

    private Double nextValue() {
        Double v;

        if(readInt(1) == 0) {
            v = recoverVByBetaStar();               // case 0
        } else if (readInt(1) == 0) {
            v = xorDecompress();                    // case 10
        } else {
            lastBetaStar = readInt(4);          // case 11
            v = recoverVByBetaStar();
        }
        return v;
    }

    private Double recoverVByBetaStar() {
        double v;
        Double vPrime = xorDecompress();
        int sp = Elf64Utils.getSP(Math.abs(vPrime));
        if (lastBetaStar == 0) {
            v = Elf64Utils.get10iN(-sp - 1);
            if (vPrime < 0) {
                v = -v;
            }
        } else {
            int alpha = lastBetaStar - sp - 1;
            v = Elf64Utils.roundUp(vPrime, alpha);
        }
        return v;
    }

    protected abstract Double xorDecompress();

    protected abstract int readInt(int len);

}
