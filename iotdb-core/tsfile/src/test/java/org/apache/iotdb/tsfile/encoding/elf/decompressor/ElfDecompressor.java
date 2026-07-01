package org.apache.iotdb.tsfile.encoding.elf.decompressor;

import org.apache.iotdb.tsfile.encoding.elf.chimp.ElfInputBitStream;
import org.apache.iotdb.tsfile.encoding.elf.xordecompressor.ElfXORDecompressor;

import java.io.IOException;

public class ElfDecompressor extends AbstractElfDecompressor {
    private final ElfXORDecompressor xorDecompressor;

    public ElfDecompressor(byte[] bytes) {
        xorDecompressor = new ElfXORDecompressor(bytes);
    }

    @Override protected Double xorDecompress() {
        return xorDecompressor.readValue();
    }

    @Override protected int readInt(int len) {
        ElfInputBitStream in = xorDecompressor.getInputStream();
        try {
            return in.readInt(len);
        } catch (IOException e) {
            throw new RuntimeException("IO error: " + e.getMessage());
        }
    }
}
