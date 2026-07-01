package org.apache.iotdb.tsfile.encoding.elf.compressor;

import org.apache.iotdb.tsfile.encoding.elf.chimp.ElfOutputBitStream;
import org.apache.iotdb.tsfile.encoding.elf.xorcompressor.ElfXORCompressor;

public class ElfCompressor extends AbstractElfCompressor {
    private final ElfXORCompressor xorCompressor;

    public ElfCompressor() {
        xorCompressor = new ElfXORCompressor();
    }

    public ElfCompressor(int xorBufferBytes) {
        xorCompressor = new ElfXORCompressor(xorBufferBytes);
    }

    @Override protected int writeInt(int n, int len) {
        ElfOutputBitStream os = xorCompressor.getOutputStream();
        os.writeInt(n, len);
        return len;
    }

    @Override protected int writeBit(boolean bit) {
        ElfOutputBitStream os = xorCompressor.getOutputStream();
        os.writeBit(bit);
        return 1;
    }

    @Override protected int xorCompress(long vPrimeLong) {
        return xorCompressor.addValue(vPrimeLong);
    }

    @Override public byte[] getBytes() {
        return xorCompressor.getOut();
    }

    @Override public void close() {
        // we write one more bit here, for marking an end of the stream.
        writeInt(2,2);  // case 10
        xorCompressor.close();
    }
}
