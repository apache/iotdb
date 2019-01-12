package org.apache.iotdb.tsfile.read.common;

import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;

import java.nio.ByteBuffer;

/**
 * used in query
 */
public class Chunk {

    private ChunkHeader chunkHeader;
    private ByteBuffer chunkData;

    public Chunk(ChunkHeader header, ByteBuffer buffer) {
        this.chunkHeader = header;
        this.chunkData = buffer;
    }

    public ChunkHeader getHeader() {
        return chunkHeader;
    }

    public ByteBuffer getData() {
        return chunkData;
    }
}
