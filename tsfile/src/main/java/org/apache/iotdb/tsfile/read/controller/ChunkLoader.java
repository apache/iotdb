package org.apache.iotdb.tsfile.read.controller;

import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;

import java.io.IOException;


public interface ChunkLoader {

    /**
     * read all content of any chunk
     */
    Chunk getChunk(ChunkMetaData chunkMetaData) throws IOException;


    /**
     * close the file reader
     */
    void close() throws IOException;

}
