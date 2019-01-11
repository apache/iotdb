package cn.edu.tsinghua.tsfile.read.controller;

import cn.edu.tsinghua.tsfile.read.common.Chunk;
import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;

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
