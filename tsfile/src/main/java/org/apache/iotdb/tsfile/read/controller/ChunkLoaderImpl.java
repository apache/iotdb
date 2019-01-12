package org.apache.iotdb.tsfile.read.controller;

import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.common.cache.LRUCache;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;

import java.io.IOException;

/**
 * Read one Chunk and cache it into a LRUCache
 */
public class ChunkLoaderImpl implements ChunkLoader {

    private static final int DEFAULT_CHUNK_CACHE_SIZE = 100000;
    private TsFileSequenceReader reader;
    private LRUCache<ChunkMetaData, Chunk> chunkCache;

    public ChunkLoaderImpl(TsFileSequenceReader fileSequenceReader) {
        this(fileSequenceReader, DEFAULT_CHUNK_CACHE_SIZE);
    }

    public ChunkLoaderImpl(TsFileSequenceReader fileSequenceReader, int cacheSize) {

        this.reader = fileSequenceReader;

        chunkCache = new LRUCache<ChunkMetaData, Chunk>(cacheSize) {

            @Override
            public Chunk loadObjectByKey(ChunkMetaData metaData) throws IOException {
                return reader.readMemChunk(metaData);
            }
        };
    }

    @Override
    public Chunk getChunk(ChunkMetaData chunkMetaData) throws IOException {
        Chunk chunk = chunkCache.get(chunkMetaData);
        return new Chunk(chunk.getHeader(), chunk.getData().duplicate());
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

}
