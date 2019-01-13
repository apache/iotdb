/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
