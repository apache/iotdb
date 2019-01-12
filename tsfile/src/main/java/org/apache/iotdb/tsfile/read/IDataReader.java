package org.apache.iotdb.tsfile.read;

import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.common.Chunk;

import java.io.IOException;

public interface IDataReader {

    public Chunk readMemChunk(ChunkMetaData metaData) throws IOException;
}
