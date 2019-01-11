package cn.edu.tsinghua.tsfile.read;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.read.common.Chunk;

import java.io.IOException;

public interface IDataReader {

    public Chunk readMemChunk(ChunkMetaData metaData) throws IOException;
}
