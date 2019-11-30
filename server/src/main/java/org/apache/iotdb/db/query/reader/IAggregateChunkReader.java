package org.apache.iotdb.db.query.reader;

import java.io.IOException;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

/**
 * @Author: LiuDaWei
 * @Create: 2019年11月30日
 */
public interface IAggregateChunkReader {

  boolean hasNextChunk() throws IOException;

  ChunkMetaData nextChunkMeta();

  ChunkReader readChunk() throws IOException;
}
