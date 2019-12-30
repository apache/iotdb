package org.apache.iotdb.db.query.reader;

import java.io.IOException;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;

public class MemChunkLoader implements IChunkLoader {
  private final ReadOnlyMemChunk chunk;

  public MemChunkLoader(ReadOnlyMemChunk chunk) {
    this.chunk = chunk;
  }


  @Override
  public Chunk getChunk(ChunkMetaData chunkMetaData) throws IOException {
    return null;
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public void clear() {

  }

  public ReadOnlyMemChunk getChunk() {
    return chunk;
  }
}
