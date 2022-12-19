package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.Request;

import org.apache.iotdb.lsm.request.IFlushRequest;

public class FlushRequest extends IFlushRequest {

  private long chunkMaxSize;

  public FlushRequest(int index, Object memNode, long chunkMaxSize) {
    super(index, memNode);
    this.chunkMaxSize = chunkMaxSize;
  }

  public long getChunkMaxSize() {
    return chunkMaxSize;
  }

  public void setChunkMaxSize(long chunkMaxSize) {
    this.chunkMaxSize = chunkMaxSize;
  }
}
