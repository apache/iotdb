package org.apache.iotdb.tool.core.model;

import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;

import java.util.List;

/**
 * ChunkListInfo: the chunk list of a ChunkGroup
 *
 * @author shenguanchu
 */
public class ChunkListInfo {
  private List<IChunkMetadata> chunkMetadataList;
  private List<ChunkHeader> chunkHeaderList;

  public ChunkListInfo() {}

  public ChunkListInfo(List<IChunkMetadata> chunkMetadataList, List<ChunkHeader> chunkHeaderList) {
    this.chunkMetadataList = chunkMetadataList;
    this.chunkHeaderList = chunkHeaderList;
  }

  public List<IChunkMetadata> getChunkMetadataList() {
    return chunkMetadataList;
  }

  public void setChunkMetadataList(List<IChunkMetadata> chunkMetadataList) {
    this.chunkMetadataList = chunkMetadataList;
  }

  public List<ChunkHeader> getChunkHeaderList() {
    return chunkHeaderList;
  }

  public void setChunkHeaderList(List<ChunkHeader> chunkHeaderList) {
    this.chunkHeaderList = chunkHeaderList;
  }
}
