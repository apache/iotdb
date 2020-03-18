package org.apache.iotdb.tsfile.read.controller;

import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;

import java.io.IOException;
import java.util.List;

public interface IChunkMetadataLoader {

  /**
   * read all chunk metadata of one time series in one file.
   */
  List<ChunkMetadata> getChunkMetadataList() throws IOException;

}
