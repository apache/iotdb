package org.apache.iotdb.db.query.reader.chunk.metadata;

import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.IChunkMetadataLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.io.IOException;
import java.util.List;

public class MemChunkMetadataLoader implements IChunkMetadataLoader {

  private TsFileResource resource;
  private Path seriesPath;
  private QueryContext context;
  private Filter timeFilter;

  public MemChunkMetadataLoader(TsFileResource resource, Path seriesPath, QueryContext context, Filter timeFilter) {
    this.resource = resource;
    this.seriesPath = seriesPath;
    this.context = context;
    this.timeFilter = timeFilter;
  }

  @Override
  public List<ChunkMetadata> getChunkMetadataList() throws IOException {
    List<ChunkMetadata> chunkMetadataList = resource.getChunkMetadataList();

    DiskChunkMetadataLoader.setDiskChunkLoader(chunkMetadataList, resource, seriesPath, context);

    List<ReadOnlyMemChunk> memChunks = resource.getReadOnlyMemChunk();
    if (memChunks != null) {
      for (ReadOnlyMemChunk readOnlyMemChunk : memChunks) {
        if (!memChunks.isEmpty()) {
          chunkMetadataList.add(readOnlyMemChunk.getChunkMetaData());
        }
      }
    }
    /*
     * remove not satisfied ChunkMetaData
     */
    chunkMetadataList.removeIf(chunkMetaData -> (timeFilter != null && !timeFilter
            .satisfyStartEndTime(chunkMetaData.getStartTime(), chunkMetaData.getEndTime()))
            || chunkMetaData.getStartTime() > chunkMetaData.getEndTime());
    return chunkMetadataList;
  }
}
