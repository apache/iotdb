package org.apache.iotdb.db.query.reader.chunk.metadata;

import org.apache.iotdb.db.engine.cache.DeviceMetaDataCache;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.chunk.DiskChunkLoader;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.IChunkMetadataLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.io.IOException;
import java.util.List;

public class DiskChunkMetadataLoader implements IChunkMetadataLoader {

  private TsFileResource resource;
  private Path seriesPath;
  private QueryContext context;
  private Filter timeFilter;

  public DiskChunkMetadataLoader(TsFileResource resource, Path seriesPath, QueryContext context, Filter timeFilter) {
    this.resource = resource;
    this.seriesPath = seriesPath;
    this.context = context;
    this.timeFilter = timeFilter;
  }

  @Override
  public List<ChunkMetadata> getChunkMetadataList() throws IOException {
    List<ChunkMetadata> chunkMetadataList = DeviceMetaDataCache.getInstance().get(resource, seriesPath);

    setDiskChunkLoader(chunkMetadataList, resource, seriesPath, context);

    /*
     * remove not satisfied ChunkMetaData
     */
    chunkMetadataList.removeIf(chunkMetaData -> (timeFilter != null && !timeFilter
            .satisfyStartEndTime(chunkMetaData.getStartTime(), chunkMetaData.getEndTime()))
            || chunkMetaData.getStartTime() > chunkMetaData.getEndTime());
    return chunkMetadataList;
  }

  public static void setDiskChunkLoader(List<ChunkMetadata> chunkMetadataList, TsFileResource resource, Path seriesPath, QueryContext context) throws IOException {
    List<Modification> pathModifications =
            context.getPathModifications(resource.getModFile(), seriesPath.getFullPath());

    if (!pathModifications.isEmpty()) {
      QueryUtils.modifyChunkMetaData(chunkMetadataList, pathModifications);
    }

    TsFileSequenceReader tsFileSequenceReader =
            FileReaderManager.getInstance().get(resource.getPath(), resource.isClosed());
    for (ChunkMetadata data : chunkMetadataList) {
      data.setChunkLoader(new DiskChunkLoader(tsFileSequenceReader));
    }
  }

}
