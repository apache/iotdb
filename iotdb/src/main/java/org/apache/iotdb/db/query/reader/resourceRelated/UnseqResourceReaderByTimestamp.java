package org.apache.iotdb.db.query.reader.resourceRelated;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.chunkRelated.DiskChunkReaderByTimestamp;
import org.apache.iotdb.db.query.reader.chunkRelated.MemChunkReaderByTimestamp;
import org.apache.iotdb.db.query.reader.universal.PriorityMergeReaderByTimestamp;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderByTimestamp;

public class UnseqResourceReaderByTimestamp extends PriorityMergeReaderByTimestamp {

  public UnseqResourceReaderByTimestamp(Path seriesPath,
      List<TsFileResource> unseqResources, QueryContext context) throws IOException {
    int priorityValue = 1;

    for (TsFileResource tsFileResource : unseqResources) {

      // store only one opened file stream into manager, to avoid too many opened files
      TsFileSequenceReader tsFileReader = FileReaderManager.getInstance()
          .get(tsFileResource.getFile().getPath(), tsFileResource.isClosed());

      List<ChunkMetaData> metaDataList;
      if (tsFileResource.isClosed()) {
        MetadataQuerierByFileImpl metadataQuerier = new MetadataQuerierByFileImpl(tsFileReader);
        metaDataList = metadataQuerier.getChunkMetaDataList(seriesPath);
        // mod
        List<Modification> pathModifications = context
            .getPathModifications(tsFileResource.getModFile(),
                seriesPath.getFullPath());
        if (!pathModifications.isEmpty()) {
          QueryUtils.modifyChunkMetaData(metaDataList, pathModifications);
        }
      } else {
        metaDataList = tsFileResource.getChunkMetaDatas();
      }

      // add reader for chunk
      ChunkLoaderImpl chunkLoader = new ChunkLoaderImpl(tsFileReader);
      for (ChunkMetaData chunkMetaData : metaDataList) {

        Chunk chunk = chunkLoader.getChunk(chunkMetaData);
        ChunkReaderByTimestamp chunkReader = new ChunkReaderByTimestamp(chunk);

        addReaderWithPriority(new DiskChunkReaderByTimestamp(chunkReader),
            priorityValue);

        priorityValue++;
      }

      // add reader for MemTable
      if (!tsFileResource.isClosed()) {
        addReaderWithPriority(
            new MemChunkReaderByTimestamp(tsFileResource.getReadOnlyMemChunk()), priorityValue++);
      }
    }

    // TODO add external sort when needed
  }
}
