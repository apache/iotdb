package org.apache.iotdb.db.query.reader.resourceRelated;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.chunkRelated.DiskChunkReader;
import org.apache.iotdb.db.query.reader.chunkRelated.MemChunkReader;
import org.apache.iotdb.db.query.reader.universal.PriorityMergeReader;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.common.constant.StatisticConstant;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.iotdb.tsfile.read.filter.DigestForFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderWithFilter;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderWithoutFilter;

public class UnseqResourceMergeReader extends PriorityMergeReader {

  public UnseqResourceMergeReader(Path seriesPath, List<TsFileResource> unseqResources,
      QueryContext context, Filter filter) throws IOException {
    int priorityValue = 1;

    for (TsFileResource tsFileResource : unseqResources) {

      // store only one opened file stream into manager, to avoid too many opened files
      TsFileSequenceReader tsFileReader = FileReaderManager.getInstance()
          .get(tsFileResource.getFile().getPath(), tsFileResource.isClosed());

      // get modified chunk metadatas
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

      // add readers for chunks
      // TODO future advancement: decrease the duplicated code
      ChunkLoaderImpl chunkLoader = new ChunkLoaderImpl(tsFileReader);
      for (ChunkMetaData chunkMetaData : metaDataList) {

        DigestForFilter digest = new DigestForFilter(chunkMetaData.getStartTime(),
            chunkMetaData.getEndTime(),
            chunkMetaData.getDigest().getStatistics().get(StatisticConstant.MIN_VALUE),
            chunkMetaData.getDigest().getStatistics().get(StatisticConstant.MAX_VALUE),
            chunkMetaData.getTsDataType());

        if (filter != null && !filter.satisfy(digest)) {
          continue;
        }

        Chunk chunk = chunkLoader.getChunk(chunkMetaData);
        ChunkReader chunkReader = filter != null ? new ChunkReaderWithFilter(chunk, filter)
            : new ChunkReaderWithoutFilter(chunk);

        addReaderWithPriority(new DiskChunkReader(chunkReader), priorityValue);

        priorityValue++;
      }

      // add reader for MemTable
      if (!tsFileResource.isClosed()) {
        addReaderWithPriority(
            new MemChunkReader(tsFileResource.getReadOnlyMemChunk(), filter), priorityValue++);
      }
    }

  }
}
