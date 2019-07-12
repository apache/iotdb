package org.apache.iotdb.db.query.reader.resourceRelated;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.chunkRelated.FileSeriesReaderAdapter;
import org.apache.iotdb.db.query.reader.chunkRelated.UnSealedTsFileIterateReader;
import org.apache.iotdb.db.query.reader.universal.IterateReader;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.ChunkLoader;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReader;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderWithFilter;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderWithoutFilter;

public class SeqResourceIterateReader extends IterateReader {

  private Path seriesPath;

  private boolean enableReverse;

  private List<TsFileResource> seqResources;
  private Filter filter;
  private QueryContext context;

  public SeqResourceIterateReader(Path seriesPath, List<TsFileResource> seqResources,
      Filter filter, QueryContext context, boolean isReverse) {
    super(seqResources.size());
    if (isReverse) {
      Collections.reverse(seqResources);
    }
    this.seqResources = seqResources;
    this.seriesPath = seriesPath;
    this.enableReverse = isReverse;
    this.context = context;
    this.filter = filter;
  }

  public SeqResourceIterateReader(Path seriesPath, List<TsFileResource> seqResources,
      Filter timeFilter, QueryContext context) {
    this(seriesPath, seqResources, timeFilter, context, false);
  }

  @Override
  public boolean constructNextReader(int idx) throws IOException {
    TsFileResource tsFileResource = seqResources.get(idx);
    if (tsFileResource.isClosed()) {
      if (singleTsFileSatisfied(tsFileResource, filter)) {
        currentSeriesReader = new FileSeriesReaderAdapter(
            initSealedTsFileReader(tsFileResource, filter, context));
        return true;
      } else {
        return false;
      }
    } else {
      currentSeriesReader = new UnSealedTsFileIterateReader(tsFileResource, filter, enableReverse);
      return true;
    }
  }

  private boolean singleTsFileSatisfied(TsFileResource tsfile, Filter filter) {

    if (filter == null) {
      return true;
    }

    long startTime = tsfile.getStartTimeMap().get(seriesPath.getDevice());
    long endTime = tsfile.getEndTimeMap().get(seriesPath.getDevice());
    return filter.satisfyStartEndTime(startTime, endTime);
  }

  private FileSeriesReader initSealedTsFileReader(TsFileResource tsfile, Filter filter,
      QueryContext context)
      throws IOException {

    // to avoid too many opened files
    TsFileSequenceReader tsFileReader = FileReaderManager.getInstance()
        .get(tsfile.getFile().getPath(), true);

    MetadataQuerierByFileImpl metadataQuerier = new MetadataQuerierByFileImpl(tsFileReader);
    List<ChunkMetaData> metaDataList = metadataQuerier.getChunkMetaDataList(seriesPath);

    List<Modification> pathModifications = context.getPathModifications(tsfile.getModFile(),
        seriesPath.getFullPath());
    if (!pathModifications.isEmpty()) {
      QueryUtils.modifyChunkMetaData(metaDataList, pathModifications);
    }

    ChunkLoader chunkLoader = new ChunkLoaderImpl(tsFileReader);

    if (enableReverse) {
      Collections.reverse(metaDataList);
    }

    FileSeriesReader seriesReader;
    if (filter == null) {
      seriesReader = new FileSeriesReaderWithoutFilter(chunkLoader, metaDataList);
    } else {
      seriesReader = new FileSeriesReaderWithFilter(chunkLoader, metaDataList, filter);
    }
    return seriesReader;
  }
}