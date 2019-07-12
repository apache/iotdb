package org.apache.iotdb.db.query.reader.resourceRelated;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.chunkRelated.FileSeriesReaderByTimestampAdapter;
import org.apache.iotdb.db.query.reader.chunkRelated.UnSealedTsFileReaderByTimestamp;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.ChunkLoader;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderByTimestamp;

public class SeqResourceReaderByTimestamp implements IReaderByTimestamp {

  protected Path seriesPath;
  private List<TsFileResource> tsFileResourceList;
  private int nextIntervalFileIndex;
  protected IReaderByTimestamp seriesReader;
  private QueryContext context;

  public SeqResourceReaderByTimestamp(Path seriesPath,
      List<TsFileResource> tsFileResourceList,
      QueryContext context) {
    this.seriesPath = seriesPath;
    this.tsFileResourceList = tsFileResourceList;
    this.nextIntervalFileIndex = 0;
    this.seriesReader = null;
    this.context = context;
  }

  @Override
  public Object getValueInTimestamp(long timestamp) throws IOException {
    Object value = null;
    if (seriesReader != null) {
      value = seriesReader.getValueInTimestamp(timestamp);
      // if get value or no value in this timestamp, return.
      if (value != null || seriesReader.hasNext()) {
        return value;
      }
    }
    constructReader(timestamp);
    if (seriesReader != null) {
      value = seriesReader.getValueInTimestamp(timestamp);
      if (value != null || seriesReader.hasNext()) {
        return value;
      }
    }

    return value;
  }

  @Override
  public boolean hasNext() throws IOException {
    // TODO 这里的unsealed没有？以及这个函数是有用的吗？
    if (seriesReader != null && seriesReader.hasNext()) {
      return true;
    }
    while (nextIntervalFileIndex < tsFileResourceList.size()) {
      initSealedTsFileReader(tsFileResourceList.get(nextIntervalFileIndex), context);
      nextIntervalFileIndex++;
      if (seriesReader.hasNext()) {
        return true;
      }
    }
    return false;
  }

  private void constructReader(long timestamp) throws IOException {
    while (nextIntervalFileIndex < tsFileResourceList.size()) {
      TsFileResource tsFile = tsFileResourceList.get(nextIntervalFileIndex);
      nextIntervalFileIndex++;
      // init unsealed tsfile.
      if (!tsFile.isClosed()) {
        initUnSealedTsFileReader(tsFile);
        return;
      }
      // init sealed tsfile.
      if (singleTsFileSatisfied(tsFile, timestamp)) {
        initSealedTsFileReader(tsFile, context);
        return;
      }
    }
  }

  private boolean singleTsFileSatisfied(TsFileResource fileNode, long timestamp) {
    if (fileNode.isClosed()) {
      return fileNode.getEndTimeMap().get(seriesPath.getDevice()) >= timestamp;
    }
    return true;
  }

  private void initUnSealedTsFileReader(TsFileResource tsFile)
      throws IOException {
    seriesReader = new UnSealedTsFileReaderByTimestamp(tsFile);
  }

  private void initSealedTsFileReader(TsFileResource fileNode, QueryContext context)
      throws IOException {

    // to avoid too many opened files
    TsFileSequenceReader tsFileReader = FileReaderManager.getInstance()
        .get(fileNode.getFile().getPath(), true);

    MetadataQuerierByFileImpl metadataQuerier = new MetadataQuerierByFileImpl(tsFileReader);
    List<ChunkMetaData> metaDataList = metadataQuerier.getChunkMetaDataList(seriesPath);

    List<Modification> pathModifications = context.getPathModifications(fileNode.getModFile(),
        seriesPath.getFullPath());
    if (!pathModifications.isEmpty()) {
      QueryUtils.modifyChunkMetaData(metaDataList, pathModifications);
    }
    ChunkLoader chunkLoader = new ChunkLoaderImpl(tsFileReader);

    seriesReader = new FileSeriesReaderByTimestampAdapter(
        new FileSeriesReaderByTimestamp(chunkLoader, metaDataList));
  }
}