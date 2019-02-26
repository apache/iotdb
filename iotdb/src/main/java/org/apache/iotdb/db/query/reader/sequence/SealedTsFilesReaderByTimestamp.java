package org.apache.iotdb.db.query.reader.sequence;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.engine.filenode.IntervalFileNode;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.merge.EngineReaderByTimeStamp;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.ChunkLoader;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.iotdb.tsfile.read.reader.series.SeriesReaderByTimestamp;
import org.apache.iotdb.tsfile.utils.Pair;

public class SealedTsFilesReaderByTimestamp implements EngineReaderByTimeStamp {

  private Path seriesPath;
  private List<IntervalFileNode> sealedTsFiles;
  private int usedIntervalFileIndex;
  private SeriesReaderByTimestamp seriesReader;

  /**
   * init with seriesPath and sealedTsFiles.
   */
  public SealedTsFilesReaderByTimestamp(Path seriesPath, List<IntervalFileNode> sealedTsFiles) {
    this.seriesPath = seriesPath;
    this.sealedTsFiles = sealedTsFiles;
    this.usedIntervalFileIndex = 0;
    this.seriesReader = null;
  }

  @Override
  public Object getValueInTimestamp(long timestamp) throws IOException {
    Object value = null;
    if (seriesReader != null) {
      value = seriesReader.getValueInTimestamp(timestamp);
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
  public Pair<Long, Object> getValueGtEqTimestamp(long timestamp) throws IOException {
    Object value = getValueInTimestamp(timestamp);
    if (value != null) {
      return new Pair<>(timestamp, value);
    }
    if (seriesReader != null && seriesReader.hasNext()) {
      return seriesReader.next();
    }
    return null;
  }

  @Override
  public void close() throws IOException {
    // file streams are managed uniformly.
  }

  // construct reader from the file that might overlap this timestamp
  private void constructReader(long timestamp) throws IOException {
    while (usedIntervalFileIndex < sealedTsFiles.size()) {
      if (singleTsFileSatisfied(sealedTsFiles.get(usedIntervalFileIndex), timestamp)) {
        initSingleTsFileReader(sealedTsFiles.get(usedIntervalFileIndex));
      }
      usedIntervalFileIndex++;
    }
  }

  /**
   * Judge whether the file should be skipped
   */
  private boolean singleTsFileSatisfied(IntervalFileNode fileNode, long timestamp) {
    long endTime = fileNode.getEndTime(seriesPath.getDevice());
    return endTime >= timestamp;
  }

  private void initSingleTsFileReader(IntervalFileNode fileNode) throws IOException {

    // to avoid too many opened files
    TsFileSequenceReader tsFileReader = FileReaderManager.getInstance()
        .get(fileNode.getFilePath(), false);

    MetadataQuerierByFileImpl metadataQuerier = new MetadataQuerierByFileImpl(tsFileReader);
    List<ChunkMetaData> metaDataList = metadataQuerier.getChunkMetaDataList(seriesPath);
    ChunkLoader chunkLoader = new ChunkLoaderImpl(tsFileReader);

    seriesReader = new SeriesReaderByTimestamp(chunkLoader, metaDataList);

  }
}
