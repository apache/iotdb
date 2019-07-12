package org.apache.iotdb.db.query.reader.chunkRelated;

import java.io.IOException;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.ChunkLoader;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderByTimestamp;

public class UnSealedTsFileReaderByTimestamp implements IReaderByTimestamp {

  protected Path seriesPath;

  private FileSeriesReaderByTimestamp unSealedReader;

  private IReaderByTimestamp memSeriesReader;

  private boolean unSealedReaderEnded;

  public UnSealedTsFileReaderByTimestamp(TsFileResource tsFileResource) throws IOException {
    TsFileSequenceReader unClosedTsFileReader = FileReaderManager.getInstance()
        .get(tsFileResource.getFile().getPath(), false);
    ChunkLoader chunkLoader = new ChunkLoaderImpl(unClosedTsFileReader);
    unSealedReader = new FileSeriesReaderByTimestamp(chunkLoader,
        tsFileResource.getChunkMetaDatas());

    memSeriesReader = new MemChunkReaderByTimestamp(tsFileResource.getReadOnlyMemChunk());
    unSealedReaderEnded = false;
  }

  @Override
  public Object getValueInTimestamp(long timestamp) throws IOException {
    Object value = null;
    if (!unSealedReaderEnded) {
      value = unSealedReader.getValueInTimestamp(timestamp);
    }
    if (value != null || unSealedReader.hasNext()) {
      return value;
    } else {
      unSealedReaderEnded = true;
    }
    return memSeriesReader.getValueInTimestamp(timestamp);
  }

  @Override
  public boolean hasNext() throws IOException {
    if (unSealedReaderEnded) {
      return memSeriesReader.hasNext();
    }
    return (unSealedReader.hasNext() || memSeriesReader.hasNext());
  }

}
