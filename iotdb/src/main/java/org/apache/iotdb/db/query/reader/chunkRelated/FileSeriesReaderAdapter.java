package org.apache.iotdb.db.query.reader.chunkRelated;

import java.io.IOException;
import org.apache.iotdb.db.query.reader.IAggregateReader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReader;

// TODO 是不是改成SealedTsFileReader以便和UnSealedTsFileReader对称？

public class FileSeriesReaderAdapter implements IAggregateReader {

  private FileSeriesReader fileSeriesReader;

  public FileSeriesReaderAdapter(FileSeriesReader fileSeriesReader) {
    this.fileSeriesReader = fileSeriesReader;
  }

  @Override
  public PageHeader nextPageHeader() throws IOException {
    return fileSeriesReader.nextPageHeader();
  }

  @Override
  public void skipPageData() {
    fileSeriesReader.skipPageData();
  }

  @Override
  public boolean hasNext() throws IOException {
    return fileSeriesReader.hasNextBatch();
  }

  @Override
  public BatchData nextBatch() throws IOException {
    return fileSeriesReader.nextBatch();
  }

  @Override
  public void close() throws IOException {
    fileSeriesReader.close();
  }
}
