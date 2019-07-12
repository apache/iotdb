package org.apache.iotdb.db.query.reader.universal;

import java.io.IOException;
import org.apache.iotdb.db.query.reader.IAggregateReader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.read.common.BatchData;

public abstract class IterateReader implements IAggregateReader {

  protected IAggregateReader currentSeriesReader;
  private boolean curReaderInitialized;
  private int nextSeriesReaderIndex;
  private int readerSize;


  public IterateReader(int readerSize) {
    this.curReaderInitialized = false;
    this.nextSeriesReaderIndex = 0;
    this.readerSize = readerSize;
  }

  @Override
  public boolean hasNext() throws IOException {

    if (curReaderInitialized && currentSeriesReader.hasNext()) {
      return true;
    } else {
      curReaderInitialized = false;
    }

    while (nextSeriesReaderIndex < readerSize) {
      //seqResourceSeriesReaderList.get(nextSeriesReaderIndex++)
      boolean satisfied = constructNextReader(nextSeriesReaderIndex++);
      if (satisfied && currentSeriesReader.hasNext()) {
        curReaderInitialized = true;
        return true;
      }
    }
    return false;
  }

  public abstract boolean constructNextReader(int idx) throws IOException;

  @Override
  public BatchData nextBatch() throws IOException {
    return currentSeriesReader.nextBatch();
  }

  @Override
  public PageHeader nextPageHeader() throws IOException {
    return currentSeriesReader.nextPageHeader();
  }

  @Override
  public void skipPageData() throws IOException {
    currentSeriesReader.skipPageData();
  }

  @Override
  public void close() {
    // file stream is managed in QueryResourceManager.
  }
}
