package org.apache.iotdb.db.query.reader.seriesRelated;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.ManagedSeriesReader;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;


public class RawDataReader implements IBatchReader, ManagedSeriesReader {

  private final SeriesReader seriesReader;
  private boolean hasRemaining;
  private boolean managedByQueryManager;

  public RawDataReader(SeriesReader seriesReader) {
    this.seriesReader = seriesReader;
  }

  public RawDataReader(Path seriesPath, TSDataType dataType, QueryContext context,
      QueryDataSource dataSource, Filter timeFilter, Filter valueFilter) {
    this.seriesReader = new SeriesReader(seriesPath, dataType, context, dataSource, timeFilter,
        valueFilter);
  }

  @TestOnly
  public RawDataReader(Path seriesPath, TSDataType dataType, QueryContext context,
      List<TsFileResource> seqFileResource, List<TsFileResource> unseqFileResource,
      Filter timeFilter, Filter valueFilter) {
    this.seriesReader = new SeriesReader(seriesPath, dataType, context, seqFileResource,
        unseqFileResource, timeFilter, valueFilter);
  }

  private BatchData batchData;
  private boolean hasCachedBatchData = false;

  /**
   * This method overrides the AbstractDataReader.hasNextOverlappedPage for pause reads, to achieve
   * a continuous read
   */
  @Override
  public boolean hasNextBatch() throws IOException {

    if (hasCachedBatchData) {
      return true;
    }

    while (seriesReader.hasNextChunk()) {
      while (seriesReader.hasNextPage()) {
        if (!seriesReader.isPageOverlapped()) {
          batchData = seriesReader.nextPage();
          hasCachedBatchData = true;
          return true;
        }
        while (seriesReader.hasNextOverlappedPage()) {
          batchData = seriesReader.nextOverlappedPage();
          hasCachedBatchData = true;
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public BatchData nextBatch() throws IOException {
    if (hasCachedBatchData || hasNextBatch()) {
      hasCachedBatchData = false;
      return batchData;
    }
    throw new IOException("no next batch");
  }

  @Override
  public void close() throws IOException {
  }


  @Override
  public boolean isManagedByQueryManager() {
    return managedByQueryManager;
  }

  @Override
  public void setManagedByQueryManager(boolean managedByQueryManager) {
    this.managedByQueryManager = managedByQueryManager;
  }

  @Override
  public boolean hasRemaining() {
    return hasRemaining;
  }

  @Override
  public void setHasRemaining(boolean hasRemaining) {
    this.hasRemaining = hasRemaining;
  }

}
