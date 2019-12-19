package org.apache.iotdb.db.query.reader.resourceRelated;

import java.io.IOException;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.ReaderTestHelper;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.junit.Assert;
import org.junit.Test;

public class NewUnseqResourceMergeReaderTest extends ReaderTestHelper{

  private QueryContext context = EnvironmentUtils.TEST_QUERY_CONTEXT;

  /**
   * chunk : [1,10], [1,10], [1,10], [10,20], [21,30]
   */
  @Override protected void insertData() throws IOException, QueryProcessException {
    insertOneRecord(1000, 1000);
    storageGroupProcessor.putAllWorkingTsFileProcessorIntoClosingList();

    for (int j = 1; j <= 10; j++) {
      insertOneRecord(j, j);
    }
    storageGroupProcessor.getWorkUnSequenceTsFileProcessor().syncFlush();

    for (int j = 1; j <= 10; j++) {
      insertOneRecord(j, j);
    }
    storageGroupProcessor.getWorkUnSequenceTsFileProcessor().syncFlush();

    for (int j = 1; j <= 10; j++) {
      insertOneRecord(j, j*10);
    }
    storageGroupProcessor.getWorkUnSequenceTsFileProcessor().syncFlush();

    for (int j = 10; j <= 20; j++) {
      insertOneRecord(j, j);
    }
    storageGroupProcessor.getWorkUnSequenceTsFileProcessor().syncFlush();

    for (int j = 21; j <= 30; j++) {
      insertOneRecord(j, j);
    }
    storageGroupProcessor.getWorkUnSequenceTsFileProcessor().syncFlush();

  }

  @Test
  public void testNewUnseqResourceMergeReaderWithoutFilter() throws IOException {
    Path path = new Path(deviceId, measurementId);
    QueryDataSource queryDataSource = storageGroupProcessor
        .query(deviceId, measurementId, context, null);
    IBatchReader reader = new NewUnseqResourceMergeReader(path, dataType,
        queryDataSource.getUnseqResources(), EnvironmentUtils.TEST_QUERY_CONTEXT, null);

    int cnt = 0;
    BatchData batchData;
    while (reader.hasNextBatch()) {
      batchData = reader.nextBatch();
      while (batchData.hasCurrent()) {
        cnt++;
        batchData.next();
      }
    }
    Assert.assertEquals(30, cnt);
  }

}
