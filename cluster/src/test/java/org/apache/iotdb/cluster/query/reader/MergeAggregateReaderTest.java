package org.apache.iotdb.cluster.query.reader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.cluster.common.TestAggregateReader;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.IAggregateReader;
import org.junit.Test;

public class MergeAggregateReaderTest {

  @Test
  public void test() throws IOException {
    List<IAggregateReader> aggregateReaders = new ArrayList<>();
    int step = 10;
    int readerDataSize = 10000;
    int pageSize = 1000;
    int readerNum = 10;
    for (int i = 0; i < readerNum; i++) {
      List<BatchData> batchDataList = TestUtils.getTestBatches(i * step * readerDataSize, readerDataSize,
          pageSize, step, TSDataType.INT64);
      aggregateReaders.add(new TestAggregateReader(batchDataList, TSDataType.INT64));
    }
    MergeAggregateReader aggregateReader = new MergeAggregateReader(aggregateReaders);

    for (int i = 0; i < 100; i++) {
      assertTrue(aggregateReader.hasNextBatch());
      PageHeader header = aggregateReader.nextPageHeader();
      assertEquals(i * pageSize * step, header.getStartTime());
      assertEquals((i + 1) * pageSize * step - 1, header.getEndTime());
      assertEquals(pageSize, header.getNumOfValues());
      assertEquals(i * pageSize * step, header.getStatistics().getFirstValue());
      assertEquals((i + 1) * pageSize * step - 1, header.getStatistics().getLastValue());
      if (i % 2 == 0) {
        BatchData batchData = aggregateReader.nextBatch();
        for (int j = i * pageSize * step; j < (i + 1) * pageSize * step - 1; j++) {
          assertTrue(batchData.hasCurrent());
          assertEquals(j, batchData.currentTime());
          assertEquals(j, batchData.getLong());
          batchData.next();
        }
        assertFalse(batchData.hasCurrent());
      } else {
        aggregateReader.skipPageData();
      }
    }
    assertFalse(aggregateReader.hasNextBatch());
  }
}
