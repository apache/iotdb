package org.apache.iotdb.cluster.query.reader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.cluster.common.TestPointReader;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.junit.Test;

public class BatchedPointReaderTest {

  @Test
  public void testAsPointReader() throws IOException {
    List<TimeValuePair> data = TestUtils.getTestTimeValuePairs(0, 10000, 10, TSDataType.INT64);
    IPointReader innerReader = new TestPointReader(data);
    BatchedPointReader batchedPointReader = new BatchedPointReader(innerReader, TSDataType.INT64);
    for (int i = 0; i < 10000; i++) {
      assertTrue(batchedPointReader.hasNext());
      assertEquals(data.get(i), batchedPointReader.current());
      assertEquals(data.get(i), batchedPointReader.next());
    }
  }

  @Test
  public void testAsBatchReader() throws IOException {
    List<TimeValuePair> data = TestUtils.getTestTimeValuePairs(0, 10000, 10, TSDataType.INT64);
    IPointReader innerReader = new TestPointReader(data);
    BatchedPointReader batchedPointReader = new BatchedPointReader(innerReader, TSDataType.INT64);
    int cnt = 0;
    while (cnt < 10000) {
      assertTrue(batchedPointReader.hasNextBatch());
      BatchData batchData = batchedPointReader.nextBatch();
      while (batchData.hasCurrent()) {
        assertEquals(data.get(cnt).getTimestamp(), batchData.currentTime());
        assertEquals(data.get(cnt).getValue().getLong(), batchData.getLong());
        batchData.next();
        cnt ++;
      }
    }
  }
}
