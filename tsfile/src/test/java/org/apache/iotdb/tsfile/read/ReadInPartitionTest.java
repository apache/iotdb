package org.apache.iotdb.tsfile.read;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.iotdb.tsfile.common.constant.QueryConstant;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.TsFileGeneratorForTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ReadInPartitionTest {

  private static final String FILE_PATH = TsFileGeneratorForTest.outputDataFile;
  private TsFileSequenceReader reader;
  private static ReadOnlyTsFile roTsFile = null;

  @Before
  public void before() throws InterruptedException, WriteProcessException, IOException {
    TsFileGeneratorForTest.generateFile(1000000, 1024 * 1024, 10000);
    reader = new TsFileSequenceReader(FILE_PATH);
  }

  @After
  public void after() throws IOException {
    roTsFile.close();
    TsFileGeneratorForTest.after();
  }

  @Test
  public void test() throws IOException {
    HashMap<String, Long> params = new HashMap<>();
//    params.put(QueryConstant.PARTITION_START_OFFSET, 0L);
//    params.put(QueryConstant.PARTITION_END_OFFSET, 603242L);

    params.put(QueryConstant.PARTITION_START_OFFSET, 603242L);
    params.put(QueryConstant.PARTITION_END_OFFSET, 993790L);

    roTsFile = new ReadOnlyTsFile(reader, params);

    ArrayList<Path> paths = new ArrayList<>();
    paths.add(new Path("d1.s6"));
    paths.add(new Path("d2.s1"));
    QueryExpression queryExpression = QueryExpression.create(paths, null);

    QueryDataSet queryDataSet = roTsFile.query(queryExpression);

    int cnt = 0;
    while (queryDataSet.hasNext()) {
      RowRecord r = queryDataSet.next();
//      if (cnt == 1) {
//        assertEquals(1480562618970L, r.getTimestamp());
//      } else if (cnt == 2) {
//        assertEquals(1480562618971L, r.getTimestamp());
//      } else if (cnt == 3) {
//        assertEquals(1480562618973L, r.getTimestamp());
//      }
      System.out.println(r);
      cnt++;
    }
//    assertEquals(7, cnt);

  }
}
