/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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
import org.junit.Assert;
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
  public void test1() throws IOException {
    HashMap<String, Long> params = new HashMap<>();
    params.put(QueryConstant.PARTITION_START_OFFSET, 0L);
    params.put(QueryConstant.PARTITION_END_OFFSET, 603242L);

    roTsFile = new ReadOnlyTsFile(reader, params);

    ArrayList<Path> paths = new ArrayList<>();
    paths.add(new Path("d1.s6"));
    paths.add(new Path("d2.s1"));
    QueryExpression queryExpression = QueryExpression.create(paths, null);

    QueryDataSet queryDataSet = roTsFile.query(queryExpression);

    int cnt = 0;
    while (queryDataSet.hasNext()) {
      RowRecord r = queryDataSet.next();
      cnt++;
      if (cnt == 1) {
        Assert.assertEquals("1480562618000\t0.0\t1", r.toString());
      } else if (cnt == 9352) {
        Assert.assertEquals("1480562664755\tnull\t467551", r.toString());
      }
    }
    Assert.assertEquals(9353, cnt);
  }

  @Test
  public void test2() throws IOException {
    HashMap<String, Long> params = new HashMap<>();
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
      cnt++;
      if (cnt == 1) {
        Assert.assertEquals("1480562664765\tnull\t467651", r.toString());
      }
    }
    Assert.assertEquals(1, cnt);
  }

  @Test
  public void test3() throws IOException {
    HashMap<String, Long> params = new HashMap<>();
    params.put(QueryConstant.PARTITION_START_OFFSET, 993790L);
    params.put(QueryConstant.PARTITION_END_OFFSET, 1608255L);

    roTsFile = new ReadOnlyTsFile(reader, params);

    ArrayList<Path> paths = new ArrayList<>();
    paths.add(new Path("d1.s6"));
    paths.add(new Path("d2.s1"));
    QueryExpression queryExpression = QueryExpression.create(paths, null);

    QueryDataSet queryDataSet = roTsFile.query(queryExpression);

    int cnt = 0;
    while (queryDataSet.hasNext()) {
      RowRecord r = queryDataSet.next();
      cnt++;
      if (cnt == 1) {
        Assert.assertEquals("1480562664770\t5196.0\t467701", r.toString());
      } else if (cnt == 9936) {
        Assert.assertEquals("1480562711445\tnull\t934451", r.toString());
      }
    }
    Assert.assertEquals(9337, cnt);
  }

  @Test
  public void test4() throws IOException {
    HashMap<String, Long> params = new HashMap<>();
    params.put(QueryConstant.PARTITION_START_OFFSET, 1608255L);
    params.put(QueryConstant.PARTITION_END_OFFSET, 1999353L);

    roTsFile = new ReadOnlyTsFile(reader, params);

    ArrayList<Path> paths = new ArrayList<>();
    paths.add(new Path("d1.s6"));
    paths.add(new Path("d2.s1"));
    QueryExpression queryExpression = QueryExpression.create(paths, null);

    QueryDataSet queryDataSet = roTsFile.query(queryExpression);

    int cnt = 0;
    while (queryDataSet.hasNext()) {
      RowRecord r = queryDataSet.next();
      cnt++;
    }
    Assert.assertEquals(0, cnt);
  }
}
