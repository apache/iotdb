/*
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

package org.apache.iotdb.db.query.externalsort;

import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.chunk.ChunkReaderWrap;
import org.apache.iotdb.db.query.reader.universal.FakedSeriesReader;
import org.apache.iotdb.db.query.reader.universal.PriorityMergeReader;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.reader.IPointReader;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ExternalSortEngineTest {

  private String baseDir =
      TestConstant.BASE_OUTPUT_PATH.concat("externalSortTestTmp").concat(File.separator);
  private long queryId = EnvironmentUtils.TEST_QUERY_JOB_ID;
  private SimpleExternalSortEngine engine;
  private String preBaseDir;
  private int preMinExternalSortSourceCount;

  @Before
  public void before() {
    engine = SimpleExternalSortEngine.getInstance();
    preBaseDir = engine.getQueryDir();
    preMinExternalSortSourceCount = engine.getMinExternalSortSourceCount();
    engine.setQueryDir(baseDir);
  }

  @After
  public void after() throws IOException, StorageEngineException {
    engine.setQueryDir(preBaseDir);
    engine.setMinExternalSortSourceCount(preMinExternalSortSourceCount);
    EnvironmentUtils.cleanAllDir();
    QueryResourceManager.getInstance().endQuery(queryId);
    deleteExternalTempDir();
  }

  @Test
  public void testSimple() throws IOException {
    engine.setMinExternalSortSourceCount(2);
    List<IPointReader> readerList1 = genSimple();
    List<IPointReader> readerList2 = genSimple();
    List<ChunkReaderWrap> chunkReaderWrapList = new ArrayList<>();
    readerList1.forEach(x -> chunkReaderWrapList.add(new FakeChunkReaderWrap(x)));
    readerList1 = engine.executeForIPointReader(queryId, chunkReaderWrapList);
    PriorityMergeReader reader1 = new PriorityMergeReader(readerList1, 1);
    PriorityMergeReader reader2 = new PriorityMergeReader(readerList2, 1);
    //    check(reader1, reader2);
    reader1.close();
    reader2.close();
  }

  @Test
  public void testBig() throws IOException {
    engine.setMinExternalSortSourceCount(50);
    int lineCount = 100;
    int valueCount = 10000;
    List<long[]> data = genData(lineCount, valueCount);

    List<IPointReader> readerList1 = genReaders(data);
    List<IPointReader> readerList2 = genReaders(data);
    List<ChunkReaderWrap> chunkReaderWrapList = new ArrayList<>();
    readerList1.forEach(x -> chunkReaderWrapList.add(new FakeChunkReaderWrap(x)));
    readerList1 = engine.executeForIPointReader(queryId, chunkReaderWrapList);
    PriorityMergeReader reader1 = new PriorityMergeReader(readerList1, 1);
    PriorityMergeReader reader2 = new PriorityMergeReader(readerList2, 1);

    //    check(reader1, reader2);
    reader1.close();
    reader2.close();
  }

  public void efficiencyTest() throws IOException {
    engine.setMinExternalSortSourceCount(50);
    int lineCount = 100000;
    int valueCount = 100;
    List<long[]> data = genData(lineCount, valueCount);

    List<IPointReader> readerList1 = genReaders(data);
    List<ChunkReaderWrap> chunkReaderWrapList = new ArrayList<>();
    readerList1.forEach(x -> chunkReaderWrapList.add(new FakeChunkReaderWrap(x)));

    long startTimestamp = System.currentTimeMillis();
    readerList1 = engine.executeForIPointReader(queryId, chunkReaderWrapList);
    PriorityMergeReader reader1 = new PriorityMergeReader();
    for (int i = 0; i < readerList1.size(); i++) {
      reader1.addReader(readerList1.get(i), i);
    }
    while (reader1.hasNextTimeValuePair()) {
      reader1.nextTimeValuePair();
    }
    System.out.println(
        "Time used WITH external sort:" + (System.currentTimeMillis() - startTimestamp) + "ms");

    List<IPointReader> readerList2 = genReaders(data);
    startTimestamp = System.currentTimeMillis();
    PriorityMergeReader reader2 = new PriorityMergeReader();
    for (int i = 0; i < readerList2.size(); i++) {
      reader2.addReader(readerList2.get(i), i);
    }
    while (reader2.hasNextTimeValuePair()) {
      reader2.nextTimeValuePair();
    }
    System.out.println(
        "Time used WITHOUT external sort:" + (System.currentTimeMillis() - startTimestamp) + "ms");

    // reader1.close();
    reader2.close();
  }

  private List<long[]> genData(int lineCount, int valueCountEachLine) {
    Random rand = new Random();
    List<long[]> data = new ArrayList<>();
    for (int i = 0; i < lineCount; i++) {
      long[] tmp = new long[valueCountEachLine];
      long start = rand.nextInt(Integer.MAX_VALUE);
      for (int j = 0; j < valueCountEachLine; j++) {
        tmp[j] = start++;
      }
      data.add(tmp);
    }
    return data;
  }

  private List<IPointReader> genReaders(List<long[]> data) {
    List<IPointReader> readerList = new ArrayList<>();
    for (int i = 0; i < data.size(); i++) {
      readerList.add(new FakedSeriesReader(data.get(i), i));
    }
    return readerList;
  }

  private void check(IPointReader reader1, IPointReader reader2) throws IOException {
    while (reader1.hasNextTimeValuePair() && reader2.hasNextTimeValuePair()) {
      TimeValuePair tv1 = reader1.nextTimeValuePair();
      TimeValuePair tv2 = reader2.nextTimeValuePair();
      Assert.assertEquals(tv1.getTimestamp(), tv2.getTimestamp());
      Assert.assertEquals(tv1.getValue(), tv2.getValue());
    }
    Assert.assertFalse(reader2.hasNextTimeValuePair());
    Assert.assertFalse(reader1.hasNextTimeValuePair());
  }

  private List<IPointReader> genSimple() {
    IPointReader reader1 = new FakedSeriesReader(new long[] {1, 2, 3, 4, 5}, 1L);
    IPointReader reader2 = new FakedSeriesReader(new long[] {1, 5, 6, 7, 8}, 2L);
    IPointReader reader3 = new FakedSeriesReader(new long[] {4, 5, 6, 7, 10}, 3L);

    List<IPointReader> readerList = new ArrayList<>();
    readerList.add(reader1);
    readerList.add(reader2);
    readerList.add(reader3);
    return readerList;
  }

  private void deleteExternalTempDir() throws IOException {
    File file = new File(baseDir);
    if (!file.delete()) {
      throw new IOException("delete external sort tmp file dir error.");
    }
  }
}
