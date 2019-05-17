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
import java.util.List;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.expression.ExpressionType;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.expression.util.ExpressionOptimizer;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.TsFileGeneratorForTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/*
  This test is designed for the TsFileExecutor's execute(queryExpression, params) function.

  The test target here is the logic of converting the query partition constraint to an additional time filter.

  Note that the correctness of the constructed additional time filter, which is guaranteed and tested in
  MetadataQuerierByFileImplTest and TimeRangeTest, is not the test focus here.

 */
public class ReadInPartitionTest {

  private static final String FILE_PATH = TsFileGeneratorForTest.outputDataFile;
  private static ReadOnlyTsFile roTsFile = null;
  private ArrayList<TimeRange> d1s6timeRangeList = new ArrayList<>();
  private ArrayList<TimeRange> d2s1timeRangeList = new ArrayList<>();
  private ArrayList<long[]> d1chunkGroupMetaDataOffsetList = new ArrayList<>();
  private ArrayList<long[]> d2chunkGroupMetaDataOffsetList = new ArrayList<>();


  @Before
  public void before() throws InterruptedException, WriteProcessException, IOException {
    TsFileGeneratorForTest.generateFile(1000000, 1024 * 1024, 10000);
    TsFileSequenceReader reader = new TsFileSequenceReader(FILE_PATH);
    roTsFile = new ReadOnlyTsFile(reader);

    // Because the size of the generated chunkGroupMetaData may differ under different test environments,
    // we get metadata from the real-time generated TsFile instead of using a fixed parameter setting.
    TsFileMetaData metaData = reader.readFileMetadata();
    TsDeviceMetadataIndex d1MetadataIndex = metaData.getDeviceMap().get("d1");
    TsDeviceMetadataIndex d2MetadataIndex = metaData.getDeviceMap().get("d2");

    TsDeviceMetadata d1Metadata = reader.readTsDeviceMetaData(d1MetadataIndex);
    List<ChunkGroupMetaData> d1chunkGroupMetaDataList = d1Metadata.getChunkGroupMetaDataList();
    for (ChunkGroupMetaData chunkGroupMetaData : d1chunkGroupMetaDataList) {
      // get a series of [startOffsetOfChunkGroup, endOffsetOfChunkGroup] from the chunkGroupMetaData of d1
      long[] chunkGroupMetaDataOffset = new long[2];
      chunkGroupMetaDataOffset[0] = chunkGroupMetaData.getStartOffsetOfChunkGroup();
      chunkGroupMetaDataOffset[1] = chunkGroupMetaData.getEndOffsetOfChunkGroup();
      d1chunkGroupMetaDataOffsetList.add(chunkGroupMetaDataOffset);

      List<ChunkMetaData> chunkMetaDataList = chunkGroupMetaData.getChunkMetaDataList();
      for (ChunkMetaData chunkMetaData : chunkMetaDataList) {
        if (chunkMetaData.getMeasurementUid().equals("s6")) {
          // get a series of [startTime, endTime] of d1.s6 from the chunkGroupMetaData of d1
          d1s6timeRangeList
              .add(new TimeRange(chunkMetaData.getStartTime(), chunkMetaData.getEndTime()));
        }
      }
    }

    TsDeviceMetadata d2Metadata = reader.readTsDeviceMetaData(d2MetadataIndex);
    List<ChunkGroupMetaData> d2chunkGroupMetaDataList = d2Metadata.getChunkGroupMetaDataList();
    for (ChunkGroupMetaData chunkGroupMetaData : d2chunkGroupMetaDataList) {
      // get a series of [startOffsetOfChunkGroup, endOffsetOfChunkGroup] from the chunkGroupMetaData of d2
      long[] chunkGroupMetaDataOffset = new long[2];
      chunkGroupMetaDataOffset[0] = chunkGroupMetaData.getStartOffsetOfChunkGroup();
      chunkGroupMetaDataOffset[1] = chunkGroupMetaData.getEndOffsetOfChunkGroup();
      d2chunkGroupMetaDataOffsetList.add(chunkGroupMetaDataOffset);

      List<ChunkMetaData> chunkMetaDataList = chunkGroupMetaData.getChunkMetaDataList();
      for (ChunkMetaData chunkMetaData : chunkMetaDataList) {
        if (chunkMetaData.getMeasurementUid().equals("s1")) {
          // get a series of [startTime, endTime] of d2.s1 from the chunkGroupMetaData of d1
          d2s1timeRangeList
              .add(new TimeRange(chunkMetaData.getStartTime(), chunkMetaData.getEndTime()));
        }
      }
    }
  }

  @After
  public void after() throws IOException {
    roTsFile.close();
    TsFileGeneratorForTest.after();
  }

  @Test
  public void test0() throws IOException {
    ArrayList<Path> paths = new ArrayList<>();
    paths.add(new Path("d1.s6"));
    paths.add(new Path("d2.s1"));
    QueryExpression queryExpression = QueryExpression.create(paths, null);

    QueryDataSet queryDataSet = roTsFile.query(queryExpression, 0L, 0L);

    // test the transformed expression
    Assert.assertNull(queryExpression.getExpression());

    // test the equivalence of the query result
    Assert.assertFalse(queryDataSet.hasNext());
  }

  @Test
  public void test1() throws IOException, QueryFilterOptimizationException {
    ArrayList<Path> paths = new ArrayList<>();
    paths.add(new Path("d1.s6"));
    paths.add(new Path("d2.s1"));
    QueryExpression queryExpression = QueryExpression.create(paths, null);

    QueryDataSet queryDataSet = roTsFile
        .query(queryExpression, d1chunkGroupMetaDataOffsetList.get(0)[0],
            d1chunkGroupMetaDataOffsetList.get(0)[1]);
    // get the transformed expression
    IExpression transformedExpression = queryExpression.getExpression();

    // test the transformed expression
    Assert.assertEquals(ExpressionType.GLOBAL_TIME, transformedExpression.getType());

    IExpression expectedTimeExpression = d1s6timeRangeList.get(0).getExpression();
    String expected = ExpressionOptimizer.getInstance().optimize(expectedTimeExpression,
        queryExpression.getSelectedSeries()).toString();
    Assert.assertEquals(expected, transformedExpression.toString());

    // test the equivalence of the query result:
    QueryDataSet queryDataSet_eq = roTsFile.query(queryExpression);
    while (queryDataSet.hasNext() && queryDataSet_eq.hasNext()) {
      RowRecord r = queryDataSet.next();
      RowRecord r2 = queryDataSet_eq.next();
      Assert.assertEquals(r2.toString(), r.toString());
    }
    Assert.assertEquals(queryDataSet_eq.hasNext(), queryDataSet.hasNext());
  }

  @Test
  public void test2() throws IOException, QueryFilterOptimizationException {
    ArrayList<Path> paths = new ArrayList<>();
    paths.add(new Path("d1.s6"));
    paths.add(new Path("d2.s1"));
    IExpression expression = new GlobalTimeExpression(TimeFilter.gt(50L));
    QueryExpression queryExpression = QueryExpression.create(paths, expression);

    QueryDataSet queryDataSet = roTsFile
        .query(queryExpression, d1chunkGroupMetaDataOffsetList.get(0)[0],
            d1chunkGroupMetaDataOffsetList.get(0)[1]);
    // get the transformed expression
    IExpression transformedExpression = queryExpression.getExpression();

    // test the transformed expression
    Assert.assertEquals(ExpressionType.GLOBAL_TIME, transformedExpression.getType());

    IExpression expectedTimeExpression = BinaryExpression
        .and(expression, d1s6timeRangeList.get(0).getExpression());
    String expected = ExpressionOptimizer.getInstance().optimize(expectedTimeExpression,
        queryExpression.getSelectedSeries()).toString();
    Assert.assertEquals(expected, transformedExpression.toString());

    // test the equivalence of the query result:
    QueryDataSet queryDataSet_eq = roTsFile.query(queryExpression);
    while (queryDataSet.hasNext() && queryDataSet_eq.hasNext()) {
      RowRecord r = queryDataSet.next();
      RowRecord r2 = queryDataSet_eq.next();
      Assert.assertEquals(r2.toString(), r.toString());
    }
    Assert.assertEquals(queryDataSet_eq.hasNext(), queryDataSet.hasNext());
  }

  @Test
  public void test3() throws IOException, QueryFilterOptimizationException {
    ArrayList<Path> paths = new ArrayList<>();
    paths.add(new Path("d1.s6"));
    paths.add(new Path("d2.s1"));
    Filter filter = ValueFilter.gt(10L);
    IExpression expression = new SingleSeriesExpression(new Path("d1.s3"), filter);
    QueryExpression queryExpression = QueryExpression.create(paths, expression);

    QueryDataSet queryDataSet = roTsFile
        .query(queryExpression, d1chunkGroupMetaDataOffsetList.get(0)[0],
            d1chunkGroupMetaDataOffsetList.get(0)[1]);
    // get the transformed expression
    IExpression transformedExpression = queryExpression.getExpression();

    // test the transformed expression
    Assert.assertEquals(ExpressionType.SERIES, transformedExpression.getType());

    IExpression expectedTimeExpression = BinaryExpression
        .and(expression, d1s6timeRangeList.get(0).getExpression());
    String expected = ExpressionOptimizer.getInstance().optimize(expectedTimeExpression,
        queryExpression.getSelectedSeries()).toString();
    Assert.assertEquals(expected, transformedExpression.toString());

    // test the equivalence of the query result:
    QueryDataSet queryDataSet_eq = roTsFile.query(queryExpression);
    while (queryDataSet.hasNext() && queryDataSet_eq.hasNext()) {
      RowRecord r = queryDataSet.next();
      RowRecord r2 = queryDataSet_eq.next();
      Assert.assertEquals(r2.toString(), r.toString());
    }
    Assert.assertEquals(queryDataSet_eq.hasNext(), queryDataSet.hasNext());
  }
}
