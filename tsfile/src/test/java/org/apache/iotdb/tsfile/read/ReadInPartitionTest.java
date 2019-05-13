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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iotdb.tsfile.common.constant.QueryConstant;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.TsFileGeneratorForTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadInPartitionTest {

  private static final String FILE_PATH = TsFileGeneratorForTest.outputDataFile;
  private TsFileSequenceReader reader;
  private static ReadOnlyTsFile roTsFile = null;
  private static final Logger LOG = LoggerFactory.getLogger(ReadInPartitionTest.class);

  @Before
  public void before() throws InterruptedException, WriteProcessException, IOException {
    TsFileGeneratorForTest.generateFile(1000000, 1024 * 1024, 10000);
    reader = new TsFileSequenceReader(FILE_PATH);

    LOG.info("file length: {}", new File(FILE_PATH).length());
    LOG.info("file magic head: {}", reader.readHeadMagic());
    LOG.info("file magic tail: {}", reader.readTailMagic());
    LOG.info("Level 1 metadata position: {}", reader.getFileMetadataPos());
    LOG.info("Level 1 metadata size: {}", reader.getFileMetadataPos());
    TsFileMetaData metaData = reader.readFileMetadata();
    System.out.println("[Metadata]");
    List<TsDeviceMetadataIndex> deviceMetadataIndexList = metaData.getDeviceMap().values().stream()
        .sorted((x, y) -> (int) (x.getOffset() - y.getOffset())).collect(Collectors.toList());
    for (TsDeviceMetadataIndex index : deviceMetadataIndexList) {
      TsDeviceMetadata deviceMetadata = reader.readTsDeviceMetaData(index);
      List<ChunkGroupMetaData> chunkGroupMetaDataList = deviceMetadata.getChunkGroupMetaDataList();
      for (ChunkGroupMetaData chunkGroupMetaData : chunkGroupMetaDataList) {
        LOG.info("t[Device]Device:{}", chunkGroupMetaData.getDeviceID());
        LOG.info("chunkGroupMetaData.start:{}, end:{}",
            chunkGroupMetaData.getStartOffsetOfChunkGroup(),
            chunkGroupMetaData.getEndOffsetOfChunkGroup());

//        for (ChunkMetaData chunkMetadata : chunkGroupMetaData.getChunkMetaDataList()) {
//          System.out.println("\t\tMeasurement:" + chunkMetadata.getMeasurementUid());
//          System.out.println("\t\tFile offset:" + chunkMetadata.getOffsetOfChunkHeader());
//        }

      }
    }
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
