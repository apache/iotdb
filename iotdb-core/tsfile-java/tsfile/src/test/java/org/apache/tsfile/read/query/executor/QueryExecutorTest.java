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
package org.apache.tsfile.read.query.executor;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.controller.CachedChunkLoaderImpl;
import org.apache.tsfile.read.controller.IChunkLoader;
import org.apache.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.tsfile.read.expression.IExpression;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.expression.impl.BinaryExpression;
import org.apache.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.factory.FilterFactory;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.tsfile.read.filter.factory.ValueFilterApi;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.TsFileGeneratorForTest;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.apache.tsfile.read.filter.factory.ValueFilterApi.DEFAULT_MEASUREMENT_INDEX;

public class QueryExecutorTest {

  private static final String FILE_PATH = TsFileGeneratorForTest.outputDataFile;
  private TsFileSequenceReader fileReader;
  private MetadataQuerierByFileImpl metadataQuerierByFile;
  private IChunkLoader chunkLoader;
  private int rowCount = 10000;
  private TsFileExecutor queryExecutorWithQueryFilter;

  @Before
  public void before() throws IOException {
    TSFileDescriptor.getInstance().getConfig().setTimeEncoder("TS_2DIFF");
    TsFileGeneratorForTest.generateFile(rowCount, 16 * 1024 * 1024, 10000);
    fileReader = new TsFileSequenceReader(FILE_PATH);
    metadataQuerierByFile = new MetadataQuerierByFileImpl(fileReader);
    chunkLoader = new CachedChunkLoaderImpl(fileReader);
    queryExecutorWithQueryFilter = new TsFileExecutor(metadataQuerierByFile, chunkLoader);
  }

  @After
  public void after() throws IOException {
    fileReader.close();
    TsFileGeneratorForTest.after();
  }

  @Test
  public void query1() throws IOException {
    Filter filter = TimeFilterApi.lt(1480562618100L);
    Filter filter2 =
        ValueFilterApi.gt(
            DEFAULT_MEASUREMENT_INDEX,
            new Binary("dog", TSFileConfig.STRING_CHARSET),
            TSDataType.TEXT);

    IExpression IExpression =
        BinaryExpression.and(
            new SingleSeriesExpression(new Path("d1", "s1", true), filter),
            new SingleSeriesExpression(new Path("d1", "s4", true), filter2));

    QueryExpression queryExpression =
        QueryExpression.create()
            .addSelectedPath(new Path("d1", "s1", true))
            .addSelectedPath(new Path("d1", "s2", true))
            .addSelectedPath(new Path("d1", "s4", true))
            .addSelectedPath(new Path("d1", "s5", true))
            .setExpression(IExpression);
    long startTimestamp = System.currentTimeMillis();
    QueryDataSet queryDataSet = queryExecutorWithQueryFilter.execute(queryExpression);
    long aimedTimestamp = 1480562618000L;
    while (queryDataSet.hasNext()) {
      RowRecord rowRecord = queryDataSet.next();
      Assert.assertEquals(aimedTimestamp, rowRecord.getTimestamp());
      aimedTimestamp += 8;
    }
    long endTimestamp = System.currentTimeMillis();
  }

  @Test
  public void queryWithoutFilter() throws IOException {
    QueryExecutor queryExecutor = new TsFileExecutor(metadataQuerierByFile, chunkLoader);

    QueryExpression queryExpression =
        QueryExpression.create()
            .addSelectedPath(new Path("d1", "s1", true))
            .addSelectedPath(new Path("d1", "s2", true))
            .addSelectedPath(new Path("d1", "s3", true))
            .addSelectedPath(new Path("d1", "s4", true))
            .addSelectedPath(new Path("d1", "s5", true));

    long aimedTimestamp = 1480562618000L;
    int count = 0;
    long startTimestamp = System.currentTimeMillis();
    QueryDataSet queryDataSet = queryExecutor.execute(queryExpression);
    while (queryDataSet.hasNext()) {
      RowRecord rowRecord = queryDataSet.next();
      Assert.assertEquals(aimedTimestamp, rowRecord.getTimestamp());
      aimedTimestamp++;
      count++;
    }
    Assert.assertEquals(rowCount, count);
    long endTimestamp = System.currentTimeMillis();
  }

  @Test
  public void queryWithGlobalTimeFilter() throws IOException {
    QueryExecutor queryExecutor = new TsFileExecutor(metadataQuerierByFile, chunkLoader);

    IExpression IExpression =
        new GlobalTimeExpression(
            FilterFactory.and(
                TimeFilterApi.gtEq(1480562618100L), TimeFilterApi.lt(1480562618200L)));
    QueryExpression queryExpression =
        QueryExpression.create()
            .addSelectedPath(new Path("d1", "s1", true))
            .addSelectedPath(new Path("d1", "s2", true))
            .addSelectedPath(new Path("d1", "s3", true))
            .addSelectedPath(new Path("d1", "s4", true))
            .addSelectedPath(new Path("d1", "s5", true))
            .setExpression(IExpression);

    long aimedTimestamp = 1480562618100L;
    int count = 0;
    long startTimestamp = System.currentTimeMillis();
    QueryDataSet queryDataSet = queryExecutor.execute(queryExpression);
    while (queryDataSet.hasNext()) {
      RowRecord rowRecord = queryDataSet.next();
      Assert.assertEquals(aimedTimestamp, rowRecord.getTimestamp());
      aimedTimestamp++;
      count++;
    }
    Assert.assertEquals(100, count);
    long endTimestamp = System.currentTimeMillis();
  }
}
