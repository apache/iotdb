/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.dataregion;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeTTLCache;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.read.TimeValuePair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.createTimeseries;

public class TTLQueryTest extends AbstractCompactionTest {

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    DataNodeTTLCache.getInstance().clearAllTTL();
  }

  /** Device d1, d3 and d5 is deleted by TTL. */
  @Test
  public void queryWithDeletedDeviceByTTL()
      throws IOException, MetadataException, WriteProcessException, QueryProcessException {
    createFiles(5, 5, 10, 50, 0, 0, 0, 0, false, true);
    createFiles(5, 5, 10, 50, 1707137815000L, 0, 0, 0, false, true);
    createFiles(5, 6, 10, 100, 0, 10000, 0, 50, false, false);
    createFiles(5, 6, 10, 100, 1707137815000L, 10000, 0, 50, false, false);

    DataRegion dataRegion = new DataRegion(COMPACTION_TEST_SG, "0");
    dataRegion.getTsFileManager().addAll(seqResources, true);
    dataRegion.getTsFileManager().addAll(unseqResources, false);
    tsFileManager = dataRegion.getTsFileManager();

    List<PartialPath> pathList = new ArrayList<>();
    pathList.add(new PartialPath(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d1", "s0"));
    pathList.add(new PartialPath(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d3", "s1"));
    pathList.add(new PartialPath(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d5", "s2"));

    QueryDataSource queryDataSource =
        dataRegion.query(pathList, null, EnvironmentUtils.TEST_QUERY_CONTEXT, null, null);
    Assert.assertEquals(10, queryDataSource.getSeqResources().size());
    Assert.assertEquals(10, queryDataSource.getUnseqResources().size());
    Map<PartialPath, List<TimeValuePair>> sourceDatas =
        readSourceFiles(createTimeseries(6, 10, false), Collections.emptyList());
    Assert.assertEquals(1000, sourceDatas.get(pathList.get(0)).size());
    Assert.assertEquals(1000, sourceDatas.get(pathList.get(1)).size());
    Assert.assertEquals(1000, sourceDatas.get(pathList.get(2)).size());
    Assert.assertTrue(sourceDatas.get(pathList.get(0)).get(0).getTimestamp() < 1707137815000L);
    Assert.assertTrue(sourceDatas.get(pathList.get(1)).get(0).getTimestamp() < 1707137815000L);
    Assert.assertTrue(sourceDatas.get(pathList.get(2)).get(0).getTimestamp() < 1707137815000L);

    // set ttl
    DataNodeTTLCache.getInstance()
        .setTTL(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d1", 315360000000L);
    DataNodeTTLCache.getInstance()
        .setTTL(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d3", 315360000000L);
    DataNodeTTLCache.getInstance()
        .setTTL(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d5", 315360000000L);

    queryDataSource =
        dataRegion.query(pathList, null, EnvironmentUtils.TEST_QUERY_CONTEXT, null, null);
    Assert.assertEquals(10, queryDataSource.getSeqResources().size());
    Assert.assertEquals(10, queryDataSource.getUnseqResources().size());
    sourceDatas = readSourceFiles(createTimeseries(6, 10, false), Collections.emptyList());
    Assert.assertEquals(500, sourceDatas.get(pathList.get(0)).size());
    Assert.assertEquals(500, sourceDatas.get(pathList.get(1)).size());
    Assert.assertEquals(500, sourceDatas.get(pathList.get(2)).size());
    Assert.assertTrue(sourceDatas.get(pathList.get(0)).get(0).getTimestamp() >= 1707137815000L);
    Assert.assertTrue(sourceDatas.get(pathList.get(1)).get(0).getTimestamp() >= 1707137815000L);
    Assert.assertTrue(sourceDatas.get(pathList.get(2)).get(0).getTimestamp() >= 1707137815000L);

    pathList.clear();
    pathList.add(new PartialPath(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d5", "s1"));
    pathList.add(new PartialPath(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d5", "s2"));
    queryDataSource =
        dataRegion.query(
            pathList,
            COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d5",
            EnvironmentUtils.TEST_QUERY_CONTEXT,
            null,
            null);
    Assert.assertEquals(0, queryDataSource.getSeqResources().size());
    Assert.assertEquals(10, queryDataSource.getUnseqResources().size());

    // reset ttl to 1
    pathList.clear();
    pathList.add(new PartialPath(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d1", "s0"));
    pathList.add(new PartialPath(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d3", "s1"));
    pathList.add(new PartialPath(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d5", "s2"));
    pathList.add(new PartialPath(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d0", "s2"));

    DataNodeTTLCache.getInstance()
        .setTTL(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d1", 1L);
    DataNodeTTLCache.getInstance()
        .setTTL(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d3", 1);
    DataNodeTTLCache.getInstance()
        .setTTL(COMPACTION_TEST_SG + IoTDBConstant.PATH_SEPARATOR + "d5", 1);
    queryDataSource =
        dataRegion.query(pathList, null, EnvironmentUtils.TEST_QUERY_CONTEXT, null, null);
    Assert.assertEquals(10, queryDataSource.getSeqResources().size());
    Assert.assertEquals(10, queryDataSource.getUnseqResources().size());
    sourceDatas = readSourceFiles(createTimeseries(6, 10, false), Collections.emptyList());
    Assert.assertEquals(0, sourceDatas.get(pathList.get(0)).size());
    Assert.assertEquals(0, sourceDatas.get(pathList.get(1)).size());
    Assert.assertEquals(0, sourceDatas.get(pathList.get(2)).size());
    Assert.assertEquals(1000, sourceDatas.get(pathList.get(3)).size());
  }
}
