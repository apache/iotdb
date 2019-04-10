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
package org.apache.iotdb.cluster.qp.executor;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.utils.EnvironmentUtils;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.QueryProcessor;
import org.apache.iotdb.db.qp.executor.OverflowQPExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.sys.MetadataPlan;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class NonQueryExecutorTest {

  private CompressionType compressionType = CompressionType.valueOf(TSFileConfig.compressor);

  private MManager mManager = MManager.getInstance();

  private QueryProcessor processor = new QueryProcessor(new OverflowQPExecutor());

  private NonQueryExecutor executor = new NonQueryExecutor();

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    mManager.setStorageLevelToMTree("root.vehicle");
    mManager.setStorageLevelToMTree("root.vehicle1");
    mManager.addPathToMTree("root.vehicle.device1.sensor1", TSDataType.valueOf("INT32"),
        TSEncoding.valueOf("RLE"), compressionType, Collections
            .emptyMap());
    mManager.addPathToMTree("root.vehicle.device1.sensor2", TSDataType.valueOf("INT32"),
        TSEncoding.valueOf("RLE"), compressionType, Collections
            .emptyMap());
    mManager.addPathToMTree("root.vehicle.device1.sensor3", TSDataType.valueOf("INT32"),
        TSEncoding.valueOf("RLE"), compressionType, Collections
            .emptyMap());
    mManager.addPathToMTree("root.vehicle.device2.sensor1", TSDataType.valueOf("INT32"),
        TSEncoding.valueOf("RLE"), compressionType, Collections
            .emptyMap());
    mManager.addPathToMTree("root.vehicle.device2.sensor2", TSDataType.valueOf("INT32"),
        TSEncoding.valueOf("RLE"), compressionType, Collections
            .emptyMap());
    mManager.addPathToMTree("root.vehicle.device2.sensor3", TSDataType.valueOf("INT32"),
        TSEncoding.valueOf("RLE"), compressionType, Collections
            .emptyMap());
    mManager.addPathToMTree("root.vehicle1.device1.sensor1", TSDataType.valueOf("INT32"),
        TSEncoding.valueOf("RLE"), compressionType, Collections
            .emptyMap());
    mManager.addPathToMTree("root.vehicle1.device1.sensor2", TSDataType.valueOf("INT32"),
        TSEncoding.valueOf("RLE"), compressionType, Collections
            .emptyMap());
    mManager.addPathToMTree("root.vehicle1.device1.sensor3", TSDataType.valueOf("INT32"),
        TSEncoding.valueOf("RLE"), compressionType, Collections
            .emptyMap());
    mManager.addPathToMTree("root.vehicle1.device2.sensor1", TSDataType.valueOf("INT32"),
        TSEncoding.valueOf("RLE"), compressionType, Collections
            .emptyMap());
    mManager.addPathToMTree("root.vehicle1.device2.sensor2", TSDataType.valueOf("INT32"),
        TSEncoding.valueOf("RLE"), compressionType, Collections
            .emptyMap());
    mManager.addPathToMTree("root.vehicle1.device2.sensor3", TSDataType.valueOf("INT32"),
        TSEncoding.valueOf("RLE"), compressionType, Collections
            .emptyMap());
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void getStorageGroupFromDeletePlan() throws Exception{

    String deleteStatement = "DELETE FROM root.vehicle.device.sensor,root.device0.sensor1 WHERE time <= 5000";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(deleteStatement);
    try {
      executor.getStorageGroupFromDeletePlan((DeletePlan) plan);
    } catch (Exception e){
      assertEquals("org.apache.iotdb.db.exception.PathErrorException: The prefix of the seriesPath root.device0.sensor1 is not one storage group seriesPath", e.getMessage());
    }

    deleteStatement = "DELETE FROM root.vehicle.device.sensor,root.vehicle1.device0.sensor1 WHERE time <= 5000";
    plan = processor.parseSQLToPhysicalPlan(deleteStatement);
    try {
      executor.getStorageGroupFromDeletePlan((DeletePlan) plan);
    } catch (Exception e){
      assertEquals("Delete function in distributed iotdb only supports single storage group", e.getMessage());
    }

    deleteStatement = "DELETE FROM root.vehicle.device1.sensor1, root.vehicle.device2.sensor2 WHERE time <= 5000";
    plan = processor.parseSQLToPhysicalPlan(deleteStatement);
    String storageGroup = executor.getStorageGroupFromDeletePlan((DeletePlan) plan);
    assertEquals("root.vehicle", storageGroup);


    deleteStatement = "DELETE FROM root WHERE time <= 5000";
    plan = processor.parseSQLToPhysicalPlan(deleteStatement);
    try {
      executor.getStorageGroupFromDeletePlan((DeletePlan) plan);
    } catch (Exception e){
      assertEquals("Delete function in distributed iotdb only supports single storage group", e.getMessage());
    }

    deleteStatement = "DELETE FROM root.vehicle1.device0.sensor1, root.vehicle1.vehicle3.sensor2 WHERE time <= 5000";
    plan = processor.parseSQLToPhysicalPlan(deleteStatement);
    storageGroup = executor.getStorageGroupFromDeletePlan((DeletePlan) plan);
    assertEquals("root.vehicle1", storageGroup);

  }

  @Test
  public void getGroupIdFromMetadataPlan() throws Exception{

    String setSG = "set storage group to root.vehicle2";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(setSG);
    String groupId = executor.getGroupIdFromMetadataPlan((MetadataPlan) plan);
    assertEquals(groupId, ClusterConfig.METADATA_GROUP_ID);

  }
}