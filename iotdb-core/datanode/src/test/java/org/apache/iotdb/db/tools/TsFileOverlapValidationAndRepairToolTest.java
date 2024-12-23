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

package org.apache.iotdb.db.tools;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.tools.validate.TsFileOverlapValidationAndRepairTool;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class TsFileOverlapValidationAndRepairToolTest extends AbstractCompactionTest {
  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
  }

  @Test
  public void test1() throws IOException {
    String device1 = "d1", device2 = "d2";
    TsFileResource resource1 = createTsFileAndResource();
    resource1.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device1), 10);
    resource1.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device1), 30);
    resource1.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device2), 20);
    resource1.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device2), 40);
    resource1.serialize();
    resource1.close();

    TsFileResource resource2 = createTsFileAndResource();
    resource2.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device1), 29);
    resource2.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device1), 30);
    resource2.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device2), 50);
    resource2.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device2), 60);
    resource2.serialize();
    resource2.close();

    TsFileResource resource3 = createTsFileAndResource();
    resource3.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device1), 40);
    resource3.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device1), 50);
    resource3.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device2), 70);
    resource3.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device2), 80);
    resource3.serialize();
    resource3.close();

    int overlapFileNum =
        TsFileOverlapValidationAndRepairTool.checkTimePartitionHasOverlap(
            Arrays.asList(resource1, resource2, resource3));

    Assert.assertEquals(1, overlapFileNum);
  }

  @Test
  public void test2() throws IOException {
    String device1 = "d1", device2 = "d2";
    TsFileResource resource1 = createTsFileAndResource();
    resource1.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device1), 10);
    resource1.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device1), 30);
    resource1.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device2), 20);
    resource1.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device2), 40);
    resource1.serialize();
    resource1.close();

    TsFileResource resource2 = createTsFileAndResource();
    resource2.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device1), 29);
    resource2.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device1), 30);
    resource2.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device2), 50);
    resource2.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device2), 60);
    resource2.serialize();
    resource2.close();

    TsFileResource resource3 = createTsFileAndResource();
    resource3.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device1), 30);
    resource3.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device1), 50);
    resource3.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device2), 70);
    resource3.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device2), 80);
    resource3.serialize();
    resource3.close();

    int overlapFileNum =
        TsFileOverlapValidationAndRepairTool.checkTimePartitionHasOverlap(
            Arrays.asList(resource1, resource2, resource3));

    Assert.assertEquals(2, overlapFileNum);
  }

  @Test
  public void test3() throws IOException {
    String device1 = "d1", device2 = "d2";
    TsFileResource resource1 = createTsFileAndResource();
    resource1.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device1), 10);
    resource1.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device1), 30);
    resource1.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device2), 20);
    resource1.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device2), 40);
    resource1.serialize();
    resource1.close();

    TsFileResource resource2 = createTsFileAndResource();
    resource2.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device1), 10);
    resource2.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device1), 20);
    resource2.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device2), 50);
    resource2.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device2), 60);
    resource2.serialize();
    resource2.close();

    TsFileResource resource3 = createTsFileAndResource();
    resource3.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device1), 30);
    resource3.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device1), 50);
    resource3.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device2), 70);
    resource3.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device2), 80);
    resource3.serialize();
    resource3.close();

    int overlapFileNum =
        TsFileOverlapValidationAndRepairTool.checkTimePartitionHasOverlap(
            Arrays.asList(resource1, resource2, resource3));

    Assert.assertEquals(2, overlapFileNum);
  }

  @Test
  public void test4() throws IOException {
    String device1 = "d1", device2 = "d2";
    TsFileResource resource1 = createTsFileAndResource();
    resource1.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device1), 10);
    resource1.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device1), 30);
    resource1.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device2), 20);
    resource1.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device2), 40);
    resource1.serialize();
    resource1.close();

    TsFileResource resource2 = createTsFileAndResource();
    resource2.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device1), 10);
    resource2.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device1), 50);
    resource2.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device2), 50);
    resource2.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device2), 60);
    resource2.serialize();
    resource2.close();

    TsFileResource resource3 = createTsFileAndResource();
    resource3.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device1), 40);
    resource3.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device1), 50);
    resource3.updateStartTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device2), 70);
    resource3.updateEndTime(IDeviceID.Factory.DEFAULT_FACTORY.create(device2), 80);
    resource3.serialize();
    resource3.close();

    int overlapFileNum =
        TsFileOverlapValidationAndRepairTool.checkTimePartitionHasOverlap(
            Arrays.asList(resource1, resource2, resource3));

    Assert.assertEquals(1, overlapFileNum);
  }

  private TsFileResource createTsFileAndResource() throws IOException {
    TsFileResource resource = createEmptyFileAndResource(true);
    TsFileIOWriter writer = new TsFileIOWriter(resource.getTsFile());
    writer.endFile();
    writer.close();
    return resource;
  }
}
