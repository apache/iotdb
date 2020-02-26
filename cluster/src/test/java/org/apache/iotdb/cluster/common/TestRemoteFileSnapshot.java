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

package org.apache.iotdb.cluster.common;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.cluster.RemoteTsFileResource;
import org.apache.iotdb.cluster.log.snapshot.FileSnapshot;
import org.apache.iotdb.cluster.log.snapshot.RemoteSnapshot;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRemoteFileSnapshot extends FileSnapshot implements RemoteSnapshot {

  private static final Logger logger = LoggerFactory.getLogger(TestRemoteFileSnapshot.class);

  private List<RemoteTsFileResource> remoteDataFiles;
  private Set<MeasurementSchema> remoteMeasurementSchemas;

  public TestRemoteFileSnapshot(List<RemoteTsFileResource> dataFiles,
      Set<MeasurementSchema> measurementSchemas) {
    remoteDataFiles = dataFiles;
    remoteMeasurementSchemas = measurementSchemas;
  }

  @Override
  public void getRemoteSnapshot() {
    for (MeasurementSchema remoteMeasurementSchema : remoteMeasurementSchemas) {
      SchemaUtils.registerTimeseries(remoteMeasurementSchema);
    }
    int startTime = 12345;
    for (RemoteTsFileResource tsFileResource : remoteDataFiles) {
      // fake a file for each resource
      String[] splits = tsFileResource.getFile().getPath().split("\\\\");
      String storageGroup = splits[splits.length - 2];
      InsertPlan insertPlan = new InsertPlan();
      insertPlan.setDeviceId(storageGroup);
      insertPlan.setTime(startTime++);
      insertPlan.setMeasurements(new String[] {TestUtils.getTestMeasurement(0)});
      insertPlan.setDataTypes(new TSDataType[] {TSDataType.DOUBLE});
      insertPlan.setValues(new String[] {"0.0"});
      try {
        StorageEngine.getInstance().getProcessor(storageGroup).insert(insertPlan);
        StorageEngine.getInstance().asyncCloseProcessor(storageGroup, true);
      } catch (QueryProcessException | StorageEngineException | StorageGroupNotSetException e) {
        logger.error("Cannot fake files in TestRemoteFileSnapshot", e);
      }
    }
  }

  @Override
  public ByteBuffer serialize() {
    return null;
  }

  @Override
  public void deserialize(ByteBuffer buffer) {

  }
}
