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

package org.apache.iotdb.db.storageengine.dataregion.compaction.alterDataType;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TreeDeviceSchemaCacheManager;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.DoubleDataPoint;
import org.apache.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AbstractCompactionAlterDataTypeTest extends AbstractCompactionTest {

  protected final String oldThreadName = Thread.currentThread().getName();
  protected final IDeviceID device =
      IDeviceID.Factory.DEFAULT_FACTORY.create(COMPACTION_TEST_SG + ".d1");

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
    Thread.currentThread().setName("pool-1-IoTDB-Compaction-Worker-1");
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    CompactionUtils.setSchemaFetcher(null);
    TreeDeviceSchemaCacheManager.getInstance().cleanUp();
    super.tearDown();
    Thread.currentThread().setName(oldThreadName);
  }

  protected TsFileResource generateInt32AlignedSeriesFile(TimeRange timeRange, boolean seq)
      throws IOException, WriteProcessException {
    List<IMeasurementSchema> measurementSchemas1 = new ArrayList<>();
    measurementSchemas1.add(new MeasurementSchema("s1", TSDataType.INT32));
    measurementSchemas1.add(new MeasurementSchema("s2", TSDataType.INT32));

    TsFileResource resource = createEmptyFileAndResource(seq);
    resource.setStatusForTest(TsFileResourceStatus.COMPACTING);
    try (TsFileWriter writer = new TsFileWriter(resource.getTsFile())) {
      writer.registerAlignedTimeseries(new Path(device), measurementSchemas1);
      for (long i = timeRange.getMin(); i <= timeRange.getMax(); i++) {
        TSRecord record = new TSRecord(device, i);
        record.addTuple(new IntDataPoint("s1", (int) i));
        record.addTuple(new IntDataPoint("s2", (int) i));
        writer.writeRecord(record);
      }
      writer.flush();
    }
    resource.updateStartTime(device, timeRange.getMin());
    resource.updateEndTime(device, timeRange.getMax());
    resource.serialize();
    return resource;
  }

  protected TsFileResource generateDoubleAlignedSeriesFile(TimeRange timeRange, boolean seq)
      throws IOException, WriteProcessException {

    List<IMeasurementSchema> measurementSchemas2 = new ArrayList<>();
    measurementSchemas2.add(new MeasurementSchema("s1", TSDataType.DOUBLE));
    measurementSchemas2.add(new MeasurementSchema("s2", TSDataType.DOUBLE));
    TsFileResource resource = createEmptyFileAndResource(seq);
    resource.setStatusForTest(TsFileResourceStatus.COMPACTING);
    try (TsFileWriter writer = new TsFileWriter(resource.getTsFile())) {
      writer.registerAlignedTimeseries(new Path(device), measurementSchemas2);
      for (long i = timeRange.getMin(); i <= timeRange.getMax(); i++) {
        TSRecord record = new TSRecord(device, i);
        record.addTuple(new DoubleDataPoint("s1", (double) i));
        record.addTuple(new DoubleDataPoint("s2", (double) i));
        writer.writeRecord(record);
      }
      writer.flush();
    }
    resource.updateStartTime(device, timeRange.getMin());
    resource.updateEndTime(device, timeRange.getMax());
    resource.serialize();
    return resource;
  }

  protected TsFileResource generateInt32NonAlignedSeriesFile(TimeRange timeRange, boolean seq)
      throws IOException, WriteProcessException {
    MeasurementSchema measurementSchema = new MeasurementSchema("s1", TSDataType.INT32);
    TsFileResource resource = createEmptyFileAndResource(seq);
    resource.setStatusForTest(TsFileResourceStatus.COMPACTING);
    try (TsFileWriter writer = new TsFileWriter(resource.getTsFile())) {
      writer.registerTimeseries(new Path(device), measurementSchema);
      for (long i = timeRange.getMin(); i <= timeRange.getMax(); i++) {
        TSRecord record = new TSRecord(device, i);
        record.addTuple(new IntDataPoint("s1", (int) i));
        writer.writeRecord(record);
      }
      writer.flush();
    }
    resource.updateStartTime(device, timeRange.getMin());
    resource.updateEndTime(device, timeRange.getMax());
    resource.serialize();
    return resource;
  }

  protected TsFileResource generateFloatNonAlignedSeriesFile(TimeRange timeRange, boolean seq)
      throws IOException, WriteProcessException {
    MeasurementSchema measurementSchema = new MeasurementSchema("s1", TSDataType.FLOAT);
    TsFileResource resource = createEmptyFileAndResource(seq);
    resource.setStatusForTest(TsFileResourceStatus.COMPACTING);
    try (TsFileWriter writer = new TsFileWriter(resource.getTsFile())) {
      writer.registerTimeseries(new Path(device), measurementSchema);
      for (long i = timeRange.getMin(); i <= timeRange.getMax(); i++) {
        TSRecord record = new TSRecord(device, i);
        record.addTuple(new FloatDataPoint("s1", (float) i));
        writer.writeRecord(record);
      }
      writer.flush();
    }
    resource.updateStartTime(device, timeRange.getMin());
    resource.updateEndTime(device, timeRange.getMax());
    resource.serialize();
    return resource;
  }

  protected TsFileResource generateDoubleNonAlignedSeriesFile(TimeRange timeRange, boolean seq)
      throws IOException, WriteProcessException {
    MeasurementSchema measurementSchema2 = new MeasurementSchema("s1", TSDataType.DOUBLE);
    TsFileResource resource = createEmptyFileAndResource(seq);
    resource.setStatusForTest(TsFileResourceStatus.COMPACTING);
    try (TsFileWriter writer = new TsFileWriter(resource.getTsFile())) {
      writer.registerTimeseries(new Path(device), measurementSchema2);
      for (long i = timeRange.getMin(); i <= timeRange.getMax(); i++) {
        TSRecord record = new TSRecord(device, i);
        record.addTuple(new DoubleDataPoint("s1", (double) i));
        writer.writeRecord(record);
      }
      writer.flush();
    }
    resource.updateStartTime(device, timeRange.getMin());
    resource.updateEndTime(device, timeRange.getMax());
    resource.serialize();
    return resource;
  }
}
