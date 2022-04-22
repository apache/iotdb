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

package org.apache.iotdb.db.metadata.cache;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DataNodeSchemaEntryCacheTest {
  DataNodeSchemaEntryCache dataNodeSchemaEntryCache;

  @Before
  public void setUp() throws Exception {
    dataNodeSchemaEntryCache = DataNodeSchemaEntryCache.getInstance();
  }

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testschemaNodeCache() throws IllegalPathException, InterruptedException {
    MeasurementSchema measurementSchema1 =
        new MeasurementSchema(
            "temperature", TSDataType.INT32, TSEncoding.PLAIN, CompressionType.SNAPPY, null);
    PartialPath path1 = new PartialPath("root.sg1.d1.temperature");
    MeasurementPath measurementPath1 = new MeasurementPath(path1, measurementSchema1);

    SchemaEntry schemaEntry =
        dataNodeSchemaEntryCache.getSingleSchemaEntryWithAutoCreate(measurementPath1);
    Assert.assertEquals(TSDataType.INT32, schemaEntry.getTsDataType());
    Assert.assertEquals(TSEncoding.PLAIN, schemaEntry.getTsEncoding());
    Assert.assertEquals(CompressionType.SNAPPY, schemaEntry.getCompressionType());

    PartialPath path1_1 = new PartialPath("root.sg1.d1.temperature");
    SchemaEntry schemaEntry1 = dataNodeSchemaEntryCache.getSingleSchemaEntryWithAutoCreate(path1_1);
    Assert.assertEquals(schemaEntry, schemaEntry1);
    Assert.assertEquals(1, dataNodeSchemaEntryCache.getSchemaEntryCache().estimatedSize());

    PartialPath path1_2 = new PartialPath("root.sg1.d1.temperature");
    SchemaEntry schemaEntry2 = dataNodeSchemaEntryCache.getSingleSchemaEntry(path1_2);
    Assert.assertEquals(schemaEntry, schemaEntry2);
    Assert.assertEquals(1, dataNodeSchemaEntryCache.getSchemaEntryCache().estimatedSize());

    List<String> measurementList = new ArrayList<>();
    List<IMeasurementSchema> schemaList = new ArrayList<>();

    measurementList.add("s1");
    schemaList.add(
        new MeasurementSchema(
            "s1", TSDataType.INT32, TSEncoding.PLAIN, CompressionType.SNAPPY, null));

    measurementList.add("s2");
    schemaList.add(
        new MeasurementSchema(
            "s2", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.SNAPPY, null));

    measurementList.add("s3");
    schemaList.add(
        new MeasurementSchema(
            "s3", TSDataType.FLOAT, TSEncoding.PLAIN, CompressionType.SNAPPY, null));

    AlignedPath alignedPath = new AlignedPath("root.sg1.d2", measurementList, schemaList);
    Map<PartialPath, SchemaEntry> schemaEntryMap =
        dataNodeSchemaEntryCache.getBatchSchemaEntryWithAutoCreate(alignedPath);

    Assert.assertEquals(3, schemaEntryMap.size());
    for (int i = 0; i < 3; i++) {
      Assert.assertEquals(3, schemaEntryMap.get(i));
    }
    Assert.assertEquals(3, schemaEntryMap.size());
  }
}
