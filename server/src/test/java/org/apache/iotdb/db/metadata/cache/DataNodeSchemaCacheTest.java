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

public class DataNodeSchemaCacheTest {
  DataNodeSchemaCache dataNodeSchemaCache;

  @Before
  public void setUp() throws Exception {
    dataNodeSchemaCache = DataNodeSchemaCache.getInstance();
  }

  @After
  public void tearDown() throws Exception {
    dataNodeSchemaCache.cleanUp();
  }

  @Test
  public void testGetSingleSchemaEntity() throws IllegalPathException {
    MeasurementSchema measurementSchema1 =
        new MeasurementSchema(
            "temperature", TSDataType.INT32);
    PartialPath path1 = new PartialPath("root.sg1.d1.temperature");
    MeasurementPath measurementPath1 = new MeasurementPath(path1, measurementSchema1);

    SchemaCacheEntity schemaCacheEntity =
        dataNodeSchemaCache.getSingleSchemaEntity(measurementPath1, true);
    Assert.assertEquals(TSDataType.INT32, schemaCacheEntity.getTsDataType());
    Assert.assertEquals(TSEncoding.PLAIN, schemaCacheEntity.getTsEncoding());
    Assert.assertEquals(CompressionType.SNAPPY, schemaCacheEntity.getCompressionType());

    PartialPath path1_1 = new PartialPath("root.sg1.d1.temperature");
    SchemaCacheEntity schemaCacheEntity1 = dataNodeSchemaCache.getSingleSchemaEntity(path1_1, true);
    Assert.assertEquals(schemaCacheEntity, schemaCacheEntity1);
    Assert.assertEquals(1, dataNodeSchemaCache.getSchemaEntityCache().estimatedSize());

    PartialPath path1_2 = new PartialPath("root.sg1.d1.cpu");
    SchemaCacheEntity schemaCacheEntity2 =
        dataNodeSchemaCache.getSingleSchemaEntity(path1_2, false);
    Assert.assertNull(schemaCacheEntity2);
    Assert.assertEquals(1, dataNodeSchemaCache.getSchemaEntityCache().estimatedSize());

    SchemaCacheEntity schemaCacheEntity3 = dataNodeSchemaCache.getSingleSchemaEntity(path1_2, true);
    Assert.assertNull(schemaCacheEntity3);
    Assert.assertEquals(1, dataNodeSchemaCache.getSchemaEntityCache().estimatedSize());

    MeasurementSchema measurementSchema4 =
        new MeasurementSchema(
            "cpu", TSDataType.INT64);
    PartialPath path4 = new PartialPath("root.sg1.d1.cpu");
    MeasurementPath measurementPath4 = new MeasurementPath(path4, measurementSchema4);
    SchemaCacheEntity schemaCacheEntity4 =
        dataNodeSchemaCache.getSingleSchemaEntity(measurementPath4, true);
    Assert.assertEquals(TSDataType.INT64, schemaCacheEntity4.getTsDataType());
    Assert.assertEquals(TSEncoding.PLAIN, schemaCacheEntity4.getTsEncoding());
    Assert.assertEquals(CompressionType.SNAPPY, schemaCacheEntity4.getCompressionType());
    Assert.assertEquals(2, dataNodeSchemaCache.getSchemaEntityCache().estimatedSize());
  }

  @Test
  public void testGetAlignedSchemaEntity() throws IllegalPathException {
    List<String> measurementList = new ArrayList<>();
    List<IMeasurementSchema> schemaList = new ArrayList<>();

    measurementList.add("s1");
    schemaList.add(
        new MeasurementSchema(
            "s1", TSDataType.INT32));

    measurementList.add("s2");
    schemaList.add(
        new MeasurementSchema(
            "s2", TSDataType.INT64));

    measurementList.add("s3");
    schemaList.add(
        new MeasurementSchema(
            "s3", TSDataType.FLOAT));

    AlignedPath alignedPath1 = new AlignedPath("root.sg1.d2", measurementList, schemaList);
    Map<PartialPath, SchemaCacheEntity> schemaEntityMap1 =
        dataNodeSchemaCache.getAlignedSchemaEntity(alignedPath1, true);
    Assert.assertEquals(3, schemaEntityMap1.size());

    measurementList.add("s4");
    schemaList.add(
        new MeasurementSchema(
            "s4", TSDataType.BOOLEAN));

    measurementList.add("s5");
    schemaList.add(
        new MeasurementSchema(
            "s5", TSDataType.TEXT));
    AlignedPath alignedPath2 = new AlignedPath("root.sg1.d2", measurementList, schemaList);
    Map<PartialPath, SchemaCacheEntity> schemaEntityMap2 =
        dataNodeSchemaCache.getAlignedSchemaEntity(alignedPath2, false);
    Assert.assertEquals(3, schemaEntityMap2.size());

    Map<PartialPath, SchemaCacheEntity> schemaEntityMap3 =
        dataNodeSchemaCache.getAlignedSchemaEntity(alignedPath2, true);
    Assert.assertEquals(5, schemaEntityMap3.size());
  }
}
