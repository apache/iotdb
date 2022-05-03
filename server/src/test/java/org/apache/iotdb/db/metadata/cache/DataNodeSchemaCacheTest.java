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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
  public void testGetSchemaEntity() throws IllegalPathException {
    PartialPath device1 = new PartialPath("root.sg1.d1");
    String[] measurements = new String[3];
    measurements[0] = "s1";
    measurements[1] = "s2";
    measurements[2] = "s3";
    TSDataType[] tsDataTypes = new TSDataType[3];
    tsDataTypes[0] = TSDataType.INT32;
    tsDataTypes[1] = TSDataType.FLOAT;
    tsDataTypes[2] = TSDataType.BOOLEAN;

    Map<PartialPath, SchemaCacheEntity> schemaCacheEntityMap1 =
        dataNodeSchemaCache.getSchemaEntityWithAutoCreate(
            device1, measurements, tsDataTypes, false);
    Assert.assertEquals(
        TSDataType.INT32,
        schemaCacheEntityMap1.get(new PartialPath("root.sg1.d1.s1")).getTsDataType());
    Assert.assertEquals(
        TSDataType.FLOAT,
        schemaCacheEntityMap1.get(new PartialPath("root.sg1.d1.s2")).getTsDataType());
    Assert.assertEquals(
        TSDataType.BOOLEAN,
        schemaCacheEntityMap1.get(new PartialPath("root.sg1.d1.s3")).getTsDataType());
    Assert.assertEquals(3, dataNodeSchemaCache.getSchemaEntityCache().estimatedSize());

    String[] otherMeasurements = new String[3];
    otherMeasurements[0] = "s3";
    otherMeasurements[1] = "s4";
    otherMeasurements[2] = "s5";
    TSDataType[] otherTsDataTypes = new TSDataType[3];
    otherTsDataTypes[0] = TSDataType.BOOLEAN;
    otherTsDataTypes[1] = TSDataType.TEXT;
    otherTsDataTypes[2] = TSDataType.INT64;

    Map<PartialPath, SchemaCacheEntity> schemaCacheEntityMap2 =
        dataNodeSchemaCache.getSchemaEntityWithAutoCreate(
            device1, otherMeasurements, otherTsDataTypes, false);
    Assert.assertEquals(
        TSDataType.BOOLEAN,
        schemaCacheEntityMap2.get(new PartialPath("root.sg1.d1.s3")).getTsDataType());
    Assert.assertEquals(
        TSDataType.TEXT,
        schemaCacheEntityMap2.get(new PartialPath("root.sg1.d1.s4")).getTsDataType());
    Assert.assertEquals(
        TSDataType.INT64,
        schemaCacheEntityMap2.get(new PartialPath("root.sg1.d1.s5")).getTsDataType());
    Assert.assertEquals(5, dataNodeSchemaCache.getSchemaEntityCache().estimatedSize());
  }
}
