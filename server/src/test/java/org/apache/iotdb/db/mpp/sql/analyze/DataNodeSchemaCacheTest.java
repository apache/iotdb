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

package org.apache.iotdb.db.mpp.sql.analyze;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DataNodeSchemaCacheTest {
  DataNodeSchemaCache dataNodeSchemaCache;

  @Before
  public void setUp() throws Exception {
    dataNodeSchemaCache = DataNodeSchemaCache.getInstance();
  }

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testschemaNodeCache() throws IllegalPathException, InterruptedException {
    MeasurementSchema measurementSchema1 =
        new MeasurementSchema(
            "1", TSDataType.INT32, TSEncoding.PLAIN, CompressionType.SNAPPY, null);
    PartialPath path1 = new PartialPath("root.sg1.d1.temperature");
    MeasurementPath measurementPath1 = new MeasurementPath(path1, measurementSchema1);
    Assert.assertTrue(dataNodeSchemaCache.validate(measurementPath1));

    MeasurementSchema measurementSchema1_1 =
        new MeasurementSchema(
            "1", TSDataType.INT32, TSEncoding.BITMAP, CompressionType.SNAPPY, null);
    PartialPath path1_1 = new PartialPath("root.sg1.d1.temperature");
    MeasurementPath measurementPath1_1 = new MeasurementPath(path1_1, measurementSchema1_1);
    Assert.assertFalse(dataNodeSchemaCache.validate(measurementPath1_1));

    MeasurementSchema measurementSchema2 =
        new MeasurementSchema(
            "2", TSDataType.INT32, TSEncoding.PLAIN, CompressionType.SNAPPY, null);
    PartialPath path2 = new PartialPath("root.sg2.d2.cpu");
    MeasurementPath measurementPath2 = new MeasurementPath(path2, measurementSchema2);
    Assert.assertTrue(dataNodeSchemaCache.validate(measurementPath2));

    MeasurementSchema measurementSchema3 =
        new MeasurementSchema(
            "3", TSDataType.INT32, TSEncoding.PLAIN, CompressionType.SNAPPY, null);
    PartialPath path3 = new PartialPath("root.sg3.d2.c1.graph");
    MeasurementPath measurementPath3 = new MeasurementPath(path3, measurementSchema3);
    Assert.assertTrue(dataNodeSchemaCache.validate(measurementPath3));

    Assert.assertEquals(3, dataNodeSchemaCache.getSchemaNodeCache().estimatedSize());
    Assert.assertEquals(3, dataNodeSchemaCache.getRoot().getRoot().getChildren().size());
    Assert.assertTrue(dataNodeSchemaCache.getRoot().getRoot().getChildren().containsKey("sg3"));

    dataNodeSchemaCache.invalidate(measurementPath3);
    Thread.sleep(1000);

    Assert.assertEquals(2, dataNodeSchemaCache.getSchemaNodeCache().estimatedSize());
    Assert.assertEquals(2, dataNodeSchemaCache.getRoot().getRoot().getChildren().size());
    Assert.assertFalse(dataNodeSchemaCache.getRoot().getRoot().getChildren().containsKey("sg3"));
  }
}
