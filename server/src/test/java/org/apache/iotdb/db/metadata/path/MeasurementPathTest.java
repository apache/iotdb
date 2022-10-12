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

package org.apache.iotdb.db.metadata.path;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.Assert;
import org.junit.Test;

public class MeasurementPathTest {

  @Test
  public void testTransformDataToString() throws IllegalPathException {
    MeasurementPath rawPath =
        new MeasurementPath(
            new PartialPath("root.sg.d.s"), new MeasurementSchema("s", TSDataType.INT32), true);
    rawPath.setMeasurementAlias("alias");
    String string = MeasurementPath.transformDataToString(rawPath);
    MeasurementPath newPath = MeasurementPath.parseDataFromString(string);
    Assert.assertEquals(rawPath.getFullPath(), newPath.getFullPath());
    Assert.assertEquals(rawPath.getMeasurementAlias(), newPath.getMeasurementAlias());
    Assert.assertEquals(rawPath.getMeasurementSchema(), newPath.getMeasurementSchema());
    Assert.assertEquals(rawPath.isUnderAlignedEntity(), newPath.isUnderAlignedEntity());
  }
}
