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
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;

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

  @Test
  public void testMeasurementPathSerde() throws IllegalPathException, IOException {
    IMeasurementSchema schema = new MeasurementSchema("s1", TSDataType.TEXT);
    MeasurementPath expectedPath =
        new MeasurementPath(new PartialPath("root.sg.d1.s1"), schema, true);
    expectedPath.setMeasurementAlias("alias_s1");
    MeasurementPath actualPath = serdeWithByteBuffer(expectedPath);
    assertMeasurementPathEquals(actualPath, expectedPath);
    actualPath = serdeWithStream(expectedPath);
    assertMeasurementPathEquals(actualPath, expectedPath);

    HashMap<String, String> tagMap = new HashMap<>();
    tagMap.put("k1", "v1");
    expectedPath.setTagMap(tagMap);
    actualPath = serdeWithByteBuffer(expectedPath);
    assertMeasurementPathEquals(actualPath, expectedPath);
    actualPath = serdeWithStream(expectedPath);
    assertMeasurementPathEquals(actualPath, expectedPath);
  }

  @Test
  public void testCloneAndCopy() throws IllegalPathException {
    IMeasurementSchema schema = new MeasurementSchema("s1", TSDataType.TEXT);
    MeasurementPath expectedPath =
        new MeasurementPath(new PartialPath("root.sg.d1.s1"), schema, true);
    expectedPath.setMeasurementAlias("alias_s1");

    MeasurementPath actualPath = expectedPath.clone();
    assertMeasurementPathEquals(actualPath, expectedPath);
    actualPath = (MeasurementPath) expectedPath.copy();
    assertMeasurementPathEquals(actualPath, expectedPath);

    HashMap<String, String> tagMap = new HashMap<>();
    tagMap.put("k1", "v1");
    expectedPath.setTagMap(tagMap);
    actualPath = expectedPath.clone();
    assertMeasurementPathEquals(actualPath, expectedPath);
    actualPath = (MeasurementPath) expectedPath.copy();
    assertMeasurementPathEquals(actualPath, expectedPath);
  }

  private MeasurementPath serdeWithByteBuffer(MeasurementPath origin) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    origin.serialize(byteBuffer);
    byteBuffer.flip();
    Assert.assertEquals((byte) 0, ReadWriteIOUtils.readByte(byteBuffer));
    return MeasurementPath.deserialize(byteBuffer);
  }

  private MeasurementPath serdeWithStream(MeasurementPath origin) throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    origin.serialize(dataOutputStream);
    ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
    Assert.assertEquals((byte) 0, ReadWriteIOUtils.readByte(byteBuffer));
    return MeasurementPath.deserialize(byteBuffer);
  }

  private void assertMeasurementPathEquals(MeasurementPath actual, MeasurementPath expected) {
    Assert.assertEquals(expected, actual);
    Assert.assertEquals(expected.isUnderAlignedEntity(), actual.isUnderAlignedEntity());
    Assert.assertEquals(expected.getTagMap(), actual.getTagMap());
    Assert.assertEquals(expected.getMeasurementSchema(), actual.getMeasurementSchema());
    Assert.assertEquals(expected.getMeasurementAlias(), actual.getMeasurementAlias());
  }
}
