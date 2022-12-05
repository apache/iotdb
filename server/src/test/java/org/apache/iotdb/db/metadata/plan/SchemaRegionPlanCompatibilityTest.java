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

package org.apache.iotdb.db.metadata.plan;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.plan.schemaregion.SchemaRegionPlanType;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.SchemaRegionPlanDeserializer;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IActivateTemplateInClusterPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IAutoCreateDeviceMNodePlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IChangeAliasPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IChangeTagOffsetPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.ICreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.ICreateTimeSeriesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IDeleteTimeSeriesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IPreDeleteTimeSeriesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.IRollbackPreDeleteTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.ActivateTemplateInClusterPlan;
import org.apache.iotdb.db.qp.physical.sys.AutoCreateDeviceMNodePlan;
import org.apache.iotdb.db.qp.physical.sys.ChangeAliasPlan;
import org.apache.iotdb.db.qp.physical.sys.ChangeTagOffsetPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.PreDeleteTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.RollbackPreDeleteTimeSeriesPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class SchemaRegionPlanCompatibilityTest {

  private static final SchemaRegionPlanDeserializer DESERIALIZER =
      new SchemaRegionPlanDeserializer();
  private static final ByteBuffer BUFFER = ByteBuffer.allocate(1024);

  private static int serializeToBuffer(PhysicalPlan oldPlan) {
    BUFFER.clear();
    oldPlan.serialize(BUFFER);
    int position = BUFFER.position();
    BUFFER.flip();
    return position;
  }

  private static <T> T deserializeFromBuffer() {
    return (T) DESERIALIZER.deserialize(BUFFER);
  }

  private static int getCurrentBufferPosition() {
    return BUFFER.position();
  }

  @Test
  public void testPlanTypeCompatibility() {

    String[] typeNames =
        new String[] {
          "CREATE_TIMESERIES",
          "DELETE_TIMESERIES",
          "CHANGE_TAG_OFFSET",
          "CHANGE_ALIAS",
          "AUTO_CREATE_DEVICE_MNODE",
          "CREATE_ALIGNED_TIMESERIES",
          "ACTIVATE_TEMPLATE_IN_CLUSTER",
          "PRE_DELETE_TIMESERIES_IN_CLUSTER",
          "ROLLBACK_PRE_DELETE_TIMESERIES"
        };
    for (String typeName : typeNames) {
      Assert.assertEquals(
          SchemaRegionPlanType.valueOf(typeName).getPlanType(),
          PhysicalPlan.PhysicalPlanType.valueOf(typeName).ordinal());
    }
  }

  @Test
  public void testActivateTemplateInClusterPlanSerializationCompatibility()
      throws IllegalPathException {
    ActivateTemplateInClusterPlan oldPlan = new ActivateTemplateInClusterPlan();
    oldPlan.setActivatePath(new PartialPath("root.sg.d"));
    oldPlan.setTemplateSetLevel(1);
    oldPlan.setAligned(true);
    oldPlan.setTemplateId(1);

    int position = serializeToBuffer(oldPlan);

    IActivateTemplateInClusterPlan newPlan = deserializeFromBuffer();

    Assert.assertEquals(oldPlan.getActivatePath(), newPlan.getActivatePath());
    Assert.assertEquals(oldPlan.getTemplateSetLevel(), newPlan.getTemplateSetLevel());
    Assert.assertEquals(oldPlan.getTemplateId(), newPlan.getTemplateId());
    Assert.assertEquals(oldPlan.isAligned(), newPlan.isAligned());

    Assert.assertEquals(position, getCurrentBufferPosition());
  }

  @Test
  public void testAutoCreateDeviceMNodePlanSerializationCompatibility()
      throws IllegalPathException {
    AutoCreateDeviceMNodePlan oldPlan = new AutoCreateDeviceMNodePlan();
    oldPlan.setPath(new PartialPath("root.sg.d"));

    int position = serializeToBuffer(oldPlan);

    IAutoCreateDeviceMNodePlan newPlan = deserializeFromBuffer();

    Assert.assertEquals(oldPlan.getPath(), newPlan.getPath());

    Assert.assertEquals(position, getCurrentBufferPosition());
  }

  @Test
  public void testChangeAliasPlanSerializationCompatibility() throws IllegalPathException {
    ChangeAliasPlan oldPlan = new ChangeAliasPlan();
    oldPlan.setPath(new PartialPath("root.sg.d.s"));
    oldPlan.setAlias("alias");

    int position = serializeToBuffer(oldPlan);

    IChangeAliasPlan newPlan = deserializeFromBuffer();

    Assert.assertEquals(oldPlan.getPath(), newPlan.getPath());
    Assert.assertEquals(oldPlan.getAlias(), newPlan.getAlias());

    Assert.assertEquals(position, getCurrentBufferPosition());
  }

  @Test
  public void testChangeTagOffsetPlanSerializationCompatibility() throws IllegalPathException {
    ChangeTagOffsetPlan oldPlan = new ChangeTagOffsetPlan();
    oldPlan.setPath(new PartialPath("root.sg.d.s"));
    oldPlan.setOffset(10);

    int position = serializeToBuffer(oldPlan);

    IChangeTagOffsetPlan newPlan = deserializeFromBuffer();

    Assert.assertEquals(oldPlan.getPath(), newPlan.getPath());
    Assert.assertEquals(oldPlan.getOffset(), newPlan.getOffset());

    Assert.assertEquals(position, getCurrentBufferPosition());
  }

  @Test
  public void testCreateAlignedTimeSeriesPlanSerializationCompatibility()
      throws IllegalPathException {
    CreateAlignedTimeSeriesPlan oldPlan = new CreateAlignedTimeSeriesPlan();
    oldPlan.setDevicePath(new PartialPath("root.sg.d"));
    oldPlan.setMeasurements(Arrays.asList("s1", "s2"));
    oldPlan.setDataTypes(Arrays.asList(TSDataType.INT32, TSDataType.FLOAT));
    oldPlan.setEncodings(Arrays.asList(TSEncoding.GORILLA, TSEncoding.BITMAP));
    oldPlan.setCompressors(Arrays.asList(CompressionType.SNAPPY, CompressionType.GZIP));
    oldPlan.setAliasList(Arrays.asList("status", "temperature"));
    Map<String, String> tagMap = new HashMap<>();
    tagMap.put("tag-key", "tag-value");
    oldPlan.setTagsList(Arrays.asList(null, tagMap));
    Map<String, String> attributeMap = new HashMap<>();
    attributeMap.put("attribute-key", "attribute-value");
    oldPlan.setAttributesList(Arrays.asList(attributeMap, null));
    oldPlan.setTagOffsets(Arrays.asList(10L, 20L));

    int position = serializeToBuffer(oldPlan);

    ICreateAlignedTimeSeriesPlan newPlan = deserializeFromBuffer();

    Assert.assertEquals(oldPlan.getDevicePath(), newPlan.getDevicePath());
    Assert.assertEquals(oldPlan.getMeasurements(), newPlan.getMeasurements());
    Assert.assertEquals(oldPlan.getDataTypes(), newPlan.getDataTypes());
    Assert.assertEquals(oldPlan.getEncodings(), newPlan.getEncodings());
    Assert.assertEquals(oldPlan.getCompressors(), newPlan.getCompressors());
    Assert.assertEquals(oldPlan.getAliasList(), newPlan.getAliasList());
    Assert.assertEquals(oldPlan.getTagsList(), newPlan.getTagsList());
    Assert.assertEquals(oldPlan.getAttributesList(), newPlan.getAttributesList());
    Assert.assertEquals(oldPlan.getTagOffsets(), newPlan.getTagOffsets());

    Assert.assertEquals(position, getCurrentBufferPosition());
  }

  @Test
  public void testCreateTimeSeriesPlanSerializationCompatibility() throws IllegalPathException {
    CreateTimeSeriesPlan oldPlan = new CreateTimeSeriesPlan();
    oldPlan.setPath(new PartialPath("root.sg.d.s"));
    oldPlan.setDataType(TSDataType.DOUBLE);
    oldPlan.setEncoding(TSEncoding.FREQ);
    oldPlan.setCompressor(CompressionType.UNCOMPRESSED);
    oldPlan.setAlias(null);
    Map<String, String> tagMap = new HashMap<>();
    tagMap.put("tag-key", "tag-value");
    oldPlan.setTags(tagMap);
    Map<String, String> attributeMap = new HashMap<>();
    attributeMap.put("attribute-key", "attribute-value");
    oldPlan.setAttributes(attributeMap);
    oldPlan.setTagOffset(30L);

    int position = serializeToBuffer(oldPlan);

    ICreateTimeSeriesPlan newPlan = deserializeFromBuffer();

    Assert.assertEquals(oldPlan.getPath(), newPlan.getPath());
    Assert.assertEquals(oldPlan.getDataType(), newPlan.getDataType());
    Assert.assertEquals(oldPlan.getEncoding(), newPlan.getEncoding());
    Assert.assertEquals(oldPlan.getCompressor(), newPlan.getCompressor());
    Assert.assertEquals(oldPlan.getAlias(), newPlan.getAlias());
    Assert.assertEquals(oldPlan.getTags(), newPlan.getTags());
    Assert.assertEquals(oldPlan.getAttributes(), newPlan.getAttributes());
    Assert.assertEquals(oldPlan.getTagOffset(), newPlan.getTagOffset());

    Assert.assertEquals(position, getCurrentBufferPosition());
  }

  @Test
  public void testDeleteTimeSeriesPlanSerializationCompatibility() throws IllegalPathException {
    DeleteTimeSeriesPlan oldPlan = new DeleteTimeSeriesPlan();
    oldPlan.setDeletePathList(
        Arrays.asList(new PartialPath("root.sg.*.s"), new PartialPath("root.**.d.s")));

    int position = serializeToBuffer(oldPlan);

    IDeleteTimeSeriesPlan newPlan = deserializeFromBuffer();

    Assert.assertEquals(oldPlan.getDeletePathList(), newPlan.getDeletePathList());

    Assert.assertEquals(position, getCurrentBufferPosition());
  }

  @Test
  public void testPreDeleteTimeSeriesSerializationCompatibility() throws IllegalPathException {
    PreDeleteTimeSeriesPlan oldPlan = new PreDeleteTimeSeriesPlan();
    oldPlan.setPath(new PartialPath("root.**.s"));

    int position = serializeToBuffer(oldPlan);

    IPreDeleteTimeSeriesPlan newPlan = deserializeFromBuffer();

    Assert.assertEquals(oldPlan.getPath(), newPlan.getPath());

    Assert.assertEquals(position, getCurrentBufferPosition());
  }

  @Test
  public void testRollbackPreDeleteTimeSeriesSerializationCompatibility()
      throws IllegalPathException {
    RollbackPreDeleteTimeSeriesPlan oldPlan = new RollbackPreDeleteTimeSeriesPlan();
    oldPlan.setPath(new PartialPath("root.sg.**"));

    int position = serializeToBuffer(oldPlan);

    IRollbackPreDeleteTimeSeriesPlan newPlan = deserializeFromBuffer();

    Assert.assertEquals(oldPlan.getPath(), newPlan.getPath());

    Assert.assertEquals(position, getCurrentBufferPosition());
  }
}
