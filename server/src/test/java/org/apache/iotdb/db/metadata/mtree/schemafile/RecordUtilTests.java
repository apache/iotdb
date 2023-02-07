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
package org.apache.iotdb.db.metadata.mtree.schemafile;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mtree.store.disk.ICachedMNodeContainer;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.RecordUtils;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class RecordUtilTests {

  @Before
  public void setUp() {
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void internalNodeTest() throws MetadataException {
    IMNode oneNode = new InternalMNode(null, "abcd");
    ICachedMNodeContainer.getCachedMNodeContainer(oneNode).setSegmentAddress(1234567L);
    oneNode.setUseTemplate(true);
    ByteBuffer buffer = RecordUtils.node2Buffer(oneNode);
    buffer.clear();
    IMNode node2 = RecordUtils.buffer2Node("abcd", buffer);
    Assert.assertEquals(
        ICachedMNodeContainer.getCachedMNodeContainer(node2).getSegmentAddress(), 1234567L);
    Assert.assertEquals(node2.isUseTemplate(), oneNode.isUseTemplate());
  }

  @Test
  public void measurementTest() throws MetadataException {
    Map<String, String> props = new HashMap<>();
    props.put("ka", "va");
    props.put("kb", "vb");

    IMeasurementSchema schema =
        new MeasurementSchema(
            "amn", TSDataType.FLOAT, TSEncoding.BITMAP, CompressionType.GZIP, props);
    IMNode amn = MeasurementMNode.getMeasurementMNode(null, "amn", schema, "anothername");

    ByteBuffer tBuf = RecordUtils.node2Buffer(amn);
    tBuf.clear();
    Assert.assertFalse(
        RecordUtils.buffer2Node("name", tBuf).getAsMeasurementMNode().isPreDeleted());

    amn.getAsMeasurementMNode().setPreDeleted(true);

    tBuf = RecordUtils.node2Buffer(amn);
    tBuf.clear();
    Assert.assertTrue(RecordUtils.buffer2Node("name", tBuf).getAsMeasurementMNode().isPreDeleted());

    ByteBuffer buffer = RecordUtils.node2Buffer(amn);
    buffer.clear();
    IMNode node2 = RecordUtils.buffer2Node("amn", buffer);

    Assert.assertTrue(
        amn.getAsMeasurementMNode().getSchema().equals(node2.getAsMeasurementMNode().getSchema()));
    Assert.assertEquals(
        node2.getAsMeasurementMNode().getAlias(), amn.getAsMeasurementMNode().getAlias());
    Assert.assertEquals(true, node2.getAsMeasurementMNode().isPreDeleted());
    Assert.assertEquals(props, node2.getAsMeasurementMNode().getSchema().getProps());
  }
}
