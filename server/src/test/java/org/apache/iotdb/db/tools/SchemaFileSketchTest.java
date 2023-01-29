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

package org.apache.iotdb.db.tools;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupEntityMNode;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.ISchemaFile;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFile;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngineMode;
import org.apache.iotdb.db.tools.schema.SchemaFileSketchTool;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class SchemaFileSketchTest {

  @Before
  public void setUp() {
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setSchemaEngineMode(SchemaEngineMode.Schema_File.toString());
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    File sketch = new File("sketch_schemafile.txt");
    sketch.deleteOnExit();
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setSchemaEngineMode(SchemaEngineMode.Memory.toString());
  }

  private void prepareData() throws IOException, MetadataException {
    int TEST_SCHEMA_REGION_ID = 0;
    ISchemaFile sf = SchemaFile.initSchemaFile("root.test.vRoot1", TEST_SCHEMA_REGION_ID);

    Iterator<IMNode> ite = getTreeBFT(getFlatTree(500, "aa"));
    while (ite.hasNext()) {
      IMNode cur = ite.next();
      if (!cur.isMeasurement()) {
        sf.writeMNode(cur);
      }
    }

    sf.close();
  }

  @Test
  public void testSchemaFileSketch() throws Exception {
    prepareData();
    File file =
        new File(
            "target"
                + File.separator
                + "tmp"
                + File.separator
                + "system"
                + File.separator
                + "schema"
                + File.separator
                + "root.test.vRoot1"
                + File.separator
                + "0"
                + File.separator
                + MetadataConstant.SCHEMA_FILE_NAME);
    File sketchFile = new File("sketch_schemafile.txt");

    SchemaFileSketchTool.sketchFile(file.getAbsolutePath(), sketchFile.getAbsolutePath());
    ISchemaFile sf = SchemaFile.loadSchemaFile(file);
    try {
      StringWriter sw = new StringWriter();
      ((SchemaFile) sf).inspect(new PrintWriter(sw));
      Assert.assertEquals(
          sw.toString(), new String(Files.readAllBytes(Paths.get(sketchFile.getAbsolutePath()))));
    } finally {
      sf.close();
    }
  }

  private Iterator<IMNode> getTreeBFT(IMNode root) {
    return new Iterator<IMNode>() {
      Queue<IMNode> queue = new LinkedList<IMNode>();

      {
        this.queue.add(root);
      }

      @Override
      public boolean hasNext() {
        return queue.size() > 0;
      }

      @Override
      public IMNode next() {
        IMNode curNode = queue.poll();
        if (!curNode.isMeasurement() && curNode.getChildren().size() > 0) {
          for (IMNode child : curNode.getChildren().values()) {
            queue.add(child);
          }
        }
        return curNode;
      }
    };
  }

  private IMNode getFlatTree(int flatSize, String id) {
    IMNode root = new InternalMNode(null, "root");
    IMNode test = new InternalMNode(root, "test");
    IMNode internalNode = new StorageGroupEntityMNode(null, "vRoot1", 0L);

    for (int idx = 0; idx < flatSize; idx++) {
      String measurementId = id + idx;
      IMeasurementSchema schema = new MeasurementSchema(measurementId, TSDataType.FLOAT);
      IMeasurementMNode mNode =
          MeasurementMNode.getMeasurementMNode(
              internalNode.getAsEntityMNode(), measurementId, schema, measurementId + "als");
      internalNode.addChild(mNode);
    }

    test.addChild(internalNode);
    return internalNode;
  }
}
