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
import org.apache.iotdb.db.metadata.mnode.EntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.ISchemaPage;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.RecordUtils;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFileConfig;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaPage;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SchemaPageTest {

  @Before
  public void setUp() {
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void flatTreeInsert() throws IOException, MetadataException {
    ISchemaPage page =
        ISchemaPage.initSegmentedPage(ByteBuffer.allocate(SchemaFileConfig.PAGE_LENGTH), 0);
    IMNode root = virtualFlatMTree(15);
    for (int i = 0; i < 7; i++) {
      page.getAsSegmentedPage().allocNewSegment(SchemaFileConfig.SEG_SIZE_LST[0]);
      int cnt = 0;
      for (IMNode child : root.getChildren().values()) {
        cnt++;
        try {
          page.getAsSegmentedPage()
              .write((short) i, child.getName(), RecordUtils.node2Buffer(child));
        } catch (MetadataException e) {
          e.printStackTrace();
        }
        if (cnt > i) {
          break;
        }
      }
    }

    ByteBuffer newBuf = ByteBuffer.allocate(SchemaFileConfig.PAGE_LENGTH);
    page.syncPageBuffer();
    page.getPageBuffer(newBuf);
    SchemaPage newPage = ISchemaPage.loadSchemaPage(newBuf);
    Assert.assertEquals(newPage.inspect(), page.inspect());
    System.out.println(newPage.inspect());
  }

  @Test
  public void essentialPageTest() throws MetadataException, IOException {
    ByteBuffer buf = ByteBuffer.allocate(SchemaFileConfig.PAGE_LENGTH);
    ISchemaPage page = ISchemaPage.initSegmentedPage(buf, 0);
    page.getAsSegmentedPage().allocNewSegment((short) 500);
    Assert.assertFalse(page.getAsInternalPage() != null);

    page.getAsSegmentedPage().deleteSegment((short) 0);
    page = ISchemaPage.initInternalPage(buf, 0, 0);
    Assert.assertTrue(page.getAsInternalPage() != null);

    page.getAsInternalPage().insertRecord("aaa", 256);
    page.getAsInternalPage().setNextSegAddress(999L);
    page.syncPageBuffer();

    SchemaPage nPage = ISchemaPage.loadSchemaPage(buf);

    Assert.assertTrue(nPage.getAsInternalPage() != null);
    Assert.assertEquals(999L, nPage.getAsInternalPage().getNextSegAddress());
    Assert.assertEquals(256, (int) nPage.getAsInternalPage().getRecordByKey("aab"));
  }

  private IMNode virtualFlatMTree(int childSize) {
    IMNode internalNode = new EntityMNode(null, "vRoot1");

    for (int idx = 0; idx < childSize; idx++) {
      String measurementId = "mid" + idx;
      IMeasurementSchema schema = new MeasurementSchema(measurementId, TSDataType.FLOAT);
      IMeasurementMNode mNode =
          MeasurementMNode.getMeasurementMNode(
              internalNode.getAsEntityMNode(), measurementId, schema, measurementId + "als");
      internalNode.addChild(mNode);
    }
    return internalNode;
  }

  public void print(Object o) {
    System.out.println(o);
  }

  private void printBuffer(ByteBuffer buf) {
    int pos = buf.position();
    int lim = buf.limit();
    ByteBuffer bufRep = buf.slice();
    while (pos < lim) {
      System.out.print(buf.get(pos));
      System.out.print(" ");
      pos++;
    }
    System.out.println("");
  }
}
