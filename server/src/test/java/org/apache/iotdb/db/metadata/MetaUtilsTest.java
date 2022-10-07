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
package org.apache.iotdb.db.metadata;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class MetaUtilsTest {

  @Test
  public void testGetMultiFullPaths() {
    InternalMNode rootNode = new InternalMNode(null, "root");

    // builds the relationship of root.a and root.aa
    InternalMNode aNode = new InternalMNode(rootNode, "a");
    rootNode.addChild(aNode.getName(), aNode);
    InternalMNode aaNode = new InternalMNode(rootNode, "aa");
    rootNode.addChild(aaNode.getName(), aaNode);

    // builds the relationship of root.a.b and root.aa.bb
    InternalMNode bNode = new InternalMNode(aNode, "b");
    aNode.addChild(bNode.getName(), bNode);
    InternalMNode bbNode = new InternalMNode(aaNode, "bb");
    aaNode.addChild(bbNode.getName(), bbNode);

    // builds the relationship of root.aa.bb.cc
    InternalMNode ccNode = new InternalMNode(bbNode, "cc");
    bbNode.addChild(ccNode.getName(), ccNode);

    List<String> multiFullPaths = MetaUtils.getMultiFullPaths(rootNode);
    Assert.assertSame(2, multiFullPaths.size());

    multiFullPaths.forEach(
        fullPath -> {
          if (fullPath.contains("aa")) {
            Assert.assertEquals("root.aa.bb.cc", fullPath);
          } else {
            Assert.assertEquals("root.a.b", fullPath);
          }
        });
  }

  @Test
  public void testGroupAlignedPath() throws MetadataException {
    List<PartialPath> pathList = new ArrayList<>();

    MeasurementPath path1 =
        new MeasurementPath(new PartialPath("root.sg.device1.s1"), TSDataType.INT32);
    pathList.add(path1);
    MeasurementPath path2 =
        new MeasurementPath(new PartialPath("root.sg.device1.s2"), TSDataType.INT32);
    pathList.add(path2);

    MeasurementPath path3 =
        new MeasurementPath(new PartialPath("root.sg.aligned_device.s1"), TSDataType.INT32);
    path3.setUnderAlignedEntity(true);
    pathList.add(path3);
    MeasurementPath path4 =
        new MeasurementPath(new PartialPath("root.sg.aligned_device.s2"), TSDataType.INT32);
    path4.setUnderAlignedEntity(true);
    pathList.add(path4);

    AlignedPath alignedPath = new AlignedPath(path3);
    alignedPath.addMeasurement(path4);

    List<PartialPath> result = MetaUtils.groupAlignedPaths(pathList);
    assertTrue(result.contains(path1));
    assertTrue(result.contains(path2));
    assertTrue(result.contains(alignedPath));
  }
}
