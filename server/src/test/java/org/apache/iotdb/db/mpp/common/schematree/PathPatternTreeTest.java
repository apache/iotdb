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

package org.apache.iotdb.db.mpp.common.schematree;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class PathPatternTreeTest {

  @Test
  public void pathPatternTreeTest1() throws IllegalPathException, IOException {
    checkPathPatternTree(
        Arrays.asList(
            new PartialPath("root.sg1.d1.s1"),
            new PartialPath("root.sg1.d1.s2"),
            new PartialPath("root.sg1.d1.*"),
            new PartialPath("root.sg1.d1.s3")),
        Collections.singletonList(new PartialPath("root.sg1.d1.*")),
        Collections.singletonList(new PartialPath("root.sg1.d1")));
  }

  @Test
  public void pathPatternTreeTest2() throws IllegalPathException, IOException {
    checkPathPatternTree(
        Arrays.asList(
            new PartialPath("root.sg1.d1.t1.s1"),
            new PartialPath("root.sg1.d1.t2.s2"),
            new PartialPath("root.sg1.*.t1.s1"),
            new PartialPath("root.sg1.d2.t1.s1")),
        Arrays.asList(new PartialPath("root.sg1.d1.t2.s2"), new PartialPath("root.sg1.*.t1.s1")),
        Arrays.asList(new PartialPath("root.sg1.d1.t2"), new PartialPath("root.sg1.*")));
  }

  @Test
  public void pathPatternTreeTest3() throws IllegalPathException, IOException {
    checkPathPatternTree(
        Arrays.asList(
            new PartialPath("root.sg1.d1.s1"),
            new PartialPath("root.sg1.d1.s2"),
            new PartialPath("root.sg1.d1.t1.s1"),
            new PartialPath("root.sg1.*.s1"),
            new PartialPath("root.sg1.d2.s1")),
        Arrays.asList(
            new PartialPath("root.sg1.d1.s2"),
            new PartialPath("root.sg1.d1.t1.s1"),
            new PartialPath("root.sg1.*.s1")),
        Arrays.asList(
            new PartialPath("root.sg1.d1"),
            new PartialPath("root.sg1.d1.t1"),
            new PartialPath("root.sg1.*")));
  }

  @Test
  public void pathPatternTreeTest4() throws IllegalPathException, IOException {
    checkPathPatternTree(
        Arrays.asList(
            new PartialPath("root.sg1.d1.s1"),
            new PartialPath("root.sg1.d1.s2"),
            new PartialPath("root.sg1.d1.t1.s1"),
            new PartialPath("root.sg1.d2.s3"),
            new PartialPath("root.**"),
            new PartialPath("root.sg1.d1.s1"),
            new PartialPath("root.sg1.d1.s2"),
            new PartialPath("root.sg1.d1.t1.s1"),
            new PartialPath("root.sg1.d2.s3")),
        Collections.singletonList(new PartialPath("root.**")),
        Collections.singletonList(new PartialPath("root.**")));
  }

  @Test
  public void pathPatternTreeTest5() throws IllegalPathException, IOException {
    checkPathPatternTree(
        Arrays.asList(
            new PartialPath("root.sg1.d1.s1"),
            new PartialPath("root.sg1.d1.s2"),
            new PartialPath("root.sg1.d1.t1.s1"),
            new PartialPath("root.sg1.d2.s1"),
            new PartialPath("root.sg1.**.s1")),
        Arrays.asList(new PartialPath("root.sg1.d1.s2"), new PartialPath("root.sg1.**.s1")),
        Arrays.asList(new PartialPath("root.sg1.d1"), new PartialPath("root.sg1.**")));
  }

  @Test
  public void pathPatternTreeTest6() throws IllegalPathException, IOException {
    checkPathPatternTree(
        Arrays.asList(
            new PartialPath("root.sg1.d1.s1"),
            new PartialPath("root.sg1.d1.s2"),
            new PartialPath("root.sg1.d1.t1.s1"),
            new PartialPath("root.sg1.d2.s1"),
            new PartialPath("root.sg1.d2.s2"),
            new PartialPath("root.sg1.d2.*"),
            new PartialPath("root.sg1.**.s1"),
            new PartialPath("root.sg1.*.s2"),
            new PartialPath("root.sg1.d3.s1"),
            new PartialPath("root.sg1.d3.s2"),
            new PartialPath("root.sg1.d3.t1.s1"),
            new PartialPath("root.sg1.d3.t1.s2")),
        Arrays.asList(
            new PartialPath("root.sg1.d2.*"),
            new PartialPath("root.sg1.**.s1"),
            new PartialPath("root.sg1.*.s2"),
            new PartialPath("root.sg1.d3.t1.s2")),
        Arrays.asList(
            new PartialPath("root.sg1.d2"),
            new PartialPath("root.sg1.**"),
            new PartialPath("root.sg1.*"),
            new PartialPath("root.sg1.d3.t1")));
  }

  private void checkPathPatternTree(
      List<PartialPath> paths,
      List<PartialPath> compressedPaths,
      List<PartialPath> compressedDevicePaths)
      throws IOException {
    PathPatternTree patternTree = new PathPatternTree();
    for (PartialPath path : paths) {
      patternTree.appendPathPattern(path);
    }
    patternTree.constructTree();

    PathPatternTree resultPatternTree = new PathPatternTree();
    for (PartialPath path : compressedPaths) {
      resultPatternTree.appendPathPattern(path);
    }
    resultPatternTree.constructTree();

    Assert.assertTrue(resultPatternTree.equalWith(patternTree));

    Assert.assertEquals(
        compressedDevicePaths.stream()
            .map(PartialPath::getFullPath)
            .sorted()
            .collect(Collectors.toList()),
        patternTree.getAllDevicePatterns().stream().sorted().collect(Collectors.toList()));

    PublicBAOS outputStream = new PublicBAOS();
    resultPatternTree.serialize(outputStream);
    ByteBuffer buffer = ByteBuffer.allocate(outputStream.size());
    buffer.put(outputStream.getBuf(), 0, outputStream.size());
    buffer.flip();
    PathPatternTree tmpPathPatternTree = PathPatternTree.deserialize(buffer);
    Assert.assertTrue(resultPatternTree.equalWith(tmpPathPatternTree));
  }

  @Test
  public void testPathPatternTreeSplit() throws Exception {
    List<PartialPath> partialPathList =
        Arrays.asList(
            new PartialPath("root.sg1.d1.t1.s1"),
            new PartialPath("root.sg1.d1.t2.s2"),
            new PartialPath("root.sg1.*.t1.s1"),
            new PartialPath("root.sg1.d2.t1.s1"));

    PathPatternTree patternTree = new PathPatternTree();
    for (PartialPath path : partialPathList) {
      patternTree.appendPathPattern(path);
    }
    patternTree.constructTree();

    Assert.assertEquals(
        Arrays.asList(new PartialPath("root.sg1.*.t1.s1"), new PartialPath("root.sg1.d1.t2.s2")),
        patternTree.getAllPathPatterns());
  }
}
