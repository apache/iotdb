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

package org.apache.iotdb.db.mpp.common;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;

import org.junit.Assert;
import org.junit.Test;

public class PathPatternTreeTest {

  @Test
  public void pathPatternTreeTest1() throws IllegalPathException {
    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPath(new PartialPath("root.sg1.d1.s1"));
    patternTree.appendPath(new PartialPath("root.sg1.d1.s2"));
    patternTree.appendPath(new PartialPath("root.sg1.d1.*"));
    patternTree.appendPath(new PartialPath("root.sg1.d1.s3"));
    patternTree.constructTree();

    PathPatternTree resultPatternTree = new PathPatternTree();
    resultPatternTree.appendPath(new PartialPath("root.sg1.d1.*"));
    resultPatternTree.constructTree();

    Assert.assertTrue(resultPatternTree.equalWith(patternTree));
  }

  @Test
  public void pathPatternTreeTest2() throws IllegalPathException {
    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPath(new PartialPath("root.sg1.d1.s1"));
    patternTree.appendPath(new PartialPath("root.sg1.d1.s2"));
    patternTree.appendPath(new PartialPath("root.sg1.d1.t1.s1"));
    patternTree.appendPath(new PartialPath("root.sg1.*.s1"));
    patternTree.appendPath(new PartialPath("root.sg1.d2.s1"));
    patternTree.constructTree();

    PathPatternTree resultPatternTree = new PathPatternTree();
    resultPatternTree.appendPath(new PartialPath("root.sg1.d1.s2"));
    resultPatternTree.appendPath(new PartialPath("root.sg1.d1.t1.s1"));
    resultPatternTree.appendPath(new PartialPath("root.sg1.*.s1"));
    resultPatternTree.constructTree();

    Assert.assertTrue(resultPatternTree.equalWith(patternTree));
  }

  @Test
  public void pathPatternTreeTest3() throws IllegalPathException {
    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPath(new PartialPath("root.sg1.d1.s1"));
    patternTree.appendPath(new PartialPath("root.sg1.d1.s2"));
    patternTree.appendPath(new PartialPath("root.sg1.d1.t1.s1"));
    patternTree.appendPath(new PartialPath("root.sg1.d2.s3"));
    patternTree.appendPath(new PartialPath("root.**"));
    patternTree.appendPath(new PartialPath("root.sg1.d1.s1"));
    patternTree.appendPath(new PartialPath("root.sg1.d1.s2"));
    patternTree.appendPath(new PartialPath("root.sg1.d1.t1.s1"));
    patternTree.appendPath(new PartialPath("root.sg1.d2.s3"));
    patternTree.constructTree();

    PathPatternTree resultPatternTree = new PathPatternTree();
    resultPatternTree.appendPath(new PartialPath("root.**"));
    resultPatternTree.constructTree();

    Assert.assertTrue(resultPatternTree.equalWith(patternTree));
  }

  @Test
  public void pathPatternTreeTest4() throws IllegalPathException {
    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPath(new PartialPath("root.sg1.d1.s1"));
    patternTree.appendPath(new PartialPath("root.sg1.d1.s2"));
    patternTree.appendPath(new PartialPath("root.sg1.d1.t1.s1"));
    patternTree.appendPath(new PartialPath("root.sg1.d2.s1"));
    patternTree.appendPath(new PartialPath("root.sg1.**.s1"));
    patternTree.constructTree();

    PathPatternTree resultPatternTree = new PathPatternTree();
    resultPatternTree.appendPath(new PartialPath("root.sg1.d1.s2"));
    resultPatternTree.appendPath(new PartialPath("root.sg1.**.s1"));
    resultPatternTree.constructTree();

    Assert.assertTrue(resultPatternTree.equalWith(patternTree));
  }

  @Test
  public void pathPatternTreeTest5() throws IllegalPathException {
    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPath(new PartialPath("root.sg1.d1.s1"));
    patternTree.appendPath(new PartialPath("root.sg1.d1.s2"));
    patternTree.appendPath(new PartialPath("root.sg1.d1.t1.s1"));
    patternTree.appendPath(new PartialPath("root.sg1.d2.s1"));
    patternTree.appendPath(new PartialPath("root.sg1.d2.s2"));
    patternTree.appendPath(new PartialPath("root.sg1.d2.*"));
    patternTree.appendPath(new PartialPath("root.sg1.**.s1"));
    patternTree.appendPath(new PartialPath("root.sg1.*.s2"));
    patternTree.appendPath(new PartialPath("root.sg1.d3.s1"));
    patternTree.appendPath(new PartialPath("root.sg1.d3.s2"));
    patternTree.appendPath(new PartialPath("root.sg1.d3.t1.s1"));
    patternTree.appendPath(new PartialPath("root.sg1.d3.t1.s2"));
    patternTree.constructTree();

    PathPatternTree resultPatternTree = new PathPatternTree();
    resultPatternTree.appendPath(new PartialPath("root.sg1.d2.*"));
    resultPatternTree.appendPath(new PartialPath("root.sg1.**.s1"));
    resultPatternTree.appendPath(new PartialPath("root.sg1.*.s2"));
    resultPatternTree.appendPath(new PartialPath("root.sg1.d3.t1.s2"));
    resultPatternTree.constructTree();

    Assert.assertTrue(resultPatternTree.equalWith(patternTree));
  }
}
