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

package org.apache.iotdb.commons.path;

import org.apache.iotdb.commons.path.PathPatternNode.VoidSerializer;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class PathPatternNodeTest {

  @Test
  public void testNonTrivialWildcardChildCacheLifecycle() {
    final PathPatternNode<Void, VoidSerializer> parent = newNode("parent");
    final PathPatternNode<Void, VoidSerializer> wildcardChild = newNode("device*");

    parent.addChild(wildcardChild);
    final List<PathPatternNode<Void, VoidSerializer>> matchedChildren =
        parent.getMatchChildren("device1");
    assertEquals(1, matchedChildren.size());
    assertSame(wildcardChild, matchedChildren.get(0));

    parent.deleteChild(wildcardChild);
    assertTrue(parent.getMatchChildren("device1").isEmpty());

    parent.addChild(wildcardChild);
    parent.clear();
    assertTrue(parent.getMatchChildren("device1").isEmpty());
  }

  @Test
  public void testReplacingNonTrivialWildcardChildKeepsCache() {
    final PathPatternNode<Void, VoidSerializer> parent = newNode("parent");
    final PathPatternNode<Void, VoidSerializer> originalChild = newNode("device*");
    final PathPatternNode<Void, VoidSerializer> replacementChild = newNode("device*");

    parent.addChild(originalChild);
    parent.addChild(replacementChild);

    final List<PathPatternNode<Void, VoidSerializer>> matchedChildren =
        parent.getMatchChildren("device1");
    assertEquals(1, matchedChildren.size());
    assertSame(replacementChild, matchedChildren.get(0));
  }

  private PathPatternNode<Void, VoidSerializer> newNode(final String name) {
    return new PathPatternNode<>(name, VoidSerializer.getInstance());
  }
}
