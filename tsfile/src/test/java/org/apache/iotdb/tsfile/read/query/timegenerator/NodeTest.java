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
package org.apache.iotdb.tsfile.read.query.timegenerator;

import org.apache.iotdb.tsfile.read.query.timegenerator.node.AndNode;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.LeafNode;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.Node;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.NodeType;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.OrNode;
import org.apache.iotdb.tsfile.read.reader.FakedBatchReader;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class NodeTest {

  @Test
  public void testType() {
    Assert.assertEquals(NodeType.LEAF, new LeafNode(null).getType());
    Assert.assertEquals(NodeType.AND, new AndNode(null, null).getType());
    Assert.assertEquals(NodeType.OR, new OrNode(null, null).getType());
  }

  @Test
  public void testLeafNode() throws IOException {
    int index = 0;
    long[] timestamps = new long[] {1, 2, 3, 4, 5, 6, 7};
    IBatchReader batchReader = new FakedBatchReader(timestamps);
    Node leafNode = new LeafNode(batchReader);
    while (leafNode.hasNext()) {
      Assert.assertEquals(timestamps[index++], leafNode.next());
    }
  }

  @Test
  public void testOrNode() throws IOException {
    long[] ret = new long[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20};
    long[] left = new long[] {1, 3, 5, 7, 9, 10, 20};
    long[] right = new long[] {2, 3, 4, 5, 6, 7, 8};
    testOr(ret, left, right);
    testOr(new long[] {}, new long[] {}, new long[] {});
    testOr(new long[] {1}, new long[] {1}, new long[] {});
    testOr(new long[] {1}, new long[] {1}, new long[] {1});
    testOr(new long[] {1, 2}, new long[] {1}, new long[] {1, 2});
    testOr(new long[] {1, 2}, new long[] {1, 2}, new long[] {1, 2});
    testOr(new long[] {1, 2, 3}, new long[] {1, 2}, new long[] {1, 2, 3});
  }

  private void testOr(long[] ret, long[] left, long[] right) throws IOException {
    int index = 0;
    Node orNode =
        new OrNode(
            new LeafNode(new FakedBatchReader(left)), new LeafNode(new FakedBatchReader(right)));
    while (orNode.hasNext()) {
      long value = orNode.next();
      Assert.assertEquals(ret[index++], value);
    }
    Assert.assertEquals(ret.length, index);
  }

  @Test
  public void testAndNode() throws IOException {
    testAnd(new long[] {}, new long[] {1, 2, 3, 4}, new long[] {});
    testAnd(new long[] {}, new long[] {1, 2, 3, 4, 8}, new long[] {5, 6, 7});
    testAnd(new long[] {2}, new long[] {1, 2, 3, 4}, new long[] {2, 5, 6});
    testAnd(new long[] {1, 2, 3}, new long[] {1, 2, 3, 4}, new long[] {1, 2, 3});
    testAnd(new long[] {1, 2, 3, 9}, new long[] {1, 2, 3, 4, 9}, new long[] {1, 2, 3, 8, 9});
  }

  private void testAnd(long[] ret, long[] left, long[] right) throws IOException {
    int index = 0;
    Node andNode =
        new AndNode(
            new LeafNode(new FakedBatchReader(left)), new LeafNode(new FakedBatchReader(right)));
    while (andNode.hasNext()) {
      long value = andNode.next();
      Assert.assertEquals(ret[index++], value);
    }
    Assert.assertEquals(ret.length, index);
  }
}
