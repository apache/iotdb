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

package org.apache.iotdb.commons.consensus.iotv2.consistency;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

public class ConsistencyMerkleTreeTest {

  @Test
  public void shouldPreserveDualDigestOnFlush() {
    ConsistencyMerkleTree merkleTree = new ConsistencyMerkleTree();
    DualDigest first = new DualDigest(0x01L, 0x10L);
    DualDigest second = new DualDigest(0x02L, 0x20L);

    merkleTree.onTsFileFlushed(1L, first);
    merkleTree.onTsFileFlushed(1L, second);

    Assert.assertEquals(
        new DualDigest(0x03L, 0x30L), merkleTree.getPartitionNode(1L).getPartitionDigest());
    Assert.assertEquals(new DualDigest(0x03L, 0x30L), merkleTree.getRegionDigest());
  }

  @Test
  public void shouldPreserveDualDigestOnCompaction() {
    ConsistencyMerkleTree merkleTree = new ConsistencyMerkleTree();
    DualDigest sourceOne = new DualDigest(0x01L, 0x10L);
    DualDigest sourceTwo = new DualDigest(0x02L, 0x20L);
    DualDigest targetOne = new DualDigest(0x04L, 0x40L);
    DualDigest targetTwo = new DualDigest(0x08L, 0x80L);

    merkleTree.onTsFileFlushed(7L, sourceOne);
    merkleTree.onTsFileFlushed(7L, sourceTwo);
    merkleTree.onCompaction(
        Collections.singletonList(sourceOne), Arrays.asList(targetOne, targetTwo), 7L);

    Assert.assertEquals(
        new DualDigest(0x0EL, 0xE0L), merkleTree.getPartitionNode(7L).getPartitionDigest());
    Assert.assertEquals(new DualDigest(0x0EL, 0xE0L), merkleTree.getRegionDigest());
  }
}
