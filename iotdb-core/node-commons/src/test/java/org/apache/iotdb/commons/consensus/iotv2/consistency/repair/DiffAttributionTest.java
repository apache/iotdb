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

package org.apache.iotdb.commons.consensus.iotv2.consistency.repair;

import org.apache.iotdb.commons.consensus.iotv2.consistency.ibf.CompositeKeyCodec;
import org.apache.iotdb.commons.consensus.iotv2.consistency.ibf.DiffEntry;
import org.apache.iotdb.commons.consensus.iotv2.consistency.ibf.RowRefIndex;
import org.apache.iotdb.commons.consensus.iotv2.consistency.merkle.MerkleEntry;
import org.apache.iotdb.commons.consensus.iotv2.consistency.merkle.MerkleFileContent;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DiffAttributionTest {

  @Test
  public void shouldUseActualBucketBoundariesWhenAttributingDiffs() {
    DiffAttribution diffAttribution = new DiffAttribution();
    RowRefIndex rowRefIndex =
        new RowRefIndex.Builder()
            .addDevice("root.sg.d1", Collections.singletonList("s1"))
            .setTimeBucketStart(0L)
            .build();
    long compositeKey = CompositeKeyCodec.encode(0, 0, 250L, 0L, 1L);
    DiffEntry diffEntry = new DiffEntry(compositeKey, 1L, DiffEntry.DiffType.LEADER_HAS);
    List<MerkleFileContent> merkleFiles =
        Arrays.asList(
            new MerkleFileContent(
                1L,
                10L,
                Collections.singletonList(
                    new MerkleEntry("root.sg.d1", "s1", 100L, 200L, 10, 101L)),
                "first.tsfile"),
            new MerkleFileContent(
                2L,
                20L,
                Collections.singletonList(
                    new MerkleEntry("root.sg.d1", "s1", 200L, 300L, 10, 202L)),
                "second.tsfile"));

    Map<String, List<DiffEntry>> attributed =
        diffAttribution.attributeToSourceTsFiles(
            Collections.singletonList(diffEntry), rowRefIndex, merkleFiles);

    Assert.assertFalse(attributed.containsKey("first.tsfile"));
    Assert.assertEquals(Collections.singletonList(diffEntry), attributed.get("second.tsfile"));
  }
}
