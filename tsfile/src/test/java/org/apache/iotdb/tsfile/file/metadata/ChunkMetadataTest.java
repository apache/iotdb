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
package org.apache.iotdb.tsfile.file.metadata;

import org.apache.iotdb.tsfile.read.common.TimeRange;

import org.junit.Assert;
import org.junit.Test;

public class ChunkMetadataTest {
  @Test
  public void testInsertIntoSortedDeletions() {
    ChunkMetadata chunkMetadata = new ChunkMetadata();
    // add into empty list
    chunkMetadata.insertIntoSortedDeletions(new TimeRange(6, 27));
    Assert.assertArrayEquals(
        new TimeRange[] {new TimeRange(6, 27)},
        chunkMetadata.getDeleteIntervalList().toArray(new TimeRange[0]));
    // add not overlap
    chunkMetadata.insertIntoSortedDeletions(new TimeRange(61, 69));
    Assert.assertArrayEquals(
        new TimeRange[] {new TimeRange(6, 27), new TimeRange(61, 69)},
        chunkMetadata.getDeleteIntervalList().toArray(new TimeRange[0]));
    // add left overlap
    chunkMetadata.insertIntoSortedDeletions(new TimeRange(0, 19));
    Assert.assertArrayEquals(
        new TimeRange[] {new TimeRange(0, 27), new TimeRange(61, 69)},
        chunkMetadata.getDeleteIntervalList().toArray(new TimeRange[0]));
    chunkMetadata.insertIntoSortedDeletions(new TimeRange(59, 60));
    Assert.assertArrayEquals(
        new TimeRange[] {new TimeRange(0, 27), new TimeRange(59, 60), new TimeRange(61, 69)},
        chunkMetadata.getDeleteIntervalList().toArray(new TimeRange[0]));
    // add right overlap
    chunkMetadata.insertIntoSortedDeletions(new TimeRange(27, 30));
    Assert.assertArrayEquals(
        new TimeRange[] {new TimeRange(0, 30), new TimeRange(59, 60), new TimeRange(61, 69)},
        chunkMetadata.getDeleteIntervalList().toArray(new TimeRange[0]));
    chunkMetadata.insertIntoSortedDeletions(new TimeRange(65, 70));
    Assert.assertArrayEquals(
        new TimeRange[] {new TimeRange(0, 30), new TimeRange(59, 60), new TimeRange(61, 70)},
        chunkMetadata.getDeleteIntervalList().toArray(new TimeRange[0]));
    // add internal
    chunkMetadata.insertIntoSortedDeletions(new TimeRange(40, 50));
    Assert.assertArrayEquals(
        new TimeRange[] {
          new TimeRange(0, 30), new TimeRange(40, 50), new TimeRange(59, 60), new TimeRange(61, 70)
        },
        chunkMetadata.getDeleteIntervalList().toArray(new TimeRange[0]));
    chunkMetadata.insertIntoSortedDeletions(new TimeRange(30, 40));
    Assert.assertArrayEquals(
        new TimeRange[] {new TimeRange(0, 50), new TimeRange(59, 60), new TimeRange(61, 70)},
        chunkMetadata.getDeleteIntervalList().toArray(new TimeRange[0]));
  }
}
