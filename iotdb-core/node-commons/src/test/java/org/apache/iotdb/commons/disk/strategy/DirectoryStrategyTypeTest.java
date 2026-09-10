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

package org.apache.iotdb.commons.disk.strategy;

import org.junit.Assert;
import org.junit.Test;

public class DirectoryStrategyTypeTest {

  @Test
  public void fromSimpleClassName() {
    Assert.assertEquals(
        DirectoryStrategyType.SEQUENCE_STRATEGY,
        DirectoryStrategyType.fromClassName("SequenceStrategy"));
    Assert.assertEquals(
        DirectoryStrategyType.MAX_DISK_USABLE_SPACE_FIRST_STRATEGY,
        DirectoryStrategyType.fromClassName("MaxDiskUsableSpaceFirstStrategy"));
    Assert.assertEquals(
        DirectoryStrategyType.MIN_FOLDER_OCCUPIED_SPACE_FIRST_STRATEGY,
        DirectoryStrategyType.fromClassName("MinFolderOccupiedSpaceFirstStrategy"));
    Assert.assertEquals(
        DirectoryStrategyType.RANDOM_ON_DISK_USABLE_SPACE_STRATEGY,
        DirectoryStrategyType.fromClassName("RandomOnDiskUsableSpaceStrategy"));
  }

  @Test
  public void fromFullyQualifiedClassName() {
    Assert.assertEquals(
        DirectoryStrategyType.SEQUENCE_STRATEGY,
        DirectoryStrategyType.fromClassName(SequenceStrategy.class.getName()));
    Assert.assertEquals(
        DirectoryStrategyType.MIN_FOLDER_OCCUPIED_SPACE_FIRST_STRATEGY,
        DirectoryStrategyType.fromClassName(MinFolderOccupiedSpaceFirstStrategy.class.getName()));
  }

  @Test
  public void nullOrUnknownFallsBackToSequence() {
    // The configured default (dn_multi_dir_strategy=SequenceStrategy) and any unrecognized value
    // must resolve to SEQUENCE_STRATEGY.
    Assert.assertEquals(
        DirectoryStrategyType.SEQUENCE_STRATEGY, DirectoryStrategyType.fromClassName(null));
    Assert.assertEquals(
        DirectoryStrategyType.SEQUENCE_STRATEGY,
        DirectoryStrategyType.fromClassName("NoSuchStrategy"));
  }
}
