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

package org.apache.iotdb.db.storageengine.load;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class LoadTsFileChecksumUtilsTest {

  @Test
  public void testCombineIsOrderSensitive() {
    // rotate-then-XOR aggregation must detect pieces applied in a different order, unlike plain
    // XOR which is commutative.
    final long ab = LoadTsFileChecksumUtils.combine(LoadTsFileChecksumUtils.combine(0L, 1L), 2L);
    final long ba = LoadTsFileChecksumUtils.combine(LoadTsFileChecksumUtils.combine(0L, 2L), 1L);
    Assert.assertNotEquals(ab, ba);
  }

  @Test
  public void testChecksumIncludesPieceIndex() {
    // Two empty pieces at different indexes must not produce the same checksum, so a reordered
    // payload cannot silently match an earlier piece at another index.
    final long checksumOfPiece0 = LoadTsFileChecksumUtils.checksum(0L, Collections.emptyList());
    final long checksumOfPiece1 = LoadTsFileChecksumUtils.checksum(1L, Collections.emptyList());
    Assert.assertNotEquals(checksumOfPiece0, checksumOfPiece1);
  }
}
