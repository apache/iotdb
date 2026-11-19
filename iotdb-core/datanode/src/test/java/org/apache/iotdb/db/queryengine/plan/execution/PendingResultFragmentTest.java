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

package org.apache.iotdb.db.queryengine.plan.execution;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class PendingResultFragmentTest {

  @Test
  public void testReserveWhenBlockBecomesPendingAndReleaseOnClear() {
    RecordingAccount account = new RecordingAccount();
    PendingResultFragment fragment = new PendingResultFragment(account);
    ByteBuffer block = ByteBuffer.allocate(100);

    fragment.reset(block, 30);
    assertEquals(List.of(100L), account.reserved);
    assertEquals(0, account.freed);

    // continuing the same block must not reserve again
    fragment.reset(block, 60);
    assertEquals(List.of(100L), account.reserved);
    assertSame(block, fragment.getBlock());
    assertEquals(60, fragment.getNextOffset());

    fragment.clear();
    assertEquals(100L, account.freed);
    assertEquals(0L, account.current);
    assertEquals(null, fragment.getBlock());

    // repeated clear is a no-op
    fragment.clear();
    assertEquals(100L, account.freed);
  }

  @Test
  public void testNewBlockReplacesPendingOne() {
    RecordingAccount account = new RecordingAccount();
    PendingResultFragment fragment = new PendingResultFragment(account);

    fragment.reset(ByteBuffer.allocate(100), 30);
    fragment.reset(ByteBuffer.allocate(50), 10);

    // the first reservation is released, the second one is taken
    assertEquals(List.of(100L, 50L), account.reserved);
    assertEquals(100L, account.freed);
    assertEquals(50L, account.current);
  }

  @Test
  public void testNullAccountDisablesAccounting() {
    PendingResultFragment fragment = new PendingResultFragment(null);
    fragment.reset(ByteBuffer.allocate(100), 30);
    fragment.clear();
    assertEquals(null, fragment.getBlock());
  }

  private static final class RecordingAccount implements PendingResultFragment.Account {

    private final List<Long> reserved = new ArrayList<>();
    private long freed;
    private long current;

    @Override
    public long reserve(long bytes) {
      reserved.add(bytes);
      current += bytes;
      return bytes;
    }

    @Override
    public void free(long bytes) {
      freed += bytes;
      current -= bytes;
    }
  }
}
