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

package org.apache.iotdb.consensus.raft.core.log;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LogIndexTest {

  private static LogEntry e(long index, long term) {
    return new LogEntry(index, term, EntryType.DATA, EntryMeta.EMPTY, ByteBuffer.allocate(3));
  }

  @Test
  public void contiguousFrontierSkipsHoles() {
    LogIndex log = new LogIndex();
    assertEquals(-1, log.contiguousEnd());
    assertEquals(-1, log.lastIndex());
    log.put(e(0, 1), true);
    log.put(e(2, 1), true); // hole at 1
    assertEquals(0, log.contiguousEnd());
    assertEquals(2, log.lastIndex());
    assertTrue(log.has(2));
    assertFalse(log.has(1));
    log.put(e(1, 1), true); // hole filled: frontier jumps to 2
    assertEquals(2, log.contiguousEnd());
    assertEquals(Arrays.asList(0L, 1L, 2L), new ArrayList<>(log.slots()));
  }

  @Test
  public void durableFrontierLagsUntilMarked() {
    LogIndex log = new LogIndex();
    log.put(e(0, 1), true);
    log.put(e(1, 1), false);
    assertEquals(1, log.contiguousEnd());
    assertEquals(0, log.durableContiguousEnd());
    log.markDurable(1);
    assertEquals(1, log.durableContiguousEnd());
  }

  @Test
  public void removeFromTruncatesSuffixAndFrontier() {
    LogIndex log = new LogIndex();
    for (int i = 0; i < 5; i++) {
      log.put(e(i, 1), true);
    }
    List<Long> removed = log.removeFrom(2);
    assertEquals(Arrays.asList(2L, 3L, 4L), removed);
    assertEquals(1, log.contiguousEnd());
    assertEquals(1, log.lastIndex());
    assertTrue(log.removeFrom(10).isEmpty());
  }

  @Test
  public void overwriteSameIndexInsideFrontierKeepsFrontier() {
    LogIndex log = new LogIndex();
    log.put(e(0, 1), true);
    log.put(e(1, 1), true);
    log.put(e(1, 2), true);
    assertEquals(2, log.termAt(1));
    assertEquals(1, log.contiguousEnd());
  }

  @Test
  public void purgeBeforeMovesFirstIndexAndSnapshotTerm() {
    LogIndex log = new LogIndex();
    for (int i = 0; i < 4; i++) {
      log.put(e(i, 3), true);
    }
    log.purgeBefore(2, 3);
    assertEquals(2, log.firstIndex());
    assertEquals(3, log.termAt(1)); // snapshot term for firstIndex-1
    assertEquals(3, log.contiguousEnd());
    assertFalse(log.has(1));
    log.removeFrom(2);
    assertEquals(1, log.contiguousEnd());
    assertEquals(1, log.lastIndex());
  }

  @Test
  public void chainValidatorCanStopTheFrontier() {
    LogIndex log = new LogIndex((view, prev, next) -> view.termAt(next) >= view.termAt(prev));
    log.put(e(0, 2), true);
    log.put(e(1, 1), true); // term goes backwards: not chained
    log.put(e(2, 3), true);
    assertEquals(0, log.contiguousEnd());
    log.setChainValidator(ChainValidator.CONTIGUOUS);
    assertEquals(2, log.contiguousEnd());
  }

  /**
   * B3: overwriting a slot inside the contiguous frontier (a dated overwrite, a recovery merge)
   * used to recompute both frontiers from the first index, O(log size) per overwrite. With a
   * presence-only chain it is O(1): the frontier stays, the durable frontier drops below the slot
   * while its new copy is not durable and comes back once it is.
   */
  @Test
  public void overwritingASlotInsideTheFrontierDoesNotRescanTheLog() {
    int[] links = {0};
    ChainValidator counting =
        new ChainValidator() {
          @Override
          public boolean linked(LogView view, long prev, long next) {
            links[0]++;
            return true;
          }

          @Override
          public boolean presenceOnly() {
            return true;
          }
        };
    LogIndex log = new LogIndex(counting);
    int n = 100_000;
    for (int i = 0; i < n; i++) {
      log.put(e(i, 1), true);
    }
    assertEquals(n - 1, log.contiguousEnd());
    links[0] = 0;
    log.put(e(5, 1), false); // a new copy of e5, not durable yet
    log.put(e(7, 1), false);
    assertEquals(0, links[0]);
    assertEquals(n - 1, log.contiguousEnd());
    assertEquals(4, log.durableContiguousEnd());
    log.markDurable(5);
    assertEquals(6, log.durableContiguousEnd());
    log.markDurable(7);
    assertEquals(n - 1, log.durableContiguousEnd());
    log.put(e(9, 1), true); // a durable copy keeps the durable frontier
    assertEquals(n - 1, log.durableContiguousEnd());
    assertEquals(0, links[0]);
    log.removeFrom(6); // the cut takes the frontiers with it
    assertEquals(5, log.contiguousEnd());
    assertEquals(5, log.durableContiguousEnd());
  }

  /** A validator that reads the slots (TRaft's chain) still recomputes the frontier, as before. */
  @Test
  public void overwritingInsideTheFrontierStillRechecksAContentChain() {
    LogIndex log = new LogIndex((view, prev, next) -> view.termAt(next) >= view.termAt(prev));
    for (int i = 0; i < 4; i++) {
      log.put(e(i, 2), true);
    }
    assertEquals(3, log.contiguousEnd());
    log.put(e(1, 1), true); // term 2 -> term 1: the chain breaks after e0
    assertEquals(0, log.contiguousEnd());
    log.put(e(1, 2), false);
    assertEquals(3, log.contiguousEnd());
    assertEquals(0, log.durableContiguousEnd());
    log.markDurable(1);
    assertEquals(3, log.durableContiguousEnd());
  }

  @Test
  public void listenersSeePutsAndRemovals() {
    LogIndex log = new LogIndex();
    List<String> seen = new ArrayList<>();
    log.addListener(
        new LogIndex.Listener() {
          @Override
          public void onPut(LogIndex l, long index) {
            seen.add("put" + index);
          }

          @Override
          public void onRemoveFrom(LogIndex l, long from) {
            seen.add("rm" + from);
          }

          @Override
          public void onPurgeBefore(LogIndex l, long newFirst) {
            seen.add("purge" + newFirst);
          }
        });
    log.put(e(0, 1), true);
    log.put(e(1, 1), true);
    log.removeFrom(1);
    log.purgeBefore(1, 1);
    assertEquals(Arrays.asList("put0", "put1", "rm1", "purge1"), seen);
  }
}
