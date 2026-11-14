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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * In-memory mirror of the log: one slot per index (holes allowed), terms, types and protocol
 * metadata, a durable bit per slot, and the incrementally maintained contiguous frontier.
 *
 * <p>The durable contiguous end is the contiguous end, or the slot before the first held slot that
 * is not durable if that comes first; the held slots that are not durable are kept in a sorted set,
 * so that storing, overwriting and marking a slot cost O(log n) whatever its position. With a
 * presence-only chain validator ({@link ChainValidator#presenceOnly}) an overwrite inside the
 * contiguous frontier leaves the frontier where it is; with any other validator (TRaft's chained
 * frontier) it recomputes the frontier from the first index, as it always did.
 */
public final class LogIndex implements LogView {

  /** Notified after every structural change (TRaft's working-partition tracker subscribes). */
  public interface Listener {
    void onPut(LogIndex log, long index);

    /** Called before every slot with index >= from is removed. */
    void onRemoveFrom(LogIndex log, long from);

    void onPurgeBefore(LogIndex log, long newFirst);

    /** The copy slot {@code index} holds was marked durable (it was not). */
    default void onDurable(LogIndex log, long index) {}
  }

  private static final class Slot {
    final long term;
    final EntryType type;
    final EntryMeta meta;
    final int size;

    /** Which put stored this copy (see {@link #writeSeqAt}). */
    final long seq;

    boolean durable;

    Slot(long term, EntryType type, EntryMeta meta, int size, long seq, boolean durable) {
      this.term = term;
      this.type = type;
      this.meta = meta;
      this.size = size;
      this.seq = seq;
      this.durable = durable;
    }
  }

  private final TreeMap<Long, Slot> slots = new TreeMap<>();

  /** The held slots whose copy is not durable yet. */
  private final TreeSet<Long> notDurable = new TreeSet<>();

  private final List<Listener> listeners = new ArrayList<>();
  private ChainValidator chain;
  private long firstIndex = 0;
  private long snapshotTerm = 0;
  private long contiguousEnd = -1;
  private long putSeq;

  public LogIndex() {
    this(ChainValidator.CONTIGUOUS);
  }

  public LogIndex(ChainValidator chain) {
    this.chain = chain;
  }

  public void setChainValidator(ChainValidator chain) {
    this.chain = chain;
    recomputeFrontiers();
  }

  public void addListener(Listener l) {
    listeners.add(l);
  }

  public void removeListener(Listener l) {
    listeners.remove(l);
  }

  // ----------------------------------------------------------------- mutation

  public void put(LogEntry e, boolean durable) {
    put(e.index, e.term, e.type, e.meta, e.size(), durable);
  }

  public void put(
      long index, long term, EntryType type, EntryMeta meta, int size, boolean durable) {
    if (index < firstIndex) {
      throw new IllegalArgumentException("index " + index + " < firstIndex " + firstIndex);
    }
    slots.put(index, new Slot(term, type, meta, size, ++putSeq, durable));
    if (durable) {
      notDurable.remove(index);
    } else {
      notDurable.add(index);
    }
    if (index > contiguousEnd) {
      extendFrontiers();
    } else if (!chain.presenceOnly()) {
      recomputeFrontiers(); // overwrite inside the frontier: the new copy may break a chain link
    }
    // else: an overwrite inside the frontier of a presence-only chain moves no frontier (the
    // durable one follows the slot's new durable bit)
    for (Listener l : listeners) {
      l.onPut(this, index);
    }
  }

  public void markDurable(long index) {
    Slot s = slots.get(index);
    if (s != null && !s.durable) {
      s.durable = true;
      notDurable.remove(index);
      for (Listener l : listeners) {
        l.onDurable(this, index);
      }
    }
  }

  /** Remove every slot with index >= from. Returns the removed indexes (ascending). */
  public List<Long> removeFrom(long from) {
    List<Long> removed = new ArrayList<>(slots.tailMap(from, true).keySet());
    if (removed.isEmpty()) {
      return removed;
    }
    for (Listener l : listeners) {
      l.onRemoveFrom(this, from); // notified before the removal so that they can read the slots
    }
    slots.tailMap(from, true).clear();
    notDurable.tailSet(from, true).clear();
    if (contiguousEnd >= from) {
      contiguousEnd = from - 1;
    }
    return removed;
  }

  /** Drop every slot with index < newFirst and move the first index (after a snapshot). */
  public void purgeBefore(long newFirst, long snapshotTerm) {
    slots.headMap(newFirst, false).clear();
    notDurable.headSet(newFirst, false).clear();
    this.firstIndex = Math.max(firstIndex, newFirst);
    this.snapshotTerm = snapshotTerm;
    if (contiguousEnd < firstIndex - 1) {
      contiguousEnd = firstIndex - 1;
    }
    recomputeFrontiers();
    for (Listener l : listeners) {
      l.onPurgeBefore(this, newFirst);
    }
  }

  private void extendFrontiers() {
    while (slots.containsKey(contiguousEnd + 1)
        && chain.linked(this, contiguousEnd, contiguousEnd + 1)) {
      contiguousEnd++;
    }
  }

  private void recomputeFrontiers() {
    contiguousEnd = firstIndex - 1;
    extendFrontiers();
  }

  // --------------------------------------------------------------------- view

  @Override
  public long firstIndex() {
    return firstIndex;
  }

  @Override
  public long lastIndex() {
    return slots.isEmpty() ? firstIndex - 1 : slots.lastKey();
  }

  @Override
  public long contiguousEnd() {
    return contiguousEnd;
  }

  /**
   * The end of the durable contiguous prefix: the contiguous end, or the slot before the first held
   * slot that is not durable if that comes first.
   */
  @Override
  public long durableContiguousEnd() {
    if (notDurable.isEmpty()) {
      return contiguousEnd;
    }
    return Math.min(contiguousEnd, notDurable.first() - 1);
  }

  @Override
  public boolean has(long index) {
    return slots.containsKey(index);
  }

  private Slot slot(long index) {
    Slot s = slots.get(index);
    if (s == null) {
      throw new IllegalArgumentException("no slot " + index);
    }
    return s;
  }

  @Override
  public long termAt(long index) {
    if (index == firstIndex - 1) {
      return snapshotTerm;
    }
    return slot(index).term;
  }

  @Override
  public EntryType typeAt(long index) {
    return slot(index).type;
  }

  @Override
  public EntryMeta metaAt(long index) {
    return slot(index).meta;
  }

  @Override
  public int sizeAt(long index) {
    return slot(index).size;
  }

  /**
   * The sequence number of the put that stored the copy {@code index} holds now (-1 if none): every
   * put, also one of an identical entry, gets a new one. A write made durable marks the slot only
   * if the slot still holds the copy that write stored: the slot may have been cut and stored again
   * since, by a write still pending.
   */
  public long writeSeqAt(long index) {
    Slot s = slots.get(index);
    return s == null ? -1 : s.seq;
  }

  @Override
  public boolean isDurable(long index) {
    Slot s = slots.get(index);
    return s != null && s.durable;
  }

  @Override
  public NavigableSet<Long> slots() {
    return Collections.unmodifiableNavigableSet(slots.navigableKeySet());
  }

  @Override
  public long snapshotTerm() {
    return snapshotTerm;
  }

  public int size() {
    return slots.size();
  }

  @Override
  public String toString() {
    StringBuilder sb =
        new StringBuilder("Log[first=")
            .append(firstIndex)
            .append(" ce=")
            .append(contiguousEnd)
            .append(" slots=");
    for (java.util.Map.Entry<Long, Slot> en : slots.entrySet()) {
      sb.append('e').append(en.getKey()).append("(t").append(en.getValue().term).append(')');
    }
    return sb.append(']').toString();
  }
}
