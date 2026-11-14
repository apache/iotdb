/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.consensus.traft.paper.core;

import java.io.IOException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.RandomAccess;
import java.util.function.Supplier;

/** Wire and durable data shared by the protocol, transport and journal. */
public final class TRaftProtocol {
  private TRaftProtocol() {}

  public enum Kind {
    DATA,
    NOOP,
    BARRIER
  }

  public enum Role {
    FOLLOWER,
    CANDIDATE,
    LEADER_CANDIDATE,
    LEADER
  }

  public enum MessageType {
    VOTE_REQUEST,
    VOTE,
    REPORT_REQUEST,
    REPORT,
    ENTRY,
    ENTRY_ACK,
    PROPOSAL,
    PROPOSAL_ACK,
    FETCH_ENTRY,
    TRANSFER_ENTRY,
    FETCH_PROPOSAL,
    CONFIRM,
    COMMIT,
    NACK,
    TRANSFER_ACK
  }

  public enum RecordType {
    TERM_VOTE,
    ENTRY,
    RETAIN_PROPOSAL,
    INSTALL,
    COMMIT,
    APPLIED,
    /**
     * Compaction frontier: {@code frontier} is the highest dropped partition and {@code reference}
     * the opening proposal of the next one (the anchor). Appended last, so a binary that predates
     * it fails closed on decoding instead of misreading the journal.
     */
    COMPACT
  }

  public interface DurableStore {
    List<DurableRecord> load() throws IOException;

    /** Return only after the complete record is durable. Failure fences the engine. */
    void append(DurableRecord record) throws IOException;

    /**
     * Return only when every record is durable, in list order. Failure may leave a durable prefix;
     * callers must fence without emitting ACKs. This is a durability batch, not rollback atomicity.
     * The default preserves the append contract; stores may force several complete frames together.
     */
    default void appendBatch(List<DurableRecord> records) throws IOException {
      for (DurableRecord record : records) {
        append(record);
      }
    }

    /**
     * Write a record whose durability may be deferred until the next forced append, the store's
     * close, or a bounded runtime flush. The record must still become durable in journal order: a
     * later forced record is durable only together with every deferred record written before it.
     * The engine uses this only for installation markers of majority-confirmed proposals, whose
     * evidence (the forced retained proposal and payloads) already survives a crash; losing the
     * marker merely loses local confirmation knowledge, never acknowledged data. The default keeps
     * the forced contract.
     */
    default void appendDeferred(DurableRecord record) throws IOException {
      append(record);
    }

    /** Replace a durable payload with an indexed disk-backed handle if desired. */
    default Entry residentEntry(Entry entry) {
      return entry;
    }
  }

  public interface Application {
    /**
     * The adapter must make retries of this immutable identity logically idempotent. Besides the
     * engine's events, this also runs during engine construction: replaying a COMPACT record
     * applies, in partition order, the entries of the partitions it covers that the journal does
     * not show as applied, before their in-memory state is dropped.
     */
    void apply(Entry entry) throws IOException;

    /**
     * Whether successful applications need individual durable APPLIED records. An adapter may
     * return false only when startup reconstructs its state from a consistent snapshot and all
     * eligible committed entries, deduplicating snapshot effects by immutable entry identity.
     * Commitment must be established before application. This does not relax data or proposal ACK
     * durability and does not make arbitrary external side effects replayable. This contract is
     * fixed for the engine's lifetime.
     */
    default boolean requiresDurableAppliedRecords() {
      return true;
    }

    /**
     * Whether learned commitment needs an individual durable COMMIT record. False is allowed only
     * with requiresDurableAppliedRecords() also false and a snapshot/replay application. After a
     * restart, stored entries alone are not commitment evidence: the adapter restores its
     * consistent snapshot and any legacy durable commit knowledge, then waits for the current
     * leader's commit notifications or quorum recovery with a fresh current-term no-op before
     * applying the remaining entries. Payloads, votes, exact-prefix proposals and installed state
     * remain durable. This mode assumes fixed membership and keeps the complete history in the
     * journal. Compaction drops only in-memory state, and a COMPACT record is durable commit
     * evidence for the partitions it covers: construction applies them again from the journal
     * without waiting for a leader. Arbitrary external side effects remain unsupported. This
     * contract is fixed for the engine's lifetime.
     */
    default boolean requiresDurableCommitRecords() {
      return true;
    }
  }

  public static final class EntryId implements Comparable<EntryId> {
    public final long term;
    public final long index;

    public EntryId(long term, long index) {
      this.term = term;
      this.index = index;
    }

    @Override
    public int compareTo(EntryId o) {
      int c = Long.compare(index, o.index);
      return c != 0 ? c : Long.compare(term, o.term);
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof EntryId && term == ((EntryId) o).term && index == ((EntryId) o).index;
    }

    @Override
    public int hashCode() {
      // These identities are keys in the replication hot path. Preserve Objects.hash's result
      // without allocating a varargs array and boxing two longs for every lookup.
      return 31 * (31 + Long.hashCode(term)) + Long.hashCode(index);
    }

    @Override
    public String toString() {
      return term + ":" + index;
    }
  }

  /**
   * The identities (term, start) to (term, start + size - 1). An ordinary proposal's members are
   * such a run: within a term the Leader assigns consecutive indices and every new entry joins the
   * newest partition. Element identities are made on access; membership is O(1).
   */
  public static final class IdRange extends AbstractList<EntryId> implements RandomAccess {
    public final long term;
    public final long start;
    private final int size;

    public IdRange(long term, long start, int size) {
      if (size < 0 || (size > 0 && start - 1 > Long.MAX_VALUE - size)) {
        throw new IllegalArgumentException("invalid identity range");
      }
      this.term = term;
      this.start = start;
      this.size = size;
    }

    @Override
    public EntryId get(int i) {
      if (i < 0 || i >= size) {
        throw new IndexOutOfBoundsException("member " + i + " of " + size);
      }
      return new EntryId(term, start + i);
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public int indexOf(Object o) {
      if (!(o instanceof EntryId)) {
        return -1;
      }
      EntryId id = (EntryId) o;
      return id.term == term && id.index >= start && id.index - start < size
          ? (int) (id.index - start)
          : -1;
    }

    @Override
    public int lastIndexOf(Object o) {
      return indexOf(o);
    }

    @Override
    public boolean contains(Object o) {
      return indexOf(o) >= 0;
    }

    @Override
    public List<EntryId> subList(int from, int to) {
      if (from < 0 || to > size || from > to) {
        throw new IndexOutOfBoundsException("range " + from + ".." + to + " of " + size);
      }
      return new IdRange(term, start + from, to - from);
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof IdRange) {
        IdRange r = (IdRange) o;
        return size == r.size && (size == 0 || (term == r.term && start == r.start));
      }
      return super.equals(o);
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }
  }

  /** Loads the payload of a resident entry: one source serves every entry of a journal. */
  public interface PayloadSource {
    byte[] load(EntryId id, long locator);
  }

  /**
   * An immutable log entry. Its identity (creation term, index) determines its content: only the
   * Leader of a term creates entries of that term, each at a distinct index, so a payload never
   * needs to be compared again once its identity matches, as in Raft. Metadata-only copies (in
   * reports and manifests) carry the payload length but no bytes.
   */
  public static final class Entry {
    public final EntryId id;
    public final long partition;
    public final long timestamp;
    public final int prevSize;
    public final Kind kind;
    private final byte[] payload;
    private final int payloadLength;
    // A resident entry loads its payload from source by locator (a journal offset).
    private final PayloadSource source;
    private final long locator;
    private final boolean metadataOnly;

    public Entry(
        EntryId id, long partition, long timestamp, int prevSize, Kind kind, byte[] payload) {
      this(
          id,
          partition,
          timestamp,
          prevSize,
          kind,
          payload == null ? new byte[0] : payload.clone(),
          true);
    }

    /** Takes ownership of {@code payload}: the caller must not modify it afterwards. */
    public static Entry owning(
        EntryId id, long partition, long timestamp, int prevSize, Kind kind, byte[] payload) {
      return new Entry(id, partition, timestamp, prevSize, kind, payload, true);
    }

    private Entry(
        EntryId id,
        long partition,
        long timestamp,
        int prevSize,
        Kind kind,
        byte[] payload,
        boolean owned) {
      this.id = Objects.requireNonNull(id);
      this.partition = partition;
      this.timestamp = timestamp;
      this.prevSize = prevSize;
      this.kind = Objects.requireNonNull(kind);
      this.payload = Objects.requireNonNull(payload);
      this.payloadLength = payload.length;
      this.source = null;
      this.locator = 0;
      this.metadataOnly = false;
    }

    private Entry(Entry metadata, PayloadSource source, long locator) {
      this.id = metadata.id;
      this.partition = metadata.partition;
      this.timestamp = metadata.timestamp;
      this.prevSize = metadata.prevSize;
      this.kind = metadata.kind;
      this.payloadLength = metadata.payloadLength;
      this.payload = null;
      this.source = Objects.requireNonNull(source);
      this.locator = locator;
      this.metadataOnly = false;
    }

    private Entry(
        EntryId id, long partition, long timestamp, int prevSize, Kind kind, int payloadLength) {
      this.id = Objects.requireNonNull(id);
      this.partition = partition;
      this.timestamp = timestamp;
      this.prevSize = prevSize;
      this.kind = Objects.requireNonNull(kind);
      if (payloadLength < 0) {
        throw new IllegalArgumentException("invalid payload metadata");
      }
      this.payloadLength = payloadLength;
      this.payload = null;
      this.source = null;
      this.locator = 0;
      this.metadataOnly = true;
    }

    public static Entry metadata(Entry entry) {
      return entry.metadataOnly
          ? entry
          : metadata(
              entry.id,
              entry.partition,
              entry.timestamp,
              entry.prevSize,
              entry.kind,
              entry.payloadLength);
    }

    public static Entry metadata(
        EntryId id, long partition, long timestamp, int prevSize, Kind kind, int payloadLength) {
      return new Entry(id, partition, timestamp, prevSize, kind, payloadLength);
    }

    public boolean hasPayload() {
      return !metadataOnly;
    }

    /**
     * Lazily retrieves the immutable payload. The loader must always return exactly the durable
     * bytes for this identity; callers receive defensive copies.
     */
    public static Entry lazy(Entry metadata, Supplier<byte[]> loader) {
      Objects.requireNonNull(loader);
      return new Entry(metadata, (id, locator) -> loader.get(), 0);
    }

    /** As lazy, loading through a shared source: no loader object per entry. */
    public static Entry resident(Entry metadata, PayloadSource source, long locator) {
      return new Entry(metadata, source, locator);
    }

    public byte[] payload() {
      if (metadataOnly) {
        throw new IllegalStateException("entry payload has not been fetched");
      }
      byte[] bytes = source == null ? payload : source.load(id, locator);
      if (bytes == null || bytes.length != payloadLength) {
        throw new IllegalStateException("durable payload length changed");
      }
      return bytes.clone();
    }

    /**
     * The payload bytes without a defensive copy, for encoders that only read them. Callers must
     * not modify the returned array.
     */
    public byte[] payloadView() {
      if (metadataOnly) {
        throw new IllegalStateException("entry payload has not been fetched");
      }
      byte[] bytes = source == null ? payload : source.load(id, locator);
      if (bytes == null || bytes.length != payloadLength) {
        throw new IllegalStateException("durable payload length changed");
      }
      return bytes;
    }

    public int payloadSize() {
      return payloadLength;
    }

    /** Metadata equality: identity, placement and payload length. Content follows the identity. */
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Entry)) {
        return false;
      }
      Entry e = (Entry) o;
      return id.equals(e.id)
          && partition == e.partition
          && timestamp == e.timestamp
          && prevSize == e.prevSize
          && kind == e.kind
          && payloadLength == e.payloadLength;
    }

    @Override
    public int hashCode() {
      return id.hashCode();
    }
  }

  /**
   * An immutable exact-prefix manifest. Its prefix is prefix(base) plus members. Base identifies
   * the opening proposal of the partition being frozen. Its first entry is therefore included in
   * members. This retains a shared prefix chain without copying historical entry identities into
   * every new proposal.
   */
  public static final class Proposal {
    public final EntryId base;
    public final List<EntryId> members;
    public final Entry first;
    public final boolean recovery;

    public Proposal(EntryId base, Collection<EntryId> members, Entry first, boolean recovery) {
      this.base = base;
      this.members = ids(members);
      this.first = Objects.requireNonNull(first);
      this.recovery = recovery;
    }

    public EntryId id() {
      return first.id;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Proposal)) {
        return false;
      }
      Proposal p = (Proposal) o;
      return Objects.equals(base, p.base)
          && members.equals(p.members)
          && first.equals(p.first)
          && recovery == p.recovery;
    }

    @Override
    public int hashCode() {
      return first.id.hashCode();
    }
  }

  public static final class Rank implements Comparable<Rank> {
    public final long term;
    public final long partition;
    public final int count;

    public Rank(long term, long partition, int count) {
      this.term = term;
      this.partition = partition;
      this.count = count;
    }

    @Override
    public int compareTo(Rank o) {
      int c = Long.compare(term, o.term);
      if (c == 0) {
        c = Long.compare(partition, o.partition);
      }
      return c != 0 ? c : Integer.compare(count, o.count);
    }
  }

  public static final class Report {
    public final List<Entry> installed;
    public final List<Entry> stored;
    public final List<Proposal> retained;
    public final String generation;
    public final int pageIndex;
    public final int totalPages;
    public final List<ProposalSlice> proposalSlices;

    public Report(
        Collection<Entry> installed, Collection<Entry> stored, Collection<Proposal> retained) {
      this(installed, stored, retained, "", 0, 1, Collections.<ProposalSlice>emptyList());
    }

    public Report(
        Collection<Entry> installed,
        Collection<Entry> stored,
        Collection<Proposal> retained,
        String generation,
        int pageIndex,
        int totalPages,
        Collection<ProposalSlice> proposalSlices) {
      this.installed = immutable(installed);
      this.stored = immutable(stored);
      this.retained = immutable(retained);
      this.generation = Objects.requireNonNull(generation);
      this.pageIndex = pageIndex;
      this.totalPages = totalPages;
      this.proposalSlices = immutable(proposalSlices);
    }
  }

  /** One bounded member segment of a retained manifest in a fixed report snapshot. */
  public static final class ProposalSlice {
    public final EntryId base;
    public final Entry first;
    public final boolean recovery;
    public final int offset;
    public final int totalMembers;
    public final List<EntryId> members;

    public ProposalSlice(
        EntryId base,
        Entry first,
        boolean recovery,
        int offset,
        int totalMembers,
        Collection<EntryId> members) {
      this.base = base;
      this.first = Objects.requireNonNull(first);
      this.recovery = recovery;
      this.offset = offset;
      this.totalMembers = totalMembers;
      this.members = ids(members);
    }
  }

  /** Fields not relevant to type are null, empty or -1. Use the factories below. */
  public static final class Message {
    public final MessageType type;
    public final int from;
    public final int to;
    public final long term;
    public final Entry entry;
    public final Proposal proposal;
    public final EntryId reference;
    public final Rank rank;
    public final Report report;
    public final long frontier;
    public final List<EntryId> identities;

    public Message(
        MessageType type,
        int from,
        int to,
        long term,
        Entry entry,
        Proposal proposal,
        EntryId reference,
        Rank rank,
        Report report,
        long frontier,
        Collection<EntryId> identities) {
      this.type = type;
      this.from = from;
      this.to = to;
      this.term = term;
      this.entry = entry;
      this.proposal = proposal;
      this.reference = reference;
      this.rank = rank;
      this.report = report;
      this.frontier = frontier;
      this.identities = ids(identities);
    }
  }

  public static final class DurableRecord {
    public final RecordType type;
    public final long term;
    public final int vote;
    public final Entry entry;
    public final Proposal proposal;
    public final EntryId reference;
    public final long frontier;
    public final List<EntryId> identities;

    public DurableRecord(
        RecordType type,
        long term,
        int vote,
        Entry entry,
        Proposal proposal,
        EntryId reference,
        long frontier,
        Collection<EntryId> identities) {
      this.type = type;
      this.term = term;
      this.vote = vote;
      this.entry = entry;
      this.proposal = proposal;
      this.reference = reference;
      this.frontier = frontier;
      this.identities = ids(identities);
    }
  }

  /** As immutable; a run of consecutive indices of one term is kept as an IdRange. */
  private static List<EntryId> ids(Collection<EntryId> values) {
    if (values instanceof IdRange) {
      return (IdRange) values;
    }
    if (values == null || values.isEmpty()) {
      return Collections.emptyList();
    }
    EntryId first = null;
    long next = 0;
    for (EntryId id : values) {
      if (id == null || (first != null && (id.term != first.term || id.index != next))) {
        return Collections.unmodifiableList(new ArrayList<>(values));
      }
      if (first == null) {
        first = id;
        next = id.index;
      }
      next++;
    }
    return new IdRange(first.term, first.index, values.size());
  }

  private static <T> List<T> immutable(Collection<T> values) {
    return values == null
        ? Collections.<T>emptyList()
        : Collections.unmodifiableList(new ArrayList<>(values));
  }
}
