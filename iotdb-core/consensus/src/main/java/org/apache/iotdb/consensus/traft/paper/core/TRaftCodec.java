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

import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.DurableRecord;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Entry;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.EntryId;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.IdRange;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Kind;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Message;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.MessageType;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Proposal;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.ProposalSlice;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Rank;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.RecordType;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Report;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Explicit bounded versioned encoding; no Java object deserialization.
 *
 * <p>Version 4 is compact: identities, partitions, counts and lengths are unsigned LEB128 varints;
 * terms, votes, frontiers, ranks and timestamps are zigzag varints; optional fields are announced
 * in one flags byte instead of a presence byte each. An entry is a kind/payload flags byte, its
 * identity, partition, prevSize, timestamp and payload length, then the payload bytes. Content is
 * bound to the identity alone (one Leader per term creates each index), so no digest is carried.
 */
public final class TRaftCodec {
  private static final int VERSION = 5;
  private static final int MAX_FRAME = 256 * 1024 * 1024;
  private static final int MAX_ITEMS = 1_000_000;
  // Upper bounds of one varint-encoded identity and of an entry's metadata.
  private static final long ID_BOUND = 20;
  private static final long ENTRY_META_BOUND = 64;

  /** Report page budget: one metadata entry, one slice header, and one slice member. */
  public static final int REPORT_ENTRY_BOUND = (int) ENTRY_META_BOUND;

  public static final int REPORT_SLICE_BOUND = 64 + (int) (ENTRY_META_BOUND + ID_BOUND);
  public static final int REPORT_MEMBER_BOUND = (int) ID_BOUND;

  // Message flags.
  private static final int M_ENTRY = 1;
  private static final int M_PROPOSAL = 1 << 1;
  private static final int M_REFERENCE = 1 << 2;
  private static final int M_RANK = 1 << 3;
  private static final int M_REPORT = 1 << 4;
  private static final int M_IDENTITIES = 1 << 5;
  // Record flags.
  private static final int R_ENTRY = 1;
  private static final int R_PROPOSAL = 1 << 1;
  private static final int R_REFERENCE = 1 << 2;
  private static final int R_IDENTITIES = 1 << 3;
  // Entry flags: the low two bits are the kind.
  private static final int E_KIND = 3;
  private static final int E_PAYLOAD = 1 << 2;
  // Proposal flags. P_CANONICAL: base and members follow from first (canonical). P_FIRST: first is
  // the metadata of the entry written just before the proposal, in the same message or record.
  private static final int P_BASE = 1;
  private static final int P_RECOVERY = 1 << 1;
  private static final int P_CANONICAL = 1 << 2;
  private static final int P_FIRST = 1 << 3;

  private static final MessageType[] MESSAGE_TYPES = MessageType.values();
  private static final RecordType[] RECORD_TYPES = RecordType.values();
  private static final Kind[] KINDS = Kind.values();
  // Per-thread encode buffer: the result is copied out, so the next encode reuses it.
  private static final ThreadLocal<byte[]> SCRATCH =
      ThreadLocal.withInitial(() -> new byte[16 << 10]);

  private TRaftCodec() {}

  /** Maximum complete encoded message size, including payload and all control metadata. */
  public static int maxMessageBytes() {
    return MAX_FRAME;
  }

  /** Conservative wire budget without loading any lazy payload. */
  public static long encodedMessageUpperBound(Message message) {
    long size =
        64L
            + entryUpperBound(message.entry, false)
            + proposalUpperBound(message.proposal)
            + ID_BOUND
            + idsUpperBound(message.identities);
    Report report = message.report;
    if (report != null) {
      size += 64L + 3L * report.generation.length();
      size += REPORT_ENTRY_BOUND * (report.installed.size() + (long) report.stored.size());
      for (Proposal proposal : report.retained) {
        size += proposalUpperBound(proposal);
      }
      for (ProposalSlice slice : report.proposalSlices) {
        // A range costs one member whatever its length.
        size +=
            REPORT_SLICE_BOUND
                + REPORT_MEMBER_BOUND
                    * (slice.members instanceof IdRange ? 1L : slice.members.size());
      }
    }
    return size;
  }

  private static long entryUpperBound(Entry entry, boolean metadataOnly) {
    return entry == null
        ? 1
        : ENTRY_META_BOUND + (!metadataOnly && entry.hasPayload() ? entry.payloadSize() : 0);
  }

  private static long proposalUpperBound(Proposal proposal) {
    return proposal == null
        ? 1
        : 16L + ID_BOUND + idsUpperBound(proposal.members) + ENTRY_META_BOUND;
  }

  /** Without the count, which the callers' fixed allowances cover. */
  private static long idsUpperBound(List<EntryId> ids) {
    return ids instanceof IdRange ? ID_BOUND : ID_BOUND * (long) ids.size();
  }

  public static byte[] encodeMessage(Message m) throws IOException {
    return encodeMessage(m, new HashMap<EntryId, byte[]>());
  }

  /** Share this short-lived map across one drain/scheduling round, then release it. */
  public static byte[] encodeMessage(Message m, Map<EntryId, byte[]> roundPayloads)
      throws IOException {
    Out out = new Out(encodedMessageUpperBound(m));
    out.writeByte(VERSION);
    out.writeByte(m.type.ordinal());
    int flags =
        (m.entry != null ? M_ENTRY : 0)
            | (m.proposal != null ? M_PROPOSAL : 0)
            | (m.reference != null ? M_REFERENCE : 0)
            | (m.rank != null ? M_RANK : 0)
            | (m.report != null ? M_REPORT : 0)
            | (!m.identities.isEmpty() ? M_IDENTITIES : 0);
    out.writeByte(flags);
    out.writeSigned(m.from);
    out.writeSigned(m.to);
    out.writeSigned(m.term);
    out.writeSigned(m.frontier);
    if (m.entry != null) {
      writeEntry(out, m.entry, roundPayloads, true);
    }
    if (m.proposal != null) {
      writeProposal(out, m.proposal, roundPayloads, true, m.entry);
    }
    if (m.reference != null) {
      writeId(out, m.reference);
    }
    if (m.rank != null) {
      out.writeSigned(m.rank.term);
      out.writeSigned(m.rank.partition);
      out.writeSigned(m.rank.count);
    }
    if (m.report != null) {
      writeReport(out, m.report, roundPayloads);
    }
    if (!m.identities.isEmpty()) {
      writeIds(out, m.identities);
    }
    return finish(out);
  }

  public static Message decodeMessage(byte[] bytes) throws IOException {
    In in = input(bytes);
    MessageType type = enumValue(MESSAGE_TYPES, in.readUnsignedByte());
    int flags = in.readUnsignedByte();
    if ((flags & ~(M_ENTRY | M_PROPOSAL | M_REFERENCE | M_RANK | M_REPORT | M_IDENTITIES)) != 0) {
      throw new IOException("unknown message flags");
    }
    int from = intValue(in.readSigned());
    int to = intValue(in.readSigned());
    long term = in.readSigned();
    long frontier = in.readSigned();
    Entry entry = (flags & M_ENTRY) != 0 ? readEntry(in) : null;
    Proposal proposal = (flags & M_PROPOSAL) != 0 ? readProposal(in, entry) : null;
    EntryId reference = (flags & M_REFERENCE) != 0 ? readId(in) : null;
    Rank rank =
        (flags & M_RANK) != 0
            ? new Rank(in.readSigned(), in.readSigned(), intValue(in.readSigned()))
            : null;
    Report report = (flags & M_REPORT) != 0 ? readReport(in) : null;
    List<EntryId> identities = (flags & M_IDENTITIES) != 0 ? readIds(in) : null;
    end(in);
    return new Message(
        type, from, to, term, entry, proposal, reference, rank, report, frontier, identities);
  }

  public static byte[] encodeRecord(DurableRecord r) throws IOException {
    Out out = new Out(recordUpperBound(r));
    out.writeByte(VERSION);
    out.writeByte(r.type.ordinal());
    int flags =
        (r.entry != null ? R_ENTRY : 0)
            | (r.proposal != null ? R_PROPOSAL : 0)
            | (r.reference != null ? R_REFERENCE : 0)
            | (!r.identities.isEmpty() ? R_IDENTITIES : 0);
    out.writeByte(flags);
    out.writeSigned(r.term);
    out.writeSigned(r.vote);
    out.writeSigned(r.frontier);
    // A record writes at most one payload: nothing to share.
    Map<EntryId, byte[]> payloads = null;
    if (r.entry != null) {
      writeEntry(out, r.entry, payloads, true);
    }
    if (r.proposal != null) {
      writeProposal(out, r.proposal, payloads, false, r.entry);
    }
    if (r.reference != null) {
      writeId(out, r.reference);
    }
    if (!r.identities.isEmpty()) {
      writeIds(out, r.identities);
    }
    return finish(out);
  }

  /** Whether the first three bytes of an encoded record begin an ENTRY record with a proposal. */
  public static boolean isEntryRecordWithProposal(byte[] head) {
    return head.length >= 3
        && (head[0] & 0xff) == VERSION
        && (head[1] & 0xff) == RecordType.ENTRY.ordinal()
        && (head[2] & R_PROPOSAL) != 0;
  }

  public static DurableRecord decodeRecord(byte[] bytes) throws IOException {
    In in = input(bytes);
    RecordType type = enumValue(RECORD_TYPES, in.readUnsignedByte());
    int flags = in.readUnsignedByte();
    if ((flags & ~(R_ENTRY | R_PROPOSAL | R_REFERENCE | R_IDENTITIES)) != 0) {
      throw new IOException("unknown record flags");
    }
    long term = in.readSigned();
    int vote = intValue(in.readSigned());
    long frontier = in.readSigned();
    Entry entry = (flags & R_ENTRY) != 0 ? readEntry(in) : null;
    Proposal proposal = (flags & R_PROPOSAL) != 0 ? readProposal(in, entry) : null;
    EntryId reference = (flags & R_REFERENCE) != 0 ? readId(in) : null;
    List<EntryId> identities = (flags & R_IDENTITIES) != 0 ? readIds(in) : null;
    end(in);
    return new DurableRecord(type, term, vote, entry, proposal, reference, frontier, identities);
  }

  private static long recordUpperBound(DurableRecord r) {
    return 48L
        + entryUpperBound(r.entry, false)
        + (r.proposal == null
            ? 1
            : proposalUpperBound(r.proposal) + entryUpperBound(r.proposal.first, false))
        + ID_BOUND
        + idsUpperBound(r.identities);
  }

  private static byte[] finish(Out out) throws IOException {
    if (out.size > MAX_FRAME) {
      throw new IOException("TRaft frame too large");
    }
    return out.toByteArray();
  }

  private static In input(byte[] bytes) throws IOException {
    if (bytes.length > MAX_FRAME) {
      throw new IOException("TRaft frame too large");
    }
    In in = new In(bytes);
    if (in.readUnsignedByte() != VERSION) {
      throw new IOException("unsupported TRaft encoding version");
    }
    return in;
  }

  private static void end(In in) throws IOException {
    if (in.available() != 0) {
      throw new IOException("trailing TRaft frame bytes");
    }
  }

  private static <T> T enumValue(T[] values, int index) throws IOException {
    if (index >= values.length) {
      throw new IOException("unknown enum value");
    }
    return values[index];
  }

  private static int intValue(long value) throws IOException {
    if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
      throw new IOException("integer field out of range");
    }
    return (int) value;
  }

  /** A collection length: at most MAX_ITEMS, and at most one item per remaining byte. */
  private static int count(In in) throws IOException {
    long count = in.readUnsigned();
    if (count < 0 || count > MAX_ITEMS || count > in.available()) {
      throw new IOException("invalid collection length");
    }
    return (int) count;
  }

  private static void writeId(Out out, EntryId id) throws IOException {
    out.writeUnsigned(id.term);
    out.writeUnsigned(id.index);
  }

  private static EntryId readId(In in) throws IOException {
    long term = in.readUnsigned();
    long index = in.readUnsigned();
    if (term <= 0 || index <= 0) {
      throw new IOException("invalid entry identity");
    }
    return new EntryId(term, index);
  }

  /** The count shifted left by one; a set low bit marks a range, written as term and start. */
  private static void writeIds(Out out, Collection<EntryId> ids) throws IOException {
    if (ids instanceof IdRange) {
      IdRange range = (IdRange) ids;
      out.writeUnsigned(((long) range.size() << 1) | 1);
      out.writeUnsigned(range.term);
      out.writeUnsigned(range.start);
      return;
    }
    out.writeUnsigned((long) ids.size() << 1);
    for (EntryId id : ids) {
      writeId(out, id);
    }
  }

  private static List<EntryId> readIds(In in) throws IOException {
    long header = in.readUnsigned();
    long size = header >>> 1;
    if ((header & 1) != 0) {
      long term = in.readUnsigned();
      long start = in.readUnsigned();
      if (size < 1
          || size > MAX_ITEMS
          || term <= 0
          || start <= 0
          || start - 1 > Long.MAX_VALUE - size) {
        throw new IOException("invalid identity range");
      }
      return new IdRange(term, start, (int) size);
    }
    if (size > MAX_ITEMS || size > in.available()) {
      throw new IOException("invalid collection length");
    }
    List<EntryId> ids = new ArrayList<>((int) size);
    for (int i = 0; i < size; i++) {
      ids.add(readId(in));
    }
    return ids;
  }

  /** {@code withPayload} false writes the metadata of an entry that carries a payload. */
  private static void writeEntry(
      Out out, Entry e, Map<EntryId, byte[]> payloads, boolean withPayload) throws IOException {
    boolean carries = withPayload && e.hasPayload();
    out.writeByte(e.kind.ordinal() | (carries ? E_PAYLOAD : 0));
    writeId(out, e.id);
    out.writeUnsigned(e.partition);
    out.writeUnsigned(e.prevSize);
    out.writeSigned(e.timestamp);
    out.writeUnsigned(e.payloadSize());
    if (carries) {
      byte[] payload = payloads == null ? null : payloads.get(e.id);
      if (payload == null) {
        payload = e.payloadView();
        if (payloads != null) {
          payloads.put(e.id, payload);
        }
      }
      out.write(payload);
    }
  }

  private static Entry readEntry(In in) throws IOException {
    int flags = in.readUnsignedByte();
    if ((flags & ~(E_KIND | E_PAYLOAD)) != 0) {
      throw new IOException("unknown entry flags");
    }
    Kind kind = enumValue(KINDS, flags & E_KIND);
    EntryId id = readId(in);
    long part = in.readUnsigned();
    long prevSize = in.readUnsigned();
    long timestamp = in.readSigned();
    long length = in.readUnsigned();
    if (part < 0
        || prevSize < 0
        || prevSize > Integer.MAX_VALUE
        || length < 0
        || length > MAX_FRAME) {
      throw new IOException("invalid entry encoding");
    }
    if (kind == Kind.NOOP && length != 0) {
      throw new IOException("no-op contains payload");
    }
    if ((flags & E_PAYLOAD) == 0) {
      return Entry.metadata(id, part, timestamp, (int) prevSize, kind, (int) length);
    }
    if (length > in.available()) {
      throw new IOException("truncated entry payload");
    }
    byte[] payload = new byte[(int) length];
    in.readFully(payload);
    return Entry.owning(id, part, timestamp, (int) prevSize, kind, payload);
  }

  /**
   * {@code metadataOnly} sends the first entry's metadata instead of its payload. {@code enclosing}
   * is the entry written just before the proposal, or null.
   */
  private static void writeProposal(
      Out out, Proposal p, Map<EntryId, byte[]> payloads, boolean metadataOnly, Entry enclosing)
      throws IOException {
    boolean canonical = canonical(p);
    boolean sameFirst =
        enclosing != null && (metadataOnly || !p.first.hasPayload()) && p.first.equals(enclosing);
    out.writeByte(
        (p.base != null && !canonical ? P_BASE : 0)
            | (p.recovery ? P_RECOVERY : 0)
            | (canonical ? P_CANONICAL : 0)
            | (sameFirst ? P_FIRST : 0));
    if (!canonical) {
      if (p.base != null) {
        writeId(out, p.base);
      }
      writeIds(out, p.members);
    }
    if (!sameFirst) {
      writeEntry(out, p.first, payloads, !metadataOnly);
    }
  }

  /**
   * An ordinary proposal: its members are the first.prevSize identities just below first, in
   * first's term, and its base is the lowest of them.
   */
  private static boolean canonical(Proposal p) {
    int size = p.first.prevSize;
    long start = p.first.id.index - size;
    return size > 0
        && start > 0
        && p.base != null
        && p.base.term == p.first.id.term
        && p.base.index == start
        && p.members.equals(new IdRange(p.first.id.term, start, size));
  }

  private static Proposal readProposal(In in, Entry enclosing) throws IOException {
    int flags = in.readUnsignedByte();
    if ((flags & ~(P_BASE | P_RECOVERY | P_CANONICAL | P_FIRST)) != 0
        || (flags & (P_BASE | P_CANONICAL)) == (P_BASE | P_CANONICAL)) {
      throw new IOException("unknown proposal flags");
    }
    boolean canonical = (flags & P_CANONICAL) != 0;
    EntryId base = (flags & P_BASE) != 0 ? readId(in) : null;
    List<EntryId> members = canonical ? null : readIds(in);
    Entry first;
    if ((flags & P_FIRST) != 0) {
      if (enclosing == null) {
        throw new IOException("proposal first without an enclosing entry");
      }
      first = Entry.metadata(enclosing);
    } else {
      first = readEntry(in);
    }
    if (canonical) {
      long start = first.id.index - first.prevSize;
      if (first.prevSize <= 0 || start <= 0) {
        throw new IOException("invalid canonical proposal");
      }
      base = new EntryId(first.id.term, start);
      members = new IdRange(first.id.term, start, first.prevSize);
    }
    return new Proposal(base, members, first, (flags & P_RECOVERY) != 0);
  }

  private static void writeReport(Out out, Report report, Map<EntryId, byte[]> payloads)
      throws IOException {
    out.writeUTF(report.generation);
    out.writeUnsigned(report.pageIndex);
    out.writeUnsigned(report.totalPages);
    out.writeUnsigned(report.installed.size());
    for (Entry e : report.installed) {
      writeEntry(out, e, payloads, false);
    }
    out.writeUnsigned(report.stored.size());
    for (Entry e : report.stored) {
      writeEntry(out, e, payloads, false);
    }
    out.writeUnsigned(report.retained.size());
    for (Proposal p : report.retained) {
      writeProposal(out, p, payloads, true, null);
    }
    out.writeUnsigned(report.proposalSlices.size());
    for (ProposalSlice slice : report.proposalSlices) {
      out.writeByte((slice.base != null ? P_BASE : 0) | (slice.recovery ? P_RECOVERY : 0));
      if (slice.base != null) {
        writeId(out, slice.base);
      }
      writeEntry(out, slice.first, payloads, false);
      out.writeUnsigned(slice.offset);
      out.writeUnsigned(slice.totalMembers);
      writeIds(out, slice.members);
    }
  }

  private static Report readReport(In in) throws IOException {
    String generation = in.readUTF();
    long page = in.readUnsigned();
    long totalPages = in.readUnsigned();
    if (generation.length() > 64
        || page < 0
        || totalPages < 1
        || page >= totalPages
        || totalPages > Integer.MAX_VALUE) {
      throw new IOException("invalid report page header");
    }
    List<Entry> installed = readEntries(in);
    List<Entry> stored = readEntries(in);
    int size = count(in);
    List<Proposal> proposals = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      proposals.add(readProposal(in, null));
    }
    int slicesSize = count(in);
    List<ProposalSlice> slices = new ArrayList<>(slicesSize);
    for (int i = 0; i < slicesSize; i++) {
      int flags = in.readUnsignedByte();
      if ((flags & ~(P_BASE | P_RECOVERY)) != 0) {
        throw new IOException("unknown proposal slice flags");
      }
      EntryId base = (flags & P_BASE) != 0 ? readId(in) : null;
      Entry first = readEntry(in);
      long offset = in.readUnsigned();
      long total = in.readUnsigned();
      List<EntryId> ids = readIds(in);
      if (offset < 0
          || total < 0
          || total > Integer.MAX_VALUE
          || offset > total
          || ids.size() > total - offset) {
        throw new IOException("invalid proposal slice");
      }
      slices.add(
          new ProposalSlice(
              base, first, (flags & P_RECOVERY) != 0, (int) offset, (int) total, ids));
    }
    return new Report(
        installed, stored, proposals, generation, (int) page, (int) totalPages, slices);
  }

  private static List<Entry> readEntries(In in) throws IOException {
    int size = count(in);
    List<Entry> entries = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      entries.add(readEntry(in));
    }
    return entries;
  }

  /** Growable buffer; the capacity hint is an upper bound when known. */
  private static final class Out {
    private byte[] buf;
    private int size;

    Out(long capacityHint) {
      byte[] scratch = SCRATCH.get();
      buf =
          capacityHint <= scratch.length
              ? scratch
              : new byte[(int) Math.min(capacityHint, 1 << 20)];
    }

    private void ensure(int extra) throws IOException {
      long need = (long) size + extra;
      if (need > MAX_FRAME) {
        throw new IOException("TRaft frame too large");
      }
      if (need > buf.length) {
        buf = Arrays.copyOf(buf, (int) Math.min(MAX_FRAME, Math.max(need, 2L * buf.length)));
      }
    }

    void writeByte(int value) throws IOException {
      ensure(1);
      buf[size++] = (byte) value;
    }

    /** Unsigned LEB128 of a non-negative value. */
    void writeUnsigned(long value) throws IOException {
      if (value < 0) {
        throw new IOException("negative varint field");
      }
      writeRaw(value);
    }

    /** Zigzag LEB128: small magnitudes of either sign take few bytes. */
    void writeSigned(long value) throws IOException {
      writeRaw((value << 1) ^ (value >> 63));
    }

    private void writeRaw(long value) throws IOException {
      ensure(10);
      while ((value & ~0x7FL) != 0) {
        buf[size++] = (byte) ((value & 0x7F) | 0x80);
        value >>>= 7;
      }
      buf[size++] = (byte) value;
    }

    void write(byte[] bytes) throws IOException {
      ensure(bytes.length);
      System.arraycopy(bytes, 0, buf, size, bytes.length);
      size += bytes.length;
    }

    /** Modified UTF-8 with a two-byte length; only recovery report generations use it. */
    void writeUTF(String value) throws IOException {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      new DataOutputStream(bytes).writeUTF(value);
      write(bytes.toByteArray());
    }

    byte[] toByteArray() {
      return Arrays.copyOf(buf, size);
    }
  }

  /** Bounds-checked reader; reading past the end raises EOFException. */
  private static final class In {
    private final byte[] buf;
    private int pos;

    In(byte[] buf) {
      this.buf = buf;
    }

    int available() {
      return buf.length - pos;
    }

    private void require(int count) throws EOFException {
      if (buf.length - pos < count) {
        throw new EOFException();
      }
    }

    int readUnsignedByte() throws IOException {
      require(1);
      return buf[pos++] & 0xff;
    }

    /** Unsigned LEB128, at most ten bytes; a value above Long.MAX_VALUE reads as negative. */
    long readUnsigned() throws IOException {
      long value = 0;
      for (int shift = 0; shift < 70; shift += 7) {
        int b = readUnsignedByte();
        if (shift == 63 && (b & 0x7E) != 0) {
          throw new IOException("varint overflow");
        }
        value |= (long) (b & 0x7F) << shift;
        if ((b & 0x80) == 0) {
          return value;
        }
      }
      throw new IOException("varint too long");
    }

    long readSigned() throws IOException {
      long raw = readUnsigned();
      return (raw >>> 1) ^ -(raw & 1);
    }

    void readFully(byte[] target) throws IOException {
      require(target.length);
      System.arraycopy(buf, pos, target, 0, target.length);
      pos += target.length;
    }

    String readUTF() throws IOException {
      require(2);
      int length = ((buf[pos] & 0xff) << 8) | (buf[pos + 1] & 0xff);
      require(2 + length);
      String value = new DataInputStream(new ByteArrayInputStream(buf, pos, 2 + length)).readUTF();
      pos += 2 + length;
      return value;
    }
  }
}
