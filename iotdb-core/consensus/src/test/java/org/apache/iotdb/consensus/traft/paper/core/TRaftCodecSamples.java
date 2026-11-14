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

package org.apache.iotdb.consensus.traft.paper.core;

import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.DurableRecord;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Entry;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.EntryId;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Kind;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Message;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.MessageType;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Proposal;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.ProposalSlice;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Rank;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.RecordType;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Report;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Messages and records that exercise every field of the TRaft wire and journal encodings. */
final class TRaftCodecSamples {
  private TRaftCodecSamples() {}

  static EntryId id(long term, long index) {
    return new EntryId(term, index);
  }

  static Entry data(long term, long index, long partition, long timestamp, byte... payload) {
    return new Entry(id(term, index), partition, timestamp, 3, Kind.DATA, payload);
  }

  static List<Message> messages() {
    Entry entry = data(4, 17, 2, -9, (byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5);
    Entry barrier = new Entry(id(4, 18), 2, 0, 3, Kind.BARRIER, new byte[0]);
    Entry noop = new Entry(id(5, 1), 3, 0, 0, Kind.NOOP, new byte[0]);
    Proposal proposal =
        new Proposal(id(4, 10), Arrays.asList(id(4, 11), id(4, 12), id(4, 17)), entry, false);
    Proposal recovery = new Proposal(null, Collections.singletonList(id(5, 1)), noop, true);
    // An ordinary opening: its members are the run below it, and it travels with its payload.
    Entry opening = data(4, 20, 3, 50, (byte) 6);
    Proposal ordinary =
        new Proposal(id(4, 17), Arrays.asList(id(4, 17), id(4, 18), id(4, 19)), opening, false);
    Report report =
        new Report(
            Collections.singletonList(entry),
            Arrays.asList(barrier, noop),
            Collections.singletonList(proposal),
            "gen-é",
            1,
            3,
            Collections.singletonList(
                new ProposalSlice(id(4, 10), entry, true, 2, 9, Arrays.asList(id(4, 12)))));
    return Arrays.asList(
        new Message(
            MessageType.ENTRY,
            1,
            2,
            4,
            entry,
            null,
            id(4, 10),
            null,
            null,
            7,
            Arrays.asList(id(4, 11), id(4, 12))),
        new Message(
            MessageType.PROPOSAL,
            1,
            3,
            4,
            null,
            proposal,
            null,
            new Rank(4, 2, 3),
            null,
            -1,
            Collections.<EntryId>emptyList()),
        new Message(
            MessageType.ENTRY_ACK,
            2,
            1,
            4,
            null,
            null,
            id(4, 17),
            null,
            null,
            -1,
            Arrays.asList(id(4, 15), id(4, 16))),
        new Message(
            MessageType.TRANSFER_ACK,
            3,
            1,
            5,
            barrier,
            recovery,
            null,
            new Rank(5, 3, 1),
            report,
            12,
            Collections.<EntryId>emptyList()),
        new Message(
            MessageType.PROPOSAL,
            1,
            2,
            4,
            opening,
            new Proposal(ordinary.base, ordinary.members, Entry.metadata(opening), false),
            null,
            null,
            null,
            -1,
            Collections.<EntryId>emptyList()));
  }

  static List<DurableRecord> records() {
    Entry entry = data(4, 17, 2, 1000, (byte) 9, (byte) 8, (byte) 7);
    Proposal proposal = new Proposal(id(4, 10), Arrays.asList(id(4, 11), id(4, 17)), entry, false);
    Entry opening = data(4, 20, 3, 50, (byte) 6);
    Proposal ordinary =
        new Proposal(
            id(4, 17),
            Arrays.asList(id(4, 17), id(4, 18), id(4, 19)),
            Entry.metadata(opening),
            false);
    return Arrays.asList(
        new DurableRecord(RecordType.TERM_VOTE, 4, 2, null, null, null, -1, null),
        new DurableRecord(RecordType.INSTALL, 4, -1, entry, null, null, -1, null),
        new DurableRecord(RecordType.ENTRY, 4, -1, Entry.metadata(entry), null, null, -1, null),
        new DurableRecord(RecordType.RETAIN_PROPOSAL, 4, -1, null, proposal, null, -1, null),
        new DurableRecord(
            RecordType.COMMIT, 4, -1, null, null, null, 2, Arrays.asList(id(4, 11), id(4, 17))),
        new DurableRecord(RecordType.COMPACT, 4, -1, null, null, id(4, 10), 1, null),
        // TRaftFileStore's frame of a boundary's ENTRY and RETAIN_PROPOSAL records.
        new DurableRecord(RecordType.ENTRY, 4, -1, opening, ordinary, null, -1, null));
  }
}
