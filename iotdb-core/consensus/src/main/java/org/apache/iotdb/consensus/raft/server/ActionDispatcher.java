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

package org.apache.iotdb.consensus.raft.server;

import org.apache.iotdb.consensus.raft.core.action.Action;
import org.apache.iotdb.consensus.raft.core.event.Event;
import org.apache.iotdb.consensus.raft.core.log.LogEntry;
import org.apache.iotdb.consensus.raft.core.msg.AppendEntriesReq;
import org.apache.iotdb.consensus.raft.storage.AppliedCheckpoint;
import org.apache.iotdb.consensus.raft.storage.LogStore;
import org.apache.iotdb.consensus.raft.storage.LogWriter;
import org.apache.iotdb.consensus.raft.storage.MembershipStore;
import org.apache.iotdb.consensus.raft.transport.Transport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/** Executes kernel actions with O(1) hand-offs; runs on the group thread. */
public final class ActionDispatcher {

  private static final Logger LOGGER = LoggerFactory.getLogger(ActionDispatcher.class);

  /** Snapshot / membership / role hooks the group server wires in. */
  public interface Hooks {
    void onRoleChanged(Action.RoleChanged a);

    void onLeaderReady();

    void onTakeSnapshot(Action.TakeSnapshot a);

    void onInstallSnapshotTo(Action.InstallSnapshotTo a);
  }

  private final String name;
  private final LogWriter writer;
  private final LogStore store;
  private final Transport transport;
  private final RaftTimer timer;
  private final RaftApplier applier;
  private final ClientRequestTable requests;
  private final MembershipStore membershipStore;
  private final AppliedCheckpoint checkpoint;

  /** Where results the kernel waits for (EntriesLoaded) go: the group's event queue. */
  private final Consumer<Event> sink;

  private final Hooks hooks;

  public ActionDispatcher(
      String name,
      LogWriter writer,
      LogStore store,
      Transport transport,
      RaftTimer timer,
      RaftApplier applier,
      ClientRequestTable requests,
      MembershipStore membershipStore,
      AppliedCheckpoint checkpoint,
      Consumer<Event> sink,
      Hooks hooks) {
    this.name = name;
    this.writer = writer;
    this.store = store;
    this.transport = transport;
    this.timer = timer;
    this.applier = applier;
    this.requests = requests;
    this.membershipStore = membershipStore;
    this.checkpoint = checkpoint;
    this.sink = sink;
    this.hooks = hooks;
  }

  public void execute(List<Action> actions) {
    for (Action a : actions) {
      try {
        one(a);
      } catch (Throwable t) {
        LOGGER.error("{}: action {} failed", name, a, t);
      }
    }
  }

  private void one(Action a) throws IOException {
    if (a instanceof Action.Persist) {
      for (LogEntry entry : ((Action.Persist) a).entries) {
        if (entry.reqId != LogEntry.NO_REQUEST) {
          requests.bind(entry.reqId, entry.index, entry.term);
        }
      }
      writer.enqueue(a);
    } else if (a instanceof Action.PersistMeta) {
      writer.enqueue(a);
    } else if (a instanceof Action.SendAppend) {
      Action.SendAppend sa = (Action.SendAppend) a;
      List<LogEntry> entries = new ArrayList<>(sa.indexes.length);
      for (long idx : sa.indexes) {
        LogEntry e = store.read(idx); // loaded once for the whole fan-out
        if (e == null) {
          LOGGER.warn("{}: entry {} vanished before sending (compacted?)", name, idx);
          return;
        }
        entries.add(e);
      }
      AppendEntriesReq req = sa.template.withEntries(entries, sa.template.speculative);
      transport.sendAppend(sa.targets, req, sa.metaByTarget);
    } else if (a instanceof Action.Send) {
      Action.Send s = (Action.Send) a;
      long msgId = s.msg instanceof AppendEntriesReq ? ((AppendEntriesReq) s.msg).msgId : 0;
      transport.send(s.to, s.msg, msgId);
    } else if (a instanceof Action.Reply) {
      Action.Reply r = (Action.Reply) a;
      if (r.handle != null && r.handle.future != null) {
        r.handle.future.complete(r.res);
      }
    } else if (a instanceof Action.Apply) {
      applier.enqueue((Action.Apply) a);
    } else if (a instanceof Action.CompleteClient) {
      requests.onOutcome((Action.CompleteClient) a);
    } else if (a instanceof Action.ArmTimer) {
      Action.ArmTimer t = (Action.ArmTimer) a;
      timer.arm(t.kind, t.delayMs);
    } else if (a instanceof Action.CancelTimer) {
      timer.cancel(((Action.CancelTimer) a).kind);
    } else if (a instanceof Action.UpdateMembership) {
      Action.UpdateMembership u = (Action.UpdateMembership) a;
      membershipStore.save(u.membership, u.committed);
    } else if (a instanceof Action.RoleChanged) {
      hooks.onRoleChanged((Action.RoleChanged) a);
    } else if (a instanceof Action.LeaderReady) {
      hooks.onLeaderReady();
    } else if (a instanceof Action.TakeSnapshot) {
      hooks.onTakeSnapshot((Action.TakeSnapshot) a);
    } else if (a instanceof Action.InstallSnapshotTo) {
      hooks.onInstallSnapshotTo((Action.InstallSnapshotTo) a);
    } else if (a instanceof Action.CompactLog) {
      Action.CompactLog c = (Action.CompactLog) a;
      store.meta().save(c.newFirstIndex, c.snapshotTerm, c.profileSummary);
      store.purgeBefore(c.newFirstIndex);
    } else if (a instanceof Action.SaveCheckpoint) {
      checkpoint.save(((Action.SaveCheckpoint) a).lastApplied);
    } else if (a instanceof Action.LoadEntries) {
      loadEntries((Action.LoadEntries) a);
    }
  }

  /**
   * Read the requested entries in order and hand them back; an index that is absent or cannot be
   * read (I/O error, undecodable record) comes back as null. The kernel always hears back, so the
   * callback waiting for the result neither hangs nor stays registered.
   */
  private void loadEntries(Action.LoadEntries le) {
    List<LogEntry> entries = new ArrayList<>(le.indexes.length);
    try {
      for (long idx : le.indexes) {
        LogEntry e = null;
        try {
          e = store.read(idx);
        } catch (IOException | RuntimeException ex) {
          LOGGER.warn("{}: cannot load e{} for token {}: {}", name, idx, le.token, ex.toString());
        }
        entries.add(e);
      }
    } finally {
      while (entries.size() < le.indexes.length) {
        entries.add(null); // an Error cut the reads short: the rest counts as unreadable
      }
      sink.accept(new Event.EntriesLoaded(le.token, entries));
    }
  }
}
