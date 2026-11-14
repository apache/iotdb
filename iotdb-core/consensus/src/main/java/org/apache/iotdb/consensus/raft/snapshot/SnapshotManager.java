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

package org.apache.iotdb.consensus.raft.snapshot;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.raft.client.SyncRaftServiceClient;
import org.apache.iotdb.consensus.raft.core.Membership;
import org.apache.iotdb.consensus.raft.core.action.Action;
import org.apache.iotdb.consensus.raft.core.event.Event;
import org.apache.iotdb.consensus.raft.core.profile.ProtocolProfile;
import org.apache.iotdb.consensus.raft.core.profile.SnapshotMetaExt;
import org.apache.iotdb.consensus.raft.storage.AtomicFiles;
import org.apache.iotdb.consensus.raft.thrift.TCleanupTransferredSnapshotReq;
import org.apache.iotdb.consensus.raft.thrift.TInstallSnapshotMetaReq;
import org.apache.iotdb.consensus.raft.thrift.TInstallSnapshotMetaRes;
import org.apache.iotdb.consensus.raft.thrift.TPeerInfo;
import org.apache.iotdb.consensus.raft.thrift.TSendSnapshotFragmentReq;
import org.apache.iotdb.consensus.raft.thrift.TSendSnapshotFragmentRes;
import org.apache.iotdb.consensus.raft.thrift.TTriggerSnapshotLoadReq;
import org.apache.iotdb.consensus.raft.thrift.TTriggerSnapshotLoadRes;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.LongSupplier;

/**
 * Snapshots of one group: {@code <groupDir>/snapshot/<term>_<index>/} (manifest + state machine
 * files), generated atomically through a {@code .tmp.} directory, sent to lagging followers in
 * fragments (installSnapshotMeta -> sendSnapshotFragment* -> triggerSnapshotLoad -> cleanup) and
 * received into {@code .recv.<id>/} before being loaded.
 */
public final class SnapshotManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotManager.class);
  private static final int FRAGMENT_BYTES = 10 * 1024 * 1024;
  private static final String TMP_PREFIX = ".tmp.";
  private static final String RECV_PREFIX = ".recv.";

  /** Loads a received snapshot into the group (state machine + kernel); returns the status. */
  public interface Installer {
    TSStatus install(SnapshotMeta received);

    /** Validate and reserve the application fence before a durable recovery marker is published. */
    default TSStatus prepare(SnapshotMeta received) {
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    }
  }

  private final TConsensusGroupId gid;
  private final File snapshotDir;
  private final IStateMachine stateMachine;
  private final ProtocolProfile profile;
  private final int retention;
  private final ExecutorService pool;
  private final Consumer<Event> sink;
  private final IClientManager<TEndPoint, SyncRaftServiceClient> syncClients;
  private final IntFunction<TEndPoint> resolver;
  private final int self;
  private final LongSupplier currentTerm;
  private final Set<Integer> installing = ConcurrentHashMap.newKeySet();
  private final Map<String, SnapshotMeta> receiving = new ConcurrentHashMap<>();
  private final Object installLock = new Object();
  private final Map<String, Long> receivingTerms = new ConcurrentHashMap<>();
  private volatile SnapshotMeta latest;
  private long recoveredInstallIndex = -1;
  private volatile boolean incompleteInstall;
  private final Map<String, Integer> pins = new HashMap<>();
  private final AtomicLong sentBytes = new AtomicLong();
  private final AtomicLong receivedBytes = new AtomicLong();
  private final AtomicLong rawBytes = new AtomicLong();
  private final AtomicLong sentFiles = new AtomicLong();
  private final AtomicLong completedTransfers = new AtomicLong();
  private final AtomicLong failedTransfers = new AtomicLong();

  /** Payload bytes, including retransmissions; excludes Thrift framing and log RPCs. */
  public static final class TransferStats {
    public final long sentBytes,
        receivedBytes,
        rawBytes,
        sentFiles,
        completedTransfers,
        failedTransfers;

    TransferStats(
        long sentBytes,
        long receivedBytes,
        long rawBytes,
        long sentFiles,
        long completedTransfers,
        long failedTransfers) {
      this.sentBytes = sentBytes;
      this.receivedBytes = receivedBytes;
      this.rawBytes = rawBytes;
      this.sentFiles = sentFiles;
      this.completedTransfers = completedTransfers;
      this.failedTransfers = failedTransfers;
    }
  }

  public TransferStats stats() {
    return new TransferStats(
        sentBytes.get(),
        receivedBytes.get(),
        rawBytes.get(),
        sentFiles.get(),
        completedTransfers.get(),
        failedTransfers.get());
  }

  private File installJournal() {
    return new File(snapshotDir, "install.pending");
  }

  /** Finish an installation interrupted after publication of its durable, verified file set. */
  private void recoverInstall() throws IOException {
    File journal = installJournal();
    if (!journal.exists()) {
      return;
    }
    byte[] bytes = AtomicFiles.read(journal);
    if (bytes == null) {
      throw new IOException("corrupt checkpoint installation journal");
    }
    String id = new String(bytes, StandardCharsets.UTF_8);
    if (!id.matches("[0-9]+_[0-9]+")) {
      throw new IOException("invalid checkpoint installation journal");
    }
    File staged = new File(snapshotDir, ".install." + id);
    File placed = new File(snapshotDir, id);
    File source = staged.isDirectory() ? staged : placed;
    SnapshotBundle.verify(source);
    SnapshotMeta meta = SnapshotMeta.load(source);
    if (meta == null || !meta.id().equals(id)) {
      throw new IOException("invalid pending checkpoint metadata");
    }
    try {
      stateMachine.loadSnapshot(source);
    } catch (RuntimeException e) {
      throw new IOException("cannot finish checkpoint installation", e);
    }
    if (!source.equals(placed)) {
      FileUtils.deleteFileOrDirectory(placed, true);
      Files.move(source.toPath(), placed.toPath(), StandardCopyOption.ATOMIC_MOVE);
    }
    SnapshotBundle.forceDirectory(snapshotDir);
    incompleteInstall = true;
    recoveredInstallIndex = meta.index;
    // The group removes the journal only after cutting the old durable log suffix as well.
  }

  public boolean hasPendingInstall() {
    return incompleteInstall || installJournal().exists();
  }

  public long recoveredInstallIndex() {
    return recoveredInstallIndex;
  }

  public void completeInstallRecovery() throws IOException {
    if (recoveredInstallIndex >= 0) {
      Files.deleteIfExists(installJournal().toPath());
      SnapshotBundle.forceDirectory(snapshotDir);
      incompleteInstall = false;
    }
  }

  /** A received snapshot is being checked and moved into place (two leaders may send at once). */
  private final AtomicBoolean placingReceived = new AtomicBoolean();

  public SnapshotManager(
      TConsensusGroupId gid,
      File groupDir,
      IStateMachine stateMachine,
      ProtocolProfile profile,
      int retention,
      ExecutorService pool,
      Consumer<Event> sink,
      IClientManager<TEndPoint, SyncRaftServiceClient> syncClients,
      IntFunction<TEndPoint> resolver,
      int self,
      LongSupplier currentTerm)
      throws IOException {
    this.gid = gid;
    this.snapshotDir = new File(groupDir, "snapshot");
    this.stateMachine = stateMachine;
    this.profile = profile;
    this.retention = Math.max(1, retention);
    this.pool = pool;
    this.sink = sink;
    this.syncClients = syncClients;
    this.resolver = resolver;
    this.self = self;
    this.currentTerm = currentTerm;
    if (!snapshotDir.exists() && !snapshotDir.mkdirs()) {
      throw new IOException("cannot create " + snapshotDir);
    }
    recoverInstall();
    latest = scanLatest();
  }

  // ------------------------------------------------------------------ local

  private SnapshotMeta scanLatest() throws IOException {
    File[] dirs = snapshotDir.listFiles(File::isDirectory);
    SnapshotMeta best = null;
    if (dirs != null) {
      for (File d : dirs) {
        if (d.getName().startsWith(TMP_PREFIX)
            || d.getName().startsWith(RECV_PREFIX)
            || d.getName().startsWith(".install.")
            || d.getName().startsWith(".verify.")) {
          FileUtils.deleteFileOrDirectory(d); // leftovers of an interrupted generation / transfer
          continue;
        }
        SnapshotMeta m = SnapshotMeta.load(d);
        if (m != null && profile.usesFileCheckpoints()) {
          SnapshotBundle.verify(d);
        }
        if (m != null && (best == null || m.index > best.index)) {
          best = m;
        }
      }
    }
    return best;
  }

  public SnapshotMeta latest() {
    return latest;
  }

  /**
   * Runs the state machine's snapshot on the snapshot pool; feeds SnapshotDone (index -1 on
   * failure).
   */
  public void take(Action.TakeSnapshot a) {
    pool.execute(() -> takeNow(a.lastApplied, a.term, a.membership, a.ext));
  }

  public void takeSynchronous(Action.TakeSnapshot action) {
    takeFenced(action);
  }

  /** Caller holds the application executor fence until the immutable checkpoint is complete. */
  public void takeFenced(Action.TakeSnapshot a) {
    takeNow(a.lastApplied, a.term, a.membership, a.ext);
  }

  private synchronized SnapshotMeta takeNow(
      long index, long term, Membership membership, SnapshotMetaExt ext) {
    SnapshotMeta meta =
        new SnapshotMeta(
            index,
            term,
            membership,
            ext == null ? null : ext.encode(),
            new File(snapshotDir, term + "_" + index));
    if (meta.dir.exists()) {
      try {
        if (profile.usesFileCheckpoints()) {
          SnapshotBundle.verify(meta.dir);
        }
      } catch (IOException e) {
        sink.accept(new Event.SnapshotDone(-1, 0, null));
        return null;
      }
      sink.accept(new Event.SnapshotDone(index, term, ext));
      return latest;
    }
    File tmp = new File(snapshotDir, TMP_PREFIX + meta.id());
    try {
      FileUtils.deleteFileOrDirectory(tmp, true);
      if (!tmp.mkdirs()) {
        throw new IOException("cannot create " + tmp);
      }
      if (!stateMachine.takeSnapshot(tmp)) {
        throw new IOException("state machine refused to take a snapshot");
      }
      new SnapshotMeta(index, term, membership, meta.ext, tmp).save();
      if (profile.usesFileCheckpoints()) {
        List<File> files = stateMachine.getSnapshotFiles(tmp);
        if (files != null) {
          files = new ArrayList<>(files);
          files.add(new File(tmp, SnapshotMeta.MANIFEST));
          SnapshotBundle.create(tmp, files);
        } else {
          throw new IOException("state machine returned no snapshot files");
        }
      } else {
        AtomicFiles.forceTree(tmp);
      }
      Files.move(tmp.toPath(), meta.dir.toPath(), StandardCopyOption.ATOMIC_MOVE);
      SnapshotBundle.forceDirectory(snapshotDir);
      latest = meta;
      retain();
      LOGGER.info("{}: snapshot {} taken", gid, meta.id());
      sink.accept(new Event.SnapshotDone(index, term, ext));
      return meta;
    } catch (Exception e) {
      LOGGER.error("{}: snapshot {} failed", gid, meta.id(), e);
      FileUtils.deleteFileOrDirectory(tmp, true);
      sink.accept(new Event.SnapshotDone(-1, 0, null));
      return null;
    }
  }

  private synchronized void retain() throws IOException {
    File[] dirs = snapshotDir.listFiles(File::isDirectory);
    if (dirs == null) {
      return;
    }
    List<SnapshotMeta> all = new ArrayList<>();
    for (File d : dirs) {
      if (d.getName().startsWith(TMP_PREFIX)
          || d.getName().startsWith(RECV_PREFIX)
          || d.getName().startsWith(".install.")
          || d.getName().startsWith(".verify.")) {
        continue;
      }
      SnapshotMeta m = SnapshotMeta.load(d);
      if (m != null) {
        all.add(m);
      }
    }
    all.sort((x, y) -> Long.compare(y.index, x.index));
    for (int i = retention; i < all.size(); i++) {
      if (!pins.containsKey(all.get(i).id())) {
        FileUtils.deleteFileOrDirectory(all.get(i).dir);
      }
    }
  }

  // --------------------------------------------------------------- transfer

  /**
   * Send the latest snapshot to {@code follower}; feeds SnapshotInstalled (index -1 on failure).
   */
  public void installTo(int follower) {
    if (!installing.add(follower)) {
      return; // a transfer to this follower is already running
    }
    pool.execute(
        () -> {
          long idx = -1;
          SnapshotMetaExt ext = null;
          try {
            SnapshotMeta snap = latest;
            if (snap == null) {
              sink.accept(new Event.TakeSnapshotReq(true));
              long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
              while ((snap = latest) == null && System.nanoTime() < deadline) {
                Thread.sleep(50);
              }
            }
            if (snap == null) {
              LOGGER.warn("{}: no snapshot available for N{}", gid, follower);
              return;
            }
            synchronized (this) {
              // Local retention must not remove a checkpoint while it is being sent.
              snap = latest;
              pins.merge(snap.id(), 1, Integer::sum);
            }
            try {
              transfer(follower, snap);
              completedTransfers.incrementAndGet();
            } finally {
              synchronized (this) {
                final String id = snap.id();
                pins.computeIfPresent(id, (k, count) -> count == 1 ? null : count - 1);
              }
            }
            idx = snap.index;
            ext = profile.decodeSnapshotMeta(snap.ext);
          } catch (Exception e) {
            failedTransfers.incrementAndGet();
            LOGGER.warn("{}: snapshot transfer to N{} failed: {}", gid, follower, e.toString());
          } finally {
            installing.remove(follower);
            sink.accept(new Event.SnapshotInstalled(follower, idx, ext));
          }
        });
  }

  private void transfer(int follower, SnapshotMeta snap) throws Exception {
    if (!profile.usesFileCheckpoints()) {
      genericTransfer(follower, snap);
      return;
    }
    TEndPoint ep = resolver.apply(follower);
    if (ep == null) {
      throw new IOException("unknown endpoint of N" + follower);
    }
    long term = currentTerm.getAsLong();
    try (SyncRaftServiceClient c = syncClients.borrowClient(ep)) {
      TInstallSnapshotMetaReq meta =
          new TInstallSnapshotMetaReq(
              gid,
              self,
              term,
              snap.id(),
              snap.index,
              snap.term,
              peers(snap.membership, true),
              peers(snap.membership, false),
              snap.membership.configIndex());
      meta.setProfileExt(snap.ext);
      TInstallSnapshotMetaRes mr = c.installSnapshotMeta(meta);
      if (mr.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new IOException("installSnapshotMeta rejected: " + mr.getStatus());
      }
      SnapshotBundle.verify(snap.dir);
      List<File> files = new ArrayList<>();
      if (profile.compressSnapshotTransfers()) {
        synchronized (snap) {
          files.add(SnapshotBundle.archive(snap.dir));
        }
      } else {
        files.add(new File(snap.dir, SnapshotBundle.INVENTORY));
        for (SnapshotBundle.Item item : SnapshotBundle.inventory(snap.dir)) {
          files.add(SnapshotBundle.resolve(snap.dir, item.path).toFile());
        }
      }
      rawBytes.addAndGet(SnapshotBundle.rawBytes(snap.dir));
      Path root = snap.dir.toPath();
      for (File f : files) {
        String rel = root.relativize(f.toPath()).toString().replace(File.separatorChar, '/');
        sentFiles.incrementAndGet();
        try (SeekableByteChannel ch = Files.newByteChannel(f.toPath())) {
          ByteBuffer buf =
              ByteBuffer.allocate((int) Math.min(FRAGMENT_BYTES, Math.max(1, f.length())));
          long offset = 0;
          while (true) {
            buf.clear();
            int n = ch.read(buf);
            if (n <= 0) {
              if (offset == 0) {
                check(
                    c.sendSnapshotFragment(
                            new TSendSnapshotFragmentReq(
                                gid, snap.id(), rel, 0, 0, ByteBuffer.allocate(0), term))
                        .getStatus(),
                    "sendSnapshotFragment");
              }
              break;
            }
            buf.flip();
            sentBytes.addAndGet(n);
            TSendSnapshotFragmentRes r =
                c.sendSnapshotFragment(
                    new TSendSnapshotFragmentReq(gid, snap.id(), rel, offset, n, buf, term));
            check(r.getStatus(), "sendSnapshotFragment");
            offset += n;
          }
        }
      }
      TTriggerSnapshotLoadRes lr =
          c.triggerSnapshotLoad(new TTriggerSnapshotLoadReq(gid, snap.id(), term));
      check(lr.getStatus(), "triggerSnapshotLoad");
      c.cleanupTransferredSnapshot(new TCleanupTransferredSnapshotReq(gid, snap.id()));
      LOGGER.info("{}: snapshot {} installed on N{}", gid, snap.id(), follower);
    }
  }

  private void genericTransfer(int follower, SnapshotMeta snap) throws Exception {
    TEndPoint ep = resolver.apply(follower);
    if (ep == null) {
      throw new IOException("unknown endpoint of N" + follower);
    }
    long term = currentTerm.getAsLong();
    try (SyncRaftServiceClient c = syncClients.borrowClient(ep)) {
      TInstallSnapshotMetaReq meta =
          new TInstallSnapshotMetaReq(
              gid,
              self,
              term,
              snap.id(),
              snap.index,
              snap.term,
              peers(snap.membership, true),
              peers(snap.membership, false),
              snap.membership.configIndex());
      meta.setProfileExt(snap.ext);
      TInstallSnapshotMetaRes mr = c.installSnapshotMeta(meta);
      if (mr.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new IOException("installSnapshotMeta rejected: " + mr.getStatus());
      }
      Path root = snap.dir.toPath();
      for (File f : stateMachine.getSnapshotFiles(snap.dir)) {
        if (f.getName().equals(SnapshotMeta.MANIFEST)) {
          continue;
        }
        String rel = root.relativize(f.toPath()).toString();
        try (SeekableByteChannel ch = Files.newByteChannel(f.toPath())) {
          ByteBuffer buf =
              ByteBuffer.allocate((int) Math.min(FRAGMENT_BYTES, Math.max(1, f.length())));
          long offset = 0;
          while (true) {
            buf.clear();
            int n = ch.read(buf);
            if (n <= 0) {
              if (offset == 0) { // empty file: still create it remotely
                TSendSnapshotFragmentRes r =
                    c.sendSnapshotFragment(
                        new TSendSnapshotFragmentReq(
                            gid, snap.id(), rel, 0, 0, ByteBuffer.allocate(0), term));
                check(r.getStatus(), "sendSnapshotFragment");
              }
              break;
            }
            buf.flip();
            TSendSnapshotFragmentRes r =
                c.sendSnapshotFragment(
                    new TSendSnapshotFragmentReq(gid, snap.id(), rel, offset, n, buf, term));
            check(r.getStatus(), "sendSnapshotFragment");
            offset += n;
          }
        }
      }
      TTriggerSnapshotLoadRes lr =
          c.triggerSnapshotLoad(new TTriggerSnapshotLoadReq(gid, snap.id(), term));
      check(lr.getStatus(), "triggerSnapshotLoad");
      c.cleanupTransferredSnapshot(new TCleanupTransferredSnapshotReq(gid, snap.id()));
      LOGGER.info("{}: snapshot {} installed on N{}", gid, snap.id(), follower);
    }
  }

  private static void check(TSStatus s, String what) throws IOException {
    if (s.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new IOException(what + " failed: " + s);
    }
  }

  private static List<TPeerInfo> peers(Membership m, boolean voters) {
    List<TPeerInfo> out = new ArrayList<>();
    for (int id : voters ? m.voters() : m.learners()) {
      TEndPoint ep = m.endpointOf(id);
      out.add(new TPeerInfo(id, ep == null ? new TEndPoint("", 0) : ep));
    }
    return out;
  }

  // ---------------------------------------------------------------- receive

  public TSStatus onInstallMeta(TInstallSnapshotMetaReq req) {
    if (!profile.usesFileCheckpoints()) {
      return genericInstallMeta(req);
    }
    synchronized (receiving) {
      return receiveMeta(req);
    }
  }

  private TSStatus receiveMeta(TInstallSnapshotMetaReq req) {
    if (req.getTerm() < currentTerm.getAsLong()
        || (profile.usesFileCheckpoints() && req.getTerm() != currentTerm.getAsLong())) {
      return RpcUtils.getStatus(
          TSStatusCode.WRITE_PROCESS_REJECT, "checkpoint term not established");
    }
    if (!req.getSnapshotId().equals(req.getLastIncludedTerm() + "_" + req.getLastIncludedIndex())
        || req.getLastIncludedIndex() < 0
        || req.getLastIncludedTerm() < 0) {
      return RpcUtils.getStatus(TSStatusCode.ILLEGAL_PARAMETER, "invalid checkpoint identity");
    }
    File dir = new File(snapshotDir, RECV_PREFIX + req.getSnapshotId());
    try {
      FileUtils.deleteFileOrDirectory(dir, true);
      if (!dir.mkdirs()) {
        throw new IOException("cannot create " + dir);
      }
      Map<Integer, TEndPoint> eps = new HashMap<>();
      List<Integer> voters = new ArrayList<>();
      List<Integer> learners = new ArrayList<>();
      for (TPeerInfo p : req.getVoters()) {
        voters.add(p.getNodeId());
        eps.put(p.getNodeId(), p.getEndPoint());
      }
      for (TPeerInfo p : req.getLearners()) {
        learners.add(p.getNodeId());
        eps.put(p.getNodeId(), p.getEndPoint());
      }
      Membership m = new Membership(voters, learners, eps, req.getConfigIndex());
      SnapshotMeta meta =
          new SnapshotMeta(
              req.getLastIncludedIndex(), req.getLastIncludedTerm(), m, req.getProfileExt(), dir);
      meta.save();
      receiving.put(req.getSnapshotId(), meta);
      receivingTerms.put(req.getSnapshotId(), req.getTerm());
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    } catch (IOException e) {
      LOGGER.warn("{}: cannot prepare snapshot {}", gid, req.getSnapshotId(), e);
      return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.toString());
    }
  }

  public TSStatus onFragment(TSendSnapshotFragmentReq req) {
    if (!profile.usesFileCheckpoints()) {
      return genericFragment(req);
    }
    synchronized (receiving) {
      return receiveFragment(req);
    }
  }

  private TSStatus receiveFragment(TSendSnapshotFragmentReq req) {
    SnapshotMeta meta = receiving.get(req.getSnapshotId());
    if (meta == null) {
      return RpcUtils.getStatus(
          TSStatusCode.INTERNAL_SERVER_ERROR, "unknown snapshot " + req.getSnapshotId());
    }
    try {
      if (!Long.valueOf(req.getTerm()).equals(receivingTerms.get(req.getSnapshotId()))
          || req.getTerm() < currentTerm.getAsLong()
          || req.getOffset() < 0
          || req.getChunkLength() < 0
          || req.getChunkLength() > FRAGMENT_BYTES
          || req.getFileChunk() == null
          || req.bufferForFileChunk().remaining() != req.getChunkLength()) {
        return RpcUtils.getStatus(TSStatusCode.ILLEGAL_PARAMETER, "invalid checkpoint fragment");
      }
      File target = SnapshotBundle.resolve(meta.dir, req.getFilePath()).toFile();
      Files.createDirectories(target.getParentFile().toPath());
      try (FileChannel ch =
          FileChannel.open(target.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
        ByteBuffer chunk = req.bufferForFileChunk();
        long pos = req.getOffset();
        if (pos > ch.size()) {
          throw new IOException("checkpoint fragment has a gap");
        }
        while (chunk != null && chunk.hasRemaining()) {
          pos += ch.write(chunk, pos);
        }
        ch.force(true);
        receivedBytes.addAndGet(req.getChunkLength());
      }
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    } catch (IOException e) {
      LOGGER.warn("{}: cannot write snapshot fragment {}", gid, req.getFilePath(), e);
      return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.toString());
    }
  }

  /** All fragments arrived: move the directory into place and let the group install it. */
  public TSStatus onLoad(TTriggerSnapshotLoadReq req, Installer installer) {
    synchronized (installLock) {
      return receiveLoad(req, installer, false);
    }
  }

  public TSStatus onLoad(
      TTriggerSnapshotLoadReq req,
      Installer installer,
      BooleanSupplier installRunning,
      String busyMessage,
      boolean heldSnapshotIsAnswered) {
    return genericLoad(req, installer, installRunning, busyMessage, heldSnapshotIsAnswered);
  }

  private TSStatus receiveLoad(
      TTriggerSnapshotLoadReq req, Installer installer, boolean heldSnapshotIsAnswered) {
    SnapshotMeta meta;
    Long sessionTerm;
    synchronized (receiving) {
      meta = receiving.remove(req.getSnapshotId());
      sessionTerm = receivingTerms.remove(req.getSnapshotId());
      if (meta != null) {
        // Retries can create a new .recv directory once this transfer is detached. They must
        // never replace files between checksum verification and installation of this one.
        File detached = new File(snapshotDir, ".verify." + java.util.UUID.randomUUID());
        try {
          Files.move(meta.dir.toPath(), detached.toPath(), StandardCopyOption.ATOMIC_MOVE);
          meta = new SnapshotMeta(meta.index, meta.term, meta.membership, meta.ext, detached);
        } catch (IOException e) {
          return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.toString());
        }
      }
    }
    if (meta == null) {
      return RpcUtils.getStatus(
          TSStatusCode.INTERNAL_SERVER_ERROR, "unknown snapshot " + req.getSnapshotId());
    }
    if (!Long.valueOf(req.getTerm()).equals(sessionTerm)
        || req.getTerm() < currentTerm.getAsLong()) {
      FileUtils.deleteFileOrDirectory(meta.dir);
      return RpcUtils.getStatus(TSStatusCode.WRITE_PROCESS_REJECT, "stale term");
    }
    boolean verified =
        new File(meta.dir, SnapshotBundle.INVENTORY).exists()
            || new File(meta.dir, SnapshotBundle.ARCHIVE).exists();
    try {
      if (new File(meta.dir, SnapshotBundle.ARCHIVE).isFile()) {
        SnapshotBundle.unpack(meta.dir);
      }
      if (verified || profile.usesFileCheckpoints()) {
        SnapshotBundle.verify(meta.dir);
        SnapshotBundle.forceDirectories(meta.dir);
        SnapshotMeta received = SnapshotMeta.load(meta.dir);
        if (received == null || received.index != meta.index || received.term != meta.term) {
          throw new IOException("checkpoint metadata differs from transfer identity");
        }
      }
    } catch (IOException e) {
      FileUtils.deleteFileOrDirectory(meta.dir, true);
      return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.toString());
    }
    if (profile.usesFileCheckpoints() && hasPendingInstall()) {
      // A prior timed-out RPC can still be loading its durable stage on the apply executor.
      // Never replace its files or journal; this node must finish/recover that installation first.
      FileUtils.deleteFileOrDirectory(meta.dir, true);
      return RpcUtils.getStatus(TSStatusCode.WRITE_PROCESS_REJECT, "checkpoint recovery pending");
    }
    SnapshotMeta held = latest;
    if (!profile.usesFileCheckpoints() && held != null && held.id().equals(meta.id())) {
      FileUtils.deleteFileOrDirectory(meta.dir, true);
      return heldSnapshotIsAnswered
          ? RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS)
          : installer.install(held);
    }
    TSStatus prepared = installer.prepare(meta);
    if (prepared.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      FileUtils.deleteFileOrDirectory(meta.dir, true);
      return prepared;
    }
    File finalDir = new File(snapshotDir, meta.id());
    File stage =
        profile.usesFileCheckpoints() ? new File(snapshotDir, ".install." + meta.id()) : finalDir;
    try {
      FileUtils.deleteFileOrDirectory(stage, true);
      Files.move(meta.dir.toPath(), stage.toPath(), StandardCopyOption.ATOMIC_MOVE);
      if (profile.usesFileCheckpoints()) {
        AtomicFiles.write(installJournal(), meta.id().getBytes(StandardCharsets.UTF_8));
        incompleteInstall = true;
        SnapshotBundle.forceDirectory(snapshotDir);
      }
    } catch (IOException e) {
      return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.toString());
    }
    SnapshotMeta placed = new SnapshotMeta(meta.index, meta.term, meta.membership, meta.ext, stage);
    TSStatus status = installer.install(placed);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      try {
        synchronized (this) {
          if (!stage.equals(finalDir)) {
            if (finalDir.isDirectory()) {
              // A local checkpoint at this same applied boundary may have finished while the
              // RPC returned. Keep its immutable, possibly pinned directory.
              SnapshotBundle.verify(finalDir);
              FileUtils.deleteFileOrDirectory(stage, true);
            } else {
              Files.move(stage.toPath(), finalDir.toPath(), StandardCopyOption.ATOMIC_MOVE);
            }
            SnapshotBundle.forceDirectory(snapshotDir);
            Files.deleteIfExists(installJournal().toPath());
            SnapshotBundle.forceDirectory(snapshotDir);
            incompleteInstall = false;
            placed = new SnapshotMeta(meta.index, meta.term, meta.membership, meta.ext, finalDir);
          }
          if (latest == null || placed.index >= latest.index) {
            latest = placed;
          }
          retain();
        }
      } catch (IOException e) {
        return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.toString());
      }
    } else if (profile.usesFileCheckpoints()
        && status.getCode() == TSStatusCode.WRITE_PROCESS_REJECT.getStatusCode()) {
      // Rejected before changing the state machine (e.g. a newer prefix is already applied).
      FileUtils.deleteFileOrDirectory(stage, true);
      try {
        Files.deleteIfExists(installJournal().toPath());
        SnapshotBundle.forceDirectory(snapshotDir);
        incompleteInstall = false;
      } catch (IOException e) {
        LOGGER.warn("{}: failed to remove rejected install journal", gid, e);
      }
    }
    return status;
  }

  private TSStatus genericInstallMeta(TInstallSnapshotMetaReq req) {
    if (req.getTerm() < currentTerm.getAsLong()) {
      return RpcUtils.getStatus(TSStatusCode.WRITE_PROCESS_REJECT, "stale term");
    }
    File dir = new File(snapshotDir, RECV_PREFIX + req.getSnapshotId());
    try {
      FileUtils.deleteFileOrDirectory(dir, true);
      if (!dir.mkdirs()) {
        throw new IOException("cannot create " + dir);
      }
      Map<Integer, TEndPoint> eps = new HashMap<>();
      List<Integer> voters = new ArrayList<>();
      List<Integer> learners = new ArrayList<>();
      for (TPeerInfo p : req.getVoters()) {
        voters.add(p.getNodeId());
        eps.put(p.getNodeId(), p.getEndPoint());
      }
      for (TPeerInfo p : req.getLearners()) {
        learners.add(p.getNodeId());
        eps.put(p.getNodeId(), p.getEndPoint());
      }
      Membership m = new Membership(voters, learners, eps, req.getConfigIndex());
      SnapshotMeta meta =
          new SnapshotMeta(
              req.getLastIncludedIndex(), req.getLastIncludedTerm(), m, req.getProfileExt(), dir);
      meta.save();
      receiving.put(req.getSnapshotId(), meta);
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    } catch (IOException e) {
      LOGGER.warn("{}: cannot prepare snapshot {}", gid, req.getSnapshotId(), e);
      return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.toString());
    }
  }

  private TSStatus genericFragment(TSendSnapshotFragmentReq req) {
    SnapshotMeta meta = receiving.get(req.getSnapshotId());
    if (meta == null) {
      return RpcUtils.getStatus(
          TSStatusCode.INTERNAL_SERVER_ERROR, "unknown snapshot " + req.getSnapshotId());
    }
    try {
      File target = new File(meta.dir, req.getFilePath());
      if (!target.getCanonicalFile().toPath().startsWith(meta.dir.getCanonicalFile().toPath())
          || target.getCanonicalFile().equals(meta.dir.getCanonicalFile())) {
        return RpcUtils.getStatus(
            TSStatusCode.ILLEGAL_PARAMETER, "illegal snapshot path " + req.getFilePath());
      }
      Files.createDirectories(target.getParentFile().toPath());
      try (FileChannel ch =
          FileChannel.open(target.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
        ByteBuffer chunk = req.bufferForFileChunk();
        long pos = req.getOffset();
        while (chunk != null && chunk.hasRemaining()) {
          pos += ch.write(chunk, pos);
        }
      }
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    } catch (IOException e) {
      LOGGER.warn("{}: cannot write snapshot fragment {}", gid, req.getFilePath(), e);
      return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.toString());
    }
  }

  /**
   * All fragments arrived: move the directory into place and let the group install it. Never while
   * an install is under way: its load may be reading the directory of this very snapshot.
   *
   * @param installRunning whether the group is still loading a received snapshot (that outlasts the
   *     answer of the RPC that triggered it)
   * @param busyMessage the answer while it does
   * @param heldSnapshotIsAnswered in-order commit: a copy of the snapshot this node holds as its
   *     latest needs no install, the node applied through it and the leader goes on behind it
   */
  private TSStatus genericLoad(
      TTriggerSnapshotLoadReq req,
      Installer installer,
      BooleanSupplier installRunning,
      String busyMessage,
      boolean heldSnapshotIsAnswered) {
    SnapshotMeta meta = receiving.remove(req.getSnapshotId());
    if (meta == null) {
      return RpcUtils.getStatus(
          TSStatusCode.INTERNAL_SERVER_ERROR, "unknown snapshot " + req.getSnapshotId());
    }
    if (req.getTerm() < currentTerm.getAsLong()) {
      FileUtils.deleteFileOrDirectory(meta.dir);
      return RpcUtils.getStatus(TSStatusCode.WRITE_PROCESS_REJECT, "stale term");
    }
    if (!placingReceived.compareAndSet(false, true)) {
      FileUtils.deleteFileOrDirectory(meta.dir, true);
      return RpcUtils.getStatus(TSStatusCode.WRITE_PROCESS_REJECT, busyMessage);
    }
    try {
      if (installRunning.getAsBoolean()) {
        FileUtils.deleteFileOrDirectory(meta.dir, true);
        return RpcUtils.getStatus(TSStatusCode.WRITE_PROCESS_REJECT, busyMessage);
      }
      return placeAndInstall(meta, installer, heldSnapshotIsAnswered);
    } finally {
      placingReceived.set(false);
    }
  }

  private TSStatus placeAndInstall(
      SnapshotMeta meta, Installer installer, boolean heldSnapshotIsAnswered) {
    File finalDir = new File(snapshotDir, meta.id());
    SnapshotMeta held = latest;
    if (held != null && held.dir.equals(finalDir)) {
      // this node's latest snapshot again (the answer of an install that outlived its RPC never
      // reached the leader, or the leader's view lags): the image behind the compacted log is never
      // replaced, a crash between the delete and the move would leave the log without it
      FileUtils.deleteFileOrDirectory(meta.dir, true);
      return heldSnapshotIsAnswered
          ? RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS)
          : installer.install(held);
    }
    try {
      org.apache.iotdb.consensus.raft.storage.AtomicFiles.forceTree(meta.dir);
      FileUtils.deleteFileOrDirectory(finalDir, true);
      Files.move(meta.dir.toPath(), finalDir.toPath(), StandardCopyOption.ATOMIC_MOVE);
      org.apache.iotdb.consensus.raft.storage.AtomicFiles.forceDirectory(snapshotDir);
    } catch (IOException e) {
      LOGGER.warn("{}: cannot move received snapshot into place", gid, e);
      return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.toString());
    }
    return installer.install(
        new SnapshotMeta(meta.index, meta.term, meta.membership, meta.ext, finalDir));
  }

  public void installed(SnapshotMeta placed) {
    // Journaled LSM installs publish only after the manager finishes the durable transaction.
    if (!profile.usesFileCheckpoints() && (latest == null || placed.index >= latest.index)) {
      latest = placed;
    }
  }

  /** Retention after an install: file I/O, for the apply thread (never the group thread). */
  public synchronized void retainAfterInstall() {
    try {
      retain();
    } catch (IOException e) {
      LOGGER.debug("{}: retention after install failed", gid, e);
    }
  }

  public TSStatus cleanup(String snapshotId) {
    SnapshotMeta meta = receiving.remove(snapshotId);
    receivingTerms.remove(snapshotId);
    if (meta != null) {
      FileUtils.deleteFileOrDirectory(meta.dir);
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }
}
