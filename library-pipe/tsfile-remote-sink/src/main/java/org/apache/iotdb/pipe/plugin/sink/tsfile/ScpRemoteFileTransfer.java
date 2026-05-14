/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.pipe.plugin.sink.tsfile;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.channel.ChannelExec;
import org.apache.sshd.client.channel.ClientChannelEvent;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.scp.client.ScpClient;
import org.apache.sshd.scp.client.ScpClientCreator;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_RATE_LIMIT_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_RATE_LIMIT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_SCP_HOST_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_SCP_PASSWORD_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_SCP_PORT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_SCP_REMOTE_PATH_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_SCP_USER_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_RATE_LIMIT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_SCP_HOST_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_SCP_PASSWORD_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_SCP_PORT_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_SCP_PORT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_SCP_REMOTE_PATH_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_SCP_USER_KEY;

class ScpRemoteFileTransfer implements RemoteFileTransfer {

  private static final Logger LOGGER = LoggerFactory.getLogger(ScpRemoteFileTransfer.class);

  private static final String CONNECTOR_SCP_OBJECT_BATCH_SIZE_BYTES_KEY =
      "connector.scp.object-batch-size-bytes";
  private static final String SINK_SCP_OBJECT_BATCH_SIZE_BYTES_KEY =
      "sink.scp.object-batch-size-bytes";
  private static final String CONNECTOR_SCP_OBJECT_PARALLELISM_KEY =
      "connector.scp.object-parallelism";
  private static final String SINK_SCP_OBJECT_PARALLELISM_KEY = "sink.scp.object-parallelism";
  private static final String CONNECTOR_SCP_OBJECT_WAITING_QUEUE_SIZE_KEY =
      "connector.scp.object-waiting-queue-size";
  private static final String SINK_SCP_OBJECT_WAITING_QUEUE_SIZE_KEY =
      "sink.scp.object-waiting-queue-size";
  private static final String CONNECTOR_SCP_OBJECT_THREAD_KEEP_ALIVE_SECONDS_KEY =
      "connector.scp.object-thread-keep-alive-seconds";
  private static final String SINK_SCP_OBJECT_THREAD_KEEP_ALIVE_SECONDS_KEY =
      "sink.scp.object-thread-keep-alive-seconds";
  private static final String TSFILE_EXTENSION = TsFileConstant.TSFILE_SUFFIX;
  private static final String UNIX_SEPARATOR = "/";
  private static final long CONNECT_TIMEOUT_MS = 10000L;
  private static final long DEFAULT_OBJECT_UPLOAD_BATCH_BYTES = 200L * 1024 * 1024;
  private static final int DEFAULT_OBJECT_UPLOAD_PARALLELISM =
      Math.max(1, Math.min(Runtime.getRuntime().availableProcessors() / 4, 16));
  private static final long DEFAULT_OBJECT_UPLOAD_THREAD_KEEP_ALIVE_SECONDS = 60L;
  private static final long EXECUTOR_CLOSE_TIMEOUT_SECONDS = 30L;
  private static final Set<ClientChannelEvent> WAIT_FOR_CLOSED =
      EnumSet.of(ClientChannelEvent.CLOSED);
  private static final AtomicInteger OBJECT_UPLOAD_THREAD_COUNTER = new AtomicInteger(0);

  private final String host;
  private final String user;
  private final String password;
  private final String remoteBaseDir;
  private final int port;
  private final long objectUploadBatchBytes;
  private final int objectUploadParallelism;
  private final int objectUploadWaitingQueueSize;
  private final long objectUploadThreadKeepAliveSeconds;
  private final RateLimiter transferRateLimiter;
  private final ExecutorService objectUploadExecutor;
  private final BlockingQueue<PooledWorkerSession> idleWorkerSessions;

  private ClientSession session;

  ScpRemoteFileTransfer(PipeParameters params) {
    this.host = params.getStringByKeys(CONNECTOR_SCP_HOST_KEY, SINK_SCP_HOST_KEY);
    this.user = params.getStringByKeys(CONNECTOR_SCP_USER_KEY, SINK_SCP_USER_KEY);
    this.remoteBaseDir =
        normalizeRemotePath(
            params.getStringByKeys(CONNECTOR_SCP_REMOTE_PATH_KEY, SINK_SCP_REMOTE_PATH_KEY));
    this.port =
        params.getIntOrDefault(
            Arrays.asList(CONNECTOR_SCP_PORT_KEY, SINK_SCP_PORT_KEY), SINK_SCP_PORT_DEFAULT_VALUE);
    this.password = params.getStringByKeys(CONNECTOR_SCP_PASSWORD_KEY, SINK_SCP_PASSWORD_KEY);
    final long configuredObjectUploadBatchBytes =
        params.getLongOrDefault(
            Arrays.asList(
                CONNECTOR_SCP_OBJECT_BATCH_SIZE_BYTES_KEY, SINK_SCP_OBJECT_BATCH_SIZE_BYTES_KEY),
            DEFAULT_OBJECT_UPLOAD_BATCH_BYTES);
    this.objectUploadBatchBytes =
        configuredObjectUploadBatchBytes > 0
            ? configuredObjectUploadBatchBytes
            : DEFAULT_OBJECT_UPLOAD_BATCH_BYTES;
    if (configuredObjectUploadBatchBytes <= 0) {
      LOGGER.warn(
          "Invalid object upload batch size {} bytes, fallback to default {} bytes",
          configuredObjectUploadBatchBytes,
          DEFAULT_OBJECT_UPLOAD_BATCH_BYTES);
    }
    final int configuredObjectUploadParallelism =
        params.getIntOrDefault(
            Arrays.asList(CONNECTOR_SCP_OBJECT_PARALLELISM_KEY, SINK_SCP_OBJECT_PARALLELISM_KEY),
            DEFAULT_OBJECT_UPLOAD_PARALLELISM);
    this.objectUploadParallelism =
        configuredObjectUploadParallelism > 0
            ? configuredObjectUploadParallelism
            : DEFAULT_OBJECT_UPLOAD_PARALLELISM;
    if (configuredObjectUploadParallelism <= 0) {
      LOGGER.warn(
          "Invalid object upload parallelism {}, fallback to default {}",
          configuredObjectUploadParallelism,
          DEFAULT_OBJECT_UPLOAD_PARALLELISM);
    }
    final int configuredObjectUploadWaitingQueueSize =
        params.getIntOrDefault(
            Arrays.asList(
                CONNECTOR_SCP_OBJECT_WAITING_QUEUE_SIZE_KEY,
                SINK_SCP_OBJECT_WAITING_QUEUE_SIZE_KEY),
            objectUploadParallelism);
    this.objectUploadWaitingQueueSize =
        configuredObjectUploadWaitingQueueSize >= 0
            ? configuredObjectUploadWaitingQueueSize
            : objectUploadParallelism;
    if (configuredObjectUploadWaitingQueueSize < 0) {
      LOGGER.warn(
          "Invalid object upload waiting queue size {}, fallback to default {}",
          configuredObjectUploadWaitingQueueSize,
          objectUploadParallelism);
    }
    final long configuredObjectUploadThreadKeepAliveSeconds =
        params.getLongOrDefault(
            Arrays.asList(
                CONNECTOR_SCP_OBJECT_THREAD_KEEP_ALIVE_SECONDS_KEY,
                SINK_SCP_OBJECT_THREAD_KEEP_ALIVE_SECONDS_KEY),
            DEFAULT_OBJECT_UPLOAD_THREAD_KEEP_ALIVE_SECONDS);
    this.objectUploadThreadKeepAliveSeconds =
        configuredObjectUploadThreadKeepAliveSeconds > 0
            ? configuredObjectUploadThreadKeepAliveSeconds
            : DEFAULT_OBJECT_UPLOAD_THREAD_KEEP_ALIVE_SECONDS;
    if (configuredObjectUploadThreadKeepAliveSeconds <= 0) {
      LOGGER.warn(
          "Invalid object upload thread keep alive seconds {}, fallback to default {}",
          configuredObjectUploadThreadKeepAliveSeconds,
          DEFAULT_OBJECT_UPLOAD_THREAD_KEEP_ALIVE_SECONDS);
    }
    this.idleWorkerSessions = new LinkedBlockingQueue<>(objectUploadParallelism);
    this.objectUploadExecutor = createObjectUploadExecutor(objectUploadParallelism);

    final double bytesPerSecond =
        params.getDoubleOrDefault(
            Arrays.asList(CONNECTOR_RATE_LIMIT_KEY, SINK_RATE_LIMIT_KEY),
            CONNECTOR_RATE_LIMIT_DEFAULT_VALUE);
    if (bytesPerSecond > 0 && !Double.isNaN(bytesPerSecond) && !Double.isInfinite(bytesPerSecond)) {
      this.transferRateLimiter = RateLimiter.create(bytesPerSecond);
      LOGGER.info("SCP sink rate limit enabled: {} bytes/s", bytesPerSecond);
    } else {
      this.transferRateLimiter = null;
    }
    LOGGER.info(
        "SCP sink object upload batch size: {} bytes, max parallelism: {}, "
            + "waiting queue size: {}, keep alive seconds: {}",
        objectUploadBatchBytes,
        objectUploadParallelism,
        objectUploadWaitingQueueSize,
        objectUploadThreadKeepAliveSeconds);
  }

  @Override
  public void transferFile(File tsFile, File modFile, File objectSourceDir, String targetName)
      throws IOException {
    try {
      syncObjectDirectory(objectSourceDir, targetName);
      final String finalTsName = computeFinalTsName(targetName);
      syncModFile(modFile, finalTsName);
      syncTsFile(tsFile, finalTsName);
    } catch (final Exception e) {
      invalidateMainSession();
      invalidateIdleWorkerSessions();
      throw new IOException("Scp transfer failed: " + targetName, e);
    }
  }

  private void syncObjectDirectory(File sourceDir, String targetName) throws IOException {
    if (sourceDir == null || !sourceDir.exists() || !sourceDir.isDirectory()) {
      return;
    }

    final ClientSession s = getSession();
    final Path sourcePath = sourceDir.toPath();
    final String remoteTargetRoot = remoteBaseDir + UNIX_SEPARATOR + targetName;
    uploadObjectChildrenInBatches(s, sourcePath, remoteTargetRoot);
  }

  private void uploadObjectChildrenInBatches(
      final ClientSession session, final Path sourcePath, final String remoteTargetRoot)
      throws IOException {
    final ExecutorCompletionService<Void> completionService =
        new ExecutorCompletionService<>(objectUploadExecutor);
    final Set<Future<Void>> pendingUploads = new HashSet<>();
    final int maxPendingUploadTasks =
        Math.max(1, objectUploadParallelism + objectUploadWaitingQueueSize);
    final List<Path> currentBatchFiles = new ArrayList<>();
    long currentBatchBytes = 0;
    String currentRemoteDir = null;
    final Set<String> preparedDirs = new HashSet<>();

    try (Stream<Path> walk = Files.walk(sourcePath)) {
      final Iterable<Path> paths = walk::iterator;

      for (final Path path : paths) {
        if (Files.isDirectory(path)) {
          final Path relativeDir = sourcePath.relativize(path);
          final String relativeDirStr = relativeDir.toString().replace("\\", UNIX_SEPARATOR);
          final String remoteDir =
              relativeDirStr.isEmpty()
                  ? remoteTargetRoot
                  : remoteTargetRoot + UNIX_SEPARATOR + relativeDirStr;
          if (!preparedDirs.contains(remoteDir)) {
            ensureRemoteDirExists(session, remoteDir);
            preparedDirs.add(remoteDir);
          }
          continue;
        }
        if (!Files.isRegularFile(path)) {
          continue;
        }

        final Path file = path;
        final Path relativeParent = sourcePath.relativize(file.getParent());
        final String relativeParentStr = relativeParent.toString().replace("\\", UNIX_SEPARATOR);
        final String fileRemoteDir =
            relativeParentStr.isEmpty()
                ? remoteTargetRoot
                : remoteTargetRoot + UNIX_SEPARATOR + relativeParentStr;

        final long fileBytes = Files.size(file);
        if (!currentBatchFiles.isEmpty()
            && (!fileRemoteDir.equals(currentRemoteDir)
                || currentBatchBytes + fileBytes > objectUploadBatchBytes)) {
          submitBatchUpload(
              completionService,
              pendingUploads,
              new ArrayList<>(currentBatchFiles),
              currentRemoteDir,
              currentBatchBytes,
              maxPendingUploadTasks);
          currentBatchFiles.clear();
          currentBatchBytes = 0;
        }

        currentRemoteDir = fileRemoteDir;
        currentBatchFiles.add(file);
        currentBatchBytes += fileBytes;
      }
    } catch (final UncheckedIOException e) {
      abortBatchUploads(
          completionService,
          pendingUploads,
          new IOException("Failed to iterate object directory: " + sourcePath, e.getCause()));
      return;
    } catch (final IOException e) {
      abortBatchUploads(completionService, pendingUploads, e);
      return;
    }

    if (!currentBatchFiles.isEmpty()) {
      submitBatchUpload(
          completionService,
          pendingUploads,
          new ArrayList<>(currentBatchFiles),
          currentRemoteDir,
          currentBatchBytes,
          maxPendingUploadTasks);
    }

    waitForAllBatchUploads(completionService, pendingUploads);
  }

  private void submitBatchUpload(
      final ExecutorCompletionService<Void> completionService,
      final Set<Future<Void>> pendingUploads,
      final List<Path> batchFiles,
      final String remoteDir,
      final long batchBytes,
      final int maxPendingUploadTasks)
      throws IOException {
    waitForBatchUploadSlot(completionService, pendingUploads, maxPendingUploadTasks);
    try {
      final Future<Void> future =
          completionService.submit(
              () -> {
                uploadObjectBatch(batchFiles, remoteDir, batchBytes);
                return null;
              });
      pendingUploads.add(future);
    } catch (final RejectedExecutionException e) {
      throw new IOException("Failed to submit async SCP object upload task for " + remoteDir, e);
    }
  }

  private void waitForBatchUploadSlot(
      final ExecutorCompletionService<Void> completionService,
      final Set<Future<Void>> pendingUploads,
      final int maxPendingUploadTasks)
      throws IOException {
    while (pendingUploads.size() >= maxPendingUploadTasks) {
      waitForNextBatchUpload(completionService, pendingUploads);
    }
  }

  private void waitForAllBatchUploads(
      final ExecutorCompletionService<Void> completionService,
      final Set<Future<Void>> pendingUploads)
      throws IOException {
    IOException firstException = null;
    while (!pendingUploads.isEmpty()) {
      try {
        waitForNextBatchUpload(completionService, pendingUploads);
      } catch (final IOException e) {
        if (firstException == null) {
          firstException = e;
          cancelPendingUploads(pendingUploads);
        } else {
          firstException.addSuppressed(e);
        }
      }
    }
    if (firstException != null) {
      throw firstException;
    }
  }

  private void abortBatchUploads(
      final ExecutorCompletionService<Void> completionService,
      final Set<Future<Void>> pendingUploads,
      final IOException originalException)
      throws IOException {
    cancelPendingUploads(pendingUploads);
    IOException firstException = originalException;
    while (!pendingUploads.isEmpty()) {
      try {
        waitForNextBatchUpload(completionService, pendingUploads);
      } catch (final IOException e) {
        firstException.addSuppressed(e);
      }
    }
    throw firstException;
  }

  private void waitForNextBatchUpload(
      final ExecutorCompletionService<Void> completionService,
      final Set<Future<Void>> pendingUploads)
      throws IOException {
    try {
      final Future<Void> completedUpload = completionService.take();
      pendingUploads.remove(completedUpload);
      completedUpload.get();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while uploading object files via SCP", e);
    } catch (final CancellationException e) {
      throw new IOException("Cancelled while uploading object files via SCP", e);
    } catch (final ExecutionException e) {
      final Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException("Failed to upload object files via SCP", cause);
    }
  }

  private void cancelPendingUploads(final Set<Future<Void>> pendingUploads) {
    for (final Future<Void> pendingUpload : pendingUploads) {
      pendingUpload.cancel(true);
    }
  }

  private void uploadObjectBatch(
      final List<Path> batchFiles, final String remoteDir, final long batchBytes)
      throws IOException {
    acquireTransferBytes(batchBytes);
    final ClientSession workerSession = borrowWorkerSession();
    boolean reusable = false;
    try {
      ScpClientCreator.instance()
          .createScpClient(workerSession)
          .upload(
              batchFiles.toArray(new Path[0]),
              remoteDir,
              EnumSet.of(ScpClient.Option.TargetIsDirectory));
      reusable = true;
    } finally {
      recycleWorkerSession(workerSession, reusable);
    }
  }

  private static ExecutorService createObjectUploadExecutor(final int maximumParallelism) {
    return IoTDBThreadPoolFactory.newFixedThreadPool(
        maximumParallelism, "pipe-scp-object-transfer");
  }

  private static final class PooledWorkerSession {

    private final ClientSession session;
    private final long idleSinceNanos;

    private PooledWorkerSession(final ClientSession session, final long idleSinceNanos) {
      this.session = session;
      this.idleSinceNanos = idleSinceNanos;
    }
  }

  private static String computeFinalTsName(final String targetName) {
    return targetName.endsWith(TSFILE_EXTENSION) ? targetName : targetName + TSFILE_EXTENSION;
  }

  private void syncTsFile(final File tsFile, final String finalTsName) throws IOException {
    acquireTransferBytes(tsFile.length());
    ScpClientCreator.instance()
        .createScpClient(getSession())
        .upload(tsFile.toPath(), remoteBaseDir + UNIX_SEPARATOR + finalTsName);
  }

  private void syncModFile(final File modFile, final String finalTsName) throws IOException {
    if (modFile == null || !modFile.exists()) {
      return;
    }
    final String remoteModPath =
        remoteBaseDir + UNIX_SEPARATOR + finalTsName + ModificationFile.FILE_SUFFIX;
    acquireTransferBytes(modFile.length());
    ScpClientCreator.instance()
        .createScpClient(getSession())
        .upload(modFile.toPath(), remoteModPath);
    LOGGER.info("Successfully transferred TsFile mod to {}", remoteModPath);
  }

  private void ensureRemoteDirExists(ClientSession s, String dir) throws IOException {
    executeRemoteCommand(s, "mkdir -p " + shellQuote(dir));
  }

  private static String normalizeRemotePath(String path) {
    return path == null ? "" : path.replace("\\", UNIX_SEPARATOR).replaceAll("/+$", "");
  }

  private static String shellQuote(String path) {
    if (path == null) {
      return "\"\"";
    }
    return "\"" + path.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }

  private void executeRemoteCommand(ClientSession s, String command) throws IOException {
    try (ChannelExec channel = s.createExecChannel(command)) {
      channel.open().verify(CONNECT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      final Set<ClientChannelEvent> events = channel.waitFor(WAIT_FOR_CLOSED, CONNECT_TIMEOUT_MS);
      if (!events.contains(ClientChannelEvent.CLOSED)) {
        throw new IOException("Remote command timed out: " + command);
      }

      final Integer exitStatus = channel.getExitStatus();
      if (exitStatus != null && exitStatus != 0) {
        throw new IOException(
            String.format("Remote command failed with exit code %s: %s", exitStatus, command));
      }
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("Failed to execute remote command: " + command, e);
    }
  }

  private SshClient getSharedClient() throws IOException {
    return ScpSshClientManager.getClient();
  }

  private ClientSession createAuthenticatedSession() throws IOException {
    final ClientSession createdSession =
        getSharedClient().connect(user, host, port).verify(CONNECT_TIMEOUT_MS).getSession();
    createdSession.addPasswordIdentity(password != null ? password : "");
    createdSession.auth().verify(CONNECT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    return createdSession;
  }

  private synchronized ClientSession getSession() throws IOException {
    if (session == null || !session.isOpen()) {
      synchronized (this) {
        if (session == null || !session.isOpen()) {
          session = createAuthenticatedSession();
        }
      }
    }
    return session;
  }

  private ClientSession borrowWorkerSession() throws IOException {
    while (true) {
      final PooledWorkerSession pooledWorkerSession = idleWorkerSessions.poll();
      if (pooledWorkerSession == null) {
        return createAuthenticatedSession();
      }
      if (!pooledWorkerSession.session.isOpen()) {
        closeSessionQuietly(pooledWorkerSession.session);
        continue;
      }
      if (hasWorkerSessionExpired(pooledWorkerSession)) {
        closeSessionQuietly(pooledWorkerSession.session);
        continue;
      }
      return pooledWorkerSession.session;
    }
  }

  private boolean hasWorkerSessionExpired(final PooledWorkerSession pooledWorkerSession) {
    return System.nanoTime() - pooledWorkerSession.idleSinceNanos
        >= TimeUnit.SECONDS.toNanos(objectUploadThreadKeepAliveSeconds);
  }

  private void recycleWorkerSession(final ClientSession workerSession, final boolean reusable) {
    if (workerSession == null) {
      return;
    }
    if (!reusable || !workerSession.isOpen() || objectUploadExecutor.isShutdown()) {
      closeSessionQuietly(workerSession);
      return;
    }
    if (!idleWorkerSessions.offer(new PooledWorkerSession(workerSession, System.nanoTime()))) {
      closeSessionQuietly(workerSession);
    }
  }

  private void acquireTransferBytes(long bytes) {
    if (transferRateLimiter == null || bytes <= 0) {
      return;
    }
    long remaining = bytes;
    while (remaining > 0) {
      final int chunk = remaining > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) remaining;
      transferRateLimiter.acquire(chunk);
      remaining -= chunk;
    }
  }

  private synchronized void invalidateMainSession() {
    if (session != null) {
      session.close(false);
      session = null;
    }
  }

  private synchronized void invalidateSession() {
    invalidateMainSession();
    invalidateIdleWorkerSessions();
  }

  private void invalidateIdleWorkerSessions() {
    PooledWorkerSession pooledWorkerSession;
    while ((pooledWorkerSession = idleWorkerSessions.poll()) != null) {
      closeSessionQuietly(pooledWorkerSession.session);
    }
  }

  private static void closeSessionQuietly(final ClientSession session) {
    if (session != null) {
      session.close(false);
    }
  }

  @Override
  public void handshake() throws IOException {
    try {
      ensureRemoteDirExists(getSession(), remoteBaseDir);
      LOGGER.info("SCP handshake OK, remote base: {}", remoteBaseDir);
    } catch (IOException e) {
      throw new IOException("Handshake failed: cannot create or access remote base directory", e);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    objectUploadExecutor.shutdown();
    try {
      if (!objectUploadExecutor.awaitTermination(
          EXECUTOR_CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        objectUploadExecutor.shutdownNow();
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      objectUploadExecutor.shutdownNow();
    }
    invalidateSession();
  }
}
