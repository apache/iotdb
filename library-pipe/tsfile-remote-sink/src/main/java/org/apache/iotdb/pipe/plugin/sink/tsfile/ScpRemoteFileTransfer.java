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
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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

  private static final String TSFILE_EXTENSION = TsFileConstant.TSFILE_SUFFIX;
  private static final String UNIX_SEPARATOR = "/";
  private static final long CONNECT_TIMEOUT_MS = 10000L;
  private static final Set<ClientChannelEvent> WAIT_FOR_CLOSED =
      EnumSet.of(ClientChannelEvent.CLOSED);

  private final String host;
  private final String user;
  private final String password;
  private final String remoteBaseDir;
  private final int port;
  private final RateLimiter transferRateLimiter;

  private SshClient client;
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
  }

  @Override
  public void transferFile(File tsFile, File objectSourceDir, String targetName)
      throws IOException {
    try {
      syncObjectDirectory(objectSourceDir, targetName);
      syncTsFile(tsFile, targetName);
    } catch (final Exception e) {
      invalidateSession();
      throw new IOException("Scp transfer failed: " + targetName, e);
    }
  }

  private void syncObjectDirectory(File sourceDir, String targetName) throws IOException {
    if (sourceDir == null || !sourceDir.exists()) {
      return;
    }

    final ClientSession s = getSession();
    final ScpClient scpClient = ScpClientCreator.instance().createScpClient(s);
    final Path sourcePath = sourceDir.toPath();
    final String remoteTargetRoot = remoteBaseDir + UNIX_SEPARATOR + targetName;

    final Set<String> currentTaskDirs = new HashSet<>();

    try (Stream<Path> walk = Files.walk(sourcePath)) {
      walk.filter(Files::isRegularFile)
          .forEach(
              path -> {
                final String relative =
                    sourcePath.relativize(path).toString().replace("\\", UNIX_SEPARATOR);
                final int lastSlash = relative.lastIndexOf(UNIX_SEPARATOR);

                final String remoteDir =
                    lastSlash == -1
                        ? remoteTargetRoot
                        : remoteTargetRoot + UNIX_SEPARATOR + relative.substring(0, lastSlash);
                try {
                  if (currentTaskDirs.add(remoteDir)) {
                    executeRemoteCommand(s, "mkdir -p " + shellQuote(remoteDir));
                  }

                  acquireTransferBytes(Files.size(path));
                  scpClient.upload(path, remoteDir, Collections.emptySet());
                } catch (IOException e) {
                  throw new UncheckedIOException("Failed to sync object file: " + path, e);
                }
              });
    } catch (final UncheckedIOException e) {
      throw new IOException(e.getMessage(), e.getCause());
    }
  }

  private void syncTsFile(File tsFile, String targetName) throws IOException {
    final String finalTsName =
        targetName.endsWith(TSFILE_EXTENSION) ? targetName : targetName + TSFILE_EXTENSION;
    acquireTransferBytes(tsFile.length());
    ScpClientCreator.instance()
        .createScpClient(getSession())
        .upload(tsFile.toPath(), remoteBaseDir + UNIX_SEPARATOR + finalTsName);
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
      channel.waitFor(WAIT_FOR_CLOSED, CONNECT_TIMEOUT_MS);
    } catch (Exception ignore) {
    }
  }

  private synchronized ClientSession getSession() throws IOException {
    if (client == null || !client.isStarted()) {
      System.setProperty("org.apache.sshd.security.provider.BC.enabled", "false");
      client = SshClient.setUpDefaultClient();
      client.start();
    }
    if (session == null || !session.isOpen()) {
      session = client.connect(user, host, port).verify(CONNECT_TIMEOUT_MS).getSession();
      session.addPasswordIdentity(password != null ? password : "");
      session.auth().verify(CONNECT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }
    return session;
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

  private synchronized void invalidateSession() {
    if (session != null) {
      session.close(false);
      session = null;
    }
    if (client != null && client.isStarted()) {
      client.stop();
      client = null;
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
  public void close() {
    invalidateSession();
  }
}
