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

package org.apache.iotdb.consensus.pipe;

import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.consensus.common.Peer;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class PipeConsensusPeerManager {

  private static final String CONFIGURATION_FILE_NAME = "configuration.dat";
  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConsensusPeerManager.class);

  private final String storageDir;
  private final Set<Peer> peers;

  public PipeConsensusPeerManager(String storageDir, List<Peer> peers) {
    this.storageDir = storageDir;
    this.peers = Collections.newSetFromMap(new ConcurrentHashMap<>());

    this.peers.addAll(peers);
    if (this.peers.size() != peers.size()) {
      LOGGER.warn("Duplicate peers in the input list, ignore the duplicates.");
    }
  }

  public void recover() throws IOException {
    try (Stream<Path> pathStream = Files.walk(Paths.get(storageDir), 1)) {
      Path[] configurationPaths =
          pathStream
              .filter(Files::isRegularFile)
              .filter(path -> path.getFileName().toString().endsWith(CONFIGURATION_FILE_NAME))
              .toArray(Path[]::new);
      ByteBuffer readBuffer;
      for (Path path : configurationPaths) {
        readBuffer = ByteBuffer.wrap(Files.readAllBytes(path));
        peers.add(Peer.deserialize(readBuffer));
      }
    }
  }

  private void persist(Peer peer) throws IOException {
    File configurationFile = new File(storageDir, generateConfigurationFileName(peer));
    if (configurationFile.exists()) {
      LOGGER.warn("Configuration file {} already exists, delete it.", configurationFile);
      FileUtils.deleteFileOrDirectory(configurationFile);
    }

    try (FileOutputStream fileOutputStream = new FileOutputStream(configurationFile)) {
      try (DataOutputStream dataOutputStream = new DataOutputStream(fileOutputStream)) {
        peer.serialize(dataOutputStream);
      } finally {
        try {
          fileOutputStream.flush();
          fileOutputStream.getFD().sync();
        } catch (IOException ignore) {
          // ignore sync exception
        }
      }
    }
  }

  private String generateConfigurationFileName(Peer peer) {
    return peer.getNodeId() + "_" + CONFIGURATION_FILE_NAME;
  }

  public void persistAll() throws IOException {
    for (Peer peer : peers) {
      persist(peer);
    }
  }

  public boolean contains(Peer peer) {
    return peers.contains(peer);
  }

  public void addAndPersist(Peer peer) throws IOException {
    peers.add(peer);
    persist(peer);
  }

  public void removeAndPersist(Peer peer) throws IOException {
    Files.deleteIfExists(Paths.get(storageDir, generateConfigurationFileName(peer)));
    peers.remove(peer);
  }

  public List<Peer> getOtherPeers(Peer thisNode) {
    return peers.stream()
        .filter(peer -> !peer.equals(thisNode))
        .collect(ImmutableList.toImmutableList());
  }

  public List<Peer> getPeers() {
    return ImmutableList.copyOf(peers);
  }

  public void deleteAllFiles() throws IOException {
    IOException exception = null;
    for (Peer peer : peers) {
      try {
        Files.deleteIfExists(Paths.get(storageDir, generateConfigurationFileName(peer)));
      } catch (IOException e) {
        LOGGER.error("Failed to delete configuration file for peer {}", peer, e);
        if (exception == null) {
          exception = e;
        } else {
          exception.addSuppressed(e);
        }
      }
    }
    if (exception != null) {
      throw exception;
    }
  }

  public void clear() throws IOException {
    deleteAllFiles();
    peers.clear();
  }
}
