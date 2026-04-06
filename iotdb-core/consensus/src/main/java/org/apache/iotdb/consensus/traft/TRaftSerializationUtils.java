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

package org.apache.iotdb.consensus.traft;

import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;
import org.apache.iotdb.mpp.rpc.thrift.TTraftAppendEntriesReq;
import org.apache.iotdb.mpp.rpc.thrift.TTraftAppendEntriesResp;
import org.apache.iotdb.mpp.rpc.thrift.TTraftInstallSnapshotReq;
import org.apache.iotdb.mpp.rpc.thrift.TTraftInstallSnapshotResp;
import org.apache.iotdb.mpp.rpc.thrift.TTraftLogEntry;
import org.apache.iotdb.mpp.rpc.thrift.TTraftRequestVoteReq;
import org.apache.iotdb.mpp.rpc.thrift.TTraftRequestVoteResp;
import org.apache.iotdb.mpp.rpc.thrift.TTraftTriggerElectionReq;
import org.apache.iotdb.mpp.rpc.thrift.TTraftTriggerElectionResp;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/** Serialization helpers shared by the local TRaft core and the RPC layer. */
final class TRaftSerializationUtils {

  private TRaftSerializationUtils() {}

  static byte[] serializePeers(List<Peer> peers) throws IOException {
    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(outputStream)) {
      BasicStructureSerDeUtil.write(peers.size(), dataOutputStream);
      for (Peer peer : peers) {
        peer.serialize(dataOutputStream);
      }
      dataOutputStream.flush();
      return outputStream.toByteArray();
    }
  }

  static List<Peer> deserializePeers(byte[] data) {
    if (data == null || data.length == 0) {
      return new ArrayList<>();
    }
    ByteBuffer buffer = ByteBuffer.wrap(data);
    int size = BasicStructureSerDeUtil.readInt(buffer);
    List<Peer> peers = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      peers.add(Peer.deserialize(buffer));
    }
    return peers;
  }

  static byte[] zipDirectory(File directory) throws IOException {
    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ZipOutputStream zipOutputStream = new ZipOutputStream(outputStream)) {
      if (directory.exists()) {
        zipDirectoryRecursive(directory, directory, zipOutputStream);
      }
      zipOutputStream.finish();
      return outputStream.toByteArray();
    }
  }

  static void unzipDirectory(byte[] data, File targetDirectory) throws IOException {
    if (!targetDirectory.exists() && !targetDirectory.mkdirs()) {
      throw new IOException(String.format("Failed to create snapshot directory %s", targetDirectory));
    }
    try (ZipInputStream zipInputStream =
        new ZipInputStream(new ByteArrayInputStream(data))) {
      ZipEntry entry;
      while ((entry = zipInputStream.getNextEntry()) != null) {
        File target = new File(targetDirectory, entry.getName());
        if (entry.isDirectory()) {
          if (!target.exists() && !target.mkdirs()) {
            throw new IOException(String.format("Failed to create directory %s", target));
          }
          continue;
        }
        File parent = target.getParentFile();
        if (parent != null && !parent.exists() && !parent.mkdirs()) {
          throw new IOException(String.format("Failed to create directory %s", parent));
        }
        Files.copy(zipInputStream, target.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
      }
    }
  }

  static TTraftAppendEntriesReq toThrift(TRaftAppendEntriesRequest request, Peer peer) {
    TTraftAppendEntriesReq thriftRequest = new TTraftAppendEntriesReq();
    thriftRequest.setRegionId(peer.getGroupId().convertToTConsensusGroupId());
    thriftRequest.setLeaderId(request.getLeaderId());
    thriftRequest.setTerm(request.getTerm());
    thriftRequest.setPrevLogIndex(request.getPrevLogIndex());
    thriftRequest.setPrevLogTerm(request.getPrevLogTerm());
    thriftRequest.setLeaderCommit(request.getLeaderCommit());
    List<TTraftLogEntry> entries = new ArrayList<>(request.getEntries().size());
    for (TRaftLogEntry entry : request.getEntries()) {
      entries.add(toThrift(entry));
    }
    thriftRequest.setEntries(entries);
    return thriftRequest;
  }

  static TRaftAppendEntriesResponse fromThrift(TTraftAppendEntriesResp response) {
    return new TRaftAppendEntriesResponse(
        response.isSuccess(), response.getTerm(), response.getMatchIndex(), response.getNextIndexHint());
  }

  static TTraftRequestVoteReq toThrift(TRaftVoteRequest request, Peer peer) {
    TTraftRequestVoteReq thriftRequest = new TTraftRequestVoteReq();
    thriftRequest.setRegionId(peer.getGroupId().convertToTConsensusGroupId());
    thriftRequest.setCandidateId(request.getCandidateId());
    thriftRequest.setTerm(request.getTerm());
    thriftRequest.setLastLogIndex(request.getLastLogIndex());
    thriftRequest.setLastLogTerm(request.getLastLogTerm());
    thriftRequest.setPartitionIndex(request.getPartitionIndex());
    thriftRequest.setCurrentPartitionIndexCount(request.getCurrentPartitionIndexCount());
    return thriftRequest;
  }

  static TRaftVoteResult fromThrift(TTraftRequestVoteResp response) {
    return new TRaftVoteResult(response.isGranted(), response.getTerm());
  }

  static TTraftInstallSnapshotReq toThrift(TRaftInstallSnapshotRequest request, Peer peer) {
    TTraftInstallSnapshotReq thriftRequest = new TTraftInstallSnapshotReq();
    thriftRequest.setRegionId(peer.getGroupId().convertToTConsensusGroupId());
    thriftRequest.setLeaderId(request.getLeaderId());
    thriftRequest.setTerm(request.getTerm());
    thriftRequest.setLastIncludedIndex(request.getLastIncludedIndex());
    thriftRequest.setLastIncludedTerm(request.getLastIncludedTerm());
    thriftRequest.setHistoricalMaxTimestamp(request.getHistoricalMaxTimestamp());
    thriftRequest.setLastPartitionIndex(request.getLastPartitionIndex());
    thriftRequest.setLastPartitionCount(request.getLastPartitionCount());
    thriftRequest.setPeers(ByteBuffer.wrap(request.getPeers()));
    thriftRequest.setSnapshot(ByteBuffer.wrap(request.getSnapshot()));
    return thriftRequest;
  }

  static TRaftInstallSnapshotResponse fromThrift(TTraftInstallSnapshotResp response) {
    return new TRaftInstallSnapshotResponse(
        response.isSuccess(), response.getTerm(), response.getLastIncludedIndex());
  }

  static TTraftTriggerElectionReq toThrift(Peer peer) {
    TTraftTriggerElectionReq request = new TTraftTriggerElectionReq();
    request.setRegionId(peer.getGroupId().convertToTConsensusGroupId());
    return request;
  }

  static TRaftTriggerElectionResponse fromThrift(TTraftTriggerElectionResp response) {
    return new TRaftTriggerElectionResponse(response.isAccepted(), response.getTerm());
  }

  static TTraftLogEntry toThrift(TRaftLogEntry entry) {
    TTraftLogEntry thriftEntry = new TTraftLogEntry();
    thriftEntry.setEntryType(entry.getEntryType().name());
    thriftEntry.setTimestamp(entry.getTimestamp());
    thriftEntry.setPartitionIndex(entry.getPartitionIndex());
    thriftEntry.setLogIndex(entry.getLogIndex());
    thriftEntry.setLogTerm(entry.getLogTerm());
    thriftEntry.setInterPartitionIndex(entry.getInterPartitionIndex());
    thriftEntry.setLastPartitionCount(entry.getLastPartitionCount());
    thriftEntry.setData(ByteBuffer.wrap(entry.getData()));
    return thriftEntry;
  }

  static TRaftLogEntry fromThrift(TTraftLogEntry entry) {
    byte[] rawData = entry.getData();
    byte[] data = rawData == null ? new byte[0] : Arrays.copyOf(rawData, rawData.length);
    return new TRaftLogEntry(
        TRaftEntryType.valueOf(entry.getEntryType()),
        entry.getTimestamp(),
        entry.getPartitionIndex(),
        entry.getLogIndex(),
        entry.getLogTerm(),
        entry.getInterPartitionIndex(),
        entry.getLastPartitionCount(),
        data);
  }

  private static void zipDirectoryRecursive(File root, File current, ZipOutputStream outputStream)
      throws IOException {
    File[] files = current.listFiles();
    if (files == null || files.length == 0) {
      if (!root.equals(current)) {
        ZipEntry entry =
            new ZipEntry(root.toPath().relativize(current.toPath()).toString() + File.separator);
        outputStream.putNextEntry(entry);
        outputStream.closeEntry();
      }
      return;
    }
    for (File file : files) {
      String entryName = root.toPath().relativize(file.toPath()).toString();
      if (file.isDirectory()) {
        zipDirectoryRecursive(root, file, outputStream);
        continue;
      }
      outputStream.putNextEntry(new ZipEntry(entryName));
      Files.copy(file.toPath(), outputStream);
      outputStream.closeEntry();
    }
  }
}
