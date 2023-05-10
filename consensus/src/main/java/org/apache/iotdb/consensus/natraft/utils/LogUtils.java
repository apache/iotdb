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

package org.apache.iotdb.consensus.natraft.utils;

import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.natraft.exception.UnknownLogTypeException;
import org.apache.iotdb.consensus.natraft.protocol.RaftMember;
import org.apache.iotdb.consensus.natraft.protocol.log.Entry;
import org.apache.iotdb.consensus.natraft.protocol.log.LogParser;
import org.apache.iotdb.consensus.natraft.protocol.log.VotingEntry;
import org.apache.iotdb.consensus.natraft.utils.Timer.Statistic;
import org.apache.iotdb.consensus.raft.thrift.AppendCompressedEntriesRequest;
import org.apache.iotdb.consensus.raft.thrift.AppendEntryRequest;
import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class LogUtils {

  private static final Logger logger = LoggerFactory.getLogger(LogUtils.class);

  public static VotingEntry buildVotingLog(Entry e, RaftMember member) {
    VotingEntry votingEntry = member.buildVotingLog(e);

    AppendEntryRequest appendEntryRequest = buildAppendEntryRequest(member);
    votingEntry.setAppendEntryRequest(appendEntryRequest);

    return votingEntry;
  }

  public static AppendEntryRequest buildAppendEntryRequest(RaftMember member) {
    AppendEntryRequest request = new AppendEntryRequest();
    request.setTerm(member.getStatus().getTerm().get());

    request.setLeader(member.getThisNode().getEndpoint());
    request.setLeaderId(member.getThisNode().getNodeId());
    // don't need lock because even if it's larger than the commitIndex when appending this log to
    // logManager, the follower can handle the larger commitIndex with no effect
    request.setLeaderCommit(member.getLogManager().getCommitLogIndex());
    request.setGroupId(member.getRaftGroupId().convertToTConsensusGroupId());

    return request;
  }

  public static VotingEntry enqueueEntry(VotingEntry sendLogRequest, RaftMember member) {
    if (member.getAllNodes().size() > 1) {
      member.getLogDispatcher().offer(sendLogRequest);
    }
    return sendLogRequest;
  }

  public static ByteBuffer compressEntries(
      List<ByteBuffer> entryByteList,
      ICompressor compressor,
      AppendCompressedEntriesRequest request) {
    PublicBAOS baos = new PublicBAOS(entryByteList.size() * 16 * 1024);
    DataOutputStream dataOutputStream = new DataOutputStream(baos);
    try {
      dataOutputStream.writeInt(entryByteList.size());
      for (ByteBuffer byteBuffer : entryByteList) {
        dataOutputStream.writeInt(byteBuffer.remaining());
        dataOutputStream.write(
            byteBuffer.array(),
            byteBuffer.arrayOffset() + byteBuffer.position(),
            byteBuffer.remaining());
      }
      Statistic.LOG_DISPATCHER_RAW_SIZE.add(baos.size());
      request.setUncompressedSize(baos.size());
      byte[] compressed = compressor.compress(baos.getBuf(), 0, baos.size());
      Statistic.LOG_DISPATCHER_COMPRESSED_SIZE.add(compressed.length);
      return ByteBuffer.wrap(compressed);
    } catch (IOException e) {
      logger.warn("Failed to compress entries", e);
    }
    return null;
  }

  public static List<ByteBuffer> decompressEntries(
      ByteBuffer buffer, IUnCompressor unCompressor, int uncompressedSize) throws IOException {
    byte[] uncompressed = new byte[uncompressedSize];
    unCompressor.uncompress(
        buffer.array(),
        buffer.arrayOffset() + buffer.position(),
        buffer.remaining(),
        uncompressed,
        0);
    ByteBuffer uncompressedBuffer = ByteBuffer.wrap(uncompressed);

    int count = uncompressedBuffer.getInt();
    List<ByteBuffer> buffers = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      int size = uncompressedBuffer.getInt();
      ByteBuffer slice = uncompressedBuffer.slice();
      slice.limit(slice.position() + size);
      buffers.add(slice);
      uncompressedBuffer.position(uncompressedBuffer.position() + size);
    }

    return buffers;
  }

  public static List<Entry> parseEntries(List<ByteBuffer> buffers, IStateMachine stateMachine)
      throws UnknownLogTypeException {
    List<Entry> entries = new ArrayList<>();
    for (int i = 0; i < buffers.size(); i++) {
      ByteBuffer buffer = buffers.get(i);
      try {
        buffer.mark();
        Entry e;
        try {
          e = LogParser.getINSTANCE().parse(buffer, stateMachine);
          buffer.reset();
          e.setSerializationCache(buffer);
        } catch (BufferUnderflowException ex) {
          buffer.reset();
          throw ex;
        }
        entries.add(e);
      } catch (RuntimeException ex) {
        buffer.reset();
        logger.error(
            "Exception occurred when parsing the {}/{} entry, buffer size: {}",
            i + 1,
            buffers.size(),
            buffer.remaining());
        throw ex;
      }
    }
    return entries;
  }
}
