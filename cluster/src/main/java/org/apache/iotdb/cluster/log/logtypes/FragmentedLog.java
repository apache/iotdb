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

package org.apache.iotdb.cluster.log.logtypes;

import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.utils.encode.rs.ReedSolomon;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.iotdb.cluster.log.Log.Types.FRAGMENTED;

public class FragmentedLog extends Log {

  private byte[][] logFragments;
  private int logLength;
  private boolean[] fragmentPresent;
  private int shardLength;
  private int dataShardNum;
  private int parityShardNum;

  public FragmentedLog() {}

  public FragmentedLog(Log base, int nodeNum) {

    ByteBuffer baseBuffer = base.serialize();
    logLength = baseBuffer.limit();
    int followerNum = nodeNum - 1;
    parityShardNum = nodeNum / 2 - 1;
    dataShardNum = followerNum - parityShardNum;
    shardLength = logLength / dataShardNum + 1;

    logFragments = new byte[parityShardNum + dataShardNum][];
    fragmentPresent = new boolean[parityShardNum + dataShardNum];
    for (int i = 0; i < parityShardNum + dataShardNum; i++) {
      logFragments[i] = new byte[shardLength];
      fragmentPresent[i] = true;
    }

    int start = 0;
    int end = Math.min(start + shardLength, logLength);
    for (int i = 0; i < dataShardNum; i++) {
      System.arraycopy(baseBuffer.array(), start, logFragments[i], 0, end - start);
      start += shardLength;
      end = Math.min(start + shardLength, logLength);
    }

    ReedSolomon reedSolomon = new ReedSolomon(dataShardNum, parityShardNum);
    reedSolomon.encodeParity(logFragments, 0, shardLength);
  }

  public FragmentedLog(FragmentedLog parent, int fragmentIndex) {
    setCurrLogIndex(parent.getCurrLogIndex());
    setCurrLogTerm(parent.getCurrLogTerm());
    setCreateTime(parent.getCreateTime());
    setEnqueueTime(parent.getEnqueueTime());
    setReceiveTime(parent.getReceiveTime());

    this.logFragments = parent.logFragments;
    this.fragmentPresent = new boolean[logFragments.length];
    fragmentPresent[fragmentIndex] = true;
    this.logLength = parent.logLength;
    this.dataShardNum = parent.dataShardNum;
    this.parityShardNum = parent.parityShardNum;
    this.shardLength = parent.shardLength;
  }

  @Override
  public ByteBuffer serialize() {
    PublicBAOS byteArrayOutputStream = new PublicBAOS(DEFAULT_BUFFER_SIZE);
    try (DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
      dataOutputStream.writeByte((byte) FRAGMENTED.ordinal());

      dataOutputStream.writeLong(getCurrLogIndex());
      dataOutputStream.writeLong(getCurrLogTerm());

      dataOutputStream.writeInt(logLength);
      dataOutputStream.writeInt(dataShardNum);
      dataOutputStream.writeInt(parityShardNum);
      dataOutputStream.writeInt(shardLength);

      for (int i = 0, fragmentPresentLength = fragmentPresent.length;
          i < fragmentPresentLength;
          i++) {
        boolean present = fragmentPresent[i];
        dataOutputStream.writeBoolean(present);
        if (present) {
          dataOutputStream.write(logFragments[i]);
        }
      }
    } catch (IOException e) {
      // unreachable
    }

    return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    setCurrLogIndex(buffer.getLong());
    setCurrLogTerm(buffer.getLong());

    logLength = buffer.getInt();
    dataShardNum = buffer.getInt();
    parityShardNum = buffer.getInt();
    shardLength = buffer.getInt();

    logFragments = new byte[dataShardNum + parityShardNum][];
    fragmentPresent = new boolean[dataShardNum + parityShardNum];

    for (int i = 0, fragmentPresentLength = fragmentPresent.length;
        i < fragmentPresentLength;
        i++) {
      boolean present = buffer.get() == 1;
      fragmentPresent[i] = present;
      if (present) {
        logFragments[i] = new byte[shardLength];
        buffer.get(logFragments[i], 0, shardLength);
      }
    }
  }
}
