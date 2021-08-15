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

package org.apache.iotdb.cluster.log.snapshot;

import org.apache.iotdb.cluster.exception.UnknownLogTypeException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogParser;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.server.member.RaftMember;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** SimpleSnapshot keeps the committed logs in a memory list. */
public class SimpleSnapshot extends Snapshot {

  private static final Logger logger = LoggerFactory.getLogger(SimpleSnapshot.class);
  private List<Log> snapshot = new ArrayList<>();

  public SimpleSnapshot(long lastIndex, long lastTerm) {
    this.lastLogIndex = lastIndex;
    this.lastLogTerm = lastTerm;
  }

  @Override
  public ByteBuffer serialize() {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
    try {
      if (snapshot != null) {
        dataOutputStream.writeInt(snapshot.size());
        for (Log log : snapshot) {
          outputStream.write(log.serialize().array());
        }
      } else {
        dataOutputStream.writeInt(0);
      }

    } catch (IOException e) {
      // unreachable
    }

    return ByteBuffer.wrap(outputStream.toByteArray());
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    snapshot = new ArrayList<>();
    int size = buffer.getInt();
    for (int i = 0; i < size; i++) {
      try {
        snapshot.add(LogParser.getINSTANCE().parse(buffer));
      } catch (UnknownLogTypeException e) {
        logger.error("Cannot recognize log", e);
      }
    }
    this.lastLogIndex =
        snapshot.isEmpty() ? -1 : snapshot.get(snapshot.size() - 1).getCurrLogIndex();
    this.lastLogTerm = snapshot.isEmpty() ? -1 : snapshot.get(snapshot.size() - 1).getCurrLogTerm();
  }

  public List<Log> getSnapshot() {
    return snapshot;
  }

  public void add(Log log) {
    snapshot.add(log);
    lastLogIndex = log.getCurrLogIndex();
    lastLogTerm = log.getCurrLogTerm();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SimpleSnapshot that = (SimpleSnapshot) o;
    return Objects.equals(snapshot, that.snapshot);
  }

  @Override
  public int hashCode() {
    return Objects.hash(snapshot);
  }

  @Override
  public SnapshotInstaller<? extends Snapshot> getDefaultInstaller(RaftMember member) {
    return DEFAULT_INSTALLER;
  }

  public static final SnapshotInstaller<SimpleSnapshot> DEFAULT_INSTALLER =
      new SnapshotInstaller<SimpleSnapshot>() {
        @Override
        public void install(SimpleSnapshot snapshot, int slot, boolean isDataMigration) {
          // default installer does nothing
        }

        @Override
        public void install(Map<Integer, SimpleSnapshot> snapshotMap, boolean isDataMigration) {
          // default installer does nothing
        }
      };
}
