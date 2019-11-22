/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.log.manage;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import org.apache.iotdb.cluster.exception.UnknownLogTypeException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.LogParser;
import org.apache.iotdb.cluster.log.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleSnapshotLogManager extends MemoryLogManager {

  private static final Logger logger = LoggerFactory.getLogger(SingleSnapshotLogManager.class);
  private Deque<Log> snapshot = new ArrayDeque<>();

  public SingleSnapshotLogManager(LogApplier logApplier) {
    super(logApplier);
  }

  @Override
  public void takeSnapshot() {
    while (!logBuffer.isEmpty() && logBuffer.getFirst().getCurrLogIndex() <= commitLogIndex) {
      snapshot.addLast(logBuffer.removeFirst());
    }
  }

  @Override
  public Snapshot getSnapshot() {
    return new SimpleSnapshot(new ArrayList<>(this.snapshot));
  }

  public static class SimpleSnapshot extends Snapshot {
    private List<Log> snapshot;

    public SimpleSnapshot() {
    }

    SimpleSnapshot(List<Log> snapshot) {
      this.snapshot = snapshot;
    }

    @Override
    public ByteBuffer serialize() {
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
      try {
        dataOutputStream.writeInt(snapshot.size());
        for (Log log : snapshot) {
          outputStream.write(log.serialize().array());
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
    }

    public List<Log> getSnapshot() {
      return snapshot;
    }
  }
}
