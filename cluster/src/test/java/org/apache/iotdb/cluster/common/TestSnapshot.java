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

package org.apache.iotdb.cluster.common;

import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.log.snapshot.SnapshotFactory;
import org.apache.iotdb.cluster.log.snapshot.SnapshotInstaller;
import org.apache.iotdb.cluster.server.member.RaftMember;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

public class TestSnapshot extends Snapshot {

  private int id;
  private ByteBuffer data;

  public TestSnapshot() {
    data = ByteBuffer.wrap(new byte[8192 * 2048]);
  }

  public TestSnapshot(int id) {
    this.id = id;
    data = ByteBuffer.wrap(new byte[8192 * 2048]);
  }

  @Override
  public ByteBuffer serialize() {
    ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES + 8192 * 2048);
    byteBuffer.putInt(id);
    byteBuffer.put(data);
    byteBuffer.flip();
    return byteBuffer;
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    id = buffer.getInt();
    data.put(buffer);
  }

  @Override
  public SnapshotInstaller<? extends Snapshot> getDefaultInstaller(RaftMember member) {
    return new SnapshotInstaller<Snapshot>() {
      @Override
      public void install(Snapshot snapshot, int slot, boolean isDataMigration) {
        // do nothing
      }

      @Override
      public void install(Map<Integer, Snapshot> snapshotMap, boolean isDataMigration) {
        // do nothing
      }
    };
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TestSnapshot that = (TestSnapshot) o;
    return id == that.id;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  public static class Factory implements SnapshotFactory<TestSnapshot> {

    public static final Factory INSTANCE = new Factory();

    @Override
    public TestSnapshot create() {
      return new TestSnapshot();
    }

    @Override
    public TestSnapshot copy(TestSnapshot origin) {
      TestSnapshot testSnapshot = create();
      testSnapshot.id = origin.id;
      testSnapshot.lastLogIndex = origin.lastLogIndex;
      testSnapshot.lastLogTerm = origin.lastLogTerm;
      return testSnapshot;
    }
  }
}
