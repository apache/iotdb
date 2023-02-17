/*
 * Licensed to the Apache Software Foundation (ASF) under one  * or more contributor license agreements.  See the NOTICE file  * distributed with this work for additional information  * regarding copyright ownership.  The ASF licenses this file  * to you under the Apache License, Version 2.0 (the  * "License"); you may not use this file except in compliance  * with the License.  You may obtain a copy of the License at  *  *     http://www.apache.org/licenses/LICENSE-2.0  *  * Unless required by applicable law or agreed to in writing,  * software distributed under the License is distributed on an  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY  * KIND, either express or implied.  See the License for the  * specific language governing permissions and limitations  * under the License.
 */

package org.apache.iotdb.consensus.natraft.protocol.log.snapshot;

import java.io.File;
import java.nio.ByteBuffer;
import org.apache.iotdb.consensus.natraft.protocol.RaftMember;

public class DirectorySnapshot extends Snapshot {
  private File directory;

  public DirectorySnapshot(File directory) {
    this.directory = directory;
  }

  @Override
  public ByteBuffer serialize() {
    byte[] bytes = directory.getAbsolutePath().getBytes();
    ByteBuffer buffer = ByteBuffer.allocate(bytes.length + Integer.BYTES);
    buffer.putLong(lastLogIndex);
    buffer.putLong(lastLogTerm);
    buffer.putInt(bytes.length);
    buffer.put(bytes);
    return buffer;
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    lastLogIndex = buffer.getLong();
    lastLogTerm = buffer.getLong();
    int size = buffer.getInt();
    byte[] bytes = new byte[size];
    buffer.get(bytes);
    directory = new File(new String(bytes));
  }

  @Override
  public void install(RaftMember member) {
    member.getStateMachine().loadSnapshot(directory);
    member.getLogManager().applySnapshot(this);
  }
}
