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
package org.apache.iotdb.db.wal.io;

import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.wal.checkpoint.Checkpoint;
import org.apache.iotdb.db.wal.checkpoint.CheckpointType;
import org.apache.iotdb.db.wal.checkpoint.MemTableInfo;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class CheckpointFileTest {
  private final File checkpointFile =
      new File(TestConstant.BASE_OUTPUT_PATH.concat("_0.checkpoint"));

  @Before
  public void setUp() throws Exception {
    if (checkpointFile.exists()) {
      Files.delete(checkpointFile.toPath());
    }
  }

  @After
  public void tearDown() throws Exception {
    if (checkpointFile.exists()) {
      Files.delete(checkpointFile.toPath());
    }
  }

  @Test
  public void testReadNormalFile() throws IOException {
    MemTableInfo fakeMemTableInfo = new MemTableInfo(new PrimitiveMemTable(), "fake.tsfile", 0);
    List<Checkpoint> expectedCheckpoints = new ArrayList<>();
    expectedCheckpoints.add(
        new Checkpoint(CheckpointType.GLOBAL_MEMORY_TABLE_INFO, Collections.emptyList()));
    expectedCheckpoints.add(
        new Checkpoint(
            CheckpointType.CREATE_MEMORY_TABLE, Collections.singletonList(fakeMemTableInfo)));
    expectedCheckpoints.add(
        new Checkpoint(
            CheckpointType.FLUSH_MEMORY_TABLE, Collections.singletonList(fakeMemTableInfo)));
    // test Checkpoint.serializedSize
    int size = Long.BYTES;
    for (Checkpoint checkpoint : expectedCheckpoints) {
      size += checkpoint.serializedSize();
    }
    ByteBuffer buffer = ByteBuffer.allocate(size);
    // test Checkpoint.serialize
    buffer.putLong(0);
    for (Checkpoint checkpoint : expectedCheckpoints) {
      checkpoint.serialize(buffer);
    }
    assertEquals(0, buffer.remaining());
    // test CheckpointWriter.write
    try (ILogWriter checkpointWriter = new CheckpointWriter(checkpointFile)) {
      checkpointWriter.write(buffer);
    }
    // test CheckpointReader.readAll
    CheckpointReader checkpointReader = new CheckpointReader(checkpointFile);
    List<Checkpoint> actualCheckpoints = checkpointReader.getCheckpoints();
    assertEquals(expectedCheckpoints, actualCheckpoints);
  }

  @Test
  public void testReadNotExistFile() throws IOException {
    if (checkpointFile.createNewFile()) {
      CheckpointReader checkpointReader = new CheckpointReader(checkpointFile);
      List<Checkpoint> actualCheckpoints = checkpointReader.getCheckpoints();
      assertEquals(0, actualCheckpoints.size());
    }
  }

  @Test
  public void testReadBrokenFile() throws IOException {
    MemTableInfo fakeMemTableInfo = new MemTableInfo(new PrimitiveMemTable(), "fake.tsfile", 0);
    List<Checkpoint> expectedCheckpoints = new ArrayList<>();
    expectedCheckpoints.add(
        new Checkpoint(CheckpointType.GLOBAL_MEMORY_TABLE_INFO, Collections.emptyList()));
    expectedCheckpoints.add(
        new Checkpoint(
            CheckpointType.CREATE_MEMORY_TABLE, Collections.singletonList(fakeMemTableInfo)));
    expectedCheckpoints.add(
        new Checkpoint(
            CheckpointType.FLUSH_MEMORY_TABLE, Collections.singletonList(fakeMemTableInfo)));
    // test Checkpoint.serializedSize
    int size = Long.BYTES + Byte.BYTES;
    for (Checkpoint checkpoint : expectedCheckpoints) {
      size += checkpoint.serializedSize();
    }
    ByteBuffer buffer = ByteBuffer.allocate(size);
    // test Checkpoint.serialize
    buffer.putLong(0);
    for (Checkpoint checkpoint : expectedCheckpoints) {
      checkpoint.serialize(buffer);
    }
    // add broken part
    buffer.put(CheckpointType.CREATE_MEMORY_TABLE.getCode());
    assertEquals(0, buffer.remaining());
    // test CheckpointWriter.write
    try (ILogWriter checkpointWriter = new CheckpointWriter(checkpointFile)) {
      checkpointWriter.write(buffer);
    }
    // test CheckpointWriter.readAll
    CheckpointReader checkpointReader = new CheckpointReader(checkpointFile);
    List<Checkpoint> actualCheckpoints = checkpointReader.getCheckpoints();
    assertEquals(expectedCheckpoints, actualCheckpoints);
  }
}
