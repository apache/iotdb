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

package org.apache.iotdb.consensus.raft.server;

import org.apache.iotdb.consensus.raft.core.action.Action;
import org.apache.iotdb.consensus.raft.core.event.Event;
import org.apache.iotdb.consensus.raft.core.log.EntryMeta;
import org.apache.iotdb.consensus.raft.core.log.EntryMetaCodec;
import org.apache.iotdb.consensus.raft.core.log.EntryType;
import org.apache.iotdb.consensus.raft.core.log.LogEntry;
import org.apache.iotdb.consensus.raft.core.log.LogIndex;
import org.apache.iotdb.consensus.raft.storage.LogMeta;
import org.apache.iotdb.consensus.raft.storage.LogStore;
import org.apache.iotdb.consensus.raft.storage.SegmentedLogStore;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** {@code Action.LoadEntries}: the dispatcher reads the store in order and feeds EntriesLoaded. */
public class ActionDispatcherTest {

  private File dir;

  @Before
  public void setUp() throws IOException {
    dir = Files.createTempDirectory("raftdispatch").toFile();
  }

  @After
  public void tearDown() {
    org.apache.iotdb.commons.utils.FileUtils.deleteFileOrDirectory(dir);
  }

  private static LogEntry e(long index, long term, String data) {
    return new LogEntry(
        index,
        term,
        EntryType.DATA,
        EntryMeta.EMPTY,
        ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8)));
  }

  /** Only the store and the sink matter for LoadEntries. */
  private static ActionDispatcher dispatcher(LogStore store, List<Event> sink) {
    return new ActionDispatcher(
        "test", null, store, null, null, null, null, null, null, sink::add, null);
  }

  @Test
  public void loadEntriesReadsInOrderWithNullForMissing() throws IOException {
    SegmentedLogStore store =
        new SegmentedLogStore(new File(dir, "log"), EntryMetaCodec.EMPTY, 1 << 20, 1 << 20);
    store.recover();
    store.write(e(0, 1, "a"));
    store.write(e(1, 1, "b"));
    store.write(e(3, 2, "d"));
    store.sync();
    List<Event> sink = new ArrayList<>();
    dispatcher(store, sink)
        .execute(Collections.singletonList(new Action.LoadEntries(new long[] {3, 0, 2, 1}, 42)));
    assertEquals(1, sink.size());
    assertTrue(sink.get(0) instanceof Event.EntriesLoaded);
    Event.EntriesLoaded loaded = (Event.EntriesLoaded) sink.get(0);
    assertEquals(42, loaded.token);
    assertEquals(4, loaded.entries.size());
    assertEquals("d", loaded.entries.get(0).payloadAsString());
    assertEquals(2, loaded.entries.get(0).term);
    assertEquals("a", loaded.entries.get(1).payloadAsString());
    assertNull(loaded.entries.get(2)); // a hole
    assertEquals("b", loaded.entries.get(3).payloadAsString());
    store.close();
  }

  /** A store whose read of {@code bad} throws {@code failure}; any other index reads "v<i>". */
  private static LogStore failingAt(long bad, Throwable failure) {
    return new LogStore() {
      @Override
      public void write(LogEntry e) {}

      @Override
      public void deleteFrom(long from) {}

      @Override
      public void sync() {}

      @Override
      public LogEntry read(long index) throws IOException {
        if (index == bad) {
          if (failure instanceof IOException) {
            throw (IOException) failure;
          }
          if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
          }
          throw (Error) failure;
        }
        return e(index, 1, "v" + index);
      }

      @Override
      public void purgeBefore(long index) {}

      @Override
      public LogIndex recover() {
        return new LogIndex();
      }

      @Override
      public LogMeta meta() {
        return null;
      }

      @Override
      public long loadCount() {
        return 0;
      }

      @Override
      public void close() {}
    };
  }

  private static List<String> payloads(Event.EntriesLoaded loaded) {
    List<String> out = new ArrayList<>();
    for (LogEntry e : loaded.entries) {
      out.add(e == null ? null : e.payloadAsString());
    }
    return out;
  }

  @Test
  public void unreadableEntryIsReportedAsMissing() {
    LogStore failing = failingAt(5, new IOException("corrupt record"));
    List<Event> sink = new ArrayList<>();
    dispatcher(failing, sink)
        .execute(Collections.singletonList(new Action.LoadEntries(new long[] {4, 5, 6}, 7)));
    Event.EntriesLoaded loaded = (Event.EntriesLoaded) sink.get(0);
    assertEquals(7, loaded.token);
    assertEquals("v4", loaded.entries.get(0).payloadAsString());
    assertNull(loaded.entries.get(1)); // the kernel still hears back (no hang)
    assertEquals("v6", loaded.entries.get(2).payloadAsString());
    // an empty request still answers
    dispatcher(failing, sink)
        .execute(Collections.singletonList(new Action.LoadEntries(new long[0], 8)));
    assertEquals(2, sink.size());
    assertTrue(((Event.EntriesLoaded) sink.get(1)).entries.isEmpty());
  }

  @Test
  public void undecodableEntryIsReportedAsMissing() {
    // a record whose checksum holds but whose metadata does not decode fails with a runtime
    // exception: the kernel must still hear back, or its callback would wait forever
    RuntimeException[] failures = {
      new IllegalArgumentException("bad profile meta"), new BufferUnderflowException()
    };
    for (RuntimeException failure : failures) {
      List<Event> sink = new ArrayList<>();
      dispatcher(failingAt(5, failure), sink)
          .execute(Collections.singletonList(new Action.LoadEntries(new long[] {4, 5, 6}, 9)));
      assertEquals(1, sink.size());
      Event.EntriesLoaded loaded = (Event.EntriesLoaded) sink.get(0);
      assertEquals(9, loaded.token);
      assertEquals(Arrays.asList("v4", null, "v6"), payloads(loaded));
    }
  }

  @Test
  public void anErrorWhileLoadingStillAnswers() {
    List<Event> sink = new ArrayList<>();
    dispatcher(failingAt(5, new NoClassDefFoundError("codec")), sink)
        .execute(Collections.singletonList(new Action.LoadEntries(new long[] {4, 5, 6}, 3)));
    assertEquals(1, sink.size());
    Event.EntriesLoaded loaded = (Event.EntriesLoaded) sink.get(0);
    assertEquals(3, loaded.token);
    assertEquals(Arrays.asList("v4", null, null), payloads(loaded)); // the rest unread
  }
}
