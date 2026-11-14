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

package org.apache.iotdb.consensus.raft.storage;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.consensus.raft.core.Membership;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MetaFilesTest {

  private File dir;

  @Before
  public void setUp() throws IOException {
    dir = Files.createTempDirectory("raftmeta").toFile();
  }

  @After
  public void tearDown() throws IOException {
    org.apache.iotdb.commons.utils.FileUtils.deleteFileOrDirectory(dir);
  }

  @Test
  public void termVoteRoundTripAndDefaults() throws IOException {
    FileMetaStore m = new FileMetaStore(dir);
    assertEquals(0, m.load().term);
    assertEquals(-1, m.load().votedFor);
    m.persist(7, 3);
    assertEquals(7, m.load().term);
    assertEquals(3, m.load().votedFor);
    m.persist(8, -1);
    assertEquals(8, new FileMetaStore(dir).load().term);
    assertEquals(-1, new FileMetaStore(dir).load().votedFor);
  }

  @Test(expected = IOException.class)
  public void corruptTermVoteFailsClosed() throws IOException {
    FileMetaStore m = new FileMetaStore(dir);
    m.persist(5, 1);
    File f = new File(dir, FileMetaStore.FILE_NAME);
    byte[] bytes = Files.readAllBytes(f.toPath());
    bytes[2] ^= 1;
    Files.write(f.toPath(), bytes);
    m.load();
  }

  @Test
  public void appliedCheckpoint() throws IOException {
    AppliedCheckpoint c = new AppliedCheckpoint(dir);
    assertEquals(-1, c.load());
    c.save(123);
    assertEquals(123, c.load());
    assertTrue(new File(dir, AppliedCheckpoint.FILE_NAME).exists());
  }

  @Test
  public void membershipRoundTrip() throws IOException {
    MembershipStore s = new MembershipStore(dir);
    assertNull(s.load());
    Map<Integer, TEndPoint> eps = new HashMap<>();
    eps.put(1, new TEndPoint("10.0.0.1", 10760));
    eps.put(2, new TEndPoint("10.0.0.2", 10760));
    eps.put(4, new TEndPoint("10.0.0.4", 10760));
    Membership m = new Membership(Arrays.asList(1, 2), Collections.singletonList(4), eps, 17);
    s.save(m, false);
    MembershipStore.Stored st = s.load();
    assertEquals(m, st.membership);
    assertEquals(false, st.committed);
    s.save(m, true);
    assertEquals(true, s.load().committed);
    assertEquals(new TEndPoint("10.0.0.4", 10760), s.load().membership.endpointOf(4));
  }

  @Test
  public void logMetaRoundTrip() throws IOException {
    LogMeta lm = new LogMeta(dir).load();
    assertEquals(0, lm.firstIndex());
    lm.save(42, 3, new byte[] {1, 2, 3});
    LogMeta again = new LogMeta(dir).load();
    assertEquals(42, again.firstIndex());
    assertEquals(3, again.snapshotTerm());
    assertArrayEquals(new byte[] {1, 2, 3}, again.profileSummary());
  }

  @Test
  public void profileMetaRoundTrip() throws IOException {
    FileMetaStore m = new FileMetaStore(dir);
    assertEquals(0, m.load().profileMeta.length);
    m.persist(5, 2, new byte[] {1, 2, 3});
    PersistentMeta p = new FileMetaStore(dir).load();
    assertEquals(5, p.term);
    assertEquals(2, p.votedFor);
    assertArrayEquals(new byte[] {1, 2, 3}, p.profileMeta);
    File f = new File(dir, FileMetaStore.FILE_NAME);
    assertEquals(8 + 4 + 4 + 3, AtomicFiles.read(f).length);
    // a later persist replaces all three values atomically
    m.persist(6, -1, new byte[] {9});
    assertArrayEquals(new byte[] {9}, m.load().profileMeta);
    assertEquals(6, m.load().term);
    assertEquals(-1, m.load().votedFor);
  }

  @Test
  public void emptyProfileMetaKeepsTheOriginalTwelveByteFormat() throws IOException {
    FileMetaStore m = new FileMetaStore(dir);
    File f = new File(dir, FileMetaStore.FILE_NAME);
    m.persist(7, 3); // Raft / TRaft: the two-argument form
    assertEquals(12, AtomicFiles.read(f).length);
    m.persist(8, 1, new byte[0]); // what the log writer passes for Raft / TRaft
    byte[] payload = AtomicFiles.read(f);
    assertEquals(12, payload.length);
    ByteBuffer b = ByteBuffer.wrap(payload);
    assertEquals(8, b.getLong()); // term i64 | votedFor i32, exactly the original layout
    assertEquals(1, b.getInt());
    m.persist(9, 1, new byte[] {4, 5}); // non-empty, then empty again: back to 12 bytes
    m.persist(9, 2, new byte[0]);
    assertEquals(12, AtomicFiles.read(f).length);
    assertEquals(0, m.load().profileMeta.length);
    assertEquals(2, m.load().votedFor);
  }

  @Test
  public void oldFormatFileStillLoads() throws IOException {
    File f = new File(dir, FileMetaStore.FILE_NAME);
    ByteBuffer old = ByteBuffer.allocate(12);
    old.putLong(11).putInt(4); // written by a version without profile meta
    AtomicFiles.write(f, old.array());
    PersistentMeta p = new FileMetaStore(dir).load();
    assertEquals(11, p.term);
    assertEquals(4, p.votedFor);
    assertEquals(0, p.profileMeta.length);
  }

  @Test
  public void inconsistentProfileMetaLengthFailsClosed() throws IOException {
    File f = new File(dir, FileMetaStore.FILE_NAME);
    ByteBuffer bad = ByteBuffer.allocate(12 + 4 + 2);
    bad.putLong(3).putInt(1).putInt(5).put((byte) 1).put((byte) 2); // claims 5 bytes, holds 2
    AtomicFiles.write(f, bad.array());
    try {
      new FileMetaStore(dir).load();
      org.junit.Assert.fail("Invalid extension accepted");
    } catch (IOException expected) {
      /* fail closed */
    }
    AtomicFiles.write(f, new byte[14]); // neither the old nor the new layout
    try {
      new FileMetaStore(dir).load();
      org.junit.Assert.fail("Truncated extension accepted");
    } catch (IOException expected) {
      /* fail closed */
    }
  }

  @Test
  public void corruptMembershipAndLogBoundariesFailClosed() throws IOException {
    for (String name : Arrays.asList(MembershipStore.FILE_NAME, LogMeta.FILE_NAME)) {
      Files.write(new File(dir, name).toPath(), new byte[] {1, 2, 3});
    }
    try {
      new MembershipStore(dir).load();
      org.junit.Assert.fail("Corrupt membership accepted");
    } catch (IOException expected) {
      /* fail closed */
    }
    try {
      new LogMeta(dir).load();
      org.junit.Assert.fail("Corrupt log boundary accepted");
    } catch (IOException expected) {
      /* fail closed */
    }
  }

  @Test
  public void logSummaryLengthCannotAllocateOutsideTheRecord() throws IOException {
    ByteBuffer bad = ByteBuffer.allocate(20).putLong(1).putLong(2).putInt(Integer.MAX_VALUE);
    AtomicFiles.write(new File(dir, LogMeta.FILE_NAME), bad.array());
    try {
      new LogMeta(dir).load();
      org.junit.Assert.fail("Oversize summary accepted");
    } catch (IOException expected) {
      /* fail closed without allocating */
    }
  }
}
