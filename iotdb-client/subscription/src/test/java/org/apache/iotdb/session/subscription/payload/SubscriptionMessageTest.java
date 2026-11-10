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

package org.apache.iotdb.session.subscription.payload;

import org.apache.iotdb.rpc.subscription.exception.SubscriptionIncompatibleHandlerException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionRuntimeException;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class SubscriptionMessageTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final SubscriptionCommitContext COMMIT_CONTEXT =
      new SubscriptionCommitContext(1, 2, "topic", "group", 3L);

  @Test
  public void testRecordMessageLifecycleAndHandlerCompatibility() {
    final Tablet tablet =
        new Tablet(
            "root.sg.d1",
            Collections.singletonList(new MeasurementSchema("s1", TSDataType.INT64)),
            1);
    tablet.addTimestamp(0, 100L);
    tablet.addValue("s1", 0, 1L);
    final SubscriptionMessage message =
        new SubscriptionMessage(
            COMMIT_CONTEXT,
            Collections.singletonMap("root.sg", Collections.singletonList(tablet)),
            false);

    assertEquals(SubscriptionMessageType.RECORD_HANDLER.getType(), message.getMessageType());
    assertFalse(message.isTimeSelected());
    assertEquals(1, message.getResultSets().size());
    assertTrue(message.getRecordTabletIterator().hasNext());
    assertTrue(message.estimateSize() > 64L);
    assertThrows(SubscriptionIncompatibleHandlerException.class, message::getTsFile);
    assertThrows(IllegalStateException.class, message::getWatermarkTimestamp);

    message.removeUserData();
    message.removeUserData();
    assertEquals(COMMIT_CONTEXT, message.getCommitContext());
    assertThrows(SubscriptionRuntimeException.class, message::getResultSets);
    assertThrows(SubscriptionRuntimeException.class, message::getRecordTabletIterator);
  }

  @Test
  public void testTsFileMessageCompatibilityAndFileOperations() throws Exception {
    final Path source = temporaryFolder.newFile("source.tsfile").toPath();
    Files.write(source, "payload".getBytes(StandardCharsets.UTF_8));
    final SubscriptionMessage message =
        new SubscriptionMessage(COMMIT_CONTEXT, source.toString(), "database", true);

    assertEquals(SubscriptionMessageType.TS_FILE.getType(), message.getMessageType());
    assertEquals("database", message.getTsFile().getDatabaseName());
    assertThrows(SubscriptionIncompatibleHandlerException.class, message::getResultSets);
    assertThrows(SubscriptionIncompatibleHandlerException.class, message::getRecordTabletIterator);
    assertThrows(
        SubscriptionIncompatibleHandlerException.class, message.getTsFile()::openTreeReader);

    final Path copy = temporaryFolder.getRoot().toPath().resolve("nested/copy.tsfile");
    assertEquals(copy, message.getTsFile().copyFile(copy));
    assertEquals("payload", Files.readString(copy));

    final Path moved = temporaryFolder.getRoot().toPath().resolve("moved/moved.tsfile");
    assertEquals(moved, message.getTsFile().moveFile(moved.toString()));
    assertFalse(Files.exists(source));
    assertTrue(Files.exists(moved));
    assertEquals(source, message.getTsFile().getPath());
  }

  @Test
  public void testTreeTsFileAndWatermarkMessages() throws Exception {
    final Path source = temporaryFolder.newFile("tree.tsfile").toPath();
    final SubscriptionMessage treeMessage =
        new SubscriptionMessage(COMMIT_CONTEXT, source.toString(), null);
    assertThrows(
        SubscriptionIncompatibleHandlerException.class, treeMessage.getTsFile()::openTableReader);
    assertEquals(source.toFile(), treeMessage.getTsFile().getFile());
    assertEquals(source, treeMessage.getTsFile().deleteFile());
    assertFalse(Files.exists(source));

    final SubscriptionMessage watermark = new SubscriptionMessage(COMMIT_CONTEXT, 999L);
    assertEquals(SubscriptionMessageType.WATERMARK.getType(), watermark.getMessageType());
    assertEquals(999L, watermark.getWatermarkTimestamp());
    assertEquals(64L, watermark.estimateSize());
    assertTrue(SubscriptionMessageType.isValidatedMessageType(watermark.getMessageType()));
    assertFalse(SubscriptionMessageType.isValidatedMessageType(Short.MAX_VALUE));
  }

  @Test
  public void testPollResultDefaultsAndSummary() {
    final PollResult empty = new PollResult(null, 3, 99L);
    assertTrue(empty.getMessages().isEmpty());
    assertEquals(3, empty.getBufferedCount());
    assertEquals(99L, empty.getWatermark());
    assertTrue(empty.toString().contains("messages=0"));
    assertTrue(empty.toString().contains("bufferedCount=3"));
  }
}
