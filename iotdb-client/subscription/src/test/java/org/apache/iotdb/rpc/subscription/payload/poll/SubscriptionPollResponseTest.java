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

package org.apache.iotdb.rpc.subscription.payload.poll;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SubscriptionPollResponseTest {

  @Test
  public void testRoundTripPreservesTimeSelected() throws IOException {
    final Map<String, Map<String, Boolean>> timeSelectedByTable = new HashMap<>();
    timeSelectedByTable.put("root.sg", Collections.singletonMap("table1", false));
    final SubscriptionPollResponse response =
        new SubscriptionPollResponse(
            SubscriptionPollResponseType.TABLETS.getType(),
            new TabletsPayload(Collections.emptyMap(), 0),
            new SubscriptionCommitContext(1, 2, "topic", "group", 3L),
            false,
            timeSelectedByTable);

    final SubscriptionPollResponse parsed =
        SubscriptionPollResponse.deserialize(SubscriptionPollResponse.serialize(response));

    assertFalse(parsed.isTimeSelected());
    assertFalse(parsed.getTimeSelectedByTable().get("root.sg").get("table1"));
  }

  @Test
  public void testDeserializeOldWireFormatDefaultsTimeSelectedToTrue() throws IOException {
    final ByteBuffer buffer =
        serializeWithoutTimeSelected(
            SubscriptionPollResponseType.TABLETS.getType(),
            new TabletsPayload(Collections.emptyMap(), 0),
            new SubscriptionCommitContext(1, 2, "topic", "group", 3L));

    final SubscriptionPollResponse parsed = SubscriptionPollResponse.deserialize(buffer);

    assertTrue(parsed.isTimeSelected());
    assertTrue(parsed.getTimeSelectedByTable().isEmpty());
  }

  @Test
  public void testEmptyTimeSelectedByTableEntriesAreIgnored() {
    final Map<String, Map<String, Boolean>> timeSelectedByTable = new HashMap<>();
    timeSelectedByTable.put("root.sg", Collections.emptyMap());
    final SubscriptionPollResponse response =
        new SubscriptionPollResponse(
            SubscriptionPollResponseType.TABLETS.getType(),
            new TabletsPayload(Collections.emptyMap(), 0),
            new SubscriptionCommitContext(1, 2, "topic", "group", 3L),
            false,
            timeSelectedByTable);

    assertTrue(response.getTimeSelectedByTable().isEmpty());
  }

  @Test
  public void testInvalidTimeSelectedByTableEntriesAreIgnored() throws IOException {
    final Map<String, Boolean> tableMap = new HashMap<>();
    tableMap.put(null, false);
    tableMap.put("table1", null);
    tableMap.put("table2", false);
    final Map<String, Map<String, Boolean>> timeSelectedByTable = new HashMap<>();
    timeSelectedByTable.put(null, Collections.singletonMap("ignored", false));
    timeSelectedByTable.put("root.sg", tableMap);
    final SubscriptionPollResponse response =
        new SubscriptionPollResponse(
            SubscriptionPollResponseType.TABLETS.getType(),
            new TabletsPayload(Collections.emptyMap(), 0),
            new SubscriptionCommitContext(1, 2, "topic", "group", 3L),
            false,
            timeSelectedByTable);

    final SubscriptionPollResponse parsed =
        SubscriptionPollResponse.deserialize(SubscriptionPollResponse.serialize(response));

    assertFalse(parsed.getTimeSelectedByTable().get("root.sg").get("table2"));
    assertFalse(parsed.getTimeSelectedByTable().get("root.sg").containsKey("table1"));
  }

  @Test
  public void testTimeSelectedByTableKeysAreNormalized() {
    final Map<String, Map<String, Boolean>> timeSelectedByTable = new HashMap<>();
    timeSelectedByTable.put("Root.SG", Collections.singletonMap("Table1", false));
    final SubscriptionPollResponse response =
        new SubscriptionPollResponse(
            SubscriptionPollResponseType.TABLETS.getType(),
            new TabletsPayload(Collections.emptyMap(), 0),
            new SubscriptionCommitContext(1, 2, "topic", "group", 3L),
            false,
            timeSelectedByTable);

    assertFalse(response.getTimeSelectedByTable().get("root.sg").get("table1"));
  }

  @Test
  public void testAllResponsePayloadTypesRoundTrip() throws IOException {
    assertEquals(
        new TabletsPayload(Collections.emptyMap(), -1),
        roundTrip(
                SubscriptionPollResponseType.TABLETS,
                new TabletsPayload(Collections.emptyMap(), -1))
            .getPayload());

    final FileInitPayload fileInit =
        (FileInitPayload)
            roundTrip(SubscriptionPollResponseType.FILE_INIT, new FileInitPayload("data.tsfile"))
                .getPayload();
    assertEquals("data.tsfile", fileInit.getFileName());

    final FilePiecePayload filePiece =
        (FilePiecePayload)
            roundTrip(
                    SubscriptionPollResponseType.FILE_PIECE,
                    new FilePiecePayload("data.tsfile", 3L, new byte[] {1, 2, 3}))
                .getPayload();
    assertEquals("data.tsfile", filePiece.getFileName());
    assertEquals(3L, filePiece.getNextWritingOffset());
    assertArrayEquals(new byte[] {1, 2, 3}, filePiece.getFilePiece());

    final FileSealPayload fileSeal =
        (FileSealPayload)
            roundTrip(
                    SubscriptionPollResponseType.FILE_SEAL,
                    new FileSealPayload("data.tsfile", 123L, "database"))
                .getPayload();
    assertEquals("data.tsfile", fileSeal.getFileName());
    assertEquals(123L, fileSeal.getFileLength());
    assertEquals("database", fileSeal.getDatabaseName());

    final ErrorPayload error =
        (ErrorPayload)
            roundTrip(SubscriptionPollResponseType.ERROR, new ErrorPayload("retry", true))
                .getPayload();
    assertEquals("retry", error.getErrorMessage());
    assertTrue(error.isCritical());

    assertNull(
        roundTrip(SubscriptionPollResponseType.TERMINATION, new TerminationPayload()).getPayload());

    final WatermarkPayload watermark =
        (WatermarkPayload)
            roundTrip(SubscriptionPollResponseType.WATERMARK, new WatermarkPayload(999L, 7))
                .getPayload();
    assertEquals(999L, watermark.getWatermarkTimestamp());
    assertEquals(7, watermark.getDataNodeId());
  }

  @Test
  public void testTypeLookupRejectsUnknownValues() {
    assertTrue(
        SubscriptionPollResponseType.isValidatedResponseType(
            SubscriptionPollResponseType.WATERMARK.getType()));
    assertFalse(SubscriptionPollResponseType.isValidatedResponseType(Short.MAX_VALUE));
    assertNull(SubscriptionPollResponseType.valueOf(Short.MAX_VALUE));
  }

  private static SubscriptionPollResponse roundTrip(
      final SubscriptionPollResponseType type, final SubscriptionPollPayload payload)
      throws IOException {
    final SubscriptionPollResponse response =
        new SubscriptionPollResponse(
            type.getType(), payload, new SubscriptionCommitContext(1, 2, "topic", "group", 3L));
    return SubscriptionPollResponse.deserialize(SubscriptionPollResponse.serialize(response));
  }

  private static ByteBuffer serializeWithoutTimeSelected(
      final short responseType,
      final SubscriptionPollPayload payload,
      final SubscriptionCommitContext commitContext)
      throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(responseType, outputStream);
      payload.serialize(outputStream);
      commitContext.serialize(outputStream);
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
  }
}
