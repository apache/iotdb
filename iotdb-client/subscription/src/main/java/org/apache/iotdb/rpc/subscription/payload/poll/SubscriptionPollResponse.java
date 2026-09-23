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

import org.apache.iotdb.rpc.subscription.i18n.SubscriptionMessages;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public class SubscriptionPollResponse {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionPollResponse.class);

  private final short responseType;

  private final SubscriptionPollPayload payload;

  private final SubscriptionCommitContext commitContext;

  private final boolean timeSelected;

  private final Map<String, Map<String, Boolean>> timeSelectedByTable;

  public SubscriptionPollResponse(
      final short responseType,
      final SubscriptionPollPayload payload,
      final SubscriptionCommitContext commitContext) {
    this(responseType, payload, commitContext, true);
  }

  public SubscriptionPollResponse(
      final short responseType,
      final SubscriptionPollPayload payload,
      final SubscriptionCommitContext commitContext,
      final boolean timeSelected) {
    this(responseType, payload, commitContext, timeSelected, Collections.emptyMap());
  }

  public SubscriptionPollResponse(
      final short responseType,
      final SubscriptionPollPayload payload,
      final SubscriptionCommitContext commitContext,
      final boolean timeSelected,
      final Map<String, Map<String, Boolean>> timeSelectedByTable) {
    this.responseType = responseType;
    this.payload = payload;
    this.commitContext = commitContext;
    this.timeSelected = timeSelected;
    this.timeSelectedByTable = copyTimeSelectedByTable(timeSelectedByTable);
  }

  public short getResponseType() {
    return responseType;
  }

  public SubscriptionPollPayload getPayload() {
    return payload;
  }

  public SubscriptionCommitContext getCommitContext() {
    return commitContext;
  }

  public boolean isTimeSelected() {
    return timeSelected;
  }

  public Map<String, Map<String, Boolean>> getTimeSelectedByTable() {
    return timeSelectedByTable;
  }

  /////////////////////////////// de/ser ///////////////////////////////

  public static ByteBuffer serialize(final SubscriptionPollResponse response) throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      response.serialize(outputStream);
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
  }

  private void serialize(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(responseType, stream);
    payload.serialize(stream);
    commitContext.serialize(stream);
    ReadWriteIOUtils.write(timeSelected, stream);
    serializeTimeSelectedByTable(stream);
  }

  public static SubscriptionPollResponse deserialize(final ByteBuffer buffer) {
    final short responseType = ReadWriteIOUtils.readShort(buffer);
    SubscriptionPollPayload payload = null;
    if (SubscriptionPollResponseType.isValidatedResponseType(responseType)) {
      switch (SubscriptionPollResponseType.valueOf(responseType)) {
        case TABLETS:
          payload = new TabletsPayload().deserialize(buffer);
          break;
        case FILE_INIT:
          payload = new FileInitPayload().deserialize(buffer);
          break;
        case FILE_PIECE:
          payload = new FilePiecePayload().deserialize(buffer);
          break;
        case FILE_SEAL:
          payload = new FileSealPayload().deserialize(buffer);
          break;
        case ERROR:
          payload = new ErrorPayload().deserialize(buffer);
          break;
        case TERMINATION:
          payload = new TerminationPayload().deserialize(buffer);
          break;
        case WATERMARK:
          payload = new WatermarkPayload().deserialize(buffer);
          break;
        default:
          LOGGER.warn(SubscriptionMessages.UNEXPECTED_RESPONSE_TYPE, responseType);
          break;
      }
    } else {
      LOGGER.warn(SubscriptionMessages.UNEXPECTED_RESPONSE_TYPE, responseType);
    }

    final SubscriptionCommitContext commitContext = SubscriptionCommitContext.deserialize(buffer);
    final boolean timeSelected = buffer.hasRemaining() ? ReadWriteIOUtils.readBool(buffer) : true;
    final Map<String, Map<String, Boolean>> timeSelectedByTable =
        buffer.hasRemaining() ? deserializeTimeSelectedByTable(buffer) : Collections.emptyMap();
    return new SubscriptionPollResponse(
        responseType, payload, commitContext, timeSelected, timeSelectedByTable);
  }

  private void serializeTimeSelectedByTable(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(timeSelectedByTable.size(), stream);
    for (final Map.Entry<String, Map<String, Boolean>> databaseEntry :
        timeSelectedByTable.entrySet()) {
      ReadWriteIOUtils.write(databaseEntry.getKey(), stream);
      ReadWriteIOUtils.write(databaseEntry.getValue().size(), stream);
      for (final Map.Entry<String, Boolean> tableEntry : databaseEntry.getValue().entrySet()) {
        ReadWriteIOUtils.write(tableEntry.getKey(), stream);
        ReadWriteIOUtils.write(tableEntry.getValue(), stream);
      }
    }
  }

  private static Map<String, Map<String, Boolean>> deserializeTimeSelectedByTable(
      final ByteBuffer buffer) {
    final int databaseSize = ReadWriteIOUtils.readInt(buffer);
    if (databaseSize <= 0) {
      return Collections.emptyMap();
    }
    final Map<String, Map<String, Boolean>> result = new HashMap<>();
    for (int i = 0; i < databaseSize; ++i) {
      final String databaseName = ReadWriteIOUtils.readString(buffer);
      final int tableSize = ReadWriteIOUtils.readInt(buffer);
      final Map<String, Boolean> tableMap = new HashMap<>();
      for (int j = 0; j < tableSize; ++j) {
        tableMap.put(ReadWriteIOUtils.readString(buffer), ReadWriteIOUtils.readBool(buffer));
      }
      result.put(databaseName, tableMap);
    }
    return copyTimeSelectedByTable(result);
  }

  private static Map<String, Map<String, Boolean>> copyTimeSelectedByTable(
      final Map<String, Map<String, Boolean>> timeSelectedByTable) {
    if (Objects.isNull(timeSelectedByTable) || timeSelectedByTable.isEmpty()) {
      return Collections.emptyMap();
    }
    final Map<String, Map<String, Boolean>> copied = new HashMap<>();
    timeSelectedByTable.forEach(
        (databaseName, tableMap) -> {
          if (Objects.isNull(databaseName) || Objects.isNull(tableMap) || tableMap.isEmpty()) {
            return;
          }
          final String normalizedDatabaseName = databaseName.trim().toLowerCase(Locale.ROOT);
          final Map<String, Boolean> copiedTableMap = new HashMap<>();
          tableMap.forEach(
              (tableName, timeSelected) -> {
                if (Objects.nonNull(tableName) && Objects.nonNull(timeSelected)) {
                  copiedTableMap.put(tableName.trim().toLowerCase(Locale.ROOT), timeSelected);
                }
              });
          if (!copiedTableMap.isEmpty()) {
            copied.put(normalizedDatabaseName, Collections.unmodifiableMap(copiedTableMap));
          }
        });
    if (copied.isEmpty()) {
      return Collections.emptyMap();
    }
    return Collections.unmodifiableMap(copied);
  }

  /////////////////////////////// stringify ///////////////////////////////

  @Override
  public String toString() {
    return "SubscriptionPollResponse" + coreReportMessage();
  }

  protected Map<String, String> coreReportMessage() {
    final Map<String, String> result = new HashMap<>();
    final SubscriptionPollResponseType type = SubscriptionPollResponseType.valueOf(responseType);
    result.put("responseType", type != null ? type.toString() : "UNKNOWN(" + responseType + ")");
    result.put("payload", payload != null ? payload.toString() : "null");
    result.put("commitContext", commitContext != null ? commitContext.toString() : "null");
    result.put("timeSelected", String.valueOf(timeSelected));
    result.put("timeSelectedByTable", String.valueOf(timeSelectedByTable));
    return result;
  }
}
