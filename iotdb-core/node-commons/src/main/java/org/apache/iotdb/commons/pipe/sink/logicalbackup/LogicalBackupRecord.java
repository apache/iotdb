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

package org.apache.iotdb.commons.pipe.sink.logicalbackup;

import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

public class LogicalBackupRecord {

  private final LogicalBackupRecordType recordType;
  private final long sequence;
  private final UUID eventGroupId;
  private final int operationIndex;
  private final long eventTime;
  private final byte requestVersion;
  private final short requestType;
  private final String metadata;
  private final byte[] payload;

  public LogicalBackupRecord(
      final LogicalBackupRecordType recordType,
      final long sequence,
      final UUID eventGroupId,
      final int operationIndex,
      final long eventTime,
      final byte requestVersion,
      final short requestType,
      final String metadata,
      final byte[] payload) {
    this.recordType = recordType;
    this.sequence = sequence;
    this.eventGroupId = eventGroupId;
    this.operationIndex = operationIndex;
    this.eventTime = eventTime;
    this.requestVersion = requestVersion;
    this.requestType = requestType;
    this.metadata = metadata;
    this.payload = payload;
  }

  public LogicalBackupRecordType getRecordType() {
    return recordType;
  }

  public long getSequence() {
    return sequence;
  }

  public UUID getEventGroupId() {
    return eventGroupId;
  }

  public int getOperationIndex() {
    return operationIndex;
  }

  public long getEventTime() {
    return eventTime;
  }

  public byte getRequestVersion() {
    return requestVersion;
  }

  public short getRequestType() {
    return requestType;
  }

  public String getMetadata() {
    return metadata;
  }

  public byte[] getPayload() {
    return payload;
  }

  public TPipeTransferReq toTPipeTransferReq() {
    return new TPipeTransferReq()
        .setVersion(requestVersion)
        .setType(requestType)
        .setBody(ByteBuffer.wrap(payload));
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof LogicalBackupRecord)) {
      return false;
    }
    final LogicalBackupRecord that = (LogicalBackupRecord) obj;
    return sequence == that.sequence
        && operationIndex == that.operationIndex
        && eventTime == that.eventTime
        && requestVersion == that.requestVersion
        && requestType == that.requestType
        && recordType == that.recordType
        && eventGroupId.equals(that.eventGroupId)
        && metadata.equals(that.metadata)
        && Arrays.equals(payload, that.payload);
  }

  @Override
  public int hashCode() {
    int result = eventGroupId.hashCode();
    result = 31 * result + recordType.hashCode();
    result = 31 * result + Long.hashCode(sequence);
    result = 31 * result + operationIndex;
    result = 31 * result + Long.hashCode(eventTime);
    result = 31 * result + requestVersion;
    result = 31 * result + requestType;
    result = 31 * result + metadata.hashCode();
    return 31 * result + Arrays.hashCode(payload);
  }
}
