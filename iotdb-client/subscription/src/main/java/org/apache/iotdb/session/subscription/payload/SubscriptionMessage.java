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
import org.apache.iotdb.rpc.subscription.i18n.SubscriptionMessages;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;

import org.apache.thrift.annotation.Nullable;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.write.record.Tablet;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SubscriptionMessage implements Comparable<SubscriptionMessage> {

  private final SubscriptionCommitContext commitContext;

  private final short messageType;

  private final SubscriptionMessageHandler handler;

  private final boolean timeSelected;

  /** Watermark timestamp, valid only when messageType == WATERMARK. */
  private final long watermarkTimestamp;

  private volatile boolean userDataRemoved = false;

  public SubscriptionMessage(
      final SubscriptionCommitContext commitContext, final Map<String, List<Tablet>> tablets) {
    this(commitContext, tablets, true);
  }

  public SubscriptionMessage(
      final SubscriptionCommitContext commitContext,
      final Map<String, List<Tablet>> tablets,
      final boolean timeSelected) {
    this(commitContext, tablets, timeSelected, null);
  }

  public SubscriptionMessage(
      final SubscriptionCommitContext commitContext,
      final Map<String, List<Tablet>> tablets,
      final boolean timeSelected,
      final Map<String, Map<String, Boolean>> timeSelectedByTable) {
    this.commitContext = commitContext;
    this.messageType = SubscriptionMessageType.RECORD_HANDLER.getType();
    this.handler = new SubscriptionRecordHandler(tablets, timeSelected, timeSelectedByTable);
    this.timeSelected = timeSelected;
    this.watermarkTimestamp = Long.MIN_VALUE;
  }

  public SubscriptionMessage(
      final SubscriptionCommitContext commitContext,
      final String absolutePath,
      @Nullable final String databaseName) {
    this(commitContext, absolutePath, databaseName, true);
  }

  public SubscriptionMessage(
      final SubscriptionCommitContext commitContext,
      final String absolutePath,
      @Nullable final String databaseName,
      final boolean timeSelected) {
    this.commitContext = commitContext;
    this.messageType = SubscriptionMessageType.TS_FILE.getType();
    this.handler = new SubscriptionTsFileHandler(absolutePath, databaseName);
    this.timeSelected = timeSelected;
    this.watermarkTimestamp = Long.MIN_VALUE;
  }

  /** Watermark message carrying server-side timestamp progress for a region. */
  public SubscriptionMessage(
      final SubscriptionCommitContext commitContext, final long watermarkTimestamp) {
    this.commitContext = commitContext;
    this.messageType = SubscriptionMessageType.WATERMARK.getType();
    this.handler = null;
    this.timeSelected = true;
    this.watermarkTimestamp = watermarkTimestamp;
  }

  public SubscriptionCommitContext getCommitContext() {
    return commitContext;
  }

  public short getMessageType() {
    return messageType;
  }

  public boolean isTimeSelected() {
    return timeSelected;
  }

  /**
   * Returns the watermark timestamp carried by this message. Only valid when {@code
   * getMessageType() == SubscriptionMessageType.WATERMARK.getType()}.
   *
   * @return the watermark timestamp
   * @throws IllegalStateException if this is not a watermark message
   */
  public long getWatermarkTimestamp() {
    if (messageType != SubscriptionMessageType.WATERMARK.getType()) {
      throw new IllegalStateException(
          SubscriptionMessages
                  .EXCEPTION_WATERMARK_TIMESTAMP_ONLY_AVAILABLE_WATERMARK_MESSAGES_ACTUAL_MESSAGE_TYPE_F8E32C57
              + messageType);
    }
    return watermarkTimestamp;
  }

  /**
   * Estimates the heap memory occupied by this message in bytes. For tablet-based messages, this
   * delegates to {@link Tablet#ramBytesUsed()} for accurate per-column estimation.
   *
   * @return estimated byte size
   */
  public long estimateSize() {
    // Object header + references + primitives (rough constant)
    long size = 64;
    if (handler instanceof SubscriptionRecordHandler) {
      final Iterator<Tablet> it = getRecordTabletIterator();
      while (it.hasNext()) {
        size += it.next().ramBytesUsed();
      }
    }
    return size;
  }

  public void removeUserData() {
    if (userDataRemoved) {
      return;
    }

    if (Objects.nonNull(handler)) {
      handler.removeUserData();
    }
    if (handler instanceof SubscriptionRecordHandler) {
      userDataRemoved = true;
    }
  }

  /////////////////////////////// override ///////////////////////////////

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final SubscriptionMessage that = (SubscriptionMessage) obj;
    return Objects.equals(this.commitContext, that.commitContext)
        && this.watermarkTimestamp == that.watermarkTimestamp
        && this.timeSelected == that.timeSelected
        && Objects.equals(this.messageType, that.messageType)
        && Objects.equals(this.handler, that.handler);
  }

  @Override
  public int hashCode() {
    return Objects.hash(commitContext, messageType, handler, timeSelected, watermarkTimestamp);
  }

  @Override
  public int compareTo(final SubscriptionMessage that) {
    return this.commitContext.compareTo(that.commitContext);
  }

  @Override
  public String toString() {
    return "SubscriptionMessage{commitContext="
        + commitContext
        + ", messageType="
        + SubscriptionMessageType.valueOf(messageType).toString()
        + ", timeSelected="
        + timeSelected
        + ", watermarkTimestamp="
        + watermarkTimestamp
        + "}";
  }

  /////////////////////////////// handlers ///////////////////////////////

  public List<ResultSet> getResultSets() {
    ensureUserDataAvailable();
    if (handler instanceof SubscriptionRecordHandler) {
      return ((SubscriptionRecordHandler) handler).getResultSets();
    }
    throw new SubscriptionIncompatibleHandlerException(
        String.format(
            SubscriptionMessages.EXCEPTION_ARG_DO_NOT_SUPPORT_GETRESULTSETS_7789852D,
            handler.getClass().getSimpleName()));
  }

  public Iterator<Tablet> getRecordTabletIterator() {
    ensureUserDataAvailable();
    if (handler instanceof SubscriptionRecordHandler) {
      final List<ResultSet> resultSets = ((SubscriptionRecordHandler) handler).getResultSets();
      return resultSets.stream()
          .map(record -> ((SubscriptionRecordHandler.SubscriptionResultSet) record).getTablet())
          .iterator();
    }
    throw new SubscriptionIncompatibleHandlerException(
        String.format(
            SubscriptionMessages.EXCEPTION_ARG_DO_NOT_SUPPORT_GETRECORDTABLETITERATOR_46B4A489,
            handler.getClass().getSimpleName()));
  }

  public SubscriptionTsFileHandler getTsFile() {
    if (handler instanceof SubscriptionTsFileHandler) {
      return (SubscriptionTsFileHandler) handler;
    }
    throw new SubscriptionIncompatibleHandlerException(
        String.format(
            SubscriptionMessages.EXCEPTION_ARG_DO_NOT_SUPPORT_GETTSFILE_40D23462,
            handler.getClass().getSimpleName()));
  }

  private void ensureUserDataAvailable() {
    if (userDataRemoved) {
      throw new SubscriptionRuntimeException(
          String.format(
              SubscriptionMessages.EXCEPTION_USER_DATA_HAS_BEEN_REMOVED_ARG_7093644B,
              getClass().getSimpleName()));
    }
  }
}
