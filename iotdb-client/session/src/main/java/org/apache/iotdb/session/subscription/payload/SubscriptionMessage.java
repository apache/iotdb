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
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;

import org.apache.thrift.annotation.Nullable;
import org.apache.tsfile.write.record.Tablet;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SubscriptionMessage implements Comparable<SubscriptionMessage> {

  private final SubscriptionCommitContext commitContext;

  private final short messageType;

  private final SubscriptionMessageHandler handler;

  public SubscriptionMessage(
      final SubscriptionCommitContext commitContext, final Map<String, List<Tablet>> tablets) {
    this.commitContext = commitContext;
    this.messageType = SubscriptionMessageType.SESSION_DATA_SETS_HANDLER.getType();
    this.handler = new SubscriptionSessionDataSetsHandler(tablets);
  }

  public SubscriptionMessage(
      final SubscriptionCommitContext commitContext,
      final String absolutePath,
      @Nullable final String databaseName) {
    this.commitContext = commitContext;
    this.messageType = SubscriptionMessageType.TS_FILE_HANDLER.getType();
    this.handler = new SubscriptionTsFileHandler(absolutePath, databaseName);
  }

  public SubscriptionCommitContext getCommitContext() {
    return commitContext;
  }

  public short getMessageType() {
    return messageType;
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
        && Objects.equals(this.messageType, that.messageType)
        && Objects.equals(this.handler, that.handler);
  }

  @Override
  public int hashCode() {
    return Objects.hash(commitContext, messageType, handler);
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
        + "}";
  }

  /////////////////////////////// handlers ///////////////////////////////

  public SubscriptionSessionDataSetsHandler getSessionDataSetsHandler() {
    if (handler instanceof SubscriptionSessionDataSetsHandler) {
      return (SubscriptionSessionDataSetsHandler) handler;
    }
    throw new SubscriptionIncompatibleHandlerException(
        String.format(
            "%s do not support getSessionDataSetsHandler().", handler.getClass().getSimpleName()));
  }

  public SubscriptionTsFileHandler getTsFileHandler() {
    if (handler instanceof SubscriptionTsFileHandler) {
      return (SubscriptionTsFileHandler) handler;
    }
    throw new SubscriptionIncompatibleHandlerException(
        String.format("%s do not support getTsFileHandler().", handler.getClass().getSimpleName()));
  }
}
