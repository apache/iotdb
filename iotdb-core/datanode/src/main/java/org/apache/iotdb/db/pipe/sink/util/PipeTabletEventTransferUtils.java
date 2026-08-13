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

package org.apache.iotdb.db.pipe.sink.util;

import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;

public final class PipeTabletEventTransferUtils {

  private PipeTabletEventTransferUtils() {}

  public static void transferByTablet(
      final TabletInsertionEvent tabletInsertionEvent,
      final String holderMessage,
      final Logger logger,
      final ThrowingBiConsumer<Tablet, Boolean, Exception> transferTablet)
      throws Exception {
    // PipeProcessor can change the type of TabletInsertionEvent.
    if (!(tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent)
        && !(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
      logger.warn(
          DataNodePipeMessages
              .THIS_CONNECTOR_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_AND_PIPERAWTABLET,
          tabletInsertionEvent);
      return;
    }

    if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
      transferTabletWrapper(
          (PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent, holderMessage, transferTablet);
    } else {
      transferTabletWrapper(
          (PipeRawTabletInsertionEvent) tabletInsertionEvent, holderMessage, transferTablet);
    }
  }

  private static void transferTabletWrapper(
      final PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent,
      final String holderMessage,
      final ThrowingBiConsumer<Tablet, Boolean, Exception> transferTablet)
      throws Exception {
    if (!pipeInsertNodeTabletInsertionEvent.increaseReferenceCount(holderMessage)) {
      return;
    }
    try {
      for (final Tablet tablet : pipeInsertNodeTabletInsertionEvent.convertToTablets()) {
        transferTablet.accept(tablet, pipeInsertNodeTabletInsertionEvent.isTableModelEvent());
      }
    } finally {
      pipeInsertNodeTabletInsertionEvent.decreaseReferenceCount(holderMessage, false);
    }
  }

  private static void transferTabletWrapper(
      final PipeRawTabletInsertionEvent pipeRawTabletInsertionEvent,
      final String holderMessage,
      final ThrowingBiConsumer<Tablet, Boolean, Exception> transferTablet)
      throws Exception {
    if (!pipeRawTabletInsertionEvent.increaseReferenceCount(holderMessage)) {
      return;
    }
    try {
      transferTablet.accept(
          pipeRawTabletInsertionEvent.convertToTablet(),
          pipeRawTabletInsertionEvent.isTableModelEvent());
    } finally {
      pipeRawTabletInsertionEvent.decreaseReferenceCount(holderMessage, false);
    }
  }

  @FunctionalInterface
  public interface ThrowingBiConsumer<T, U, E extends Exception> {
    void accept(final T t, final U u) throws E;
  }
}
