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

package org.apache.iotdb.db.pipe.sink.protocol.thrift.async.handler;

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.event.common.PipeInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.util.PipeObjectPathUtil;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletRawReqV2;
import org.apache.iotdb.db.storageengine.load.converter.TabletObjectSplitIterator;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.tsfile.write.record.Tablet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/** Lazily emits one direct raw-tablet request for each Object value or Object fragment. */
public class PipeTabletObjectRequestIterator implements Iterator<TPipeTransferReq>, AutoCloseable {

  private final PipeInsertionEvent event;
  private final List<Tablet> tablets;
  private final List<Boolean> alignments;
  private final boolean isRawObjectValueContentEvent;

  private int tabletIndex;
  private TabletObjectSplitIterator splitIterator;
  private TPipeTransferReq nextRequest;
  private boolean exhausted;

  public PipeTabletObjectRequestIterator(final PipeInsertionEvent event) {
    this.event = event;
    isRawObjectValueContentEvent =
        event instanceof PipeRawTabletInsertionEvent && event.isObjectValueContentEvent();
    if (event instanceof PipeInsertNodeTabletInsertionEvent insertEvent) {
      tablets = insertEvent.convertToTablets();
      alignments = new ArrayList<>(tablets.size());
      for (int i = 0; i < tablets.size(); i++) {
        alignments.add(insertEvent.isAligned(i));
      }
    } else {
      final PipeRawTabletInsertionEvent rawEvent = (PipeRawTabletInsertionEvent) event;
      final Tablet tablet = rawEvent.convertToTablet();
      tablets = tablet == null ? Collections.emptyList() : Collections.singletonList(tablet);
      alignments =
          tablet == null
              ? Collections.emptyList()
              : Collections.singletonList(rawEvent.isAligned());
    }
  }

  @Override
  public boolean hasNext() {
    prepareNextRequestIfNecessary();
    return nextRequest != null;
  }

  @Override
  public TPipeTransferReq next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    final TPipeTransferReq result = nextRequest;
    nextRequest = null;
    return result;
  }

  private void prepareNextRequestIfNecessary() {
    if (nextRequest != null || exhausted) {
      return;
    }
    try {
      if (isRawObjectValueContentEvent) {
        prepareRawObjectValueContentRequest();
        return;
      }
      while (true) {
        if (tryPrepareFromSplitIterator()) {
          return;
        }
        closeSplitIterator();
        if (!tryStartNextTabletSplitIterator()) {
          exhausted = true;
          return;
        }
      }
    } catch (final IOException e) {
      close();
      throw new IllegalStateException(e);
    }
  }

  private void prepareRawObjectValueContentRequest() throws IOException {
    if (tablets.isEmpty()) {
      exhausted = true;
      return;
    }
    nextRequest =
        PipeTransferTabletRawReqV2.toTPipeTransferReq(
            tablets.get(0), alignments.get(0), event.getTableModelDatabaseName());
    exhausted = true;
  }

  private boolean tryPrepareFromSplitIterator() throws IOException {
    if (splitIterator == null || !splitIterator.hasNext()) {
      return false;
    }
    nextRequest =
        PipeTransferTabletRawReqV2.toTPipeTransferReq(
            splitIterator.next(),
            alignments.get(tabletIndex - 1),
            event.getTableModelDatabaseName());
    return true;
  }

  private boolean tryStartNextTabletSplitIterator() {
    if (tabletIndex >= tablets.size()) {
      return false;
    }
    splitIterator =
        new TabletObjectSplitIterator(
            tablets.get(tabletIndex++),
            event.getTsFileResource() == null ? null : event.getTsFileResource().getTsFile(),
            event.getTsFileResource() == null || event.getPipeName() == null
                ? null
                : PipeObjectPathUtil.resolveLinkedObjectDirectory(
                    event.getTsFileResource(), event.getPipeName()),
            true,
            PipeConfig.getInstance().getPipeSinkReadFileBufferSize());
    return true;
  }

  private void closeSplitIterator() {
    if (splitIterator != null) {
      splitIterator.close();
      splitIterator = null;
    }
  }

  @Override
  public void close() {
    closeSplitIterator();
    nextRequest = null;
    exhausted = true;
  }
}
