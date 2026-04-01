/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.pipe.event.common.tsfile;

import org.apache.iotdb.db.pipe.event.common.PipeInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

public final class TsFileObjectPathIterator implements Iterator<String> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileObjectPathIterator.class);

  private final Iterator<TabletInsertionEvent> tabletEventIterator;
  private final PipeTsFileInsertionEvent tsfileEvent;

  private Iterator<String> currentPathIterator;

  private boolean hasCachedNext = false;
  private String nextPath = null;
  private boolean isClosed = false;

  public TsFileObjectPathIterator(@Nonnull PipeTsFileInsertionEvent tsFileEvent) {
    Objects.requireNonNull(tsFileEvent, "tsFileEvent cannot be null");

    this.tabletEventIterator = tsFileEvent.toTabletInsertionEvents(true).iterator();
    this.tsfileEvent = tsFileEvent;

    this.currentPathIterator = Collections.emptyIterator();
  }

  @Override
  public boolean hasNext() {
    if (isClosed) {
      return false;
    }

    if (hasCachedNext) {
      return true;
    }

    try {
      return fetchNextAvailablePath();
    } catch (Exception e) {
      LOGGER.error("Failed to fetch next path from TsFileEvent. Closing iterator.", e);
      close();
      return false;
    }
  }

  private boolean fetchNextAvailablePath() {
    while (true) {
      if (cacheNextPathFromCurrentIterator()) {
        return true;
      }
      if (!moveToNextTabletEvent()) {
        close();
        return false;
      }
    }
  }

  private boolean cacheNextPathFromCurrentIterator() {
    if (currentPathIterator == null) {
      return false;
    }
    while (currentPathIterator.hasNext()) {
      final String path = currentPathIterator.next();
      if (path != null && !path.trim().isEmpty()) {
        nextPath = path;
        hasCachedNext = true;
        return true;
      }
    }
    return false;
  }

  private boolean moveToNextTabletEvent() {
    if (tabletEventIterator == null || !tabletEventIterator.hasNext()) {
      return false;
    }
    final TabletInsertionEvent nextEvent = tabletEventIterator.next();
    if (nextEvent instanceof PipeInsertionEvent) {
      final Iterator<String> it = ((PipeInsertionEvent) nextEvent).objectPathIterator();
      currentPathIterator = it != null ? it : Collections.emptyIterator();
      return true;
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Skipping unsupported event type: {}",
          nextEvent != null ? nextEvent.getClass().getSimpleName() : "null");
    }
    currentPathIterator = Collections.emptyIterator();
    return true;
  }

  @Override
  @Nonnull
  public String next() {
    if (!hasNext()) {
      throw new NoSuchElementException("The TsFile path stream has been exhausted.");
    }

    String result = nextPath;
    nextPath = null;
    hasCachedNext = false;
    return result;
  }

  public void close() {
    if (!isClosed) {
      isClosed = true;
      nextPath = null;
      hasCachedNext = false;
      currentPathIterator = Collections.emptyIterator();
      if (tsfileEvent != null) {
        tsfileEvent.close();
      }
    }
  }
}
