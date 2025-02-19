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

package org.apache.iotdb.db.pipe.event.common.tsfile.parser.table;

import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.db.pipe.event.common.PipeInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.TsFileInsertionEventParser;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

public class TsFileInsertionEventTableParser extends TsFileInsertionEventParser {

  private final TsFileInsertionEventTableParserTabletIterator tabletIterator;

  public TsFileInsertionEventTableParser(
      final File tsFile,
      final TablePattern pattern,
      final long startTime,
      final long endTime,
      final PipeTaskMeta pipeTaskMeta,
      final PipeInsertionEvent sourceEvent)
      throws IOException {
    super(null, pattern, startTime, endTime, pipeTaskMeta, sourceEvent);

    try {
      tsFileSequenceReader = new TsFileSequenceReader(tsFile.getPath(), true, true);
      tabletIterator =
          new TsFileInsertionEventTableParserTabletIterator(
              tsFileSequenceReader,
              entry -> Objects.isNull(pattern) || pattern.matchesTable(entry.getKey()),
              startTime,
              endTime);
    } catch (final Exception e) {
      close();
      throw e;
    }
  }

  @Override
  public Iterable<TabletInsertionEvent> toTabletInsertionEvents() {
    return () ->
        new Iterator<TabletInsertionEvent>() {

          private TsFileInsertionEventTableParserTabletIterator tabletIterator = null;

          @Override
          public boolean hasNext() {
            return tabletIterator != null && !tabletIterator.hasNext();
          }

          @Override
          public TabletInsertionEvent next() {
            if (!hasNext()) {
              close();
              throw new NoSuchElementException();
            }

            final Pair<Tablet, Integer> tablet = tabletIterator.next();

            final TabletInsertionEvent next;
            if (!hasNext()) {
              next =
                  sourceEvent == null
                      ? new PipeRawTabletInsertionEvent(
                          Boolean.TRUE,
                          null,
                          null,
                          null,
                          tablet,
                          true,
                          null,
                          0,
                          pipeTaskMeta,
                          sourceEvent,
                          true)
                      : new PipeRawTabletInsertionEvent(
                          Boolean.TRUE,
                          sourceEvent.getSourceDatabaseNameFromDataRegion(),
                          sourceEvent.getRawTableModelDataBase(),
                          sourceEvent.getRawTreeModelDataBase(),
                          tablet,
                          true,
                          sourceEvent.getPipeName(),
                          sourceEvent.getCreationTime(),
                          pipeTaskMeta,
                          sourceEvent,
                          true);
              close();
            } else {
              next =
                  sourceEvent == null
                      ? new PipeRawTabletInsertionEvent(
                          Boolean.TRUE,
                          null,
                          null,
                          null,
                          tablet,
                          true,
                          null,
                          0,
                          pipeTaskMeta,
                          sourceEvent,
                          false)
                      : new PipeRawTabletInsertionEvent(
                          Boolean.TRUE,
                          sourceEvent.getSourceDatabaseNameFromDataRegion(),
                          sourceEvent.getRawTableModelDataBase(),
                          sourceEvent.getRawTreeModelDataBase(),
                          tablet,
                          true,
                          sourceEvent.getPipeName(),
                          sourceEvent.getCreationTime(),
                          pipeTaskMeta,
                          sourceEvent,
                          false);
            }
            return next;
          }
        };
  }
}
