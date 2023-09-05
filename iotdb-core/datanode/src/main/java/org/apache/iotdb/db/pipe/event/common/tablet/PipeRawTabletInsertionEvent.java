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

package org.apache.iotdb.db.pipe.event.common.tablet;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.tsfile.write.record.Tablet;

import java.util.Objects;
import java.util.function.BiConsumer;

public class PipeRawTabletInsertionEvent extends EnrichedEvent implements TabletInsertionEvent {

  private final Tablet tablet;
  private final boolean isAligned;

  private final EnrichedEvent sourceEvent;
  private boolean needToReport;

  private TabletInsertionDataContainer dataContainer;

  private PipeRawTabletInsertionEvent(
      Tablet tablet,
      boolean isAligned,
      EnrichedEvent sourceEvent,
      boolean needToReport,
      PipeTaskMeta pipeTaskMeta,
      String pattern) {
    super(pipeTaskMeta, pattern);
    this.tablet = Objects.requireNonNull(tablet);
    this.isAligned = isAligned;
    this.sourceEvent = sourceEvent;
    this.needToReport = needToReport;
  }

  public PipeRawTabletInsertionEvent(
      Tablet tablet,
      boolean isAligned,
      PipeTaskMeta pipeTaskMeta,
      EnrichedEvent sourceEvent,
      boolean needToReport) {
    this(tablet, isAligned, sourceEvent, needToReport, pipeTaskMeta, null);
  }

  @TestOnly
  public PipeRawTabletInsertionEvent(Tablet tablet, boolean isAligned) {
    this(tablet, isAligned, null, false, null, null);
  }

  @TestOnly
  public PipeRawTabletInsertionEvent(Tablet tablet, boolean isAligned, String pattern) {
    this(tablet, isAligned, null, false, null, pattern);
  }

  @Override
  public boolean internallyIncreaseResourceReferenceCount(String holderMessage) {
    return true;
  }

  @Override
  public boolean internallyDecreaseResourceReferenceCount(String holderMessage) {
    return true;
  }

  @Override
  protected void reportProgress() {
    if (needToReport) {
      super.reportProgress();
    }
  }

  @Override
  public ProgressIndex getProgressIndex() {
    return sourceEvent != null ? sourceEvent.getProgressIndex() : new MinimumProgressIndex();
  }

  @Override
  public EnrichedEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
      PipeTaskMeta pipeTaskMeta, String pattern) {
    return new PipeRawTabletInsertionEvent(
        tablet, isAligned, sourceEvent, needToReport, pipeTaskMeta, pattern);
  }

  @Override
  public boolean isGeneratedByPipe() {
    throw new UnsupportedOperationException("isGeneratedByPipe() is not supported!");
  }

  public void markAsNeedToReport() {
    this.needToReport = true;
  }

  /////////////////////////// TabletInsertionEvent ///////////////////////////

  @Override
  public Iterable<TabletInsertionEvent> processRowByRow(BiConsumer<Row, RowCollector> consumer) {
    if (dataContainer == null) {
      dataContainer =
          new TabletInsertionDataContainer(pipeTaskMeta, this, tablet, isAligned, getPattern());
    }
    return dataContainer.processRowByRow(consumer);
  }

  @Override
  public Iterable<TabletInsertionEvent> processTablet(BiConsumer<Tablet, RowCollector> consumer) {
    if (dataContainer == null) {
      dataContainer =
          new TabletInsertionDataContainer(pipeTaskMeta, this, tablet, isAligned, getPattern());
    }
    return dataContainer.processTablet(consumer);
  }

  /////////////////////////// convertToTablet ///////////////////////////

  public boolean isAligned() {
    return isAligned;
  }

  public Tablet convertToTablet() {
    if (!shouldParsePatternOrTime()) {
      return tablet;
    }

    // if notNullPattern is not "root", we need to convert the tablet
    if (dataContainer == null) {
      dataContainer =
          new TabletInsertionDataContainer(pipeTaskMeta, this, tablet, isAligned, getPattern());
    }
    return dataContainer.convertToTablet();
  }

  /////////////////////////// parsePattern ///////////////////////////

  public TabletInsertionEvent parseEventWithPattern() {
    return new PipeRawTabletInsertionEvent(
        convertToTablet(), isAligned, pipeTaskMeta, this, needToReport);
  }
}
