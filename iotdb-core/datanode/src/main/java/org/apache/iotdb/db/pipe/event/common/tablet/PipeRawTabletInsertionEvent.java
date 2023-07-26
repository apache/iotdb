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
import org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.tsfile.write.record.Tablet;

import java.util.Objects;
import java.util.function.BiConsumer;

public class PipeRawTabletInsertionEvent extends EnrichedEvent implements TabletInsertionEvent {

  private final Tablet tablet;
  private final boolean isAligned;
  private final boolean needToReport;
  private final String pattern;

  private PipeTsFileInsertionEvent sourceEvent;
  private TabletInsertionDataContainer dataContainer;

  public PipeRawTabletInsertionEvent(Tablet tablet, boolean isAligned) {
    this(tablet, isAligned, null, null, false, null);
  }

  public PipeRawTabletInsertionEvent(
      Tablet tablet,
      boolean isAligned,
      PipeTaskMeta pipeTaskMeta,
      PipeTsFileInsertionEvent sourceEvent,
      boolean needToReport) {
    this(tablet, isAligned, pipeTaskMeta, sourceEvent, needToReport, null);
  }

  public PipeRawTabletInsertionEvent(Tablet tablet, boolean isAligned, String pattern) {
    this(tablet, isAligned, null, null, false, pattern);
  }

  public PipeRawTabletInsertionEvent(
      Tablet tablet,
      boolean isAligned,
      PipeTaskMeta pipeTaskMeta,
      PipeTsFileInsertionEvent sourceEvent,
      boolean needToReport,
      String pattern) {
    super(pipeTaskMeta, pattern);
    this.tablet = Objects.requireNonNull(tablet);
    this.isAligned = isAligned;
    this.sourceEvent = sourceEvent;
    this.needToReport = needToReport;
    this.pattern = pattern;
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
        tablet, isAligned, pipeTaskMeta, sourceEvent, needToReport, pattern);
  }

  /////////////////////////// TabletInsertionEvent ///////////////////////////

  @Override
  public Iterable<TabletInsertionEvent> processRowByRow(BiConsumer<Row, RowCollector> consumer) {
    if (dataContainer == null) {
      dataContainer = new TabletInsertionDataContainer(tablet, isAligned, getPattern());
    }
    return dataContainer.processRowByRow(consumer);
  }

  @Override
  public Iterable<TabletInsertionEvent> processTablet(BiConsumer<Tablet, RowCollector> consumer) {
    if (dataContainer == null) {
      dataContainer = new TabletInsertionDataContainer(tablet, isAligned, getPattern());
    }
    return dataContainer.processTablet(consumer);
  }

  /////////////////////////// convertToTablet ///////////////////////////

  public boolean isAligned() {
    return isAligned;
  }

  public Tablet convertToTablet() {
    final String notNullPattern = getPattern();

    // if notNullPattern is "root", we don't need to convert, just return the original tablet
    if (notNullPattern.equals(PipeExtractorConstant.EXTRACTOR_PATTERN_DEFAULT_VALUE)) {
      return tablet;
    }

    // if notNullPattern is not "root", we need to convert the tablet
    if (dataContainer == null) {
      dataContainer = new TabletInsertionDataContainer(tablet, isAligned, notNullPattern);
    }
    return dataContainer.convertToTablet();
  }
}
