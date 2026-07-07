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

package org.apache.iotdb.db.pipe.event.common.util;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public final class PipeDataLossDebugUtil {

  public static final String PREFIX = "[PipeDataLossDebug]";

  private static final int MAX_PRINTED_MEASUREMENTS = 16;
  private static final int MAX_PRINTED_STATEMENTS = 16;
  private static final int MAX_PRINTED_STATUSES = 16;
  private static final int MAX_PRINTED_EVENTS = 32;
  private static final int MAX_PRINTED_PLAN_NODES = 16;

  private PipeDataLossDebugUtil() {}

  public static String formatPipe(final String pipeName, final long creationTime) {
    return "pipeName=" + pipeName + ", creationTime=" + creationTime;
  }

  public static String formatReq(final TPipeTransferReq req) {
    if (Objects.isNull(req)) {
      return "req=null";
    }

    final byte[] body = req.getBody();
    return "version="
        + req.getVersion()
        + ", type="
        + req.getType()
        + ", bodySize="
        + (Objects.isNull(body) ? "null" : body.length)
        + ", bodyHash="
        + (Objects.isNull(body) ? "null" : Arrays.hashCode(body));
  }

  public static String formatException(final Exception exception) {
    if (Objects.isNull(exception)) {
      return "exception=null";
    }
    return "exceptionClass="
        + exception.getClass().getName()
        + ", message="
        + exception.getMessage();
  }

  public static String formatEvent(final Event event) {
    if (Objects.isNull(event)) {
      return "event=null";
    }

    final StringBuilder builder =
        new StringBuilder("eventClass=")
            .append(event.getClass().getSimpleName())
            .append(", identity=")
            .append(System.identityHashCode(event));

    if (event instanceof EnrichedEvent) {
      final EnrichedEvent enrichedEvent = (EnrichedEvent) event;
      builder
          .append(", ")
          .append(formatPipe(enrichedEvent.getPipeName(), enrichedEvent.getCreationTime()))
          .append(", regionId=")
          .append(enrichedEvent.getRegionId())
          .append(", committerKey=")
          .append(enrichedEvent.getCommitterKey())
          .append(", commitId=")
          .append(enrichedEvent.getCommitId())
          .append(", commitIds=")
          .append(enrichedEvent.getCommitIds())
          .append(", referenceCount=")
          .append(enrichedEvent.getReferenceCount())
          .append(", released=")
          .append(enrichedEvent.isReleased())
          .append(", shouldReportOnCommit=")
          .append(enrichedEvent.isShouldReportOnCommit())
          .append(", progressIndex=")
          .append(enrichedEvent.getProgressIndex())
          .append(", eventTimeRange=[")
          .append(enrichedEvent.getStartTime())
          .append(",")
          .append(enrichedEvent.getEndTime())
          .append("]")
          .append(", generatedByPipe=")
          .append(safeIsGeneratedByPipe(enrichedEvent));
    }

    if (event instanceof PipeRawTabletInsertionEvent) {
      final PipeRawTabletInsertionEvent rawEvent = (PipeRawTabletInsertionEvent) event;
      builder
          .append(", device=")
          .append(rawEvent.getDeviceId())
          .append(", aligned=")
          .append(rawEvent.isAligned())
          .append(", tablet={")
          .append(rawEvent.getTabletDebugString())
          .append("}");
    } else if (event instanceof PipeInsertNodeTabletInsertionEvent) {
      final PipeInsertNodeTabletInsertionEvent insertNodeEvent =
          (PipeInsertNodeTabletInsertionEvent) event;
      builder
          .append(", device=")
          .append(insertNodeEvent.getDeviceId())
          .append(", insertNode={")
          .append(formatInsertNode(insertNodeEvent.getInsertNode()))
          .append("}");
    } else if (event instanceof PipeTsFileInsertionEvent) {
      final PipeTsFileInsertionEvent tsFileEvent = (PipeTsFileInsertionEvent) event;
      final File tsFile = tsFileEvent.getTsFile();
      builder
          .append(", tsFile=")
          .append(Objects.isNull(tsFile) ? null : tsFile.getAbsolutePath())
          .append(", tsFileLength=")
          .append(Objects.isNull(tsFile) ? null : tsFile.length());
    }

    return builder.toString();
  }

  public static String formatEvents(final Iterable<? extends Event> events) {
    if (Objects.isNull(events)) {
      return "events=null";
    }

    final List<String> samples = new ArrayList<>();
    final Map<String, Integer> pipeCounters = new LinkedHashMap<>();
    int eventCount = 0;
    for (final Event event : events) {
      ++eventCount;
      if (event instanceof EnrichedEvent) {
        final EnrichedEvent enrichedEvent = (EnrichedEvent) event;
        final String pipeKey =
            enrichedEvent.getPipeName()
                + "@"
                + enrichedEvent.getCreationTime()
                + "#"
                + enrichedEvent.getRegionId();
        pipeCounters.compute(pipeKey, (key, value) -> Objects.isNull(value) ? 1 : value + 1);
      }
      if (samples.size() < MAX_PRINTED_EVENTS) {
        samples.add(formatEvent(event));
      }
    }

    return "eventCount="
        + eventCount
        + ", pipeCounters="
        + pipeCounters
        + ", samples="
        + samples
        + (eventCount > MAX_PRINTED_EVENTS ? "...(" + eventCount + ")" : "");
  }

  public static String formatTablet(final Tablet tablet) {
    if (Objects.isNull(tablet)) {
      return "tablet=null";
    }

    final int rowSize = tablet.getRowSize();
    final List<IMeasurementSchema> schemas = tablet.getSchemas();
    final int schemaSize = Objects.isNull(schemas) ? 0 : schemas.size();
    final long markedNullCells = countMarkedCells(tablet.getBitMaps(), rowSize);
    final long[] timestamps = tablet.getTimestamps();
    final String firstTime =
        rowSize > 0 && Objects.nonNull(timestamps) && timestamps.length > 0
            ? String.valueOf(timestamps[0])
            : "null";
    final String lastTime =
        rowSize > 0 && Objects.nonNull(timestamps) && timestamps.length >= rowSize
            ? String.valueOf(timestamps[rowSize - 1])
            : "null";

    return "device="
        + safeGetDeviceId(tablet)
        + ", rowSize="
        + rowSize
        + ", schemaSize="
        + schemaSize
        + ", dataPointCount="
        + countDataPoints(rowSize, schemaSize, markedNullCells)
        + ", measurements="
        + formatMeasurements(schemas)
        + ", firstTime="
        + firstTime
        + ", lastTime="
        + lastTime
        + ", markedNullCells="
        + markedNullCells;
  }

  public static String formatTabletInsertionEvent(final TabletInsertionEvent event) {
    if (Objects.isNull(event)) {
      return "tabletInsertionEvent=null";
    }
    if (event instanceof PipeRawTabletInsertionEvent) {
      final PipeRawTabletInsertionEvent rawEvent = (PipeRawTabletInsertionEvent) event;
      return "rawTablet, aligned="
          + rawEvent.isAligned()
          + ", device="
          + rawEvent.getDeviceId()
          + ", "
          + rawEvent.getTabletDebugString();
    }
    if (event instanceof PipeInsertNodeTabletInsertionEvent) {
      final PipeInsertNodeTabletInsertionEvent insertNodeEvent =
          (PipeInsertNodeTabletInsertionEvent) event;
      return "insertNodeTablet, device="
          + insertNodeEvent.getDeviceId()
          + ", "
          + formatInsertNode(insertNodeEvent.getInsertNode());
    }
    return String.valueOf(event);
  }

  public static String formatInsertNode(final InsertNode node) {
    if (Objects.isNull(node)) {
      return "insertNode=null";
    }
    if (node instanceof InsertTabletNode) {
      return formatInsertTabletNode((InsertTabletNode) node);
    }
    if (node instanceof InsertRowsNode) {
      return formatInsertRowsNode((InsertRowsNode) node);
    }
    if (node instanceof InsertMultiTabletsNode) {
      return formatInsertMultiTabletsNode((InsertMultiTabletsNode) node);
    }
    if (node instanceof InsertRowNode) {
      return formatInsertRowNode((InsertRowNode) node);
    }
    return "type="
        + node.getType()
        + ", class="
        + node.getClass().getName()
        + ", device="
        + node.getTargetPath()
        + ", aligned="
        + node.isAligned()
        + ", generatedByPipe="
        + node.isGeneratedByPipe()
        + ", progressIndex="
        + node.getProgressIndex()
        + ", measurements="
        + formatMeasurements(node.getMeasurements());
  }

  public static String formatInsertTabletNode(final InsertTabletNode node) {
    if (Objects.isNull(node)) {
      return "insertTabletNode=null";
    }

    final int rowCount = node.getRowCount();
    final int measurementCount = measurementCount(node.getMeasurements());
    final long markedNullCells = countMarkedCells(node.getBitMaps(), rowCount);
    return "type="
        + node.getType()
        + ", device="
        + node.getTargetPath()
        + ", aligned="
        + node.isAligned()
        + ", generatedByPipe="
        + node.isGeneratedByPipe()
        + ", progressIndex="
        + node.getProgressIndex()
        + ", rowCount="
        + rowCount
        + ", measurementCount="
        + measurementCount
        + ", dataPointCount="
        + countDataPoints(rowCount, measurementCount, markedNullCells)
        + ", measurements="
        + formatMeasurements(node.getMeasurements())
        + ", firstTime="
        + firstTime(node.getTimes(), rowCount)
        + ", lastTime="
        + lastTime(node.getTimes(), rowCount)
        + ", markedNullCells="
        + markedNullCells;
  }

  public static String formatInsertRowNode(final InsertRowNode node) {
    if (Objects.isNull(node)) {
      return "insertRowNode=null";
    }

    return "type="
        + node.getType()
        + ", device="
        + node.getTargetPath()
        + ", aligned="
        + node.isAligned()
        + ", generatedByPipe="
        + node.isGeneratedByPipe()
        + ", progressIndex="
        + node.getProgressIndex()
        + ", time="
        + node.getTime()
        + ", measurementCount="
        + measurementCount(node.getMeasurements())
        + ", measurements="
        + formatMeasurements(node.getMeasurements());
  }

  public static String formatInsertRowsNode(final InsertRowsNode node) {
    if (Objects.isNull(node)) {
      return "insertRowsNode=null";
    }

    final List<InsertRowNode> insertRowNodes = node.getInsertRowNodeList();
    return "type="
        + node.getType()
        + ", generatedByPipe="
        + node.isGeneratedByPipe()
        + ", progressIndex="
        + node.getProgressIndex()
        + ", rowNodeCount="
        + insertRowNodes.size()
        + ", indexes="
        + node.getInsertRowNodeIndexList()
        + ", results="
        + formatIndexedStatuses(node.getResults())
        + ", samples="
        + insertRowNodes.stream()
            .limit(MAX_PRINTED_PLAN_NODES)
            .map(PipeDataLossDebugUtil::formatInsertRowNode)
            .collect(Collectors.toList())
        + (insertRowNodes.size() > MAX_PRINTED_PLAN_NODES
            ? "...(" + insertRowNodes.size() + ")"
            : "");
  }

  public static String formatInsertRowsOfOneDeviceNode(final InsertRowsOfOneDeviceNode node) {
    if (Objects.isNull(node)) {
      return "insertRowsOfOneDeviceNode=null";
    }

    final List<InsertRowNode> insertRowNodes = node.getInsertRowNodeList();
    return "type="
        + node.getType()
        + ", device="
        + node.getTargetPath()
        + ", aligned="
        + node.isAligned()
        + ", generatedByPipe="
        + node.isGeneratedByPipe()
        + ", progressIndex="
        + node.getProgressIndex()
        + ", rowNodeCount="
        + insertRowNodes.size()
        + ", indexes="
        + node.getInsertRowNodeIndexList()
        + ", results="
        + formatIndexedStatuses(node.getResults())
        + ", samples="
        + insertRowNodes.stream()
            .limit(MAX_PRINTED_PLAN_NODES)
            .map(PipeDataLossDebugUtil::formatInsertRowNode)
            .collect(Collectors.toList())
        + (insertRowNodes.size() > MAX_PRINTED_PLAN_NODES
            ? "...(" + insertRowNodes.size() + ")"
            : "");
  }

  public static String formatInsertMultiTabletsNode(final InsertMultiTabletsNode node) {
    if (Objects.isNull(node)) {
      return "insertMultiTabletsNode=null";
    }

    final List<InsertTabletNode> insertTabletNodes = node.getInsertTabletNodeList();
    return "type="
        + node.getType()
        + ", generatedByPipe="
        + node.isGeneratedByPipe()
        + ", progressIndex="
        + node.getProgressIndex()
        + ", tabletNodeCount="
        + insertTabletNodes.size()
        + ", parentIndexes="
        + node.getParentInsertTabletNodeIndexList()
        + ", results="
        + formatIndexedStatuses(node.getResults())
        + ", totalRows="
        + insertTabletNodes.stream().mapToLong(InsertTabletNode::getRowCount).sum()
        + ", totalDataPointCount="
        + insertTabletNodes.stream().mapToLong(PipeDataLossDebugUtil::countDataPoints).sum()
        + ", samples="
        + insertTabletNodes.stream()
            .limit(MAX_PRINTED_PLAN_NODES)
            .map(PipeDataLossDebugUtil::formatInsertTabletNode)
            .collect(Collectors.toList())
        + (insertTabletNodes.size() > MAX_PRINTED_PLAN_NODES
            ? "...(" + insertTabletNodes.size() + ")"
            : "");
  }

  public static String formatStatement(final Statement statement) {
    if (Objects.isNull(statement)) {
      return "statement=null";
    }

    if (statement instanceof InsertTabletStatement) {
      final InsertTabletStatement insertTabletStatement = (InsertTabletStatement) statement;
      final int rowCount = insertTabletStatement.getRowCount();
      final int measurementCount = measurementCount(insertTabletStatement.getMeasurements());
      final long markedNullCells =
          countMarkedCells(insertTabletStatement.getBitMaps(), insertTabletStatement.getRowCount());
      return "type="
          + statement.getType()
          + ", database="
          + insertTabletStatement.getDatabaseName().orElse(null)
          + ", writeToTable="
          + insertTabletStatement.isWriteToTable()
          + ", device="
          + insertTabletStatement.getDevicePath()
          + ", rowCount="
          + rowCount
          + ", measurementCount="
          + measurementCount
          + ", dataPointCount="
          + countDataPoints(rowCount, measurementCount, markedNullCells)
          + ", measurements="
          + formatMeasurements(insertTabletStatement.getMeasurements())
          + ", firstTime="
          + firstTime(insertTabletStatement.getTimes(), rowCount)
          + ", lastTime="
          + lastTime(insertTabletStatement.getTimes(), rowCount)
          + ", markedNullCells="
          + markedNullCells
          + ", failedMeasurements="
          + insertTabletStatement.getFailedMeasurements();
    }

    if (statement instanceof InsertMultiTabletsStatement) {
      final InsertMultiTabletsStatement insertMultiTabletsStatement =
          (InsertMultiTabletsStatement) statement;
      final List<InsertTabletStatement> statements =
          insertMultiTabletsStatement.getInsertTabletStatementList();
      return "type="
          + statement.getType()
          + ", database="
          + insertMultiTabletsStatement.getDatabaseName().orElse(null)
          + ", tabletCount="
          + statements.size()
          + ", totalRows="
          + statements.stream().mapToInt(InsertTabletStatement::getRowCount).sum()
          + ", totalDataPointCount="
          + statements.stream().mapToLong(PipeDataLossDebugUtil::countDataPoints).sum()
          + ", totalMarkedNullCells="
          + statements.stream().mapToLong(PipeDataLossDebugUtil::countMarkedCells).sum()
          + ", samples="
          + statements.stream()
              .limit(MAX_PRINTED_STATEMENTS)
              .map(PipeDataLossDebugUtil::formatStatement)
              .collect(Collectors.toList())
          + (statements.size() > MAX_PRINTED_STATEMENTS ? "...(" + statements.size() + ")" : "");
    }

    if (statement instanceof InsertRowsStatement) {
      final InsertRowsStatement insertRowsStatement = (InsertRowsStatement) statement;
      return "type="
          + statement.getType()
          + ", database="
          + insertRowsStatement.getDatabaseName().orElse(null)
          + ", rowStatementCount="
          + insertRowsStatement.getInsertRowStatementList().size()
          + ", totalDataPointCount="
          + insertRowsStatement.getInsertRowStatementList().stream()
              .mapToLong(PipeDataLossDebugUtil::countDataPoints)
              .sum()
          + ", samples="
          + insertRowsStatement.getInsertRowStatementList().stream()
              .limit(MAX_PRINTED_STATEMENTS)
              .map(PipeDataLossDebugUtil::formatStatement)
              .collect(Collectors.toList())
          + (insertRowsStatement.getInsertRowStatementList().size() > MAX_PRINTED_STATEMENTS
              ? "...(" + insertRowsStatement.getInsertRowStatementList().size() + ")"
              : "");
    }

    if (statement instanceof InsertRowStatement) {
      final InsertRowStatement insertRowStatement = (InsertRowStatement) statement;
      return "type="
          + statement.getType()
          + ", database="
          + insertRowStatement.getDatabaseName().orElse(null)
          + ", device="
          + insertRowStatement.getDevicePath()
          + ", time="
          + insertRowStatement.getTime()
          + ", measurementCount="
          + measurementCount(insertRowStatement.getMeasurements())
          + ", dataPointCount="
          + countDataPoints(insertRowStatement)
          + ", measurements="
          + formatMeasurements(insertRowStatement.getMeasurements())
          + ", failedMeasurements="
          + insertRowStatement.getFailedMeasurements();
    }

    return "type=" + statement.getType() + ", class=" + statement.getClass().getName();
  }

  public static String formatStatements(final Collection<? extends Statement> statements) {
    if (Objects.isNull(statements)) {
      return "statements=null";
    }

    return "statementCount="
        + statements.size()
        + ", totalRows="
        + statements.stream().mapToLong(PipeDataLossDebugUtil::countRows).sum()
        + ", totalDataPointCount="
        + statements.stream().mapToLong(PipeDataLossDebugUtil::countDataPoints).sum()
        + ", totalMarkedNullCells="
        + statements.stream().mapToLong(PipeDataLossDebugUtil::countMarkedCells).sum()
        + ", samples="
        + statements.stream()
            .limit(MAX_PRINTED_STATEMENTS)
            .map(PipeDataLossDebugUtil::formatStatement)
            .collect(Collectors.toList())
        + (statements.size() > MAX_PRINTED_STATEMENTS ? "...(" + statements.size() + ")" : "");
  }

  public static String formatStatus(final TSStatus status) {
    if (Objects.isNull(status)) {
      return "status=null";
    }
    return formatSingleStatus(status)
        + (status.isSetSubStatus()
            ? ", subStatuses=" + formatStatusesWithIndexes(status.getSubStatus())
            : "");
  }

  public static String formatStatusHeader(final TSStatus status) {
    if (Objects.isNull(status)) {
      return "status=null";
    }
    return "code="
        + status.getCode()
        + ", subStatusSize="
        + status.getSubStatusSize()
        + ", message="
        + status.getMessage();
  }

  public static String formatStatuses(final Collection<TSStatus> statuses) {
    if (Objects.isNull(statuses)) {
      return "statuses=null";
    }

    return statuses.stream()
            .limit(MAX_PRINTED_STATUSES)
            .map(PipeDataLossDebugUtil::formatStatus)
            .collect(Collectors.toList())
        + (statuses.size() > MAX_PRINTED_STATUSES ? "...(" + statuses.size() + ")" : "");
  }

  public static String formatStatementStatusMapping(
      final Statement statement, final TSStatus status) {
    if (Objects.isNull(statement) || Objects.isNull(status) || !status.isSetSubStatus()) {
      return "statementStatusMapping=none";
    }

    if (statement instanceof InsertMultiTabletsStatement) {
      final List<InsertTabletStatement> statements =
          ((InsertMultiTabletsStatement) statement).getInsertTabletStatementList();
      return formatIndexedStatementStatusMapping(statements, status.getSubStatus());
    }

    if (statement instanceof InsertRowsStatement) {
      final List<InsertRowStatement> statements =
          ((InsertRowsStatement) statement).getInsertRowStatementList();
      return formatIndexedStatementStatusMapping(statements, status.getSubStatus());
    }

    return "statementStatusMapping=unsupported, statementType=" + statement.getType();
  }

  public static String formatQueueState(
      final int retryEventQueueSize,
      final int retryTsFileQueueSize,
      final int tabletEventCount,
      final int tsFileEventCount) {
    return "retryEventQueueSize="
        + retryEventQueueSize
        + ", retryTsFileQueueSize="
        + retryTsFileQueueSize
        + ", countedTabletEvents="
        + tabletEventCount
        + ", countedTsFileEvents="
        + tsFileEventCount;
  }

  public static long countDataPoints(final Tablet tablet) {
    if (Objects.isNull(tablet)) {
      return 0;
    }
    return countDataPoints(
        tablet.getRowSize(),
        Objects.isNull(tablet.getSchemas()) ? 0 : tablet.getSchemas().size(),
        countMarkedCells(tablet.getBitMaps(), tablet.getRowSize()));
  }

  public static long countMarkedCells(final BitMap[] bitMaps, final int rowSize) {
    if (Objects.isNull(bitMaps) || rowSize <= 0) {
      return 0;
    }

    long count = 0;
    for (final BitMap bitMap : bitMaps) {
      if (Objects.isNull(bitMap)) {
        continue;
      }
      for (int i = 0; i < rowSize; ++i) {
        if (bitMap.isMarked(i)) {
          ++count;
        }
      }
    }
    return count;
  }

  private static String formatSingleStatus(final TSStatus status) {
    return "code="
        + status.getCode()
        + ", message="
        + status.getMessage()
        + (status.isSetRedirectNode() ? ", redirectNode=" + status.getRedirectNode() : "");
  }

  private static String formatStatusesWithIndexes(final List<TSStatus> statuses) {
    if (Objects.isNull(statuses)) {
      return "null";
    }

    final List<String> samples = new ArrayList<>();
    final List<String> failures = new ArrayList<>();
    for (int i = 0; i < statuses.size(); ++i) {
      final TSStatus status = statuses.get(i);
      final String formatted = "index=" + i + "{" + formatSingleStatus(status) + "}";
      if (samples.size() < MAX_PRINTED_STATUSES) {
        samples.add(formatted);
      }
      if (Objects.nonNull(status)
          && status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()
          && failures.size() < MAX_PRINTED_STATUSES) {
        failures.add(formatted);
      }
    }

    return "size="
        + statuses.size()
        + ", samples="
        + samples
        + (statuses.size() > MAX_PRINTED_STATUSES ? "...(" + statuses.size() + ")" : "")
        + ", failures="
        + failures;
  }

  private static String formatIndexedStatuses(final Map<Integer, TSStatus> indexedStatuses) {
    if (Objects.isNull(indexedStatuses)) {
      return "null";
    }

    return indexedStatuses.entrySet().stream()
            .limit(MAX_PRINTED_STATUSES)
            .map(entry -> "index=" + entry.getKey() + "{" + formatStatus(entry.getValue()) + "}")
            .collect(Collectors.toList())
        + (indexedStatuses.size() > MAX_PRINTED_STATUSES
            ? "...(" + indexedStatuses.size() + ")"
            : "");
  }

  private static String formatIndexedStatementStatusMapping(
      final List<? extends Statement> statements, final List<TSStatus> statuses) {
    final int size = Math.max(statements.size(), statuses.size());
    final List<String> samples = new ArrayList<>();
    final List<String> failures = new ArrayList<>();

    for (int i = 0; i < size; ++i) {
      final Statement statement = i < statements.size() ? statements.get(i) : null;
      final TSStatus status = i < statuses.size() ? statuses.get(i) : null;
      final String formatted =
          "index="
              + i
              + ", status={"
              + formatStatus(status)
              + "}, statement={"
              + formatStatement(statement)
              + "}";
      if (samples.size() < MAX_PRINTED_STATEMENTS) {
        samples.add(formatted);
      }
      if (Objects.nonNull(status)
          && status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()
          && failures.size() < MAX_PRINTED_STATEMENTS) {
        failures.add(formatted);
      }
    }

    return "mappingSize="
        + size
        + ", samples="
        + samples
        + (size > MAX_PRINTED_STATEMENTS ? "...(" + size + ")" : "")
        + ", failures="
        + failures;
  }

  private static long countRows(final Statement statement) {
    if (statement instanceof InsertTabletStatement) {
      return ((InsertTabletStatement) statement).getRowCount();
    }
    if (statement instanceof InsertMultiTabletsStatement) {
      return ((InsertMultiTabletsStatement) statement)
          .getInsertTabletStatementList().stream()
              .mapToLong(InsertTabletStatement::getRowCount)
              .sum();
    }
    if (statement instanceof InsertRowsStatement) {
      return ((InsertRowsStatement) statement).getInsertRowStatementList().size();
    }
    if (statement instanceof InsertRowStatement) {
      return 1;
    }
    return 0;
  }

  private static long countDataPoints(final Statement statement) {
    if (statement instanceof InsertTabletStatement) {
      return countDataPoints((InsertTabletStatement) statement);
    }
    if (statement instanceof InsertMultiTabletsStatement) {
      return ((InsertMultiTabletsStatement) statement)
          .getInsertTabletStatementList().stream()
              .mapToLong(PipeDataLossDebugUtil::countDataPoints)
              .sum();
    }
    if (statement instanceof InsertRowsStatement) {
      return ((InsertRowsStatement) statement)
          .getInsertRowStatementList().stream()
              .mapToLong(PipeDataLossDebugUtil::countDataPoints)
              .sum();
    }
    if (statement instanceof InsertRowStatement) {
      return countDataPoints((InsertRowStatement) statement);
    }
    return 0;
  }

  private static long countDataPoints(final InsertTabletStatement statement) {
    final int rowCount = statement.getRowCount();
    return countDataPoints(
        rowCount,
        measurementCount(statement.getMeasurements()),
        countMarkedCells(statement.getBitMaps(), rowCount));
  }

  private static long countDataPoints(final InsertTabletNode node) {
    if (Objects.isNull(node)) {
      return 0;
    }
    return countDataPoints(
        node.getRowCount(),
        measurementCount(node.getMeasurements()),
        countMarkedCells(node.getBitMaps(), node.getRowCount()));
  }

  private static long countDataPoints(final InsertRowStatement statement) {
    return Math.max(0, measurementCount(statement.getMeasurements()));
  }

  private static long countDataPoints(
      final int rowCount, final int measurementCount, final long markedNullCells) {
    return Math.max(0, (long) rowCount * measurementCount - markedNullCells);
  }

  private static long countMarkedCells(final Statement statement) {
    if (statement instanceof InsertTabletStatement) {
      final InsertTabletStatement insertTabletStatement = (InsertTabletStatement) statement;
      return countMarkedCells(
          insertTabletStatement.getBitMaps(), insertTabletStatement.getRowCount());
    }
    if (statement instanceof InsertMultiTabletsStatement) {
      return ((InsertMultiTabletsStatement) statement)
          .getInsertTabletStatementList().stream()
              .mapToLong(PipeDataLossDebugUtil::countMarkedCells)
              .sum();
    }
    return 0;
  }

  private static long countMarkedCells(final InsertTabletStatement statement) {
    return countMarkedCells(statement.getBitMaps(), statement.getRowCount());
  }

  private static String safeGetDeviceId(final Tablet tablet) {
    try {
      return tablet.getDeviceId();
    } catch (final Exception e) {
      return "unknown";
    }
  }

  private static boolean safeIsGeneratedByPipe(final EnrichedEvent event) {
    try {
      return event.isGeneratedByPipe();
    } catch (final Exception e) {
      return false;
    }
  }

  private static int measurementCount(final String[] measurements) {
    return Objects.isNull(measurements) ? 0 : measurements.length;
  }

  private static String formatMeasurements(final List<IMeasurementSchema> schemas) {
    if (Objects.isNull(schemas)) {
      return "null";
    }
    return schemas.stream()
            .limit(MAX_PRINTED_MEASUREMENTS)
            .map(schema -> Objects.isNull(schema) ? "null" : schema.getMeasurementName())
            .collect(Collectors.toList())
        + (schemas.size() > MAX_PRINTED_MEASUREMENTS ? "...(" + schemas.size() + ")" : "");
  }

  private static String formatMeasurements(final String[] measurements) {
    if (Objects.isNull(measurements)) {
      return "null";
    }
    return Arrays.stream(measurements).limit(MAX_PRINTED_MEASUREMENTS).collect(Collectors.toList())
        + (measurements.length > MAX_PRINTED_MEASUREMENTS
            ? "...(" + measurements.length + ")"
            : "");
  }

  private static String firstTime(final long[] times, final int rowCount) {
    return rowCount > 0 && Objects.nonNull(times) && times.length > 0
        ? String.valueOf(times[0])
        : "null";
  }

  private static String lastTime(final long[] times, final int rowCount) {
    return rowCount > 0 && Objects.nonNull(times) && times.length >= rowCount
        ? String.valueOf(times[rowCount - 1])
        : "null";
  }
}
