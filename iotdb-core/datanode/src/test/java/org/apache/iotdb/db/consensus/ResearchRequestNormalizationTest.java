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

package org.apache.iotdb.db.consensus;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.consensus.common.request.ConsensusWriteNormalizer;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.request.NormalizedRequest;
import org.apache.iotdb.consensus.common.request.RequestSemantics;
import org.apache.iotdb.consensus.common.request.RequestSemanticsProvider;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedInsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Test;

import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class ResearchRequestNormalizationTest {
  private static Binary binary(String value) {
    return new Binary(value.getBytes(StandardCharsets.UTF_8));
  }

  private static InsertTabletNode tablet(boolean aligned) throws Exception {
    String[] names = {"b", "i", "l", "f", "d", "text", "object", null};
    TSDataType[] types = {
      TSDataType.BOOLEAN,
      TSDataType.INT32,
      TSDataType.INT64,
      TSDataType.FLOAT,
      TSDataType.DOUBLE,
      TSDataType.TEXT,
      TSDataType.OBJECT,
      null
    };
    MeasurementSchema[] schemas = new MeasurementSchema[names.length];
    for (int i = 0; i < names.length - 1; i++) {
      schemas[i] = new MeasurementSchema(names[i], types[i]);
    }
    BitMap[] nulls = new BitMap[names.length];
    nulls[5] = new BitMap(3);
    nulls[5].mark(1);
    InsertTabletNode tablet =
        new InsertTabletNode(
            new PlanNodeId("tablet"),
            new PartialPath("root.normalized.device"),
            aligned,
            names,
            types,
            schemas,
            new long[] {Long.MIN_VALUE, 7, 7},
            nulls,
            new Object[] {
              new boolean[] {true, false, true},
              new int[] {1, 2, 3},
              new long[] {11, 12, 13},
              new float[] {1.25f, 2.5f, 3.75f},
              new double[] {1.125, 2.25, 3.5},
              new Binary[] {binary("first"), binary("masked"), binary("last")},
              new Binary[] {binary("o1"), binary("o2"), binary("o3")},
              null
            },
            3);
    tablet.setFailedMeasurementNumber(1);
    tablet.setRange(Arrays.asList(4, 5, 9, 11));
    return tablet;
  }

  @Test
  public void tabletRowsKeepTypesNullsAlignmentSchemaAndSourcePositions() throws Exception {
    for (boolean aligned : new boolean[] {false, true}) {
      InsertTabletNode parent = tablet(aligned);
      NormalizedRequest normalized = ConsensusWriteNormalizer.normalize(parent);
      assertEquals(3, normalized.size());
      assertEquals(NormalizedRequest.FailureSelection.LAST, normalized.failureSelection());
      int[] sources = {4, 9, 10};
      for (int i = 0; i < 3; i++) {
        InsertTabletNode child = (InsertTabletNode) normalized.requestAt(i);
        assertEquals(InsertTabletNode.class, child.getClass());
        assertEquals(parent.getPlanNodeId(), child.getPlanNodeId());
        assertEquals(parent.getTargetPath(), child.getTargetPath());
        assertEquals(aligned, child.isAligned());
        assertEquals(1, child.getRowCount());
        assertEquals(parent.getTimes()[i], child.getRequestSemantics().getTimestamp());
        assertArrayEquals(parent.getMeasurements(), child.getMeasurements());
        assertArrayEquals(parent.getDataTypes(), child.getDataTypes());
        assertArrayEquals(parent.getMeasurementSchemas(), child.getMeasurementSchemas());
        assertNotSame(parent.getMeasurements(), child.getMeasurements());
        assertEquals(1, child.getFailedMeasurementNumber());
        assertEquals(sources[i], normalized.sourceIndex(i));
        assertEquals(Arrays.asList(sources[i], sources[i] + 1), child.getRange());
        assertFalse(child.shouldCheckTTL()); // OBJECT keeps the tablet's TTL semantics.
        for (int column = 0; column < 7; column++) {
          assertEquals(
              Array.get(parent.getColumns()[column], i), Array.get(child.getColumns()[column], 0));
          assertNotSame(parent.getColumns()[column], child.getColumns()[column]);
        }
        assertNull(child.getColumns()[7]);
        assertNull(child.getBitMaps()[0]);
        assertEquals(i == 1, child.getBitMaps()[5].isMarked(0));
      }
      // Building a masked child must not alter the following child's bitmap or the parent.
      assertTrue(parent.getBitMaps()[5].isMarked(1));
      assertFalse(parent.getBitMaps()[5].isMarked(2));
    }
  }

  @Test
  public void tabletChildRoundTripsAsOneCompleteTypedRow() throws Exception {
    InsertTabletNode parent = tablet(true);
    InsertTabletNode child = (InsertTabletNode) parent.normalizeRequest().requestAt(1);
    InsertTabletNode decoded =
        (InsertTabletNode) PlanNodeType.deserialize(child.serializeToByteBuffer());
    assertEquals(1, decoded.getRowCount());
    assertEquals(7, decoded.getRequestSemantics().getTimestamp());
    assertTrue(decoded.isAligned());
    assertEquals(
        7, decoded.getMeasurements().length); // Only the previously failed column is omitted.
    assertTrue(decoded.getBitMaps()[5].isMarked(0));
    assertArrayEquals(new long[] {12}, (long[]) decoded.getColumns()[2]);
    assertArrayEquals(new Binary[] {binary("o2")}, (Binary[]) decoded.getColumns()[6]);
    assertFalse(decoded.shouldCheckTTL());
  }

  @Test
  public void duplicateAndDecreasingTimestampsAreSeparateEntriesInReceivedOrder() throws Exception {
    InsertTabletNode parent = tablet(false);
    parent.setTimes(new long[] {8, 8, -1});
    List<Long> timestamps = new ArrayList<>();
    ConsensusWriteNormalizer.write(
        parent,
        child -> {
          timestamps.add(((RequestSemanticsProvider) child).getRequestSemantics().getTimestamp());
          return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
        });
    assertEquals(Arrays.asList(8L, 8L, -1L), timestamps);
  }

  @Test
  public void oneRowTabletKeepsItsRoutedSourceIndex() throws Exception {
    InsertTabletNode child = (InsertTabletNode) tablet(false).normalizeRequest().requestAt(2);
    NormalizedRequest normalized = child.normalizeRequest();
    assertEquals(1, normalized.size());
    assertNotSame(child, normalized.requestAt(0));
    assertEquals(10, normalized.sourceIndex(0));
  }

  @Test
  public void rowsKeepWholeRowsAndMappedFailureResults() throws Exception {
    InsertRowNode first = row(11, 1);
    InsertRowNode second = row(11, 2);
    InsertRowNode third = row(9, 3);
    InsertRowsNode parent =
        new InsertRowsNode(
            new PlanNodeId("rows"), Arrays.asList(2, 7, 8), Arrays.asList(first, second, third));
    NormalizedRequest normalized = parent.normalizeRequest();
    assertRowCopy(first, normalized.requestAt(0));
    assertRowCopy(second, normalized.requestAt(1));
    assertRowCopy(third, normalized.requestAt(2));
    assertEquals(7, normalized.sourceIndex(1));
    TSStatus failure = new TSStatus(TSStatusCode.OUT_OF_TTL.getStatusCode());
    AtomicInteger index = new AtomicInteger();
    TSStatus result =
        ConsensusWriteNormalizer.write(
            parent,
            request ->
                index.getAndIncrement() == 1
                    ? failure
                    : new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    assertSame(failure, result);
    assertEquals(3, index.get());
    assertEquals(1, parent.getResults().size());
    assertSame(failure, parent.getResults().get(1));
    assertSame(failure, parent.getFailingStatus()[1]);
    assertEquals(Integer.valueOf(7), parent.getInsertRowNodeIndexList().get(1));
  }

  @Test
  public void applyingAChildCannotMutateLaterRowsOrParentRetryPayload() throws Exception {
    InsertRowNode first = row(5, 1);
    InsertRowNode second = row(6, 2);
    second.setMeasurements(first.getMeasurements());
    second.setDataTypes(first.getDataTypes());
    InsertRowsNode parent =
        new InsertRowsNode(
            new PlanNodeId("shared"), Arrays.asList(0, 1), Arrays.asList(first, second));
    NormalizedRequest normalized = parent.normalizeRequest();
    InsertRowNode child = (InsertRowNode) normalized.requestAt(0);
    child.getMeasurements()[0] = "changed-by-apply";
    child.getDataTypes()[0] = TSDataType.DOUBLE;
    child.getValues()[0] = 100;
    assertEquals("value", first.getMeasurements()[0]);
    assertEquals(TSDataType.INT32, second.getDataTypes()[0]);
    assertEquals(1, first.getValues()[0]);
    assertRowCopy(second, normalized.requestAt(1));
    assertRowCopy(first, parent.normalizeRequest().requestAt(0));
  }

  @Test
  public void sqlRowsOfOneDeviceUseTheSameWholeRowContract() throws Exception {
    InsertRowNode first = row(5, 1);
    InsertRowNode second = row(5, 2);
    InsertRowsOfOneDeviceNode parent =
        new InsertRowsOfOneDeviceNode(
            new PlanNodeId("sql-rows"), Arrays.asList(3, 9), Arrays.asList(first, second));
    NormalizedRequest normalized = parent.normalizeRequest();
    assertEquals(2, normalized.size());
    assertRowCopy(first, normalized.requestAt(0));
    assertRowCopy(second, normalized.requestAt(1));
    assertEquals(9, normalized.sourceIndex(1));
    TSStatus error = new TSStatus(TSStatusCode.OUT_OF_TTL.getStatusCode());
    normalized.recordResult(1, error);
    assertSame(error, parent.getFailingStatus()[1]);
    parent.markAsGeneratedByPipe();
    assertOpaque(parent);
  }

  @Test
  public void unsupportedTablePipeAndSubclassRequestsRemainWholeBarriers() throws Exception {
    InsertTabletNode table = new RelationalInsertTabletNode(new PlanNodeId("table"));
    table.setRowCount(2);
    table.setTimes(new long[] {1, 2});
    assertOpaque(table);
    assertOpaque(new InsertTabletNode(new PlanNodeId("extension")) {});
    InsertRowsNode mixed =
        new InsertRowsNode(
            new PlanNodeId("mixed"),
            Arrays.asList(0, 1),
            Arrays.asList(row(1, 1), new RelationalInsertRowNode(new PlanNodeId("table-row"))));
    assertOpaque(mixed);
    assertOpaque(new InsertRowsNode(new PlanNodeId("rows-extension")) {});
    assertOpaque(new PipeEnrichedInsertNode(tablet(true)));
    InsertTabletNode pipeTablet = tablet(false);
    pipeTablet.markAsGeneratedByPipe();
    assertOpaque(pipeTablet);
    InsertRowNode pipeRow = row(1, 1);
    pipeRow.markAsGeneratedByPipe();
    assertOpaque(pipeRow);
    InsertRowsNode pipeRows =
        new InsertRowsNode(new PlanNodeId("pipe-rows"), Arrays.asList(0), Arrays.asList(pipeRow));
    assertOpaque(pipeRows);
  }

  @Test
  public void multiTabletRowsKeepTabletOrderAndMapFailuresToTablets() throws Exception {
    InsertTabletNode first = tablet(false);
    InsertTabletNode second = tablet(true);
    second.setTimes(new long[] {20, 21, 22});
    InsertMultiTabletsNode parent =
        new InsertMultiTabletsNode(
            new PlanNodeId("multi"), Arrays.asList(4, 6), Arrays.asList(first, second));
    NormalizedRequest normalized = parent.normalizeRequest();
    assertEquals(6, normalized.size());
    assertEquals(NormalizedRequest.FailureSelection.FIRST, normalized.failureSelection());
    long[] times = {Long.MIN_VALUE, 7, 7, 20, 21, 22};
    for (int i = 0; i < times.length; i++) {
      InsertTabletNode child = (InsertTabletNode) normalized.requestAt(i);
      assertEquals(InsertTabletNode.class, child.getClass());
      assertEquals(1, child.getRowCount());
      assertEquals(times[i], child.getRequestSemantics().getTimestamp());
      assertEquals(i >= 3, child.isAligned());
      assertEquals(i < 3 ? 4 : 6, normalized.sourceIndex(i));
    }
    TSStatus failure = new TSStatus(TSStatusCode.OUT_OF_TTL.getStatusCode());
    TSStatus later = new TSStatus(TSStatusCode.OUT_OF_TTL.getStatusCode());
    AtomicInteger index = new AtomicInteger();
    TSStatus result =
        ConsensusWriteNormalizer.write(
            parent,
            request -> {
              int i = index.getAndIncrement();
              return i == 4
                  ? failure
                  : i == 5 ? later : new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
            });
    assertSame(failure, result);
    assertEquals(6, index.get());
    // DataExecutionVisitor keys multi-tablet failures by the routed tablet position.
    assertEquals(1, parent.getResults().size());
    assertSame(failure, parent.getResults().get(1));
  }

  @Test
  public void multiTabletsWithAnUnsplittableTabletRemainWhole() throws Exception {
    InsertTabletNode table = new RelationalInsertTabletNode(new PlanNodeId("table"));
    table.setRowCount(2);
    table.setTimes(new long[] {1, 2});
    assertOpaque(
        new InsertMultiTabletsNode(
            new PlanNodeId("mixed"), Arrays.asList(0, 1), Arrays.asList(tablet(false), table)));
    InsertTabletNode pipeTablet = tablet(true);
    pipeTablet.markAsGeneratedByPipe();
    assertOpaque(
        new InsertMultiTabletsNode(
            new PlanNodeId("pipe-tablet"), Arrays.asList(0), Arrays.asList(pipeTablet)));
    InsertMultiTabletsNode pipe =
        new InsertMultiTabletsNode(
            new PlanNodeId("pipe"), Arrays.asList(0), Arrays.asList(tablet(false)));
    pipe.markAsGeneratedByPipe();
    assertOpaque(pipe);
    assertOpaque(new InsertMultiTabletsNode(new PlanNodeId("empty")));
  }

  @Test
  public void oneRowWritesCarryTheirDeviceAsSeriesKey() throws Exception {
    RequestSemantics row = row(5, 1).getRequestSemantics();
    assertTrue(row.hasSeriesKey());
    assertEquals(RequestSemantics.seriesKeyOf("root.normalized.device"), row.getSeriesKey());
    NormalizedRequest rows = ConsensusWriteNormalizer.normalize(tablet(false));
    for (int i = 0; i < rows.size(); i++) {
      RequestSemantics child = ((RequestSemanticsProvider) rows.requestAt(i)).getRequestSemantics();
      assertEquals(row.getSeriesKey(), child.getSeriesKey());
    }
    assertFalse(
        RequestSemantics.seriesKeyOf("root.normalized.device")
            == RequestSemantics.seriesKeyOf("root.normalized.device2"));
    assertFalse(RequestSemantics.data(5).hasSeriesKey());
  }

  private static void assertOpaque(IConsensusRequest request) {
    NormalizedRequest normalized = ConsensusWriteNormalizer.normalize(request);
    assertEquals(1, normalized.size());
    assertSame(request, normalized.requestAt(0));
    if (request instanceof RequestSemanticsProvider) {
      assertEquals(
          RequestSemantics.Kind.BARRIER,
          ((RequestSemanticsProvider) request).getRequestSemantics().getKind());
    }
  }

  private static void assertRowCopy(InsertRowNode expected, IConsensusRequest actual) {
    InsertRowNode copy = (InsertRowNode) actual;
    assertNotSame(expected, copy);
    assertEquals(expected.getTime(), copy.getTime());
    assertEquals(expected.getTargetPath(), copy.getTargetPath());
    assertEquals(expected.isAligned(), copy.isAligned());
    assertArrayEquals(expected.getMeasurements(), copy.getMeasurements());
    assertArrayEquals(expected.getDataTypes(), copy.getDataTypes());
    assertArrayEquals(expected.getValues(), copy.getValues());
    assertNotSame(expected.getMeasurements(), copy.getMeasurements());
    assertNotSame(expected.getDataTypes(), copy.getDataTypes());
    assertNotSame(expected.getValues(), copy.getValues());
  }

  private static InsertRowNode row(long timestamp, int value) throws Exception {
    return new InsertRowNode(
        new PlanNodeId("row"),
        new PartialPath("root.normalized.device"),
        true,
        new String[] {"value", "nullable"},
        new TSDataType[] {TSDataType.INT32, TSDataType.TEXT},
        timestamp,
        new Object[] {value, null},
        false);
  }
}
