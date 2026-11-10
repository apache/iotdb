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

package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.plan.relational.analyzer.NodeRef;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.AllColumns;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.BooleanLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.DereferenceExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.DoubleLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.GenericLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Parameter;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.PivotRelation;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Select;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SelectItem;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SingleColumn;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.StringLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SubqueryExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Table;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.With;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.WithQuery;
import org.apache.iotdb.commons.queryengine.utils.cte.CteDataStore;
import org.apache.iotdb.db.exception.query.QueryTimeoutRuntimeException;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.PivotInputStore;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.execution.QueryExecution;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.memory.NotThreadSafeMemoryReservationManager;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.PivotInputPreparer;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.StatementAnalyzerFactory;
import org.apache.iotdb.db.queryengine.plan.relational.sql.util.QueryUtil;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.DateUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

/** Discovers dynamic ON values while buffering the *same* input used by the final aggregation. */
public final class CoordinatorPivotInputPreparer implements PivotInputPreparer {
  private static final long PIVOT_VALUE_MAP_ENTRY_OVERHEAD = 64L;
  private static final long PIVOT_VALUE_EXPRESSION_OVERHEAD = 128L;

  private final SessionManager sessions = SessionManager.getInstance();

  @Override
  public void prepare(
      Statement statement,
      StatementAnalyzerFactory factory,
      MPPQueryContext context,
      SessionInfo session,
      List<Expression> parameters,
      Map<NodeRef<Parameter>, Expression> lookup,
      WarningCollector warnings) {
    prepareNode(statement, null, 0, context, session, parameters, lookup);
  }

  private void prepareNode(
      Node node,
      With inheritedWith,
      int depth,
      MPPQueryContext context,
      SessionInfo session,
      List<Expression> parameters,
      Map<NodeRef<Parameter>, Expression> lookup) {
    // A CTE definition can only reference definitions preceding it; a nested query can also
    // reference outer WITH definitions. Reattaching the entire WITH clause to a discovery query
    // would include the pivot being discovered and recurse, so merge only the visible definitions.
    if (node instanceof Query) {
      Query query = (Query) node;
      With ownWith = query.getWith().orElse(null);
      if (ownWith != null && ownWith.isRecursive()) {
        // Recursive CTEs are rejected by the analyzer; fail before a discovery query reports a
        // misleading missing-table error for the CTE being defined.
        throw new SemanticException(DataNodeQueryMessages.RECURSIVE_CTE_IS_NOT_SUPPORTED_YET);
      }
      if (ownWith != null) {
        List<WithQuery> preceding = new ArrayList<>();
        for (WithQuery definition : ownWith.getQueries()) {
          With visible = mergeVisibleWith(inheritedWith, preceding, ownWith.isRecursive());
          prepareNode(definition.getQuery(), visible, depth, context, session, parameters, lookup);
          preceding.add(definition);
        }
      }
      With visible =
          ownWith == null
              ? inheritedWith
              : mergeVisibleWith(inheritedWith, ownWith.getQueries(), ownWith.isRecursive());
      for (Node child : query.getChildren()) {
        if (child != ownWith) {
          prepareNode(child, visible, depth, context, session, parameters, lookup);
        }
      }
      return;
    }
    With with = inheritedWith;
    int nextDepth =
        depth + (node instanceof PivotRelation && ((PivotRelation) node).isDynamic() ? 1 : 0);
    if (nextDepth > 8) {
      throw new SemanticException(
          DataNodeQueryMessages.EXCEPTION_DYNAMIC_PIVOT_NESTING_DEPTH_EXCEEDS_EIGHT_A3F4F166);
    }
    List<? extends Node> children = node.getChildren();
    if (children != null) {
      for (Node child : children) {
        prepareNode(child, with, nextDepth, context, session, parameters, lookup);
      }
    }
    if (node instanceof PivotRelation
        && ((PivotRelation) node).isDynamic()
        && context.getPivotInputStore((PivotRelation) node) == null) {
      materialize((PivotRelation) node, with, context, session, parameters, lookup);
    }
  }

  /**
   * Combines an outer WITH with the definitions visible at the current nesting level. Definitions
   * from the current level shadow outer definitions with the same (case-insensitive) name.
   */
  static With mergeVisibleWith(
      With inheritedWith, List<WithQuery> localQueries, boolean recursive) {
    boolean effectiveRecursive =
        recursive || (inheritedWith != null && inheritedWith.isRecursive());
    if (localQueries.isEmpty()) {
      return inheritedWith;
    }
    List<WithQuery> visible = new ArrayList<>();
    if (inheritedWith != null) {
      Set<String> shadowed = new HashSet<>();
      for (WithQuery query : localQueries) {
        shadowed.add(query.getName().getValue().toLowerCase(Locale.ENGLISH));
      }
      for (WithQuery query : inheritedWith.getQueries()) {
        if (!shadowed.contains(query.getName().getValue().toLowerCase(Locale.ENGLISH))) {
          visible.add(query);
        }
      }
    }
    visible.addAll(localQueries);
    return new With(effectiveRecursive, visible);
  }

  private void materialize(
      PivotRelation pivot,
      With with,
      MPPQueryContext context,
      SessionInfo session,
      List<Expression> parameters,
      Map<NodeRef<Parameter>, Expression> lookup) {
    List<SelectItem> items = buildDiscoverySelectItems(pivot);
    Query source = QueryUtil.simpleQuery(new Select(false, items), pivot.getInput());
    Query query =
        with == null
            ? source
            : new Query(
                Optional.of(with),
                source.getQueryBody(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    Coordinator coordinator = Coordinator.getInstance();
    long queryId = sessions.requestQueryId();
    CteDataStore store = null;
    // Front-end reservations are automatically released when Coordinator returns. Keep the
    // materialized input in its own reservation until the owning query releases its store.
    NotThreadSafeMemoryReservationManager reservation =
        new NotThreadSafeMemoryReservationManager(
            context.getQueryId(), PivotInputStore.class.getName());
    Throwable failure = null;
    try {
      context.checkTimeOut();
      long remainingTimeOut =
          context.getTimeOut() - (System.currentTimeMillis() - context.getStartTime());
      if (remainingTimeOut <= 0) {
        throw new QueryTimeoutRuntimeException(
            context.getStartTime(), System.currentTimeMillis(), context.getTimeOut());
      }
      ExecutionResult result =
          coordinator.executeForTableModel(
              query,
              new org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser(),
              sessions.getCurrSession(),
              queryId,
              session,
              DataNodeQueryMessages.MESSAGE_DISCOVER_DYNAMIC_PIVOT_INPUT_5EBD15BB,
              LocalExecutionPlanner.getInstance().metadata,
              context.getCteQueries(),
              context.getPivotInputStores(),
              MPPQueryContext.ExplainType.NONE,
              context.getExplainOutputFormat(),
              remainingTimeOut,
              false,
              context.isDebug(),
              context.isVerbose(),
              context,
              parameters,
              lookup);
      if (result.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new IoTDBRuntimeException(result.status);
      }
      QueryExecution execution = (QueryExecution) coordinator.getQueryExecution(queryId);
      DatasetHeader header = execution.getDatasetHeader();
      List<String> columns = header.getRespColumns();
      int originalCount = columns.size() - pivot.getOnItems().size();
      if (originalCount < 0) {
        throw new SemanticException(
            DataNodeQueryMessages.EXCEPTION_DYNAMIC_PIVOT_INPUT_SCHEMA_CHANGED_6A26E6B0);
      }
      List<Integer> mapping = header.getColumnIndex2TsBlockColumnIndexList();
      if (mapping == null) {
        mapping = IntStream.range(0, columns.size()).boxed().toList();
      }
      List<String> inputColumnNames = new ArrayList<>(columns.subList(0, originalCount));
      store =
          new CteDataStore(
              CteMaterializer.getTableSchema(header, "__pivot_input"),
              mapping,
              CommonDescriptor.getInstance().getConfig().getPivotInputBufferSizeInBytes(),
              CommonDescriptor.getInstance().getConfig().getPivotInputMaxRows());
      List<Map<String, PivotRelation.Value>> values = new ArrayList<>();
      for (PivotRelation.OnItem on : pivot.getOnItems()) {
        values.add(on.values().isPresent() ? null : new LinkedHashMap<>());
      }
      while (execution.hasNextResult()) {
        context.checkTimeOut();
        Optional<TsBlock> batch;
        try {
          batch = execution.getBatchResult();
        } catch (org.apache.iotdb.commons.exception.IoTDBException e) {
          throw new IoTDBRuntimeException(e.getMessage(), e, e.getErrorCode());
        }
        if (batch.isEmpty() || batch.get().isEmpty()) {
          continue;
        }
        TsBlock block = batch.get();
        CteDataStore.AddResult added = store.tryAddTsBlock(block);
        if (added != CteDataStore.AddResult.SUCCESS) {
          throw new IoTDBRuntimeException(
              added == CteDataStore.AddResult.BYTES_EXCEEDED
                  ? DataNodeQueryMessages
                      .EXCEPTION_DYNAMIC_PIVOT_INPUT_EXCEEDS_PIVOT_INPUT_BUFFER_SIZE_IN_BYTES_9E2FA334
                  : DataNodeQueryMessages
                      .EXCEPTION_DYNAMIC_PIVOT_INPUT_EXCEEDS_PIVOT_INPUT_MAX_ROWS_BC1F0F40,
              TSStatusCode.QUERY_EXECUTION_MEMORY_NOT_ENOUGH.getStatusCode(),
              true);
        }
        reservation.reserveMemoryCumulatively(block.getRetainedSizeInBytes());
        for (int i = 0; i < values.size(); i++) {
          if (values.get(i) == null) {
            continue;
          }
          Column column = block.getColumn(mapping.get(originalCount + i));
          TSDataType type = header.getRespDataTypes().get(originalCount + i);
          Map<String, PivotRelation.Value> discoveredValues = values.get(i);
          for (int row = 0; row < block.getPositionCount(); row++) {
            if (!column.isNull(row)) {
              PivotRelation.Value value = valueOf(column, row, type);
              String key = value.alias().map(Identifier::getValue).orElse("");
              if (!discoveredValues.containsKey(key)) {
                long valueBytes =
                    PIVOT_VALUE_MAP_ENTRY_OVERHEAD
                        + PIVOT_VALUE_EXPRESSION_OVERHEAD
                        + 2L * key.length();
                if (!store.tryReserveExtraBytes(valueBytes)) {
                  throw new IoTDBRuntimeException(
                      DataNodeQueryMessages
                          .EXCEPTION_DYNAMIC_PIVOT_INPUT_EXCEEDS_PIVOT_INPUT_BUFFER_SIZE_IN_BYTES_9E2FA334,
                      TSStatusCode.QUERY_EXECUTION_MEMORY_NOT_ENOUGH.getStatusCode(),
                      true);
                }
                reservation.reserveMemoryCumulatively(valueBytes);
                discoveredValues.put(key, value);
              }
            }
          }
        }
      }
      List<List<PivotRelation.Value>> discovered = new ArrayList<>();
      for (Map<String, PivotRelation.Value> set : values) {
        discovered.add(
            set == null
                ? List.of()
                : set.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey(Comparator.naturalOrder()))
                    .map(Map.Entry::getValue)
                    .toList());
      }
      reservation.reserveMemoryImmediately();
      context.addPivotInputStore(
          pivot,
          new PivotInputStore(store, discovered, inputColumnNames, originalCount, reservation));
    } catch (RuntimeException | Error t) {
      failure = t;
      if (store != null) {
        store.clear();
      }
      reservation.releaseAllReservedMemory();
      throw t;
    } finally {
      coordinator.cleanupQueryExecution(queryId, (org.apache.thrift.TBase<?, ?>) null, failure);
    }
  }

  /**
   * Builds the discovery query projection. For an explicit GROUP BY, only the columns referenced by
   * grouping, ON and USING expressions are needed; materializing the whole input can otherwise make
   * a legal query exceed the PIVOT input budget. Implicit grouping still needs all columns because
   * every unconsumed column becomes a grouping column.
   */
  List<SelectItem> buildDiscoverySelectItems(PivotRelation pivot) {
    LinkedHashSet<String> inputColumns = new LinkedHashSet<>();
    boolean canProject = pivot.getGroupBy().isPresent() && pivot.getInput() instanceof Table;
    if (canProject) {
      boolean[] unsupported = {false};
      pivot.getGroupBy().get().forEach(e -> collectDiscoveryColumns(e, inputColumns, unsupported));
      for (PivotRelation.OnItem item : pivot.getOnItems()) {
        collectDiscoveryColumns(item.expression(), inputColumns, unsupported);
      }
      for (PivotRelation.UsingItem item : pivot.getUsingItems()) {
        collectDiscoveryColumns(item.expression(), inputColumns, unsupported);
      }
      canProject = !unsupported[0] && !inputColumns.isEmpty();
    }

    List<SelectItem> items = new ArrayList<>();
    if (canProject) {
      for (String column : inputColumns) {
        items.add(new SingleColumn(new Identifier(column)));
      }
    } else {
      items.add(new AllColumns());
    }
    for (int i = 0; i < pivot.getOnItems().size(); i++) {
      items.add(
          new SingleColumn(
              pivot.getOnItems().get(i).expression(), new Identifier("__pivot_on_" + i)));
    }
    return items;
  }

  private void collectDiscoveryColumns(Node node, Set<String> inputColumns, boolean[] unsupported) {
    if (unsupported[0]) {
      return;
    }
    // Qualified references, parameters and subqueries need the original input shape to resolve;
    // fall back to SELECT * instead of risking an ambiguous discovery projection.
    if (node instanceof DereferenceExpression
        || node instanceof Parameter
        || node instanceof SubqueryExpression) {
      unsupported[0] = true;
      return;
    }
    if (node instanceof Identifier) {
      inputColumns.add(((Identifier) node).getValue());
      return;
    }
    List<? extends Node> children = node.getChildren();
    if (children != null) {
      for (Node child : children) {
        collectDiscoveryColumns(child, inputColumns, unsupported);
      }
    }
  }

  static PivotRelation.Value valueOf(Column column, int row, TSDataType type) {
    String name;
    Expression literal;
    switch (type) {
      case BOOLEAN:
        name = Boolean.toString(column.getBoolean(row));
        literal = new BooleanLiteral(name);
        break;
      case INT32:
        name = Integer.toString(column.getInt(row));
        literal = new LongLiteral(name);
        break;
      case INT64:
        name = Long.toString(column.getLong(row));
        literal = new LongLiteral(name);
        break;
      case FLOAT:
        name = Float.toString(column.getFloat(row));
        literal = new DoubleLiteral(name);
        break;
      case DOUBLE:
        name = Double.toString(column.getDouble(row));
        literal = new DoubleLiteral(name);
        break;
      case DATE:
        name = DateUtils.parseIntToLocalDate(column.getInt(row)).toString();
        literal = new GenericLiteral("DATE", name);
        break;
      case TIMESTAMP:
        name = Long.toString(column.getLong(row));
        literal = new LongLiteral(name);
        break;
      case TEXT:
      case STRING:
        name =
            column
                .getBinary(row)
                .getStringValue(org.apache.tsfile.common.conf.TSFileConfig.STRING_CHARSET);
        literal = new StringLiteral(name);
        break;
      default:
        throw new SemanticException(
            DataNodeQueryMessages.EXCEPTION_DYNAMIC_PIVOT_ON_TYPE_IS_NOT_SUPPORTED_A8F057D9);
    }
    return new PivotRelation.Value(
        literal, name.isEmpty() ? Optional.empty() : Optional.of(new Identifier(name)));
  }
}
