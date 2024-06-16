/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Field;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.RelationType;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CreateTableDeviceNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.FilterScanCombine;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.IndexScan;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.PruneUnUsedColumns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.RelationalPlanOptimizer;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.RemoveRedundantIdentityProjections;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.SimplifyExpressions;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Explain;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Table;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_ROOT;
import static org.apache.iotdb.db.queryengine.plan.expression.leaf.TimestampOperand.TIMESTAMP_EXPRESSION_STRING;

public class LogicalPlanner {
  private static final Logger LOG = LoggerFactory.getLogger(LogicalPlanner.class);
  private final MPPQueryContext context;
  private final SessionInfo sessionInfo;
  private final SymbolAllocator symbolAllocator = new SymbolAllocator();
  private final List<RelationalPlanOptimizer> relationalPlanOptimizers;
  private final Metadata metadata;
  private final IPartitionFetcher partitionFetcher;
  private final WarningCollector warningCollector;

  public LogicalPlanner(
      MPPQueryContext context,
      Metadata metadata,
      SessionInfo sessionInfo,
      IPartitionFetcher partitionFetcher,
      WarningCollector warningCollector) {
    this.context = context;
    this.metadata = metadata;
    this.sessionInfo = requireNonNull(sessionInfo, "session is null");
    this.partitionFetcher = requireNonNull(partitionFetcher, "partitionFetcher is null");
    this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");

    this.relationalPlanOptimizers =
        Arrays.asList(
            new SimplifyExpressions(),
            new PruneUnUsedColumns(),
            new FilterScanCombine(),
            new RemoveRedundantIdentityProjections(),
            new IndexScan());
  }

  public LogicalQueryPlan plan(Analysis analysis) {
    PlanNode planNode = planStatement(analysis, analysis.getStatement());

    relationalPlanOptimizers.forEach(
        optimizer ->
            optimizer.optimize(
                planNode, analysis, metadata, partitionFetcher, sessionInfo, context));

    return new LogicalQueryPlan(context, planNode);
  }

  private PlanNode planStatement(Analysis analysis, Statement statement) {
    if (statement instanceof CreateDevice) {
      return planCreateDevice((CreateDevice) statement, analysis);
    }
    return createOutputPlan(planStatementWithoutOutput(analysis, statement), analysis);
  }

  private RelationPlan planStatementWithoutOutput(Analysis analysis, Statement statement) {
    if (statement instanceof Query) {
      return createRelationPlan(analysis, (Query) statement);
    }
    if (statement instanceof Explain) {
      return createRelationPlan(analysis, (Query) ((Explain) statement).getStatement());
    }
    throw new IllegalStateException(
        "Unsupported statement type: " + statement.getClass().getSimpleName());
  }

  private PlanNode createOutputPlan(RelationPlan plan, Analysis analysis) {
    ImmutableList.Builder<Symbol> outputs = ImmutableList.builder();
    ImmutableList.Builder<String> names = ImmutableList.builder();
    List<ColumnHeader> columnHeaders = new ArrayList<>();

    int columnNumber = 0;
    // TODO perfect the logic of outputDescriptor
    RelationType outputDescriptor = analysis.getOutputDescriptor();
    for (Field field : outputDescriptor.getVisibleFields()) {
      String name = field.getName().orElse("_col" + columnNumber);

      names.add(name);
      int fieldIndex = outputDescriptor.indexOf(field);
      Symbol symbol = plan.getSymbol(fieldIndex);
      outputs.add(symbol);

      if (!TIMESTAMP_EXPRESSION_STRING.equalsIgnoreCase(name)) {
        columnHeaders.add(
            new ColumnHeader(symbol.getName(), transferTypeToTsDataType(field.getType())));
      }

      columnNumber++;
    }

    OutputNode outputNode =
        new OutputNode(
            context.getQueryId().genPlanNodeId(), plan.getRoot(), names.build(), outputs.build());

    DatasetHeader respDatasetHeader = new DatasetHeader(columnHeaders, false);
    analysis.setRespDatasetHeader(respDatasetHeader);

    return outputNode;
  }

  private RelationPlan createRelationPlan(Analysis analysis, Query query) {
    return getRelationPlanner(analysis).process(query, null);
  }

  private RelationPlan createRelationPlan(Analysis analysis, Table table) {
    return getRelationPlanner(analysis).process(table, null);
  }

  private RelationPlanner getRelationPlanner(Analysis analysis) {
    return new RelationPlanner(analysis, symbolAllocator, context, sessionInfo, ImmutableMap.of());
  }

  public TSDataType transferTypeToTsDataType(Type type) {
    switch (type.getTypeEnum()) {
      case INT32:
        return TSDataType.INT32;
      case INT64:
        return TSDataType.INT64;
      case BOOLEAN:
        return TSDataType.BOOLEAN;
      case FLOAT:
        return TSDataType.FLOAT;
      case DOUBLE:
        return TSDataType.DOUBLE;
      case TEXT:
        return TSDataType.TEXT;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Cannot transfer type: %s to TSDataType.", type.getTypeEnum()));
    }
  }

  private enum Stage {
    CREATED,
    OPTIMIZED,
    OPTIMIZED_AND_VALIDATED
  }

  private PlanNode planCreateDevice(CreateDevice statement, Analysis analysis) {
    List<PartialPath> devicePathList = new ArrayList<>(statement.getDeviceIdList().size());
    for (String[] deviceId : statement.getDeviceIdList()) {
      String[] nodes = new String[3 + deviceId.length];
      nodes[0] = PATH_ROOT;
      nodes[1] = statement.getDatabase();
      nodes[2] = statement.getTable();
      System.arraycopy(deviceId, 0, nodes, 3, deviceId.length);
      devicePathList.add(new PartialPath(nodes));
    }
    CreateTableDeviceNode node =
        new CreateTableDeviceNode(
            context.getQueryId().genPlanNodeId(),
            devicePathList,
            statement.getAttributeNameList(),
            statement.getAttributeValueList());

    context.setQueryType(QueryType.WRITE);
    analysis.setStatement(statement);

    PathPatternTree patternTree = new PathPatternTree();
    for (PartialPath devicePath : devicePathList) {
      patternTree.appendFullPath(devicePath.concatNode(ONE_LEVEL_PATH_WILDCARD));
    }
    SchemaPartition partition =
        partitionFetcher.getOrCreateSchemaPartition(
            patternTree, context.getSession().getUserName());

    analysis.setSchemaPartitionInfo(partition);

    return node;
  }
}
