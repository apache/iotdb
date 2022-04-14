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

package org.apache.iotdb.db.mpp.sql.analyze;

import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.db.exception.query.PathNumOverLimitException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.filter.QueryFilter;
import org.apache.iotdb.db.mpp.common.header.HeaderConstant;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.db.mpp.common.schematree.SchemaTree;
import org.apache.iotdb.db.mpp.sql.rewriter.ConcatPathRewriter;
import org.apache.iotdb.db.mpp.sql.rewriter.DnfFilterOptimizer;
import org.apache.iotdb.db.mpp.sql.rewriter.MergeSingleFilterOptimizer;
import org.apache.iotdb.db.mpp.sql.rewriter.RemoveNotOptimizer;
import org.apache.iotdb.db.mpp.sql.rewriter.WildcardsRemover;
import org.apache.iotdb.db.mpp.sql.statement.Statement;
import org.apache.iotdb.db.mpp.sql.statement.StatementVisitor;
import org.apache.iotdb.db.mpp.sql.statement.component.ResultColumn;
import org.apache.iotdb.db.mpp.sql.statement.component.WhereCondition;
import org.apache.iotdb.db.mpp.sql.statement.crud.*;
import org.apache.iotdb.db.mpp.sql.statement.metadata.*;
import org.apache.iotdb.db.mpp.sql.statement.sys.AuthorStatement;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.util.ExpressionOptimizer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Analyze the statement and generate Analysis. */
public class Analyzer {

  private final MPPQueryContext context;

  // TODO need to use factory to decide standalone or cluster
  private final IPartitionFetcher partitionFetcher = new FakePartitionFetcherImpl();
  // TODO need to use factory to decide standalone or cluster
  private final ISchemaFetcher schemaFetcher = new FakeSchemaFetcherImpl();

  public Analyzer(MPPQueryContext context) {
    this.context = context;
  }

  public Analysis analyze(Statement statement) {
    return new AnalyzeVisitor().process(statement, context);
  }

  /** This visitor is used to analyze each type of Statement and returns the {@link Analysis}. */
  private final class AnalyzeVisitor extends StatementVisitor<Analysis, MPPQueryContext> {

    @Override
    public Analysis visitStatement(Statement statement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(statement);
      return analysis;
    }

    @Override
    public Analysis visitQuery(QueryStatement queryStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      try {
        // check for semantic errors
        queryStatement.selfCheck();

        // concat path and construct path pattern tree
        PathPatternTree patternTree = new PathPatternTree();
        QueryStatement rewrittenStatement =
            (QueryStatement) new ConcatPathRewriter().rewrite(queryStatement, patternTree);

        // request schema fetch API
        SchemaTree schemaTree = schemaFetcher.fetchSchema(patternTree);

        // bind metadata, remove wildcards, and apply SLIMIT & SOFFSET
        rewrittenStatement =
            (QueryStatement) new WildcardsRemover().rewrite(rewrittenStatement, schemaTree);

        // fetch partition information
        Set<PartialPath> devicePathSet = new HashSet<>();
        for (ResultColumn resultColumn : queryStatement.getSelectComponent().getResultColumns()) {
          devicePathSet.addAll(
              resultColumn.collectPaths().stream()
                  .map(PartialPath::getDevicePath)
                  .collect(Collectors.toList()));
        }
        if (queryStatement.getWhereCondition() != null) {
          devicePathSet.addAll(
              queryStatement.getWhereCondition().getQueryFilter().getPathSet().stream()
                  .filter(SQLConstant::isNotReservedPath)
                  .map(PartialPath::getDevicePath)
                  .collect(Collectors.toList()));
        }
        Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap = new HashMap<>();
        for (PartialPath devicePath : devicePathSet) {
          DataPartitionQueryParam queryParam = new DataPartitionQueryParam();
          queryParam.setDevicePath(devicePath.getFullPath());
          sgNameToQueryParamsMap
              .computeIfAbsent(
                  schemaTree.getBelongedStorageGroup(devicePath), key -> new ArrayList<>())
              .add(queryParam);
        }
        DataPartition dataPartition = partitionFetcher.getDataPartition(sgNameToQueryParamsMap);

        // optimize expressions in whereCondition
        WhereCondition whereCondition = rewrittenStatement.getWhereCondition();
        if (whereCondition != null) {
          QueryFilter filter = whereCondition.getQueryFilter();
          filter = new RemoveNotOptimizer().optimize(filter);
          filter = new DnfFilterOptimizer().optimize(filter);
          filter = new MergeSingleFilterOptimizer().optimize(filter);

          // transform QueryFilter to expression
          List<PartialPath> filterPaths = new ArrayList<>(filter.getPathSet());
          HashMap<PartialPath, TSDataType> pathTSDataTypeHashMap = new HashMap<>();
          for (PartialPath filterPath : filterPaths) {
            pathTSDataTypeHashMap.put(
                filterPath,
                SQLConstant.isReservedPath(filterPath)
                    ? TSDataType.INT64
                    : filterPath.getSeriesType());
          }
          IExpression expression = filter.transformToExpression(pathTSDataTypeHashMap);
          expression =
              ExpressionOptimizer.getInstance()
                  .optimize(expression, queryStatement.getSelectComponent().getDeduplicatedPaths());
          analysis.setQueryFilter(expression);
        }
        analysis.setStatement(rewrittenStatement);
        analysis.setSchemaTree(schemaTree);
        analysis.setRespDatasetHeader(queryStatement.constructDatasetHeader());
        analysis.setDataPartitionInfo(dataPartition);
      } catch (StatementAnalyzeException
          | PathNumOverLimitException
          | QueryFilterOptimizationException e) {
        e.printStackTrace();
      }
      return analysis;
    }

    @Override
    public Analysis visitInsert(InsertStatement insertStatement, MPPQueryContext context) {
      // TODO: do analyze for insert statement
      Analysis analysis = new Analysis();
      analysis.setStatement(insertStatement);
      return analysis;
    }

    @Override
    public Analysis visitCreateTimeseries(
        CreateTimeSeriesStatement createTimeSeriesStatement, MPPQueryContext context) {
      if (createTimeSeriesStatement.getTags() != null
          && !createTimeSeriesStatement.getTags().isEmpty()
          && createTimeSeriesStatement.getAttributes() != null
          && !createTimeSeriesStatement.getAttributes().isEmpty()) {
        for (String tagKey : createTimeSeriesStatement.getTags().keySet()) {
          if (createTimeSeriesStatement.getAttributes().containsKey(tagKey)) {
            throw new SemanticException(
                String.format(
                    "Tag and attribute shouldn't have the same property key [%s]", tagKey));
          }
        }
      }
      Analysis analysis = new Analysis();
      analysis.setStatement(createTimeSeriesStatement);

      SchemaPartition schemaPartitionInfo;
      try {
        schemaPartitionInfo =
            partitionFetcher.getSchemaPartition(
                new PathPatternTree(createTimeSeriesStatement.getPath()));
      } catch (StatementAnalyzeException e) {
        throw new SemanticException("An error occurred when fetching schema partition infos");
      }
      analysis.setSchemaPartitionInfo(schemaPartitionInfo);
      return analysis;
    }

    @Override
    public Analysis visitCreateAlignedTimeseries(
        CreateAlignedTimeSeriesStatement createAlignedTimeSeriesStatement,
        MPPQueryContext context) {
      List<String> measurements = createAlignedTimeSeriesStatement.getMeasurements();
      Set<String> measurementsSet = new HashSet<>(measurements);
      if (measurementsSet.size() < measurements.size()) {
        throw new SemanticException(
            "Measurement under an aligned device is not allowed to have the same measurement name");
      }

      Analysis analysis = new Analysis();
      analysis.setStatement(createAlignedTimeSeriesStatement);

      SchemaPartition schemaPartitionInfo;
      try {
        schemaPartitionInfo =
            partitionFetcher.getSchemaPartition(
                new PathPatternTree(
                    createAlignedTimeSeriesStatement.getDevicePath(),
                    createAlignedTimeSeriesStatement.getMeasurements()));
      } catch (StatementAnalyzeException e) {
        throw new SemanticException("An error occurred when fetching schema partition infos");
      }
      analysis.setSchemaPartitionInfo(schemaPartitionInfo);
      return analysis;
    }

    @Override
    public Analysis visitAlterTimeseries(
        AlterTimeSeriesStatement alterTimeSeriesStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(alterTimeSeriesStatement);

      SchemaPartition schemaPartitionInfo;
      try {
        schemaPartitionInfo =
            partitionFetcher.getSchemaPartition(
                new PathPatternTree(alterTimeSeriesStatement.getPath()));
      } catch (StatementAnalyzeException e) {
        throw new SemanticException("An error occurred when fetching schema partition infos");
      }
      analysis.setSchemaPartitionInfo(schemaPartitionInfo);
      return analysis;
    }

    @Override
    public Analysis visitInsertTablet(
        InsertTabletStatement insertTabletStatement, MPPQueryContext context) {
      SchemaTree schemaTree =
          schemaFetcher.fetchSchemaWithAutoCreate(
              insertTabletStatement.getDevicePath(),
              insertTabletStatement.getMeasurements(),
              insertTabletStatement.getDataTypes(),
              insertTabletStatement.isAligned());

      if (!insertTabletStatement.checkDataType(schemaTree)) {
        throw new SemanticException("Data type mismatch");
      }

      Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap = new HashMap<>();
      DataPartitionQueryParam dataPartitionQueryParam = new DataPartitionQueryParam();
      dataPartitionQueryParam.setDevicePath(insertTabletStatement.getDevicePath().getFullPath());
      dataPartitionQueryParam.setTimePartitionSlotList(
          insertTabletStatement.getTimePartitionSlots());
      sgNameToQueryParamsMap.put(
          schemaTree.getBelongedStorageGroup(insertTabletStatement.getDevicePath()),
          Collections.singletonList(dataPartitionQueryParam));
      DataPartition dataPartition;
      try {
        dataPartition = partitionFetcher.getDataPartition(sgNameToQueryParamsMap);
      } catch (StatementAnalyzeException e) {
        throw new SemanticException("An error occurred when fetching data partition infos");
      }

      Analysis analysis = new Analysis();
      analysis.setSchemaTree(schemaTree);
      analysis.setStatement(insertTabletStatement);
      analysis.setDataPartitionInfo(dataPartition);
      return analysis;
    }

    @Override
    public Analysis visitShowTimeSeries(
        ShowTimeSeriesStatement showTimeSeriesStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(showTimeSeriesStatement);

      SchemaPartition schemaPartitionInfo;
      try {
        schemaPartitionInfo =
            partitionFetcher.getSchemaPartition(
                new PathPatternTree(showTimeSeriesStatement.getPathPattern()));
      } catch (StatementAnalyzeException e) {
        throw new SemanticException("An error occurred when fetching schema partition infos");
      }
      analysis.setSchemaPartitionInfo(schemaPartitionInfo);
      analysis.setRespDatasetHeader(HeaderConstant.showTimeSeriesHeader);
      return analysis;
    }

    @Override
    public Analysis visitShowStorageGroup(
        ShowStorageGroupStatement showStorageGroupStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(showStorageGroupStatement);
      analysis.setRespDatasetHeader(HeaderConstant.showStorageGroupHeader);
      return analysis;
    }

    @Override
    public Analysis visitShowDevices(
        ShowDevicesStatement showDevicesStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(showDevicesStatement);

      SchemaPartition schemaPartitionInfo;
      try {
        schemaPartitionInfo =
            partitionFetcher.getSchemaPartition(
                new PathPatternTree(showDevicesStatement.getPathPattern().concatNode("*")));
      } catch (StatementAnalyzeException e) {
        throw new SemanticException("An error occurred when fetching schema partition infos");
      }
      analysis.setSchemaPartitionInfo(schemaPartitionInfo);
      analysis.setRespDatasetHeader(
          showDevicesStatement.hasSgCol()
              ? HeaderConstant.showDevicesWithSgHeader
              : HeaderConstant.showDevicesHeader);
      return analysis;
    }

    @Override
    public Analysis visitCreateUser(AuthorStatement authorStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(authorStatement);
      return analysis;
    }

    @Override
    public Analysis visitCreateRole(AuthorStatement authorStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(authorStatement);
      return analysis;
    }

    @Override
    public Analysis visitAlterUser(AuthorStatement authorStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(authorStatement);
      return analysis;
    }

    @Override
    public Analysis visitGrantUser(AuthorStatement authorStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(authorStatement);
      return analysis;
    }

    @Override
    public Analysis visitGrantRole(AuthorStatement authorStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(authorStatement);
      return analysis;
    }

    @Override
    public Analysis visitGrantRoleToUser(AuthorStatement authorStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(authorStatement);
      return analysis;
    }

    @Override
    public Analysis visitRevokeUser(AuthorStatement authorStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(authorStatement);
      return analysis;
    }

    @Override
    public Analysis visitRevokeRole(AuthorStatement authorStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(authorStatement);
      return analysis;
    }

    @Override
    public Analysis visitRevokeRoleFromUser(
        AuthorStatement authorStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(authorStatement);
      return analysis;
    }

    @Override
    public Analysis visitDropUser(AuthorStatement authorStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(authorStatement);
      return analysis;
    }

    @Override
    public Analysis visitDropRole(AuthorStatement authorStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(authorStatement);
      return analysis;
    }

    @Override
    public Analysis visitListUser(AuthorStatement authorStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(authorStatement);
      return analysis;
    }

    @Override
    public Analysis visitListRole(AuthorStatement authorStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(authorStatement);
      return analysis;
    }

    @Override
    public Analysis visitListPrivilegesUser(
        AuthorStatement authorStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(authorStatement);
      return analysis;
    }

    @Override
    public Analysis visitListPrivilegesRole(
        AuthorStatement authorStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(authorStatement);
      return analysis;
    }

    @Override
    public Analysis visitListUserPrivileges(
        AuthorStatement authorStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(authorStatement);
      return analysis;
    }

    @Override
    public Analysis visitListRolePrivileges(
        AuthorStatement authorStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(authorStatement);
      return analysis;
    }

    @Override
    public Analysis visitListAllRoleOfUser(
        AuthorStatement authorStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(authorStatement);
      return analysis;
    }

    @Override
    public Analysis visitListAllUserOfRole(
        AuthorStatement authorStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(authorStatement);
      return analysis;
    }

    @Override
    public Analysis visitInsertRow(InsertRowStatement insertRowStatement, MPPQueryContext context) {
      // TODO remove duplicate
      SchemaTree schemaTree =
          schemaFetcher.fetchSchemaWithAutoCreate(
              insertRowStatement.getDevicePath(),
              insertRowStatement.getMeasurements(),
              insertRowStatement.getDataTypes(),
              insertRowStatement.isAligned());

      try {
        insertRowStatement.transferType(schemaTree);
      } catch (QueryProcessException e) {
        throw new SemanticException(e.getMessage());
      }

      if (!insertRowStatement.checkDataType(schemaTree)) {
        throw new SemanticException("Data type mismatch");
      }

      Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap = new HashMap<>();
      DataPartitionQueryParam dataPartitionQueryParam = new DataPartitionQueryParam();
      dataPartitionQueryParam.setDevicePath(insertRowStatement.getDevicePath().getFullPath());
      dataPartitionQueryParam.setTimePartitionSlotList(insertRowStatement.getTimePartitionSlots());
      sgNameToQueryParamsMap.put(
          schemaTree.getBelongedStorageGroup(insertRowStatement.getDevicePath()),
          Collections.singletonList(dataPartitionQueryParam));
      DataPartition dataPartition;
      try {
        dataPartition = partitionFetcher.getDataPartition(sgNameToQueryParamsMap);
      } catch (StatementAnalyzeException e) {
        throw new SemanticException("An error occurred when fetching data partition infos");
      }

      Analysis analysis = new Analysis();
      analysis.setSchemaTree(schemaTree);
      analysis.setStatement(insertRowStatement);
      analysis.setDataPartitionInfo(dataPartition);

      return analysis;
    }

    @Override
    public Analysis visitInsertRows(
        InsertRowsStatement insertRowsStatement, MPPQueryContext context) {
      // TODO remove duplicate
      SchemaTree schemaTree =
          schemaFetcher.fetchSchemaListWithAutoCreate(
              insertRowsStatement.getDevicePaths(),
              insertRowsStatement.getMeasurementsList(),
              insertRowsStatement.getDataTypesList(),
              insertRowsStatement.getAlignedList());

      try {
        insertRowsStatement.transferType(schemaTree);
      } catch (QueryProcessException e) {
        throw new SemanticException(e.getMessage());
      }

      if (!insertRowsStatement.checkDataType(schemaTree)) {
        throw new SemanticException("Data type mismatch");
      }

      Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap = new HashMap<>();
      for (InsertRowStatement insertRowStatement :
          insertRowsStatement.getInsertRowStatementList()) {
        DataPartitionQueryParam dataPartitionQueryParam = new DataPartitionQueryParam();
        dataPartitionQueryParam.setDevicePath(insertRowStatement.getDevicePath().getFullPath());
        dataPartitionQueryParam.setTimePartitionSlotList(
            insertRowStatement.getTimePartitionSlots());
        sgNameToQueryParamsMap
            .computeIfAbsent(
                schemaTree.getBelongedStorageGroup(insertRowStatement.getDevicePath()),
                key -> new ArrayList<>())
            .add(dataPartitionQueryParam);
      }
      DataPartition dataPartition;
      try {
        dataPartition = partitionFetcher.getDataPartition(sgNameToQueryParamsMap);
      } catch (StatementAnalyzeException e) {
        throw new SemanticException("An error occurred when fetching data partition infos");
      }

      Analysis analysis = new Analysis();
      analysis.setSchemaTree(schemaTree);
      analysis.setStatement(insertRowsStatement);
      analysis.setDataPartitionInfo(dataPartition);

      return analysis;
    }

    @Override
    public Analysis visitInsertMultiTablets(
        InsertMultiTabletsStatement insertMultiTabletsStatement, MPPQueryContext context) {
      // TODO remove duplicate
      SchemaTree schemaTree =
          schemaFetcher.fetchSchemaListWithAutoCreate(
              insertMultiTabletsStatement.getDevicePaths(),
              insertMultiTabletsStatement.getMeasurementsList(),
              insertMultiTabletsStatement.getDataTypesList(),
              insertMultiTabletsStatement.getAlignedList());

      if (!insertMultiTabletsStatement.checkDataType(schemaTree)) {
        throw new SemanticException("Data type mismatch");
      }

      Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap = new HashMap<>();
      for (InsertTabletStatement insertTabletStatement :
          insertMultiTabletsStatement.getInsertTabletStatementList()) {
        DataPartitionQueryParam dataPartitionQueryParam = new DataPartitionQueryParam();
        dataPartitionQueryParam.setDevicePath(insertTabletStatement.getDevicePath().getFullPath());
        dataPartitionQueryParam.setTimePartitionSlotList(
            insertTabletStatement.getTimePartitionSlots());
        sgNameToQueryParamsMap
            .computeIfAbsent(
                schemaTree.getBelongedStorageGroup(insertTabletStatement.getDevicePath()),
                key -> new ArrayList<>())
            .add(dataPartitionQueryParam);
      }
      DataPartition dataPartition;
      try {
        dataPartition = partitionFetcher.getDataPartition(sgNameToQueryParamsMap);
      } catch (StatementAnalyzeException e) {
        throw new SemanticException("An error occurred when fetching data partition infos");
      }

      Analysis analysis = new Analysis();
      analysis.setSchemaTree(schemaTree);
      analysis.setStatement(insertMultiTabletsStatement);
      analysis.setDataPartitionInfo(dataPartition);

      return analysis;
    }

    @Override
    public Analysis visitInsertRowsOfOneDevice(
        InsertRowsOfOneDeviceStatement insertRowsOfOneDeviceStatement, MPPQueryContext context) {
      // TODO remove duplicate
      SchemaTree schemaTree =
          schemaFetcher.fetchSchemaWithAutoCreate(
              insertRowsOfOneDeviceStatement.getDevicePath(),
              insertRowsOfOneDeviceStatement.getMeasurements(),
              insertRowsOfOneDeviceStatement.getDataTypes(),
              insertRowsOfOneDeviceStatement.isAligned());

      try {
        insertRowsOfOneDeviceStatement.transferType(schemaTree);
      } catch (QueryProcessException e) {
        throw new SemanticException(e.getMessage());
      }

      if (!insertRowsOfOneDeviceStatement.checkDataType(schemaTree)) {
        throw new SemanticException("Data type mismatch");
      }

      Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap = new HashMap<>();
      DataPartitionQueryParam dataPartitionQueryParam = new DataPartitionQueryParam();
      dataPartitionQueryParam.setDevicePath(
          insertRowsOfOneDeviceStatement.getDevicePath().getFullPath());
      dataPartitionQueryParam.setTimePartitionSlotList(
          insertRowsOfOneDeviceStatement.getTimePartitionSlots());
      sgNameToQueryParamsMap.put(
          schemaTree.getBelongedStorageGroup(insertRowsOfOneDeviceStatement.getDevicePath()),
          Collections.singletonList(dataPartitionQueryParam));
      DataPartition dataPartition;
      try {
        dataPartition = partitionFetcher.getDataPartition(sgNameToQueryParamsMap);
      } catch (StatementAnalyzeException e) {
        throw new SemanticException("An error occurred when fetching data partition infos");
      }

      Analysis analysis = new Analysis();
      analysis.setSchemaTree(schemaTree);
      analysis.setStatement(insertRowsOfOneDeviceStatement);
      analysis.setDataPartitionInfo(dataPartition);

      return analysis;
    }
  }
}
