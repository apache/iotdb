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
import org.apache.iotdb.commons.partition.PartitionInfo;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.PathNumOverLimitException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.sql.SQLParserException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.filter.QueryFilter;
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
import org.apache.iotdb.db.mpp.sql.statement.metadata.AlterTimeSeriesStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.SchemaFetchStatement;
import org.apache.iotdb.db.mpp.sql.statement.sys.AuthorStatement;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.util.ExpressionOptimizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
        Set<String> devicePathSet = new HashSet<>();
        for (ResultColumn resultColumn : queryStatement.getSelectComponent().getResultColumns()) {
          devicePathSet.addAll(
              resultColumn.collectPaths().stream()
                  .map(PartialPath::getDevice)
                  .collect(Collectors.toList()));
        }
        if (queryStatement.getWhereCondition() != null) {
          devicePathSet.addAll(
              queryStatement.getWhereCondition().getQueryFilter().getPathSet().stream()
                  .map(PartialPath::getDevice)
                  .collect(Collectors.toList()));
        }
        List<DataPartitionQueryParam> dataPartitionQueryParams = new ArrayList<>();
        for (String devicePath : devicePathSet) {
          DataPartitionQueryParam dataPartitionQueryParam = new DataPartitionQueryParam();
          dataPartitionQueryParam.setDevicePath(devicePath);
          dataPartitionQueryParams.add(dataPartitionQueryParam);
        }
        DataPartition dataPartition =
            partitionFetcher.fetchDataPartitionInfos(dataPartitionQueryParams);

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

      String devicePath = createTimeSeriesStatement.getPath().getDevice();
      analysis.setSchemaPartitionInfo(partitionFetcher.fetchSchemaPartitionInfo(devicePath));
      return analysis;
    }

    @Override
    public Analysis visitCreateAlignedTimeseries(
        CreateAlignedTimeSeriesStatement createAlignedTimeSeriesStatement,
        MPPQueryContext context) {
      List<String> measurements = createAlignedTimeSeriesStatement.getMeasurements();
      Set<String> measurementsSet = new HashSet<>(measurements);
      if (measurementsSet.size() < measurements.size()) {
        throw new SQLParserException(
            "Measurement under an aligned device is not allowed to have the same measurement name");
      }

      Analysis analysis = new Analysis();
      analysis.setStatement(createAlignedTimeSeriesStatement);

      String devicePath = createAlignedTimeSeriesStatement.getDevicePath().getFullPath();
      analysis.setSchemaPartitionInfo(partitionFetcher.fetchSchemaPartitionInfo(devicePath));
      return analysis;
    }

    @Override
    public Analysis visitAlterTimeseries(
        AlterTimeSeriesStatement alterTimeSeriesStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(alterTimeSeriesStatement);

      String devicePath = alterTimeSeriesStatement.getPath().getDevice();
      analysis.setSchemaPartitionInfo(partitionFetcher.fetchSchemaPartitionInfo(devicePath));
      return analysis;
    }

    @Override
    public Analysis visitInsertTablet(
        InsertTabletStatement insertTabletStatement, MPPQueryContext context) {
      DataPartitionQueryParam dataPartitionQueryParam = new DataPartitionQueryParam();
      dataPartitionQueryParam.setTimePartitionSlotList(
          insertTabletStatement.getTimePartitionSlots());
      dataPartitionQueryParam.setDevicePath(insertTabletStatement.getDevicePath().getFullPath());
      PartitionInfo partitionInfo = partitionFetcher.fetchPartitionInfo(dataPartitionQueryParam);

      SchemaTree schemaTree =
          IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled()
              ? schemaFetcher.fetchSchemaWithAutoCreate(
                  insertTabletStatement.getDevicePath(),
                  insertTabletStatement.getMeasurements(),
                  insertTabletStatement.getDataTypes(),
                  insertTabletStatement.isAligned())
              : schemaFetcher.fetchSchema(
                  new PathPatternTree(
                      insertTabletStatement.getDevicePath(),
                      insertTabletStatement.getMeasurements()));

      Analysis analysis = new Analysis();
      analysis.setSchemaTree(schemaTree);

      if (!insertTabletStatement.checkDataType(schemaTree)) {
        throw new SemanticException("Data type mismatch");
      }
      analysis.setStatement(insertTabletStatement);
      analysis.setDataPartitionInfo(partitionInfo.getDataPartitionInfo());
      analysis.setSchemaPartitionInfo(partitionInfo.getSchemaPartitionInfo());
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
      DataPartitionQueryParam dataPartitionQueryParam = new DataPartitionQueryParam();
      dataPartitionQueryParam.setDevicePath(insertRowStatement.getDevicePath().getFullPath());
      dataPartitionQueryParam.setTimePartitionSlotList(insertRowStatement.getTimePartitionSlots());
      PartitionInfo partitionInfo = partitionFetcher.fetchPartitionInfo(dataPartitionQueryParam);

      SchemaTree schemaTree =
          IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled()
              ? schemaFetcher.fetchSchemaWithAutoCreate(
                  insertRowStatement.getDevicePath(),
                  insertRowStatement.getMeasurements(),
                  insertRowStatement.getDataTypes(),
                  insertRowStatement.isAligned())
              : schemaFetcher.fetchSchema(
                  new PathPatternTree(
                      insertRowStatement.getDevicePath(), insertRowStatement.getMeasurements()));

      Analysis analysis = new Analysis();
      analysis.setSchemaTree(schemaTree);

      try {
        insertRowStatement.transferType(schemaTree);
      } catch (QueryProcessException e) {
        throw new SemanticException(e.getMessage());
      }

      if (!insertRowStatement.checkDataType(schemaTree)) {
        throw new SemanticException("Data type mismatch");
      }

      analysis.setStatement(insertRowStatement);
      analysis.setDataPartitionInfo(partitionInfo.getDataPartitionInfo());
      analysis.setSchemaPartitionInfo(partitionInfo.getSchemaPartitionInfo());

      return analysis;
    }

    @Override
    public Analysis visitInsertRows(
        InsertRowsStatement insertRowsStatement, MPPQueryContext context) {
      // TODO remove duplicate
      List<DataPartitionQueryParam> dataPartitionQueryParams = new ArrayList<>();
      for (InsertRowStatement insertRowStatement :
          insertRowsStatement.getInsertRowStatementList()) {
        DataPartitionQueryParam dataPartitionQueryParam = new DataPartitionQueryParam();
        dataPartitionQueryParam.setDevicePath(insertRowStatement.getDevicePath().getFullPath());
        dataPartitionQueryParam.setTimePartitionSlotList(
            insertRowStatement.getTimePartitionSlots());
        dataPartitionQueryParams.add(dataPartitionQueryParam);
      }

      PartitionInfo partitionInfo = partitionFetcher.fetchPartitionInfos(dataPartitionQueryParams);

      SchemaTree schemaTree = null;
      if (IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled()) {
        schemaTree =
            schemaFetcher.fetchSchemaListWithAutoCreate(
                insertRowsStatement.getDevicePaths(),
                insertRowsStatement.getMeasurementsList(),
                insertRowsStatement.getDataTypesList(),
                insertRowsStatement.getAlignedList());
      } else {
        PathPatternTree patternTree = new PathPatternTree();
        for (InsertRowStatement insertRowStatement :
            insertRowsStatement.getInsertRowStatementList()) {
          patternTree.appendPaths(
              insertRowStatement.getDevicePath(),
              Arrays.asList(insertRowStatement.getMeasurements()));
        }
        schemaFetcher.fetchSchema(patternTree);
      }
      Analysis analysis = new Analysis();
      analysis.setSchemaTree(schemaTree);

      try {
        insertRowsStatement.transferType(schemaTree);
      } catch (QueryProcessException e) {
        throw new SemanticException(e.getMessage());
      }

      if (!insertRowsStatement.checkDataType(schemaTree)) {
        throw new SemanticException("Data type mismatch");
      }

      analysis.setStatement(insertRowsStatement);
      analysis.setDataPartitionInfo(partitionInfo.getDataPartitionInfo());
      analysis.setSchemaPartitionInfo(partitionInfo.getSchemaPartitionInfo());

      return analysis;
    }

    @Override
    public Analysis visitInsertMultiTablets(
        InsertMultiTabletsStatement insertMultiTabletsStatement, MPPQueryContext context) {
      // TODO remove duplicate
      List<DataPartitionQueryParam> dataPartitionQueryParams = new ArrayList<>();
      for (InsertTabletStatement insertTabletStatement :
          insertMultiTabletsStatement.getInsertTabletStatementList()) {
        DataPartitionQueryParam dataPartitionQueryParam = new DataPartitionQueryParam();
        dataPartitionQueryParam.setDevicePath(insertTabletStatement.getDevicePath().getFullPath());
        dataPartitionQueryParam.setTimePartitionSlotList(
            insertTabletStatement.getTimePartitionSlots());
        dataPartitionQueryParams.add(dataPartitionQueryParam);
      }

      PartitionInfo partitionInfo = partitionFetcher.fetchPartitionInfos(dataPartitionQueryParams);

      SchemaTree schemaTree = null;
      if (IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled()) {
        schemaTree =
            schemaFetcher.fetchSchemaListWithAutoCreate(
                insertMultiTabletsStatement.getDevicePaths(),
                insertMultiTabletsStatement.getMeasurementsList(),
                insertMultiTabletsStatement.getDataTypesList(),
                insertMultiTabletsStatement.getAlignedList());
      } else {
        PathPatternTree patternTree = new PathPatternTree();
        for (InsertTabletStatement insertTabletStatement :
            insertMultiTabletsStatement.getInsertTabletStatementList()) {
          patternTree.appendPaths(
              insertTabletStatement.getDevicePath(),
              Arrays.asList(insertTabletStatement.getMeasurements()));
        }
        schemaFetcher.fetchSchema(patternTree);
      }
      Analysis analysis = new Analysis();
      analysis.setSchemaTree(schemaTree);

      if (!insertMultiTabletsStatement.checkDataType(schemaTree)) {
        throw new SemanticException("Data type mismatch");
      }
      analysis.setStatement(insertMultiTabletsStatement);
      analysis.setDataPartitionInfo(partitionInfo.getDataPartitionInfo());
      analysis.setSchemaPartitionInfo(partitionInfo.getSchemaPartitionInfo());

      return analysis;
    }

    @Override
    public Analysis visitInsertRowsOfOneDevice(
        InsertRowsOfOneDeviceStatement insertRowsOfOneDeviceStatement, MPPQueryContext context) {
      DataPartitionQueryParam dataPartitionQueryParam = new DataPartitionQueryParam();
      dataPartitionQueryParam.setDevicePath(
          insertRowsOfOneDeviceStatement.getDevicePath().getFullPath());
      dataPartitionQueryParam.setTimePartitionSlotList(
          insertRowsOfOneDeviceStatement.getTimePartitionSlots());

      PartitionInfo partitionInfo = partitionFetcher.fetchPartitionInfo(dataPartitionQueryParam);

      SchemaTree schemaTree = null;
      if (IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled()) {
        schemaTree =
            schemaFetcher.fetchSchemaWithAutoCreate(
                insertRowsOfOneDeviceStatement.getDevicePath(),
                insertRowsOfOneDeviceStatement.getMeasurements(),
                insertRowsOfOneDeviceStatement.getDataTypes(),
                insertRowsOfOneDeviceStatement.isAligned());
      } else {
        PathPatternTree patternTree = new PathPatternTree();
        for (InsertRowStatement insertRowStatement :
            insertRowsOfOneDeviceStatement.getInsertRowStatementList()) {
          patternTree.appendPaths(
              insertRowStatement.getDevicePath(),
              Arrays.asList(insertRowStatement.getMeasurements()));
        }
        schemaFetcher.fetchSchema(patternTree);
      }
      Analysis analysis = new Analysis();
      analysis.setSchemaTree(schemaTree);

      try {
        insertRowsOfOneDeviceStatement.transferType(schemaTree);
      } catch (QueryProcessException e) {
        throw new SemanticException(e.getMessage());
      }

      if (!insertRowsOfOneDeviceStatement.checkDataType(schemaTree)) {
        throw new SemanticException("Data type mismatch");
      }

      analysis.setStatement(insertRowsOfOneDeviceStatement);
      analysis.setDataPartitionInfo(partitionInfo.getDataPartitionInfo());
      analysis.setSchemaPartitionInfo(partitionInfo.getSchemaPartitionInfo());

      return analysis;
    }

    @Override
    public Analysis visitSchemaFetch(
        SchemaFetchStatement schemaFetchStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(schemaFetchStatement);
      analysis.setSchemaPartitionInfo(
          partitionFetcher.fetchSchemaPartitionInfos(schemaFetchStatement.getPatternTree()));
      return analysis;
    }
  }
}
