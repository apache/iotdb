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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.db.exception.query.PathNumOverLimitException;
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
import org.apache.iotdb.db.mpp.sql.statement.StatementNode;
import org.apache.iotdb.db.mpp.sql.statement.StatementVisitor;
import org.apache.iotdb.db.mpp.sql.statement.component.ResultColumn;
import org.apache.iotdb.db.mpp.sql.statement.component.WhereCondition;
import org.apache.iotdb.db.mpp.sql.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.mpp.sql.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.mpp.sql.statement.crud.InsertRowsOfOneDeviceStatement;
import org.apache.iotdb.db.mpp.sql.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.mpp.sql.statement.crud.InsertStatement;
import org.apache.iotdb.db.mpp.sql.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.mpp.sql.statement.crud.QueryStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.AlterTimeSeriesStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.CountDevicesStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.CountLevelTimeSeriesStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.CountStorageGroupStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.CountTimeSeriesStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.SchemaFetchStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.ShowDevicesStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.ShowStorageGroupStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.ShowTimeSeriesStatement;
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

  private final IPartitionFetcher partitionFetcher;
  private final ISchemaFetcher schemaFetcher;

  public Analyzer(
      MPPQueryContext context, IPartitionFetcher partitionFetcher, ISchemaFetcher schemaFetcher) {
    this.context = context;
    this.partitionFetcher = partitionFetcher;
    this.schemaFetcher = schemaFetcher;
  }

  public Analysis analyze(Statement statement) {
    return new AnalyzeVisitor().process(statement, context);
  }

  /** This visitor is used to analyze each type of Statement and returns the {@link Analysis}. */
  private final class AnalyzeVisitor extends StatementVisitor<Analysis, MPPQueryContext> {

    @Override
    public Analysis visitNode(StatementNode node, MPPQueryContext context) {
      throw new UnsupportedOperationException(
          "Unsupported statement type: " + node.getClass().getName());
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
        TypeProvider typeProvider = new TypeProvider();
        rewrittenStatement =
            (QueryStatement)
                new WildcardsRemover().rewrite(rewrittenStatement, typeProvider, schemaTree);

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
        analysis.setTypeProvider(typeProvider);
        analysis.setRespDatasetHeader(queryStatement.constructDatasetHeader());
        analysis.setDataPartitionInfo(dataPartition);
      } catch (StatementAnalyzeException
          | PathNumOverLimitException
          | QueryFilterOptimizationException e) {
        throw new StatementAnalyzeException("Meet error when analyzing the query statement");
      }
      return analysis;
    }

    @Override
    public Analysis visitInsert(InsertStatement insertStatement, MPPQueryContext context) {
      context.setQueryType(QueryType.WRITE);

      long[] timeArray = insertStatement.getTimes();
      PartialPath devicePath = insertStatement.getDevice();
      String[] measurements = insertStatement.getMeasurementList();
      if (timeArray.length == 1) {
        // construct insert row statement
        InsertRowStatement insertRowStatement = new InsertRowStatement();
        insertRowStatement.setDevicePath(devicePath);
        insertRowStatement.setTime(timeArray[0]);
        insertRowStatement.setMeasurements(measurements);
        insertRowStatement.setDataTypes(
            new TSDataType[insertStatement.getMeasurementList().length]);
        Object[] values = new Object[insertStatement.getMeasurementList().length];
        System.arraycopy(insertStatement.getValuesList().get(0), 0, values, 0, values.length);
        insertRowStatement.setValues(values);
        insertRowStatement.setNeedInferType(true);
        insertRowStatement.setAligned(insertStatement.isAligned());
        return insertRowStatement.accept(this, context);
      } else {
        // construct insert rows statement
        // construct insert statement
        InsertRowsStatement insertRowsStatement = new InsertRowsStatement();
        List<InsertRowStatement> insertRowStatementList = new ArrayList<>();
        for (int i = 0; i < timeArray.length; i++) {
          InsertRowStatement statement = new InsertRowStatement();
          statement.setDevicePath(devicePath);
          statement.setMeasurements(measurements);
          statement.setTime(timeArray[i]);
          statement.setDataTypes(new TSDataType[insertStatement.getMeasurementList().length]);
          Object[] values = new Object[insertStatement.getMeasurementList().length];
          System.arraycopy(insertStatement.getValuesList().get(i), 0, values, 0, values.length);
          statement.setValues(values);
          statement.setAligned(insertStatement.isAligned());
          statement.setNeedInferType(true);
          insertRowStatementList.add(statement);
        }
        insertRowsStatement.setInsertRowStatementList(insertRowStatementList);
        return insertRowsStatement.accept(this, context);
      }
    }

    @Override
    public Analysis visitCreateTimeseries(
        CreateTimeSeriesStatement createTimeSeriesStatement, MPPQueryContext context) {
      context.setQueryType(QueryType.WRITE);
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

      SchemaPartition schemaPartitionInfo =
          partitionFetcher.getOrCreateSchemaPartition(
              new PathPatternTree(createTimeSeriesStatement.getPath()));
      analysis.setSchemaPartitionInfo(schemaPartitionInfo);
      return analysis;
    }

    @Override
    public Analysis visitCreateAlignedTimeseries(
        CreateAlignedTimeSeriesStatement createAlignedTimeSeriesStatement,
        MPPQueryContext context) {
      context.setQueryType(QueryType.WRITE);
      List<String> measurements = createAlignedTimeSeriesStatement.getMeasurements();
      Set<String> measurementsSet = new HashSet<>(measurements);
      if (measurementsSet.size() < measurements.size()) {
        throw new SemanticException(
            "Measurement under an aligned device is not allowed to have the same measurement name");
      }

      Analysis analysis = new Analysis();
      analysis.setStatement(createAlignedTimeSeriesStatement);

      SchemaPartition schemaPartitionInfo;
      schemaPartitionInfo =
          partitionFetcher.getOrCreateSchemaPartition(
              new PathPatternTree(
                  createAlignedTimeSeriesStatement.getDevicePath(),
                  createAlignedTimeSeriesStatement.getMeasurements()));
      analysis.setSchemaPartitionInfo(schemaPartitionInfo);
      return analysis;
    }

    @Override
    public Analysis visitAlterTimeseries(
        AlterTimeSeriesStatement alterTimeSeriesStatement, MPPQueryContext context) {
      context.setQueryType(QueryType.WRITE);
      Analysis analysis = new Analysis();
      analysis.setStatement(alterTimeSeriesStatement);

      SchemaPartition schemaPartitionInfo;
      schemaPartitionInfo =
          partitionFetcher.getSchemaPartition(
              new PathPatternTree(alterTimeSeriesStatement.getPath()));
      analysis.setSchemaPartitionInfo(schemaPartitionInfo);
      return analysis;
    }

    @Override
    public Analysis visitInsertTablet(
        InsertTabletStatement insertTabletStatement, MPPQueryContext context) {
      context.setQueryType(QueryType.WRITE);

      DataPartitionQueryParam dataPartitionQueryParam = new DataPartitionQueryParam();
      dataPartitionQueryParam.setDevicePath(insertTabletStatement.getDevicePath().getFullPath());
      dataPartitionQueryParam.setTimePartitionSlotList(
          insertTabletStatement.getTimePartitionSlots());

      DataPartition dataPartition =
          partitionFetcher.getOrCreateDataPartition(
              Collections.singletonList(dataPartitionQueryParam));

      Analysis analysis = new Analysis();
      analysis.setStatement(insertTabletStatement);
      analysis.setDataPartitionInfo(dataPartition);

      return analysis;
    }

    @Override
    public Analysis visitInsertRow(InsertRowStatement insertRowStatement, MPPQueryContext context) {
      context.setQueryType(QueryType.WRITE);

      DataPartitionQueryParam dataPartitionQueryParam = new DataPartitionQueryParam();
      dataPartitionQueryParam.setDevicePath(insertRowStatement.getDevicePath().getFullPath());
      dataPartitionQueryParam.setTimePartitionSlotList(insertRowStatement.getTimePartitionSlots());

      DataPartition dataPartition =
          partitionFetcher.getOrCreateDataPartition(
              Collections.singletonList(dataPartitionQueryParam));

      Analysis analysis = new Analysis();
      analysis.setStatement(insertRowStatement);
      analysis.setDataPartitionInfo(dataPartition);

      return analysis;
    }

    @Override
    public Analysis visitInsertRows(
        InsertRowsStatement insertRowsStatement, MPPQueryContext context) {
      context.setQueryType(QueryType.WRITE);

      List<DataPartitionQueryParam> dataPartitionQueryParams = new ArrayList<>();
      for (InsertRowStatement insertRowStatement :
          insertRowsStatement.getInsertRowStatementList()) {
        DataPartitionQueryParam dataPartitionQueryParam = new DataPartitionQueryParam();
        dataPartitionQueryParam.setDevicePath(insertRowStatement.getDevicePath().getFullPath());
        dataPartitionQueryParam.setTimePartitionSlotList(
            insertRowStatement.getTimePartitionSlots());
        dataPartitionQueryParams.add(dataPartitionQueryParam);
      }

      DataPartition dataPartition =
          partitionFetcher.getOrCreateDataPartition(dataPartitionQueryParams);

      Analysis analysis = new Analysis();
      analysis.setStatement(insertRowsStatement);
      analysis.setDataPartitionInfo(dataPartition);

      return analysis;
    }

    @Override
    public Analysis visitInsertMultiTablets(
        InsertMultiTabletsStatement insertMultiTabletsStatement, MPPQueryContext context) {
      context.setQueryType(QueryType.WRITE);

      List<DataPartitionQueryParam> dataPartitionQueryParams = new ArrayList<>();
      for (InsertTabletStatement insertTabletStatement :
          insertMultiTabletsStatement.getInsertTabletStatementList()) {
        DataPartitionQueryParam dataPartitionQueryParam = new DataPartitionQueryParam();
        dataPartitionQueryParam.setDevicePath(insertTabletStatement.getDevicePath().getFullPath());
        dataPartitionQueryParam.setTimePartitionSlotList(
            insertTabletStatement.getTimePartitionSlots());
        dataPartitionQueryParams.add(dataPartitionQueryParam);
      }

      DataPartition dataPartition =
          partitionFetcher.getOrCreateDataPartition(dataPartitionQueryParams);

      Analysis analysis = new Analysis();
      analysis.setStatement(insertMultiTabletsStatement);
      analysis.setDataPartitionInfo(dataPartition);

      return analysis;
    }

    @Override
    public Analysis visitInsertRowsOfOneDevice(
        InsertRowsOfOneDeviceStatement insertRowsOfOneDeviceStatement, MPPQueryContext context) {
      context.setQueryType(QueryType.WRITE);

      DataPartitionQueryParam dataPartitionQueryParam = new DataPartitionQueryParam();
      dataPartitionQueryParam.setDevicePath(
          insertRowsOfOneDeviceStatement.getDevicePath().getFullPath());
      dataPartitionQueryParam.setTimePartitionSlotList(
          insertRowsOfOneDeviceStatement.getTimePartitionSlots());

      DataPartition dataPartition =
          partitionFetcher.getOrCreateDataPartition(
              Collections.singletonList(dataPartitionQueryParam));

      Analysis analysis = new Analysis();
      analysis.setStatement(insertRowsOfOneDeviceStatement);
      analysis.setDataPartitionInfo(dataPartition);

      return analysis;
    }

    @Override
    public Analysis visitShowTimeSeries(
        ShowTimeSeriesStatement showTimeSeriesStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(showTimeSeriesStatement);

      SchemaPartition schemaPartitionInfo =
          partitionFetcher.getSchemaPartition(
              new PathPatternTree(showTimeSeriesStatement.getPathPattern()));
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

      SchemaPartition schemaPartitionInfo =
          partitionFetcher.getSchemaPartition(
              new PathPatternTree(
                  showDevicesStatement
                      .getPathPattern()
                      .concatNode(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD)));

      analysis.setSchemaPartitionInfo(schemaPartitionInfo);
      analysis.setRespDatasetHeader(
          showDevicesStatement.hasSgCol()
              ? HeaderConstant.showDevicesWithSgHeader
              : HeaderConstant.showDevicesHeader);
      return analysis;
    }

    @Override
    public Analysis visitCountStorageGroup(
        CountStorageGroupStatement countStorageGroupStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(countStorageGroupStatement);
      analysis.setRespDatasetHeader(HeaderConstant.countStorageGroupHeader);
      return analysis;
    }

    @Override
    public Analysis visitSchemaFetch(
        SchemaFetchStatement schemaFetchStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(schemaFetchStatement);
      analysis.setSchemaPartitionInfo(schemaFetchStatement.getSchemaPartition());
      return analysis;
    }

    @Override
    public Analysis visitCountDevices(
        CountDevicesStatement countDevicesStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(countDevicesStatement);

      SchemaPartition schemaPartitionInfo =
          partitionFetcher.getSchemaPartition(
              new PathPatternTree(
                  countDevicesStatement
                      .getPartialPath()
                      .concatNode(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD)));

      analysis.setSchemaPartitionInfo(schemaPartitionInfo);
      analysis.setRespDatasetHeader(HeaderConstant.countDevicesHeader);
      return analysis;
    }

    @Override
    public Analysis visitCountTimeSeries(
        CountTimeSeriesStatement countTimeSeriesStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(countTimeSeriesStatement);

      SchemaPartition schemaPartitionInfo =
          partitionFetcher.getSchemaPartition(
              new PathPatternTree(countTimeSeriesStatement.getPartialPath()));

      analysis.setSchemaPartitionInfo(schemaPartitionInfo);
      analysis.setRespDatasetHeader(HeaderConstant.countTimeSeriesHeader);
      return analysis;
    }

    @Override
    public Analysis visitCountLevelTimeSeries(
        CountLevelTimeSeriesStatement countLevelTimeSeriesStatement, MPPQueryContext context) {
      Analysis analysis = new Analysis();
      analysis.setStatement(countLevelTimeSeriesStatement);

      SchemaPartition schemaPartitionInfo =
          partitionFetcher.getSchemaPartition(
              new PathPatternTree(countLevelTimeSeriesStatement.getPartialPath()));

      analysis.setSchemaPartitionInfo(schemaPartitionInfo);
      analysis.setRespDatasetHeader(HeaderConstant.countLevelTimeSeriesHeader);
      return analysis;
    }
  }
}
