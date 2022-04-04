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
import org.apache.iotdb.commons.partition.SchemaPartitionInfo;
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
import org.apache.iotdb.db.mpp.sql.statement.component.WhereCondition;
import org.apache.iotdb.db.mpp.sql.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.mpp.sql.statement.crud.InsertStatement;
import org.apache.iotdb.db.mpp.sql.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.mpp.sql.statement.crud.QueryStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.AlterTimeSeriesStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.ShowDevicesStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.ShowTimeSeriesStatement;

import java.util.*;

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
        Map<String, Set<PartialPath>> deviceIdToPathsMap = new HashMap<>();
        rewrittenStatement =
            (QueryStatement)
                new WildcardsRemover().rewrite(rewrittenStatement, schemaTree, deviceIdToPathsMap);

        // fetch partition information
        List<DataPartitionQueryParam> dataPartitionQueryParams = new ArrayList<>();
        for (String deviceId : deviceIdToPathsMap.keySet()) {
          DataPartitionQueryParam dataPartitionQueryParam = new DataPartitionQueryParam();
          dataPartitionQueryParam.setDevicePath(deviceId);
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
          whereCondition.setQueryFilter(filter);
        }
        analysis.setStatement(rewrittenStatement);
        analysis.setSchemaTree(schemaTree);
        analysis.setDeviceIdToPathsMap(deviceIdToPathsMap);
        analysis.setDataPartitionInfo(dataPartition);
      } catch (StatementAnalyzeException | PathNumOverLimitException e) {
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
                  insertTabletStatement.getDataTypes())
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
    public Analysis visitShowTimeSeries(
        ShowTimeSeriesStatement showTimeSeriesStatement, MPPQueryContext context) {
      SchemaPartitionInfo schemaPartitionInfo =
          partitionFetcher.fetchSchemaPartitionInfo(
              showTimeSeriesStatement.getPathPattern().getDevice());
      Analysis analysis = new Analysis();
      analysis.setStatement(showTimeSeriesStatement);
      analysis.setSchemaPartitionInfo(schemaPartitionInfo);
      return analysis;
    }

    @Override
    public Analysis visitShowDevices(
        ShowDevicesStatement showDevicesStatement, MPPQueryContext context) {
      SchemaPartitionInfo schemaPartitionInfo =
          partitionFetcher.fetchSchemaPartitionInfo(
              showDevicesStatement.getPathPattern().getFullPath());
      Analysis analysis = new Analysis();
      analysis.setStatement(showDevicesStatement);
      analysis.setSchemaPartitionInfo(schemaPartitionInfo);
      return analysis;
    }

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
                  insertRowStatement.getDataTypes())
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
  }
}
