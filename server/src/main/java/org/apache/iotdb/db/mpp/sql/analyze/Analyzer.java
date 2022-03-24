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

import org.apache.iotdb.commons.partition.DataPartitionInfo;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.partition.PartitionInfo;
import org.apache.iotdb.db.exception.query.PathNumOverLimitException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.filter.QueryFilter;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.db.mpp.common.schematree.SchemaTree;
import org.apache.iotdb.db.mpp.sql.rewriter.*;
import org.apache.iotdb.db.mpp.sql.statement.Statement;
import org.apache.iotdb.db.mpp.sql.statement.component.WhereCondition;
import org.apache.iotdb.db.mpp.sql.statement.crud.InsertStatement;
import org.apache.iotdb.db.mpp.sql.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.mpp.sql.statement.crud.QueryStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.mpp.sql.tree.StatementVisitor;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.Arrays;
import java.util.Map;

/** Analyze the statement and generate Analysis. */
public class Analyzer {

  private final MPPQueryContext context;

  // TODO need to use factory to decide standalone or cluster
  private final IPartitionFetcher partitionFetcher = StandalonePartitionFetcher.getInstance();
  // TODO need to use factory to decide standalone or cluster
  private final ISchemaFetcher schemaFetcher = StandaloneSchemaFetcher.getInstance();

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

        // bind metadata (remove wildcards and apply SLIMIT & SOFFSET as well)
        rewrittenStatement =
            (QueryStatement) new WildcardsRemover().rewrite(rewrittenStatement, schemaTree);

        // fetch partition information
        DataPartitionInfo dataPartitionInfo =
            partitionFetcher.fetchDataPartitionInfos(
                schemaTree.constructDataPartitionQueryParamList());

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
        analysis.setDataPartitionInfo(dataPartitionInfo);
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
      return analysis;
    }

    @Override
    public Analysis visitInsertTablet(
        InsertTabletStatement insertTabletStatement, MPPQueryContext context) {
      // TODO(INSERT) device + time range -> PartitionInfo
      DataPartitionQueryParam dataPartitionQueryParam = new DataPartitionQueryParam();
      dataPartitionQueryParam.setDeviceId(insertTabletStatement.getDevicePath().getFullPath());
      // TODO(INSERT) calculate the time partition id list
      //      dataPartitionQueryParam.setTimePartitionIdList();
      PartitionInfo partitionInfo = partitionFetcher.fetchPartitionInfo(dataPartitionQueryParam);

      // TODO(INSERT) get each time series schema according to SchemaPartitionInfo in PartitionInfo
      Map<String, MeasurementSchema> schemaMap =
          schemaFetcher.fetchSchema(
              insertTabletStatement.getDevicePath(),
              Arrays.asList(insertTabletStatement.getMeasurements()));

      Analysis analysis = new Analysis();
      analysis.setSchemaMap(schemaMap);
      // TODO(INSERT) do type check here
      analysis.setStatement(insertTabletStatement);
      analysis.setDataPartitionInfo(partitionInfo.getDataPartitionInfo());
      analysis.setSchemaPartitionInfo(partitionInfo.getSchemaPartitionInfo());
      return analysis;
    }
  }
}
