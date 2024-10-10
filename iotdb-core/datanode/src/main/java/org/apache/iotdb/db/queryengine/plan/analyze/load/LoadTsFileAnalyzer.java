/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.analyze.load;

import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.rpc.RpcUtils;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class LoadTsFileAnalyzer implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTsFileAnalyzer.class);

  // These are only used when constructed from tree model SQL
  private final LoadTsFileStatement loadTsFileStatement;

  // These are only used when constructed from table model SQL
  private final LoadTsFile loadTsFileTableStatement;
  protected final Metadata metadata;

  private final boolean isTableModelStatement;

  final MPPQueryContext context;

  final IPartitionFetcher partitionFetcher;
  final ISchemaFetcher schemaFetcher;

  LoadTsFileAnalyzer(
      LoadTsFileStatement loadTsFileStatement,
      MPPQueryContext context,
      IPartitionFetcher partitionFetcher,
      ISchemaFetcher schemaFetcher) {
    this.loadTsFileStatement = loadTsFileStatement;

    this.loadTsFileTableStatement = null;
    this.metadata = null;

    this.isTableModelStatement = false;

    this.context = context;

    this.partitionFetcher = partitionFetcher;
    this.schemaFetcher = schemaFetcher;
  }

  LoadTsFileAnalyzer(
      LoadTsFile loadTsFileTableStatement, Metadata metadata, MPPQueryContext context) {
    this.loadTsFileStatement = null;

    this.loadTsFileTableStatement = loadTsFileTableStatement;
    this.metadata = metadata;

    this.isTableModelStatement = true;

    this.context = context;

    this.partitionFetcher = ClusterPartitionFetcher.getInstance();
    this.schemaFetcher = ClusterSchemaFetcher.getInstance();
  }

  public abstract IAnalysis analyzeFileByFile(IAnalysis analysis);

  protected abstract void analyzeSingleTsFile(final File tsFile) throws IOException, AuthException;

  protected List<File> getTsFiles() {
    if (isTableModelStatement) {
      return loadTsFileTableStatement.getTsFiles();
    } else {
      return loadTsFileStatement.getTsFiles();
    }
  }

  protected String getStatementString() {
    if (isTableModelStatement) {
      return loadTsFileTableStatement.toString();
    } else {
      return loadTsFileStatement.toString();
    }
  }

  protected void setRealStatement(IAnalysis analysis) {
    if (isTableModelStatement) {
      // Do nothing by now.
    } else {
      analysis.setRealStatement(loadTsFileStatement);
    }
  }

  protected void addTsFileResource(TsFileResource tsFileResource) {
    if (isTableModelStatement) {
      loadTsFileTableStatement.addTsFileResource(tsFileResource);
    } else {
      loadTsFileStatement.addTsFileResource(tsFileResource);
    }
  }

  protected void addWritePointCount(long writePointCount) {
    if (isTableModelStatement) {
      loadTsFileTableStatement.addWritePointCount(writePointCount);
    } else {
      loadTsFileStatement.addWritePointCount(writePointCount);
    }
  }

  protected boolean isVerifySchema() {
    if (isTableModelStatement) {
      return true;
    } else {
      return loadTsFileStatement.isVerifySchema();
    }
  }

  protected boolean isDeleteAfterLoad() {
    if (isTableModelStatement) {
      return loadTsFileTableStatement.isDeleteAfterLoad();
    } else {
      return loadTsFileStatement.isDeleteAfterLoad();
    }
  }

  protected boolean isAutoCreateDatabase() {
    if (isTableModelStatement) {
      return loadTsFileTableStatement.isAutoCreateDatabase();
    } else {
      return loadTsFileStatement.isAutoCreateDatabase();
    }
  }

  protected int getDatabaseLevel() {
    if (isTableModelStatement) {
      return loadTsFileTableStatement.getDatabaseLevel();
    } else {
      return loadTsFileStatement.getDatabaseLevel();
    }
  }

  protected @Nullable String getDatabase() {
    if (isTableModelStatement) {
      return loadTsFileTableStatement.getDatabase();
    } else {
      return loadTsFileStatement.getDatabase();
    }
  }

  protected long getWritePointCount(
      Map<IDeviceID, List<TimeseriesMetadata>> device2TimeseriesMetadata) {
    return device2TimeseriesMetadata.values().stream()
        .flatMap(List::stream)
        .mapToLong(t -> t.getStatistics().getCount())
        .sum();
  }

  protected void setFailAnalysisForAuthException(IAnalysis analysis, AuthException e) {
    analysis.setFinishQueryAfterAnalyze(true);
    analysis.setFailStatus(RpcUtils.getStatus(e.getCode(), e.getMessage()));
  }
}
