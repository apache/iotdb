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

package org.apache.iotdb.db.queryengine.execution.operator.process.copyto;

import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.execution.operator.process.copyto.tsfile.CopyToTsFileOptions;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.planner.RelationPlan;

import java.util.List;
import java.util.Set;

public interface CopyToOptions {

  void infer(Analysis analysis, RelationPlan queryRelationPlan, List<ColumnHeader> columnHeaders);

  void check(List<ColumnHeader> columnHeaders);

  List<ColumnHeader> getRespColumnHeaders();

  Format getFormat();

  long estimatedMaxRamBytesInWrite();

  enum Format {
    TSFILE,
  }

  class Builder {
    private CopyToOptions.Format format = CopyToOptions.Format.TSFILE;
    private String targetTableName = null;
    private String targetTimeColumn = null;
    private Set<String> targetTagColumns = null;
    private long memoryThreshold = 0;

    public Builder withFormat(CopyToOptions.Format format) {
      this.format = format;
      return this;
    }

    public Builder withTargetTableName(String targetTableName) {
      this.targetTableName = targetTableName;
      return this;
    }

    public Builder withTargetTimeColumn(String targetTimeColumn) {
      this.targetTimeColumn = targetTimeColumn;
      return this;
    }

    public Builder withTargetTagColumns(Set<String> targetTagColumns) {
      this.targetTagColumns = targetTagColumns;
      return this;
    }

    public Builder withMemoryThreshold(long memoryThreshold) {
      if (memoryThreshold <= 0) {
        throw new SemanticException("The memory threshold must be greater than 0.");
      }
      this.memoryThreshold = memoryThreshold;
      return this;
    }

    public CopyToOptions build() {
      switch (format) {
        case TSFILE:
        default:
          return new CopyToTsFileOptions(
              targetTableName,
              targetTimeColumn,
              targetTagColumns,
              memoryThreshold != 0 ? memoryThreshold : 32 * 1024 * 1024);
      }
    }
  }
}
