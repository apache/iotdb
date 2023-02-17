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

package org.apache.iotdb.db.mpp.plan.analyze;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.plan.statement.Statement;

import static org.apache.iotdb.db.mpp.common.QueryId.mockQueryId;

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
    return new AnalyzeVisitor(partitionFetcher, schemaFetcher).process(statement, context);
  }

  public static void validate(Statement statement) {
    MPPQueryContext context = new MPPQueryContext(mockQueryId);

    IPartitionFetcher partitionFetcher;
    ISchemaFetcher schemaFetcher;
    if (IoTDBDescriptor.getInstance().getConfig().isClusterMode()) {
      partitionFetcher = ClusterPartitionFetcher.getInstance();
      schemaFetcher = ClusterSchemaFetcher.getInstance();
    } else {
      partitionFetcher = StandalonePartitionFetcher.getInstance();
      schemaFetcher = StandaloneSchemaFetcher.getInstance();
    }

    Analyzer analyzer = new Analyzer(context, partitionFetcher, schemaFetcher);
    analyzer.analyze(statement);
  }
}
