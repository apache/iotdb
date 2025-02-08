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

package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControl;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;

import static java.util.Objects.requireNonNull;

public class StatementAnalyzerFactory {

  private final Metadata metadata;
  private final SqlParser sqlParser;
  private final AccessControl accessControl;

  public StatementAnalyzerFactory(
      final Metadata metadata, final SqlParser sqlParser, final AccessControl accessControl) {
    this.metadata = requireNonNull(metadata, "plannerContext is null");
    this.sqlParser = sqlParser;
    this.accessControl = requireNonNull(accessControl, "accessControl is null");
  }

  public StatementAnalyzerFactory withSpecializedAccessControl(AccessControl accessControl) {
    return new StatementAnalyzerFactory(metadata, sqlParser, accessControl);
  }

  public StatementAnalyzer createStatementAnalyzer(
      final Analysis analysis,
      final MPPQueryContext context,
      final SessionInfo session,
      final WarningCollector warningCollector,
      final CorrelationSupport correlationSupport) {
    return new StatementAnalyzer(
        this,
        analysis,
        context,
        accessControl,
        warningCollector,
        session,
        metadata,
        correlationSupport);
  }

  public static StatementAnalyzerFactory createTestingStatementAnalyzerFactory(
      Metadata metadata, AccessControl accessControl) {
    return new StatementAnalyzerFactory(metadata, new SqlParser(), accessControl);
  }
}
