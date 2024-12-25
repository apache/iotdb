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

package org.apache.iotdb.db.queryengine.plan.relational.sql.rewrite;

import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.NodeRef;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.StatementAnalyzerFactory;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Parameter;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;

import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public final class StatementRewrite {
  private final Set<Rewrite> rewrites;

  public StatementRewrite(Set<Rewrite> rewrites) {
    this.rewrites = ImmutableSet.copyOf(requireNonNull(rewrites, "rewrites is null"));
  }

  public Statement rewrite(
      StatementAnalyzerFactory analyzerFactory,
      SessionInfo session,
      Statement node,
      List<Expression> parameters,
      Map<NodeRef<Parameter>, Expression> parameterLookup,
      WarningCollector warningCollector) {
    for (Rewrite rewrite : rewrites) {
      node =
          requireNonNull(
              rewrite.rewrite(
                  analyzerFactory, session, node, parameters, parameterLookup, warningCollector),
              "Statement rewrite returned null");
    }
    return node;
  }

  public interface Rewrite {
    Statement rewrite(
        StatementAnalyzerFactory analyzerFactory,
        SessionInfo session,
        Statement node,
        List<Expression> parameters,
        Map<NodeRef<Parameter>, Expression> parameterLookup,
        WarningCollector warningCollector);
  }

  public static final StatementRewrite NOOP = new StatementRewrite(ImmutableSet.of());
}
