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

package org.apache.iotdb.db.queryengine.plan.relational.sql;

import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DefaultTraversalVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;

import org.apache.tsfile.utils.RamUsageEstimator;

/**
 * Utility class for estimating memory usage of AST nodes. Uses RamUsageEstimator to calculate
 * approximate memory size.
 */
public final class AstMemoryEstimator {
  private AstMemoryEstimator() {}

  /**
   * Estimate the memory size of a Statement AST node in bytes.
   *
   * @param statement the statement AST to estimate
   * @return estimated memory size in bytes
   */
  public static long estimateMemorySize(Statement statement) {
    if (statement == null) {
      return 0L;
    }
    MemoryEstimatingVisitor visitor = new MemoryEstimatingVisitor();
    visitor.process(statement, null);
    return visitor.getTotalMemorySize();
  }

  private static class MemoryEstimatingVisitor extends DefaultTraversalVisitor<Void> {
    private long totalMemorySize = 0L;

    public long getTotalMemorySize() {
      return totalMemorySize;
    }

    @Override
    protected Void visitNode(Node node, Void context) {
      // Estimate shallow size of the node object
      long nodeSize = RamUsageEstimator.shallowSizeOfInstance(node.getClass());
      totalMemorySize += nodeSize;

      // Traverse children (DefaultTraversalVisitor handles this)
      return super.visitNode(node, context);
    }
  }
}
