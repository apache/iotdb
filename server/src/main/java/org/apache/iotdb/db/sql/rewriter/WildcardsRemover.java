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

package org.apache.iotdb.db.sql.rewriter;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.PathNumOverLimitException;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.expression.Expression;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.sql.statement.component.ResultColumn;
import org.apache.iotdb.db.sql.statement.crud.QueryStatement;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Removes wildcards (applying memory control and slimit/soffset control) */
public class WildcardsRemover {

  private int soffset = 0;
  private int currentOffset = 0;
  private int currentLimit =
      IoTDBDescriptor.getInstance().getConfig().getMaxQueryDeduplicatedPathNum() + 1;

  /** Records the path number that the MManager totally returned. */
  private int consumed = 0;

  /**
   * Since IoTDB v0.13, all DDL and DML use patternMatch as default. Before IoTDB v0.13, all DDL and
   * DML use prefixMatch.
   */
  private final boolean isPrefixMatch;

  public WildcardsRemover(QueryStatement queryStatement) {
    isPrefixMatch = queryStatement.isPrefixMatchPath();
    soffset = queryStatement.getSeriesOffset();
    currentOffset = soffset;
    final int slimit = queryStatement.getSeriesLimit();
    currentLimit = slimit == 0 ? currentLimit : Math.min(slimit, currentLimit);
  }

  private WildcardsRemover(boolean isPrefixMatch) {
    this.isPrefixMatch = isPrefixMatch;
  }

  public List<MeasurementPath> removeWildcardFrom(PartialPath path)
      throws StatementAnalyzeException {
    try {
      Pair<List<MeasurementPath>, Integer> pair =
          IoTDB.schemaEngine.getMeasurementPathsWithAlias(
              path, currentLimit, currentOffset, isPrefixMatch);
      consumed += pair.right;
      currentOffset -= Math.min(currentOffset, pair.right);
      currentLimit -= pair.left.size();
      return pair.left;
    } catch (MetadataException e) {
      throw new StatementAnalyzeException("error occurred when removing star: " + e.getMessage());
    }
  }

  public List<List<Expression>> removeWildcardsFrom(List<Expression> expressions)
      throws StatementAnalyzeException {
    // One by one, remove the wildcards from the input expressions. In most cases, an expression
    // will produce multiple expressions after removing the wildcards. We use extendedExpressions to
    // collect the produced expressions.
    List<List<Expression>> extendedExpressions = new ArrayList<>();
    for (Expression originExpression : expressions) {
      List<Expression> actualExpressions = new ArrayList<>();
      originExpression.removeWildcards(new WildcardsRemover(isPrefixMatch), actualExpressions);
      if (actualExpressions.isEmpty()) {
        // Let's ignore the eval of the function which has at least one non-existence series as
        // input. See IOTDB-1212: https://github.com/apache/iotdb/pull/3101
        return Collections.emptyList();
      }
      extendedExpressions.add(actualExpressions);
    }

    // Calculate the Cartesian product of extendedExpressions to get the actual expressions after
    // removing all wildcards. We use actualExpressions to collect them.
    List<List<Expression>> actualExpressions = new ArrayList<>();
    ConcatPathRewriter.cartesianProduct(
        extendedExpressions, actualExpressions, 0, new ArrayList<>());

    // Apply the soffset & slimit control to the actualExpressions and return the remaining
    // expressions.
    List<List<Expression>> remainingExpressions = new ArrayList<>();
    for (List<Expression> actualExpression : actualExpressions) {
      if (currentOffset != 0) {
        --currentOffset;
        continue;
      } else if (currentLimit != 0) {
        --currentLimit;
      } else {
        break;
      }
      remainingExpressions.add(actualExpression);
    }
    consumed += actualExpressions.size();
    return remainingExpressions;
  }

  /** @return should break the loop or not */
  public boolean checkIfPathNumberIsOverLimit(List<ResultColumn> resultColumns)
      throws PathNumOverLimitException {
    if (resultColumns.size()
        > IoTDBDescriptor.getInstance().getConfig().getMaxQueryDeduplicatedPathNum()) {
      throw new PathNumOverLimitException();
    }
    return currentLimit == 0;
  }

  public void checkIfSoffsetIsExceeded(List<ResultColumn> resultColumns)
      throws StatementAnalyzeException {
    if (consumed == 0 ? soffset != 0 : resultColumns.isEmpty()) {
      throw new StatementAnalyzeException(
          String.format(
              "The value of SOFFSET (%d) is equal to or exceeds the number of sequences (%d) that can actually be returned.",
              soffset, consumed));
    }
  }
}
