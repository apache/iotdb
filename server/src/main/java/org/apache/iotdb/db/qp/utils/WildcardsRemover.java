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

package org.apache.iotdb.db.qp.utils;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.exception.query.PathNumOverLimitException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.crud.LastQueryOperator;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.strategy.optimizer.ConcatPathOptimizer;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.ResultColumn;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class WildcardsRemover {

  private final ConcatPathOptimizer concatPathOptimizer;

  private final int maxDeduplicatedPathNum;
  private int soffset = 0;

  private int offset = 0;
  private int limit = Integer.MAX_VALUE;
  private int consumed = 0;

  public WildcardsRemover(
      ConcatPathOptimizer concatPathOptimizer, QueryOperator queryOperator, int fetchSize) {
    this.concatPathOptimizer = concatPathOptimizer;

    // Dataset of last query actually has only three columns, so we shouldn't limit the path num
    // while constructing logical plan
    // To avoid overflowing because logicalOptimize function may do maxDeduplicatedPathNum + 1, we
    // set it to Integer.MAX_VALUE - 1
    maxDeduplicatedPathNum =
        queryOperator instanceof LastQueryOperator
            ? Integer.MAX_VALUE - 1
            : QueryResourceManager.getInstance().getMaxDeduplicatedPathNum(fetchSize);
    if (queryOperator.getSpecialClauseComponent() != null) {
      soffset = queryOperator.getSpecialClauseComponent().getSeriesOffset();
      offset = soffset;

      final int slimit = queryOperator.getSpecialClauseComponent().getSeriesLimit();
      limit = slimit == 0 || maxDeduplicatedPathNum < slimit ? maxDeduplicatedPathNum + 1 : slimit;
    }
  }

  public WildcardsRemover(ConcatPathOptimizer concatPathOptimizer) {
    this.concatPathOptimizer = concatPathOptimizer;
    maxDeduplicatedPathNum = Integer.MAX_VALUE - 1;
  }

  public List<PartialPath> removeWildcardFrom(PartialPath path) throws LogicalOptimizeException {
    try {
      Pair<List<PartialPath>, Integer> pair =
          concatPathOptimizer.removeWildcard(path, limit, offset);

      consumed += pair.right;
      if (offset != 0) {
        int delta = offset - pair.right;
        offset = Math.max(delta, 0);
        if (delta < 0) {
          limit += delta;
        }
      } else {
        limit -= pair.right;
      }

      return pair.left;
    } catch (MetadataException e) {
      throw new LogicalOptimizeException("error occurred when removing star: " + e.getMessage());
    }
  }

  public List<List<Expression>> removeWildcardsFrom(List<Expression> expressions)
      throws LogicalOptimizeException {
    List<List<Expression>> extendedExpressions = new ArrayList<>();

    boolean atLeastOneSeriesNotExisted = false;
    for (Expression originExpression : expressions) {
      List<Expression> actualExpressions = new ArrayList<>();
      originExpression.removeWildcards(
          new WildcardsRemover(concatPathOptimizer), actualExpressions);
      if (actualExpressions.isEmpty()) {
        atLeastOneSeriesNotExisted = true;
        break;
      }
      extendedExpressions.add(actualExpressions);
    }
    if (atLeastOneSeriesNotExisted) {
      return Collections.emptyList();
    }

    List<List<Expression>> actualExpressions = new ArrayList<>();
    ConcatPathOptimizer.cartesianProduct(
        extendedExpressions, actualExpressions, 0, new ArrayList<>());

    List<List<Expression>> splitExpressions = new ArrayList<>();
    for (List<Expression> actualExpression : actualExpressions) {
      if (offset != 0) {
        --offset;
        continue;
      } else if (limit != 0) {
        --limit;
      } else {
        break;
      }
      splitExpressions.add(actualExpression);
    }
    consumed += actualExpressions.size();
    return splitExpressions;
  }

  /** @return should break the loop or not */
  public boolean checkIfPathNumberIsOverLimit(List<ResultColumn> resultColumns)
      throws PathNumOverLimitException {
    if (limit == 0) {
      if (maxDeduplicatedPathNum < resultColumns.size()) {
        throw new PathNumOverLimitException(maxDeduplicatedPathNum);
      }
      return true;
    }
    return false;
  }

  public void checkIfSoffsetIsExceeded(List<ResultColumn> resultColumns)
      throws LogicalOptimizeException {
    if (consumed == 0 ? soffset != 0 : resultColumns.isEmpty()) {
      throw new LogicalOptimizeException(
          String.format(
              "The value of SOFFSET (%d) is equal to or exceeds the number of sequences (%d) that can actually be returned.",
              soffset, consumed));
    }
  }
}
