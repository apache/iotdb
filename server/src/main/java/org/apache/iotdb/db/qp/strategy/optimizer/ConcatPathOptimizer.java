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
package org.apache.iotdb.db.qp.strategy.optimizer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.exception.query.PathNumOverLimitException;
import org.apache.iotdb.db.exception.runtime.SQLParserException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.crud.BasicFunctionOperator;
import org.apache.iotdb.db.qp.logical.crud.FilterOperator;
import org.apache.iotdb.db.qp.logical.crud.FromOperator;
import org.apache.iotdb.db.qp.logical.crud.FunctionOperator;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.logical.crud.SFWOperator;
import org.apache.iotdb.db.qp.logical.crud.SelectOperator;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * concat paths in select and from clause.
 */
public class ConcatPathOptimizer implements ILogicalOptimizer {

  private static final Logger logger = LoggerFactory.getLogger(ConcatPathOptimizer.class);
  private static final String WARNING_NO_SUFFIX_PATHS = "given SFWOperator doesn't have suffix paths, cannot concat seriesPath";
  private static final String WARNING_NO_PREFIX_PATHS = "given SFWOperator doesn't have prefix paths, cannot concat seriesPath";

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Override
  public Operator transform(Operator operator)
      throws LogicalOptimizeException, PathNumOverLimitException {
    if (!(operator instanceof SFWOperator)) {
      logger.warn("given operator isn't SFWOperator, cannot concat seriesPath");
      return operator;
    }
    SFWOperator sfwOperator = (SFWOperator) operator;
    FromOperator from = sfwOperator.getFromOperator();
    List<PartialPath> prefixPaths;
    if (from == null) {
      logger.warn(WARNING_NO_PREFIX_PATHS);
      return operator;
    } else {
      prefixPaths = from.getPrefixPaths();
      if (prefixPaths.isEmpty()) {
        logger.warn(WARNING_NO_PREFIX_PATHS);
        return operator;
      }
    }
    SelectOperator select = sfwOperator.getSelectOperator();
    List<PartialPath> initialSuffixPaths;
    if (select == null) {
      logger.warn(WARNING_NO_SUFFIX_PATHS);
      return operator;
    } else {
      initialSuffixPaths = select.getSuffixPaths();
      if (initialSuffixPaths.isEmpty()) {
        logger.warn(WARNING_NO_SUFFIX_PATHS);
        return operator;
      }
    }

    checkAggrOfSelectOperator(select);

    boolean isAlignByDevice = false;
    if (operator instanceof QueryOperator) {
      if (!((QueryOperator) operator).isAlignByDevice()
          || ((QueryOperator) operator).isLastQuery()) {
        // concat paths and remove stars
        int seriesLimit = ((QueryOperator) operator).getSeriesLimit();
        int seriesOffset = ((QueryOperator) operator).getSeriesOffset();
        concatSelect(prefixPaths, select, seriesLimit, seriesOffset);
      } else {
        isAlignByDevice = true;
        for (PartialPath path : initialSuffixPaths) {
          String device = path.getDevice();
          if (!device.isEmpty()) {
            throw new LogicalOptimizeException(
                "The paths of the SELECT clause can only be single level. In other words, "
                    + "the paths of the SELECT clause can only be measurements or STAR, without DOT."
                    + " For more details please refer to the SQL document.");
          }
        }
        // ALIGN_BY_DEVICE leaves the 1) concat, 2) remove star, 3) slimit tasks to the next phase,
        // i.e., PhysicalGenerator.transformQuery
      }
    }

    // concat filter
    FilterOperator filter = sfwOperator.getFilterOperator();
    Set<PartialPath> filterPaths = new HashSet<>();
    if (filter == null) {
      return operator;
    }
    if (!isAlignByDevice) {
      sfwOperator.setFilterOperator(concatFilter(prefixPaths, filter, filterPaths));
    }
    sfwOperator.getFilterOperator().setPathSet(filterPaths);
    // GROUP_BY_DEVICE leaves the concatFilter to PhysicalGenerator to optimize filter without prefix first

    return sfwOperator;
  }

  private List<PartialPath> judgeSelectOperator(SelectOperator selectOperator)
      throws LogicalOptimizeException {
    List<PartialPath> suffixPaths;
    if (selectOperator == null) {
      throw new LogicalOptimizeException(WARNING_NO_SUFFIX_PATHS);
    } else {
      suffixPaths = selectOperator.getSuffixPaths();
      if (suffixPaths.isEmpty()) {
        throw new LogicalOptimizeException(WARNING_NO_SUFFIX_PATHS);
      }
    }
    return suffixPaths;
  }

  private void checkAggrOfSelectOperator(SelectOperator selectOperator)
      throws LogicalOptimizeException {
    if (!selectOperator.getAggregations().isEmpty()
        && selectOperator.getSuffixPaths().size() != selectOperator.getAggregations().size()) {
      throw new LogicalOptimizeException(
          "Common queries and aggregated queries are not allowed to appear at the same time");
    }
  }

  private void extendListSafely(List<String> source, int index, List<String> target) {
    if (source != null && !source.isEmpty()) {
      target.add(source.get(index));
    }
  }

  /**
   * Extract paths from select&from cql, expand them into complete versions, and reassign them to
   * selectOperator's suffixPathList. Treat aggregations similarly.
   */
  private void concatSelect(List<PartialPath> fromPaths, SelectOperator selectOperator, int limit,
      int offset)
      throws LogicalOptimizeException, PathNumOverLimitException {
    List<PartialPath> suffixPaths = judgeSelectOperator(selectOperator);

    List<PartialPath> allPaths = new ArrayList<>();
    List<String> originAggregations = selectOperator.getAggregations();
    List<String> afterConcatAggregations = new ArrayList<>();

    for (int i = 0; i < suffixPaths.size(); i++) {
      // selectPath cannot start with ROOT, which is guaranteed by TSParser
      PartialPath selectPath = suffixPaths.get(i);
      for (PartialPath fromPath : fromPaths) {
        PartialPath fullPath = fromPath.concatPath(selectPath);
        if (selectPath.getTsAlias() != null) {
          fullPath.setTsAlias(selectPath.getTsAlias());
        }
        allPaths.add(fullPath);
        extendListSafely(originAggregations, i, afterConcatAggregations);
      }
    }

    removeStarsInPath(allPaths, afterConcatAggregations, selectOperator, limit, offset);
  }

  private FilterOperator concatFilter(List<PartialPath> fromPaths, FilterOperator operator,
      Set<PartialPath> filterPaths) throws LogicalOptimizeException {
    if (!operator.isLeaf()) {
      List<FilterOperator> newFilterList = new ArrayList<>();
      for (FilterOperator child : operator.getChildren()) {
        newFilterList.add(concatFilter(fromPaths, child, filterPaths));
      }
      operator.setChildren(newFilterList);
      return operator;
    }
    FunctionOperator functionOperator = (FunctionOperator) operator;
    PartialPath filterPath = functionOperator.getSinglePath();
    // do nothing in the cases of "where time > 5" or "where root.d1.s1 > 5"
    if (SQLConstant.isReservedPath(filterPath) || filterPath.getFirstNode()
        .startsWith(SQLConstant.ROOT)) {
      filterPaths.add(filterPath);
      return operator;
    }
    List<PartialPath> concatPaths = new ArrayList<>();
    fromPaths.forEach(fromPath -> concatPaths.add(fromPath.concatPath(filterPath)));
    List<PartialPath> noStarPaths = removeStarsInPathWithUnique(concatPaths);
    filterPaths.addAll(noStarPaths);
    if (noStarPaths.size() == 1) {
      // Transform "select s1 from root.car.* where s1 > 10" to
      // "select s1 from root.car.* where root.car.*.s1 > 10"
      functionOperator.setSinglePath(noStarPaths.get(0));
      return operator;
    } else {
      // Transform "select s1 from root.car.d1, root.car.d2 where s1 > 10" to
      // "select s1 from root.car.d1, root.car.d2 where root.car.d1.s1 > 10 and root.car.d2.s1 > 10"
      // Note that,
      // two fork tree has to be maintained while removing stars in paths for DnfFilterOptimizer
      // requirement.
      return constructBinaryFilterTreeWithAnd(noStarPaths, operator);
    }
  }

  private FilterOperator constructBinaryFilterTreeWithAnd(List<PartialPath> noStarPaths,
      FilterOperator operator) throws LogicalOptimizeException {
    FilterOperator filterBinaryTree = new FilterOperator(SQLConstant.KW_AND);
    FilterOperator currentNode = filterBinaryTree;
    for (int i = 0; i < noStarPaths.size(); i++) {
      if (i > 0 && i < noStarPaths.size() - 1) {
        FilterOperator newInnerNode = new FilterOperator(SQLConstant.KW_AND);
        currentNode.addChildOperator(newInnerNode);
        currentNode = newInnerNode;
      }
      try {
        currentNode.addChildOperator(
            new BasicFunctionOperator(operator.getTokenIntType(), noStarPaths.get(i),
                ((BasicFunctionOperator) operator).getValue()));
      } catch (SQLParserException e) {
        throw new LogicalOptimizeException(e.getMessage());
      }
    }
    return filterBinaryTree;
  }

  /**
   * replace "*" by actual paths.
   *
   * @param paths list of paths which may contain stars
   * @return a unique seriesPath list
   */
  private List<PartialPath> removeStarsInPathWithUnique(List<PartialPath> paths)
      throws LogicalOptimizeException {
    List<PartialPath> retPaths = new ArrayList<>();
    HashSet<PartialPath> pathSet = new HashSet<>();
    try {
      for (PartialPath path : paths) {
        List<PartialPath> all = removeWildcard(path, 0, 0).left;
        if (all.size() == 0) {
          throw new LogicalOptimizeException(
              String.format("Unknown time series %s in `where clause`", path));
        }
        for (PartialPath subPath : all) {
          if (!pathSet.contains(subPath)) {
            pathSet.add(subPath);
            retPaths.add(subPath);
          }
        }
      }
    } catch (MetadataException e) {
      throw new LogicalOptimizeException("error when remove star: " + e.getMessage());
    }
    return retPaths;
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void removeStarsInPath(List<PartialPath> paths, List<String> afterConcatAggregations,
      SelectOperator selectOperator, int finalLimit, int finalOffset)
      throws LogicalOptimizeException, PathNumOverLimitException {
    int offset = finalOffset;
    int limit = finalLimit == 0 ? Integer.MAX_VALUE : finalLimit;
    int consumed = 0;
    List<PartialPath> retPaths = new ArrayList<>();
    List<String> newAggregations = new ArrayList<>();

    for (int i = 0; i < paths.size(); i++) {
      try {
        Pair<List<PartialPath>, Integer> pair = removeWildcard(paths.get(i), limit, offset);

        List<PartialPath> actualPaths = pair.left;
        if (paths.get(i).getTsAlias() != null) {
          if (actualPaths.size() == 1) {
            actualPaths.get(0).setTsAlias(paths.get(i).getTsAlias());
          } else if (actualPaths.size() >= 2) {
            throw new LogicalOptimizeException(
                "alias '" + paths.get(i).getTsAlias()
                    + "' can only be matched with one time series");
          }
        }
        for (PartialPath actualPath : actualPaths) {
          retPaths.add(actualPath);
          extendListSafely(afterConcatAggregations, i, newAggregations);
        }

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
        if (limit == 0) {
          int maxDeduplicatedPathNum =
              IoTDBDescriptor.getInstance().getConfig().getMaxQueryDeduplicatedPathNum();
          if (maxDeduplicatedPathNum < retPaths.size()) {
            throw new PathNumOverLimitException(maxDeduplicatedPathNum);
          }
          break;
        }
      } catch (MetadataException e) {
        throw new LogicalOptimizeException("error when remove star: " + e.getMessage());
      }
    }

    if (consumed == 0 ? finalOffset != 0 : retPaths.isEmpty()) {
      throw new LogicalOptimizeException(String.format(
          "The value of SOFFSET (%d) is equal to or exceeds the number of sequences (%d) that can actually be returned.",
          finalOffset, consumed));
    }
    selectOperator.setSuffixPathList(retPaths);
    selectOperator.setAggregations(newAggregations);
  }

  protected Pair<List<PartialPath>, Integer> removeWildcard(PartialPath path, int limit, int offset)
      throws MetadataException {
    return IoTDB.metaManager.getAllTimeseriesPathWithAlias(path, limit, offset);
  }
}
