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

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.exception.runtime.SQLParserException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.crud.*;
import org.apache.iotdb.db.service.IoTDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * concat paths in select and from clause.
 */
public class ConcatPathOptimizer implements ILogicalOptimizer {

  private static final Logger logger = LoggerFactory.getLogger(ConcatPathOptimizer.class);
  private static final String WARNING_NO_SUFFIX_PATHS = "given SFWOperator doesn't have suffix paths, cannot concat seriesPath";
  private static final String WARNING_NO_PREFIX_PATHS = "given SFWOperator doesn't have prefix paths, cannot concat seriesPath";


  @Override
  public Operator transform(Operator operator) throws LogicalOptimizeException {
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
      if (!((QueryOperator) operator).isAlignByDevice() || ((QueryOperator) operator).isLastQuery()) {
        concatSelect(prefixPaths, select); // concat and remove star

        if (((QueryOperator) operator).hasSlimit()) {
          int seriesLimit = ((QueryOperator) operator).getSeriesLimit();
          int seriesOffset = ((QueryOperator) operator).getSeriesOffset();
          slimitTrim(select, seriesLimit, seriesOffset);
        }
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
    if(!isAlignByDevice){
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
  private void concatSelect(List<PartialPath> fromPaths, SelectOperator selectOperator)
      throws LogicalOptimizeException {
    List<PartialPath> suffixPaths = judgeSelectOperator(selectOperator);

    List<PartialPath> allPaths = new ArrayList<>();
    List<String> originAggregations = selectOperator.getAggregations();
    List<String> afterConcatAggregations = new ArrayList<>();

    for (int i = 0; i < suffixPaths.size(); i++) {
      // selectPath cannot start with ROOT, which is guaranteed by TSParser
      PartialPath selectPath = suffixPaths.get(i);
      for (PartialPath fromPath : fromPaths) {
        allPaths.add(fromPath.concatPath(selectPath));
        extendListSafely(originAggregations, i, afterConcatAggregations);
      }
    }

    removeStarsInPath(allPaths, afterConcatAggregations, selectOperator);
  }

  /**
   * Make 'SLIMIT&SOFFSET' take effect by trimming the suffixList and aggregations of the
   * selectOperator.
   *
   * @param seriesLimit is ensured to be positive integer
   * @param seriesOffset is ensured to be non-negative integer
   */
  private void slimitTrim(SelectOperator select, int seriesLimit, int seriesOffset)
      throws LogicalOptimizeException {
    List<PartialPath> suffixList = select.getSuffixPaths();
    List<String> aggregations = select.getAggregations();
    int size = suffixList.size();

    // check parameter range
    if (seriesOffset >= size) {
      throw new LogicalOptimizeException("SOFFSET <SOFFSETValue>: SOFFSETValue exceeds the range.");
    }
    int endPosition = seriesOffset + seriesLimit;
    if (endPosition > size) {
      endPosition = size;
    }

    // trim seriesPath list
    List<PartialPath> trimedSuffixList = new ArrayList<>(suffixList.subList(seriesOffset, endPosition));
    select.setSuffixPathList(trimedSuffixList);

    // trim aggregations if exists
    if (aggregations != null && !aggregations.isEmpty()) {
      List<String> trimedAggregations = new ArrayList<>(
          aggregations.subList(seriesOffset, endPosition));
      select.setAggregations(trimedAggregations);
    }
  }

  private FilterOperator concatFilter(List<PartialPath> fromPaths, FilterOperator operator,
      Set<PartialPath> filterPaths)
      throws LogicalOptimizeException {
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
    if (SQLConstant.isReservedPath(filterPath) || filterPath.getFirstNode().startsWith(SQLConstant.ROOT)) {
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
      FilterOperator operator)
      throws LogicalOptimizeException {
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
  private List<PartialPath> removeStarsInPathWithUnique(List<PartialPath> paths) throws LogicalOptimizeException {
    List<PartialPath> retPaths = new ArrayList<>();
    HashSet<PartialPath> pathSet = new HashSet<>();
    try {
      for (PartialPath path : paths) {
        List<PartialPath> all = removeWildcard(path);
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

  private void removeStarsInPath(List<PartialPath> paths, List<String> afterConcatAggregations,
      SelectOperator selectOperator) throws LogicalOptimizeException {
    List<PartialPath> retPaths = new ArrayList<>();
    List<String> newAggregations = new ArrayList<>();
    for (int i = 0; i < paths.size(); i++) {
      try {
        List<PartialPath> actualPaths = removeWildcard(paths.get(i));
        for (PartialPath actualPath : actualPaths) {
          retPaths.add(actualPath);
          if (afterConcatAggregations != null && !afterConcatAggregations.isEmpty()) {
            newAggregations.add(afterConcatAggregations.get(i));
          }
        }
      } catch (MetadataException e) {
        throw new LogicalOptimizeException("error when remove star: " + e.getMessage());
      }
    }
    selectOperator.setSuffixPathList(retPaths);
    selectOperator.setAggregations(newAggregations);
  }

  protected List<PartialPath> removeWildcard(PartialPath path) throws MetadataException {
    return IoTDB.metaManager.getAllTimeseriesPathWithAlias(path);
  }
}
