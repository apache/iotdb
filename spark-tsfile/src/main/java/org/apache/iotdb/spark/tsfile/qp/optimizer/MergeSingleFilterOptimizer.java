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
package org.apache.iotdb.spark.tsfile.qp.optimizer;

import org.apache.iotdb.spark.tsfile.qp.common.BasicOperator;
import org.apache.iotdb.spark.tsfile.qp.common.FilterOperator;
import org.apache.iotdb.spark.tsfile.qp.exception.MergeFilterException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MergeSingleFilterOptimizer implements IFilterOptimizer {

  @Override
  public FilterOperator optimize(FilterOperator filter) throws MergeFilterException {
    mergeSamePathFilter(filter);

    return filter;
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private String mergeSamePathFilter(FilterOperator filter) throws MergeFilterException {
    if (filter.isLeaf()) {
      return filter.getSinglePath();
    }
    List<FilterOperator> children = filter.getChildren();
    if (children.isEmpty()) {
      throw new MergeFilterException("this inner filter has no children!");
    }
    if (children.size() == 1) {
      throw new MergeFilterException("this inner filter has just one child!");
    }
    String childPath = mergeSamePathFilter(children.get(0));
    String tempPath;
    for (int i = 1; i < children.size(); i++) {
      tempPath = mergeSamePathFilter(children.get(i));
      // if one of children differs from others or is not single node(path = null), filter's path
      // is null
      if (tempPath == null || !tempPath.equals(childPath)) {
        childPath = null;
      }
    }
    if (childPath != null) {
      filter.setIsSingle(true);
      filter.setSinglePath(childPath);
      return childPath;
    }

    // make same paths close
    Collections.sort(children);
    List<FilterOperator> ret = new ArrayList<>();

    List<FilterOperator> tempExtrNode = null;
    int i;
    for (i = 0; i < children.size(); i++) {
      tempPath = children.get(i).getSinglePath();
      // sorted by path, all "null" paths are in the end
      if (tempPath == null) {
        break;
      }
      if (childPath == null) {
        // first child to be added
        childPath = tempPath;
        tempExtrNode = new ArrayList<>();
        tempExtrNode.add(children.get(i));
      } else if (childPath.equals(tempPath)) {
        // successive next single child with same path,merge it with previous children
        tempExtrNode.add(children.get(i));
      } else {
        // not more same, add exist nodes in tempExtrNode into a new node
        // prevent make a node which has only one child.
        if (tempExtrNode.size() == 1) {
          ret.add(tempExtrNode.get(0));
          // use exist Object directly for efficiency
          tempExtrNode.set(0, children.get(i));
          childPath = tempPath;
        } else {
          // add a new inner node
          FilterOperator newFilter = new FilterOperator(filter.getTokenIntType(), true);
          newFilter.setSinglePath(childPath);
          newFilter.setChildrenList(tempExtrNode);
          ret.add(newFilter);
          tempExtrNode = new ArrayList<>();
          tempExtrNode.add(children.get(i));
          childPath = tempPath;
        }
      }
    }
    // the last several children before "not single paths" has not been added to ret list.
    if (childPath != null) {
      if (tempExtrNode.size() == 1) {
        ret.add(tempExtrNode.get(0));
      } else {
        // add a new inner node
        FilterOperator newFil = new FilterOperator(filter.getTokenIntType(), true);
        newFil.setSinglePath(childPath);
        newFil.setChildrenList(tempExtrNode);
        ret.add(newFil);
      }
    }
    // add last null children
    for (; i < children.size(); i++) {
      ret.add(children.get(i));
    }
    if (ret.size() == 1) {
      // all children have same path, which means this filter node is a single node
      filter.setIsSingle(true);
      filter.setSinglePath(childPath);
      filter.setChildrenList(ret.get(0).getChildren());
      return childPath;
    } else {
      filter.setIsSingle(false);
      filter.setChildrenList(ret);
      return null;
    }
  }

  private boolean allIsBasic(List<FilterOperator> children) {
    for (FilterOperator child : children) {
      if (!(child instanceof BasicOperator)) {
        return false;
      }
    }
    return true;
  }
}
