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
package org.apache.iotdb.spark.tsfile.qp.common;

import org.apache.iotdb.tsfile.utils.StringContainer;

import java.util.ArrayList;
import java.util.List;

/**
 * This class is for filter operator and implements {@link Operator} .<br>
 * it may consist of more than two child FilterOperator, but if it's not leaf operator, the relation
 * is same among all of its children.(AND or OR). It's identified by tokenType.
 */
public class FilterOperator extends Operator implements Comparable<FilterOperator> {

  public List<FilterOperator> childOperators;
  // leaf filter operator means it doesn't have left and right child filterOperator. Leaf filter
  // should be BasicOperator.
  protected boolean isLeaf = false;
  // All recursive children of this filter belong to one series path when isSingle variable is true
  protected boolean isSingle = false;
  // if isSingle = false, singlePath must be null
  protected String singlePath = null;

  public FilterOperator(int tokenType) {
    super(tokenType);
    childOperators = new ArrayList<>();
  }

  public void setTokenIntType(int intType) {
    super.tokenIntType = intType;
    super.tokenSymbol = SQLConstant.tokenSymbol.get(tokenIntType);
  }

  public FilterOperator(int tokenType, boolean isSingle) {
    this(tokenType);
    this.isSingle = isSingle;
  }

  public void addHeadDeltaObjectPath(String deltaObject) {
    for (FilterOperator child : childOperators) {
      child.addHeadDeltaObjectPath(deltaObject);
    }
    if (isSingle) {
      this.singlePath = deltaObject + "." + this.singlePath;
    }
  }

  public List<FilterOperator> getChildren() {
    return childOperators;
  }

  public List<String> getAllPaths() {
    List<String> paths = new ArrayList<>();
    if (isLeaf) {
      paths.add(singlePath);
    } else {
      for (FilterOperator child : childOperators) {
        paths.addAll(child.getAllPaths());
      }
    }
    return paths;
  }

  public void setChildrenList(List<FilterOperator> children) {
    this.childOperators = children;
  }

  public void setIsSingle(boolean b) {
    this.isSingle = b;
  }

  public void setSinglePath(String p) {
    this.singlePath = p;
  }

  public String getSinglePath() {
    return singlePath;
  }

  public void addChildOPerator(FilterOperator op) {
    childOperators.add(op);
  }

  @Override
  public int compareTo(FilterOperator operator) {
    if (singlePath == null && operator.singlePath == null) {
      return 0;
    }
    if (singlePath == null) {
      return 1;
    }
    if (operator.singlePath == null) {
      return -1;
    }
    return operator.singlePath.compareTo(singlePath);
  }

  public boolean isLeaf() {
    return isLeaf;
  }

  public boolean isSingle() {
    return isSingle;
  }

  @Override
  public String toString() {
    StringContainer sc = new StringContainer();
    sc.addTail("[", this.tokenSymbol);
    if (isSingle) {
      sc.addTail("[single:", getSinglePath(), "]");
    }
    sc.addTail(" ");
    for (FilterOperator filter : childOperators) {
      sc.addTail(filter.toString());
    }
    sc.addTail("]");
    return sc.toString();
  }

  @Override
  public FilterOperator clone() {
    FilterOperator ret = new FilterOperator(this.tokenIntType);
    ret.tokenSymbol = tokenSymbol;
    ret.isLeaf = isLeaf;
    ret.isSingle = isSingle;
    if (singlePath != null) {
      ret.singlePath = singlePath;
    }
    for (FilterOperator filterOperator : this.childOperators) {
      ret.addChildOPerator(filterOperator.clone());
    }
    return ret;
  }
}
