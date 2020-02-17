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
package org.apache.iotdb.tsfile.read.query.timegenerator.node;

import java.io.IOException;
import org.apache.iotdb.tsfile.read.common.TimeColumn;

public class AndNode implements Node {

  private Node leftChild;
  private Node rightChild;

  private TimeColumn cachedValue;
  private boolean hasCachedValue;


  private TimeColumn leftTimeColumn;
  private TimeColumn rightTimeColumn;

  /**
   * Constructor of AndNode.
   *
   * @param leftChild  left child
   * @param rightChild right child
   */
  public AndNode(Node leftChild, Node rightChild) {
    this.leftChild = leftChild;
    this.rightChild = rightChild;
    this.hasCachedValue = false;
  }

  @Override
  public boolean hasNextTimeColumn() throws IOException {
    if (hasCachedValue) {
      return true;
    }
    cachedValue = new TimeColumn(1000);
    //fill data
    fillLeftData();
    fillRightData();
    /*
     *  [1,2,3,4,5]   <-   that was stopBatchTime mean
     *  [1,2,3,4,5,6]
     */
    long stopBatchTime = getStopBatchTime();

    while (leftTimeColumn.hasMoreData() && rightTimeColumn.hasMoreData()) {
      long leftValue = leftTimeColumn.currentTime();
      long rightValue = rightTimeColumn.currentTime();
      if (leftValue == rightValue) {
        this.hasCachedValue = true;
        this.cachedValue.add(leftValue);
        leftTimeColumn.next();
        rightTimeColumn.next();
      } else if (leftValue > rightValue) {
        rightTimeColumn.next();
      } else { // leftValue < rightValue
        leftTimeColumn.next();
      }

      if (leftValue > stopBatchTime && rightValue > stopBatchTime) {
        if (hasCachedValue) {
          break;
        }
      }
      /*
       *  [1,2,3,4,5]   <-   reFill data and cal stopBatchTime
       *             [6,7,8,9,10,11]
       */
      fillLeftData();
      fillRightData();
      stopBatchTime = getStopBatchTime();
    }
    return hasCachedValue;
  }

  private long getStopBatchTime() {
    long rMax = Long.MAX_VALUE;
    long lMax = Long.MAX_VALUE;
    if (leftTimeColumn.hasMoreData()) {
      lMax = leftTimeColumn.getLastTime();
    }
    if (rightTimeColumn.hasMoreData()) {
      rMax = rightTimeColumn.getLastTime();
    }
    return rMax > lMax ? lMax : rMax;
  }

  private void fillRightData() throws IOException {
    if (hasMoreData(rightTimeColumn, rightChild)) {
      rightTimeColumn = rightChild.nextTimeColumn();
    }
  }

  private void fillLeftData() throws IOException {
    if (hasMoreData(leftTimeColumn, leftChild)) {
      leftTimeColumn = leftChild.nextTimeColumn();
    }
  }

  private boolean hasMoreData(TimeColumn timeSeries, Node child) throws IOException {
    return (timeSeries == null || !timeSeries.hasMoreData()) && child.hasNextTimeColumn();
  }

  /**
   * If there is no value in current Node, -1 will be returned if {@code next()} is invoked.
   */
  @Override
  public TimeColumn nextTimeColumn() throws IOException {
    if (hasNextTimeColumn()) {
      hasCachedValue = false;
      return cachedValue;
    }
    return null;
  }

  @Override
  public NodeType getType() {
    return NodeType.AND;
  }
}
