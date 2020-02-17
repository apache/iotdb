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
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.read.common.TimeColumn;

public class AndNode implements Node {

  private final int fetchSize = TSFileDescriptor.getInstance().getConfig()
      .getFetchSizeOfTimeGenerator();

  private Node leftChild;
  private Node rightChild;

  private TimeColumn cachedTimeColumn;
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
    cachedTimeColumn = new TimeColumn(1000);
    //fill data
    fillLeftData();
    fillRightData();

    while (leftTimeColumn.hasCurrent() && rightTimeColumn.hasCurrent()) {
      long leftValue = leftTimeColumn.currentTime();
      long rightValue = rightTimeColumn.currentTime();

      if (leftValue == rightValue) {
        this.hasCachedValue = true;
        this.cachedTimeColumn.add(leftValue);
        leftTimeColumn.next();
        rightTimeColumn.next();
      } else if (leftValue > rightValue) {
        rightTimeColumn.next();
      } else { // leftValue < rightValue
        leftTimeColumn.next();
      }

      if (cachedTimeColumn.size() >= fetchSize) {
        if (hasCachedValue) {
          break;
        }
      }
      fillLeftData();
      fillRightData();
    }
    return hasCachedValue;
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

  //no more data in cache and has more data in child
  private boolean hasMoreData(TimeColumn timeSeries, Node child) throws IOException {
    return (timeSeries == null || !timeSeries.hasCurrent()) && child.hasNextTimeColumn();
  }

  /**
   * If there is no value in current Node, -1 will be returned if {@code next()} is invoked.
   */
  @Override
  public TimeColumn nextTimeColumn() throws IOException {
    if (hasNextTimeColumn()) {
      hasCachedValue = false;
      return cachedTimeColumn;
    }
    return null;
  }

  @Override
  public NodeType getType() {
    return NodeType.AND;
  }
}
