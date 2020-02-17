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

public class OrNode implements Node {

  private Node leftChild;
  private Node rightChild;

  private TimeColumn leftTimes;
  private TimeColumn rightTimes;

  private TimeColumn cachedValue;
  private boolean hasCachedValue;


  public OrNode(Node leftChild, Node rightChild) {
    this.leftChild = leftChild;
    this.rightChild = rightChild;
  }

  @Override
  public boolean hasNextTimeColumn() throws IOException {
    if (hasCachedValue) {
      return true;
    }

    return leftChild.hasNextTimeColumn() || rightChild.hasNextTimeColumn()
        || leftTimes.hasMoreData() || rightTimes.hasMoreData();
  }

  @Override
  public TimeColumn nextTimeColumn() throws IOException {
    if (hasCachedValue) {
      hasCachedValue = false;
      return cachedValue;
    }
    hasCachedValue = false;
    cachedValue = new TimeColumn(1000);

    if (!hasLeftValue() && leftChild.hasNextTimeColumn()) {
      leftTimes = leftChild.nextTimeColumn();
    }
    if (!hasRightValue() && rightChild.hasNextTimeColumn()) {
      rightTimes = rightChild.nextTimeColumn();
    }

    if (hasLeftValue() && !hasRightValue()) {
      return leftTimes;
    } else if (!hasLeftValue() && hasRightValue()) {
      return rightTimes;
    }

    long stopBatchTime = getStopBatchTime();

    while (hasLeftValue() && hasRightValue()) {
      long leftValue = leftTimes.currentTime();
      long rightValue = rightTimes.currentTime();

      if (leftValue < rightValue) {
        hasCachedValue = true;
        cachedValue.add(leftValue);
        leftTimes.next();
        if (!leftTimes.hasMoreData() && leftChild.hasNextTimeColumn()) {
          leftTimes = leftChild.nextTimeColumn();
        }
      } else if (leftValue > rightValue) {
        hasCachedValue = true;
        cachedValue.add(rightValue);
        rightTimes.next();
        if (!rightTimes.hasMoreData() && rightChild.hasNextTimeColumn()) {
          rightTimes = rightChild.nextTimeColumn();
        }
      } else {
        hasCachedValue = true;
        cachedValue.add(leftValue);
        leftTimes.next();
        rightTimes.next();
        if (!leftTimes.hasMoreData() && leftChild.hasNextTimeColumn()) {
          leftTimes = leftChild.nextTimeColumn();
        }
        if (!rightTimes.hasMoreData() && rightChild.hasNextTimeColumn()) {
          rightTimes = rightChild.nextTimeColumn();
        }
      }

      if (leftValue > stopBatchTime && rightValue > stopBatchTime) {
        break;
      }
    }
    hasCachedValue = false;
    return cachedValue;
  }

  private long getStopBatchTime() {
    long rMax = Long.MAX_VALUE;
    long lMax = Long.MAX_VALUE;
    if (leftTimes.hasMoreData()) {
      lMax = leftTimes.getLastTime();
    }
    if (rightTimes.hasMoreData()) {
      rMax = rightTimes.getLastTime();
    }
    return rMax > lMax ? lMax : rMax;
  }

  private boolean hasLeftValue() {
    return leftTimes != null && leftTimes.hasMoreData();
  }

  private boolean hasRightValue() {
    return rightTimes != null && rightTimes.hasMoreData();
  }


  @Override
  public NodeType getType() {
    return NodeType.OR;
  }
}
