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
import org.apache.iotdb.tsfile.read.common.TimeSeries;

public class OrNode implements Node {

  private Node leftChild;
  private Node rightChild;

  private TimeSeries leftTimes;
  private TimeSeries rightTimes;

  private TimeSeries cachedValue;
  private boolean hasCachedValue;


  public OrNode(Node leftChild, Node rightChild) {
    this.leftChild = leftChild;
    this.rightChild = rightChild;
  }

  @Override
  public boolean hasNext() throws IOException {
    if (hasCachedValue) {
      return true;
    }

    return leftChild.hasNext() || rightChild.hasNext()
        || leftTimes.hasMoreData() || rightTimes.hasMoreData();
  }

  @Override
  public TimeSeries next() throws IOException {
    if (hasCachedValue) {
      hasCachedValue = false;
      return cachedValue;
    }
    hasCachedValue = false;
    cachedValue = new TimeSeries(1000);

    if (!hasLeftValue() && leftChild.hasNext()) {
      leftTimes = leftChild.next();
    }
    if (!hasRightValue() && rightChild.hasNext()) {
      rightTimes = rightChild.next();
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
        if (!leftTimes.hasMoreData() && leftChild.hasNext()) {
          leftTimes = leftChild.next();
        }
      } else if (leftValue > rightValue) {
        hasCachedValue = true;
        cachedValue.add(rightValue);
        rightTimes.next();
        if (!rightTimes.hasMoreData() && rightChild.hasNext()) {
          rightTimes = rightChild.next();
        }
      } else {
        hasCachedValue = true;
        cachedValue.add(leftValue);
        leftTimes.next();
        rightTimes.next();
        if (!leftTimes.hasMoreData() && leftChild.hasNext()) {
          leftTimes = leftChild.next();
        }
        if (!rightTimes.hasMoreData() && rightChild.hasNext()) {
          rightTimes = rightChild.next();
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
