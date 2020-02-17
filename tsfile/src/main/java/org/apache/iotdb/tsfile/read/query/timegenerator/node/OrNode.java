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

public class OrNode implements Node {

  private final int fetchSize = TSFileDescriptor.getInstance().getConfig()
      .getFetchSizeOfTimeGenerator();

  private Node leftChild;
  private Node rightChild;

  private TimeColumn leftTimeColumn;
  private TimeColumn rightTimeColumn;

  private TimeColumn cachedTimeColumn;
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
        || leftTimeColumn.hasCurrent() || rightTimeColumn.hasCurrent();
  }

  @Override
  public TimeColumn nextTimeColumn() throws IOException {
    if (hasCachedValue) {
      hasCachedValue = false;
      return cachedTimeColumn;
    }
    hasCachedValue = false;
    cachedTimeColumn = new TimeColumn(1000);

    if (!hasLeftValue() && leftChild.hasNextTimeColumn()) {
      leftTimeColumn = leftChild.nextTimeColumn();
    }
    if (!hasRightValue() && rightChild.hasNextTimeColumn()) {
      rightTimeColumn = rightChild.nextTimeColumn();
    }

    if (hasLeftValue() && !hasRightValue()) {
      return leftTimeColumn;
    } else if (!hasLeftValue() && hasRightValue()) {
      return rightTimeColumn;
    }

    while (hasLeftValue() && hasRightValue()) {
      long leftValue = leftTimeColumn.currentTime();
      long rightValue = rightTimeColumn.currentTime();

      if (leftValue < rightValue) {
        hasCachedValue = true;
        cachedTimeColumn.add(leftValue);
        leftTimeColumn.next();
        if (!leftTimeColumn.hasCurrent() && leftChild.hasNextTimeColumn()) {
          leftTimeColumn = leftChild.nextTimeColumn();
        }
      } else if (leftValue > rightValue) {
        hasCachedValue = true;
        cachedTimeColumn.add(rightValue);
        rightTimeColumn.next();
        if (!rightTimeColumn.hasCurrent() && rightChild.hasNextTimeColumn()) {
          rightTimeColumn = rightChild.nextTimeColumn();
        }
      } else {
        hasCachedValue = true;
        cachedTimeColumn.add(leftValue);
        leftTimeColumn.next();
        rightTimeColumn.next();
        if (!leftTimeColumn.hasCurrent() && leftChild.hasNextTimeColumn()) {
          leftTimeColumn = leftChild.nextTimeColumn();
        }
        if (!rightTimeColumn.hasCurrent() && rightChild.hasNextTimeColumn()) {
          rightTimeColumn = rightChild.nextTimeColumn();
        }
      }

      if (cachedTimeColumn.size() >= fetchSize) {
        break;
      }
    }
    hasCachedValue = false;
    return cachedTimeColumn;
  }

  private boolean hasLeftValue() {
    return leftTimeColumn != null && leftTimeColumn.hasCurrent();
  }

  private boolean hasRightValue() {
    return rightTimeColumn != null && rightTimeColumn.hasCurrent();
  }


  @Override
  public NodeType getType() {
    return NodeType.OR;
  }
}
