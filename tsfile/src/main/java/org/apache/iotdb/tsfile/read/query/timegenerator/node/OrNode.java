/**
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

public class OrNode implements Node {

  private Node leftChild;
  private Node rightChild;

  private boolean hasCachedLeftValue;
  private long cachedLeftValue;
  private boolean hasCachedRightValue;
  private long cachedRightValue;

  public OrNode(Node leftChild, Node rightChild) {
    this.leftChild = leftChild;
    this.rightChild = rightChild;
    this.hasCachedLeftValue = false;
    this.hasCachedRightValue = false;
  }

  @Override
  public boolean hasNext() throws IOException {
    if (hasCachedLeftValue || hasCachedRightValue) {
      return true;
    }
    return leftChild.hasNext() || rightChild.hasNext();
  }

  private boolean hasLeftValue() throws IOException {
    return hasCachedLeftValue || leftChild.hasNext();
  }

  private long getLeftValue() throws IOException {
    if (hasCachedLeftValue) {
      hasCachedLeftValue = false;
      return cachedLeftValue;
    }
    return leftChild.next();
  }

  private boolean hasRightValue() throws IOException {
    return hasCachedRightValue || rightChild.hasNext();
  }

  private long getRightValue() throws IOException {
    if (hasCachedRightValue) {
      hasCachedRightValue = false;
      return cachedRightValue;
    }
    return rightChild.next();
  }

  @Override
  public long next() throws IOException {
    if (hasLeftValue() && !hasRightValue()) {
      return getLeftValue();
    } else if (!hasLeftValue() && hasRightValue()) {
      return getRightValue();
    } else if (hasLeftValue() && hasRightValue()) {
      long leftValue = getLeftValue();
      long rightValue = getRightValue();
      if (leftValue < rightValue) {
        hasCachedRightValue = true;
        cachedRightValue = rightValue;
        return leftValue;
      } else if (leftValue > rightValue) {
        hasCachedLeftValue = true;
        cachedLeftValue = leftValue;
        return rightValue;
      } else {
        return leftValue;
      }
    }
    return -1;
  }

  @Override
  public NodeType getType() {
    return NodeType.OR;
  }
}
