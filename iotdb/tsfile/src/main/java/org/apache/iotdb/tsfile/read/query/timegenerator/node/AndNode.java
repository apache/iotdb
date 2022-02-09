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
import java.util.function.BiPredicate;

public class AndNode implements Node {

  private Node leftChild;
  private Node rightChild;

  private long cachedTime;
  private boolean hasCachedTime;
  private boolean ascending = true;

  /**
   * Constructor of AndNode.
   *
   * @param leftChild left child
   * @param rightChild right child
   */
  public AndNode(Node leftChild, Node rightChild) {
    this.leftChild = leftChild;
    this.rightChild = rightChild;
    this.hasCachedTime = false;
  }

  public AndNode(Node leftChild, Node rightChild, boolean ascending) {
    this.leftChild = leftChild;
    this.rightChild = rightChild;
    this.hasCachedTime = false;
    this.ascending = ascending;
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Override
  public boolean hasNext() throws IOException {
    if (hasCachedTime) {
      return true;
    }
    if (leftChild.hasNext() && rightChild.hasNext()) {
      if (ascending) {
        return fillNextCache((l, r) -> l > r);
      }
      return fillNextCache((l, r) -> l < r);
    }
    return false;
  }

  private boolean fillNextCache(BiPredicate<Long, Long> seekRight) throws IOException {
    long leftValue = leftChild.next();
    long rightValue = rightChild.next();
    while (true) {
      if (leftValue == rightValue) {
        this.hasCachedTime = true;
        this.cachedTime = leftValue;
        return true;
      }
      if (seekRight.test(leftValue, rightValue)) {
        if (rightChild.hasNext()) {
          rightValue = rightChild.next();
        } else {
          return false;
        }
      } else { // leftValue > rightValue
        if (leftChild.hasNext()) {
          leftValue = leftChild.next();
        } else {
          return false;
        }
      }
    }
  }

  @Override
  public long next() throws IOException {
    if (hasNext()) {
      hasCachedTime = false;
      return cachedTime;
    }
    throw new IOException("no more data");
  }

  @Override
  public NodeType getType() {
    return NodeType.AND;
  }
}
