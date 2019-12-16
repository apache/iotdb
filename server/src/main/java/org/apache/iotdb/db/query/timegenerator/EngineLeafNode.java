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

package org.apache.iotdb.db.query.timegenerator;

import java.io.IOException;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.Node;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.NodeType;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;

public class EngineLeafNode implements Node {

  private IBatchReader reader;

  private BatchData data = null;

  public EngineLeafNode(IBatchReader reader) {
    this.reader = reader;
  }

  @Override
  public boolean hasNext() throws IOException {
    return reader.hasNextBatch();
  }

  @Override
  public long next() throws IOException {
    return reader.nextBatch().getTimeByIndex(0);
  }

  /**
   * check if current value is equals to input value.
   */
  public Object currentValue(long time) {
    if (data.currentTime() == time) {
      return data.currentValue();
    }
    return null;
  }

  @Override
  public NodeType getType() {
    return NodeType.LEAF;
  }

}
