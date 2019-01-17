/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.query.timegenerator;

import java.io.IOException;
import org.apache.iotdb.db.query.reader.IReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.Node;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.NodeType;

public class EngineLeafNode implements Node {

  private IReader reader;

  private BatchData data = null;

  private boolean gotData = false;

  public EngineLeafNode(IReader reader) {
    this.reader = reader;
  }

  @Override
  public boolean hasNext() throws IOException {

    return reader.hasNext();

    // if (gotData) {
    // data.next();
    // gotData = false;
    // }
    //
    // if (data == null || !data.hasNext()) {
    // if (reader.hasNextBatch())
    // data = reader.nextBatch();
    // else
    // return false;
    // }
    //
    // return data.hasNext();
  }

  @Override
  public long next() throws IOException {
    return reader.next().getTimestamp();

    // long time = data.currentTime();
    // gotData = true;
    // return time;
  }

  /**
   * check if current time of current batch is equals to input time.
   */
  public boolean currentTimeIs(long time) {
    if (!reader.currentBatch().hasNext()) {
      return false;
    }
    return reader.currentBatch().currentTime() == time;
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
