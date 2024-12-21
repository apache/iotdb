/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.execution.operator.process.window.function.aggregate;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.WindowFunction;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.Partition;

import org.apache.tsfile.block.column.ColumnBuilder;

public class AggregationWindowFunction implements WindowFunction {
  private final WindowAggregator aggregator;
  private int currentStart;
  private int currentEnd;

  public AggregationWindowFunction(WindowAggregator aggregator) {
    this.aggregator = aggregator;
    reset();
  }

  @Override
  public void reset() {
    aggregator.reset();
    currentStart = -1;
    currentEnd = -1;
  }

  @Override
  public void transform(
      Partition partition,
      ColumnBuilder builder,
      int index,
      int frameStart,
      int frameEnd,
      int peerGroupStart,
      int peerGroupEnd) {
    if (frameStart < 0) {
      // Empty frame
      reset();
    } else if (frameStart == currentStart && frameEnd >= currentEnd) {
      // Frame expansion
      if (frameEnd != currentEnd) {
        Partition region = partition.getRegion(currentEnd + 1, frameEnd);
        aggregator.addInput(region);
        currentEnd = frameEnd;
      }
    } else {
      buildNewFrame(partition, frameStart, frameEnd);
    }

    aggregator.evaluate(builder);
  }

  private void buildNewFrame(Partition partition, int frameStart, int frameEnd) {
    if (aggregator.removable()) {
      int prefix = Math.abs(currentStart - frameStart);
      int suffix = Math.abs(currentEnd - frameEnd);
      int frameLength = frameEnd - frameStart + 1;

      // Compare remove && add cost with re-computation
      if (frameLength > prefix + suffix) {
        if (currentStart < frameStart) {
          Partition region = partition.getRegion(currentStart, frameStart - 1);
          aggregator.removeInput(region);
        } else if (currentStart > frameStart) {
          Partition region = partition.getRegion(frameStart, currentStart - 1);
          aggregator.addInput(region);
        } // Do nothing when currentStart == frameStart

        if (frameEnd < currentEnd) {
          Partition region = partition.getRegion(frameEnd + 1, currentEnd);
          aggregator.removeInput(region);
        } else if (frameEnd > currentEnd) {
          Partition region = partition.getRegion(currentEnd + 1, frameEnd);
          aggregator.addInput(region);
        } // Do nothing when frameEnd == currentEnd

        currentStart = frameStart;
        currentEnd = frameEnd;
        return;
      }
    }

    // Re-compute
    aggregator.reset();
    Partition region = partition.getRegion(frameStart, frameEnd);
    aggregator.addInput(region);

    currentStart = frameStart;
    currentEnd = frameEnd;
  }

  @Override
  public boolean needPeerGroup() {
    return false;
  }
}
