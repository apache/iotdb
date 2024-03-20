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

package org.apache.iotdb.db.pipe.processor.aggregate.window.processor;

import org.apache.iotdb.db.pipe.processor.aggregate.AggregateProcessor;
import org.apache.iotdb.db.pipe.processor.aggregate.window.datastructure.TimeSeriesWindow;
import org.apache.iotdb.db.pipe.processor.aggregate.window.datastructure.WindowOutput;
import org.apache.iotdb.db.pipe.processor.aggregate.window.datastructure.WindowState;
import org.apache.iotdb.db.pipe.processor.formal.AbstractFormalProcessor;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

/**
 * {@link AbstractWindowingProcessor} is the formal processor defining the windows adoptable for
 * {@link AggregateProcessor}.
 */
public abstract class AbstractWindowingProcessor extends AbstractFormalProcessor {

  /**
   * The windowing processor may add windows to the windowList. The newly added window shall be put
   * into the end of the list. The window will soon be configured by the {@link AggregateProcessor}.
   * Typically only the timestamp is needed, however the window can use the windowList and value to
   * help judging.
   *
   * @return The added windows
   */
  public abstract Set<TimeSeriesWindow> mayAddWindow(
      List<TimeSeriesWindow> windowList, long timeStamp, boolean value);

  public abstract Set<TimeSeriesWindow> mayAddWindow(
      List<TimeSeriesWindow> windowList, long timeStamp, int value);

  public abstract Set<TimeSeriesWindow> mayAddWindow(
      List<TimeSeriesWindow> windowList, long timeStamp, long value);

  public abstract Set<TimeSeriesWindow> mayAddWindow(
      List<TimeSeriesWindow> windowList, long timeStamp, float value);

  public abstract Set<TimeSeriesWindow> mayAddWindow(
      List<TimeSeriesWindow> windowList, long timeStamp, double value);

  public abstract Set<TimeSeriesWindow> mayAddWindow(
      List<TimeSeriesWindow> windowList, long timeStamp, String value);

  /**
   * The windowing processor may decide whether a window shall be terminated when a point is
   * arrived. If yes, the processor shall set the pair of output timestamp, the time of the
   * progressIndex to be reported, and whether the window is closed in this round. If not, it shall
   * return {@code null}
   */
  public abstract Pair<WindowState, WindowOutput> updateAndMaySetWindowState(
      TimeSeriesWindow window, long timeStamp, boolean value);

  public abstract Pair<WindowState, WindowOutput> updateAndMaySetWindowState(
      TimeSeriesWindow window, long timeStamp, int value);

  public abstract Pair<WindowState, WindowOutput> updateAndMaySetWindowState(
      TimeSeriesWindow window, long timeStamp, long value);

  public abstract Pair<WindowState, WindowOutput> updateAndMaySetWindowState(
      TimeSeriesWindow window, long timeStamp, float value);

  public abstract Pair<WindowState, WindowOutput> updateAndMaySetWindowState(
      TimeSeriesWindow window, long timeStamp, double value);

  public abstract Pair<WindowState, WindowOutput> updateAndMaySetWindowState(
      TimeSeriesWindow window, long timeStamp, String value);

  public abstract WindowOutput forceOutput(TimeSeriesWindow window);

  /**
   * Serialize the customized attributes in a window to an output stream
   *
   * @param window the window to serialize
   * @param outputStream the outputStream
   */
  public void serializeCustomizedAttributes(
      TimeSeriesWindow window, DataOutputStream outputStream) {
    // Do nothing by default
  }

  /**
   * Deserialize a customized attributes in a window from an output stream
   *
   * @param byteBuffer the customized attributes in one window
   */
  public void deserializeCustomizedAttributes(TimeSeriesWindow window, ByteBuffer byteBuffer) {
    // Do nothing by default
  }

  @Override
  public void close() throws Exception {}
}
