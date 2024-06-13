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

import org.apache.iotdb.db.pipe.processor.aggregate.AbstractFormalProcessor;
import org.apache.iotdb.db.pipe.processor.aggregate.AggregateProcessor;
import org.apache.iotdb.db.pipe.processor.aggregate.window.datastructure.TimeSeriesWindow;
import org.apache.iotdb.db.pipe.processor.aggregate.window.datastructure.WindowOutput;
import org.apache.iotdb.db.pipe.processor.aggregate.window.datastructure.WindowState;
import org.apache.iotdb.pipe.api.type.Binary;

import org.apache.tsfile.utils.Pair;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.List;
import java.util.Set;

/**
 * {@link AbstractWindowingProcessor} is the formal processor defining the windows adoptable for
 * {@link AggregateProcessor}.
 */
public abstract class AbstractWindowingProcessor extends AbstractFormalProcessor {

  /**
   * The {@link AbstractWindowingProcessor} may add {@link TimeSeriesWindow}s to the windowList,
   * which will soon be configured by the {@link AggregateProcessor}. Typically only the timestamp
   * is needed, however the {@link AbstractWindowingProcessor} can use the windowList and values to
   * assist judgement.
   *
   * @return The added windows
   */
  public abstract Set<TimeSeriesWindow> mayAddWindow(
      final List<TimeSeriesWindow> windowList, final long timeStamp, final boolean value);

  public abstract Set<TimeSeriesWindow> mayAddWindow(
      final List<TimeSeriesWindow> windowList, final long timeStamp, final int value);

  public abstract Set<TimeSeriesWindow> mayAddWindow(
      final List<TimeSeriesWindow> windowList, final long timeStamp, final LocalDate value);

  public abstract Set<TimeSeriesWindow> mayAddWindow(
      final List<TimeSeriesWindow> windowList, final long timeStamp, final long value);

  public abstract Set<TimeSeriesWindow> mayAddWindow(
      final List<TimeSeriesWindow> windowList, final long timeStamp, final float value);

  public abstract Set<TimeSeriesWindow> mayAddWindow(
      final List<TimeSeriesWindow> windowList, final long timeStamp, final double value);

  public abstract Set<TimeSeriesWindow> mayAddWindow(
      final List<TimeSeriesWindow> windowList, final long timeStamp, final String value);

  public abstract Set<TimeSeriesWindow> mayAddWindow(
      final List<TimeSeriesWindow> windowList, final long timeStamp, final Binary value);

  /**
   * The {@link AbstractWindowingProcessor} may decide whether a {@link TimeSeriesWindow} shall be
   * terminated when a point is arrived. If yes, the {@link AbstractWindowingProcessor} shall set
   * the pair of output timestamp, the time of the progressIndex to be reported, and whether the
   * window is closed in this round. If not, it shall return {@code null}.
   *
   * @return The pair of {@link WindowState} and {@link WindowOutput}, the latter one including the
   *     report time and output timestamp. Note that when a report time is submitted, the {@link
   *     AbstractWindowingProcessor} may never see the time below when the system has been
   *     restarted.
   */
  public abstract Pair<WindowState, WindowOutput> updateAndMaySetWindowState(
      final TimeSeriesWindow window, final long timeStamp, final boolean value);

  public abstract Pair<WindowState, WindowOutput> updateAndMaySetWindowState(
      final TimeSeriesWindow window, final long timeStamp, final int value);

  public abstract Pair<WindowState, WindowOutput> updateAndMaySetWindowState(
      final TimeSeriesWindow window, final long timeStamp, final LocalDate value);

  public abstract Pair<WindowState, WindowOutput> updateAndMaySetWindowState(
      final TimeSeriesWindow window, final long timeStamp, final long value);

  public abstract Pair<WindowState, WindowOutput> updateAndMaySetWindowState(
      final TimeSeriesWindow window, final long timeStamp, final float value);

  public abstract Pair<WindowState, WindowOutput> updateAndMaySetWindowState(
      final TimeSeriesWindow window, final long timeStamp, final double value);

  public abstract Pair<WindowState, WindowOutput> updateAndMaySetWindowState(
      final TimeSeriesWindow window, final long timeStamp, final String value);

  public abstract Pair<WindowState, WindowOutput> updateAndMaySetWindowState(
      final TimeSeriesWindow window, final long timeStamp, final Binary value);

  public abstract WindowOutput forceOutput(final TimeSeriesWindow window);

  /**
   * Serialize the customized attributes in a window to an output stream
   *
   * @param window the window to serialize
   * @param outputStream the outputStream
   */
  public void serializeCustomizedAttributes(
      final TimeSeriesWindow window, final DataOutputStream outputStream) {
    // Do nothing by default
  }

  /**
   * Deserialize a customized attributes in a window from an output stream
   *
   * @param byteBuffer the customized attributes in one window
   */
  public void deserializeCustomizedAttributes(
      final TimeSeriesWindow window, final ByteBuffer byteBuffer) {
    // Do nothing by default
  }

  @Override
  public void close() throws Exception {}
}
