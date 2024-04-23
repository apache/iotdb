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

import org.apache.iotdb.db.pipe.processor.aggregate.window.datastructure.TimeSeriesWindow;
import org.apache.iotdb.db.pipe.processor.aggregate.window.datastructure.WindowOutput;
import org.apache.iotdb.db.pipe.processor.aggregate.window.datastructure.WindowState;

import org.apache.tsfile.utils.Pair;

import java.util.List;
import java.util.Set;

public abstract class AbstractSimpleTimeWindowingProcessor extends AbstractWindowingProcessor {
  public Set<TimeSeriesWindow> mayAddWindow(
      List<TimeSeriesWindow> windowList, long timeStamp, boolean value) {
    return mayAddWindow(windowList, timeStamp);
  }

  public Set<TimeSeriesWindow> mayAddWindow(
      List<TimeSeriesWindow> windowList, long timeStamp, int value) {
    return mayAddWindow(windowList, timeStamp);
  }

  public Set<TimeSeriesWindow> mayAddWindow(
      List<TimeSeriesWindow> windowList, long timeStamp, long value) {
    return mayAddWindow(windowList, timeStamp);
  }

  public Set<TimeSeriesWindow> mayAddWindow(
      List<TimeSeriesWindow> windowList, long timeStamp, float value) {
    return mayAddWindow(windowList, timeStamp);
  }

  public Set<TimeSeriesWindow> mayAddWindow(
      List<TimeSeriesWindow> windowList, long timeStamp, double value) {
    return mayAddWindow(windowList, timeStamp);
  }

  public Set<TimeSeriesWindow> mayAddWindow(
      List<TimeSeriesWindow> windowList, long timeStamp, String value) {
    return mayAddWindow(windowList, timeStamp);
  }

  public abstract Set<TimeSeriesWindow> mayAddWindow(
      List<TimeSeriesWindow> windowList, long timeStamp);

  @Override
  public Pair<WindowState, WindowOutput> updateAndMaySetWindowState(
      TimeSeriesWindow window, long timeStamp, boolean value) {
    return updateAndMaySetWindowState(window, timeStamp);
  }

  @Override
  public Pair<WindowState, WindowOutput> updateAndMaySetWindowState(
      TimeSeriesWindow window, long timeStamp, int value) {
    return updateAndMaySetWindowState(window, timeStamp);
  }

  @Override
  public Pair<WindowState, WindowOutput> updateAndMaySetWindowState(
      TimeSeriesWindow window, long timeStamp, long value) {
    return updateAndMaySetWindowState(window, timeStamp);
  }

  @Override
  public Pair<WindowState, WindowOutput> updateAndMaySetWindowState(
      TimeSeriesWindow window, long timeStamp, float value) {
    return updateAndMaySetWindowState(window, timeStamp);
  }

  @Override
  public Pair<WindowState, WindowOutput> updateAndMaySetWindowState(
      TimeSeriesWindow window, long timeStamp, double value) {
    return updateAndMaySetWindowState(window, timeStamp);
  }

  @Override
  public Pair<WindowState, WindowOutput> updateAndMaySetWindowState(
      TimeSeriesWindow window, long timeStamp, String value) {
    return updateAndMaySetWindowState(window, timeStamp);
  }

  public abstract Pair<WindowState, WindowOutput> updateAndMaySetWindowState(
      TimeSeriesWindow window, long timeStamp);
}
