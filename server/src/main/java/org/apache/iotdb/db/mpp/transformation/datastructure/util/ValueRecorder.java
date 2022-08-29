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

package org.apache.iotdb.db.mpp.transformation.datastructure.util;

public class ValueRecorder {

  boolean recorded = false;

  int windowFirstValueInt = 0;
  long windowFirstValueLong = 0;
  float windowFirstValueFloat = 0;
  double windowFirstValueDouble = 0;
  boolean windowFirstValueBoolean = false;
  String windowFirstValueString = "";

  public boolean hasRecorded() {
    return recorded;
  }

  public void setRecorded(boolean recorded) {
    this.recorded = recorded;
  }

  public void recordInt(int value) {
    windowFirstValueInt = value;
  }

  public void recordLong(long value) {
    windowFirstValueLong = value;
  }

  public void recordFloat(float value) {
    windowFirstValueFloat = value;
  }

  public void recordDouble(double value) {
    windowFirstValueDouble = value;
  }

  public void recordBoolean(boolean value) {
    windowFirstValueBoolean = value;
  }

  public void recordString(String value) {
    windowFirstValueString = value;
  }

  public int getInt() {
    return windowFirstValueInt;
  }

  public long getLong() {
    return windowFirstValueLong;
  }

  public float getFloat() {
    return windowFirstValueFloat;
  }

  public double getDouble() {
    return windowFirstValueDouble;
  }

  public boolean getBoolean() {
    return windowFirstValueBoolean;
  }

  public String getString() {
    return windowFirstValueString;
  }
}
