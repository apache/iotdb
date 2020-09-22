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

package org.apache.iotdb.db.query.udf.core.access;

import org.apache.iotdb.db.query.udf.api.access.Point;
import org.apache.iotdb.tsfile.utils.Binary;

public class PointImpl implements Point {

  protected long currentTime;

  protected int currentInt;
  protected long currentLong;
  protected float currentFloat;
  protected double currentDouble;
  protected boolean currentBoolean;
  protected Binary currentBinary;

  @Override
  public long getTime() {
    return currentTime;
  }

  @Override
  public int getInt() {
    return currentInt;
  }

  @Override
  public long getLong() {
    return currentLong;
  }

  @Override
  public float getFloat() {
    return currentFloat;
  }

  @Override
  public double getDouble() {
    return currentDouble;
  }

  @Override
  public boolean getBoolean() {
    return currentBoolean;
  }

  @Override
  public Binary getBinary() {
    return currentBinary;
  }

  @Override
  public String getString() {
    return currentBinary.toString();
  }

  public void setCurrentTime(long currentTime) {
    this.currentTime = currentTime;
  }

  public void setCurrentInt(int currentInt) {
    this.currentInt = currentInt;
  }

  public void setCurrentLong(long currentLong) {
    this.currentLong = currentLong;
  }

  public void setCurrentFloat(float currentFloat) {
    this.currentFloat = currentFloat;
  }

  public void setCurrentDouble(double currentDouble) {
    this.currentDouble = currentDouble;
  }

  public void setCurrentBoolean(boolean currentBoolean) {
    this.currentBoolean = currentBoolean;
  }

  public void setCurrentBinary(Binary currentBinary) {
    this.currentBinary = currentBinary;
  }
}
