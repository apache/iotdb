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

// Sim-Piece code forked from https://github.com/xkitsios/Sim-Piece.git

package org.apache.iotdb.db.query.simpiece;

public class SimPieceSegment {

  private final long initTimestamp;
  private final double aMin;
  private final double aMax;
  private final double a;
  private final double b;

  public SimPieceSegment(long initTimestamp, double a, double b) {
    this(initTimestamp, a, a, b);
  }

  public SimPieceSegment(long initTimestamp, double aMin, double aMax, double b) {
    this.initTimestamp = initTimestamp;
    this.aMin = aMin;
    this.aMax = aMax;
    this.a = (aMin + aMax) / 2;
    this.b = b;
  }

  public long getInitTimestamp() {
    return initTimestamp;
  }

  public double getAMin() {
    return aMin;
  }

  public double getAMax() {
    return aMax;
  }

  public double getA() {
    return a;
  }

  public double getB() {
    return b;
  }
}
