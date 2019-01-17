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
package org.apache.iotdb.db.engine.overflow.utils;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

/**
 * TimePair represents an overflow operation.
 */

public class TimePair {

  public long s; // start time
  public long e; // end time
  public byte[] v; // value
  public OverflowOpType opType = null;
  public TSDataType dataType;
  public MergeStatus mergestatus;

  /**
   * construct function for TiemPair.
   *
   * @param s -use to initial startTime
   * @param e -use to initial endTime
   */
  public TimePair(long s, long e) {
    this.s = s;
    this.e = e;
    this.v = null;
  }

  /**
   * construct function for TiemPair.
   *
   * @param s -use to initial startTime
   * @param e -use to initial endTime
   * @param v -use to initial value
   */
  public TimePair(long s, long e, byte[] v) {
    this.s = s;
    this.e = e;
    this.v = v;
  }

  public TimePair(long s, long e, MergeStatus status) {
    this(s, e);
    this.mergestatus = status;
  }

  public TimePair(long s, long e, byte[] v, TSDataType dataType) {
    this(s, e, v);
    this.dataType = dataType;
  }

  public TimePair(long s, long e, byte[] v, OverflowOpType overflowOpType) {
    this(s, e, v);
    this.opType = overflowOpType;
  }

  public TimePair(long s, long e, byte[] v, OverflowOpType overflowOpType, TSDataType dataType) {
    this(s, e, v, overflowOpType);
    this.dataType = dataType;
  }

  /**
   * construct function for TimePair.
   *
   * @param s -use to initial startTiem
   * @param e -use to initial endTime
   * @param v -use to initial value
   * @param type -use to initial opType
   * @param status -use to initial mergestatus
   */
  public TimePair(long s, long e, byte[] v, OverflowOpType type, MergeStatus status) {
    this(s, e, v);
    this.opType = type;
    this.mergestatus = status;
  }

  /**
   * Set TimePair s = -1 and e = -1 means reset.
   */
  public void reset() {
    s = -1;
    e = -1;
    mergestatus = MergeStatus.DONE;
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer().append(this.s).append(",").append(this.e);
    if (this.opType != null) {
      sb.append(",").append(this.opType);
    }
    return sb.toString();
  }
}
