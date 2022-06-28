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
package org.apache.iotdb.db.wal.buffer;

import org.apache.iotdb.db.qp.physical.crud.DeletePlan;

/** This class provides a signal to help wal buffer dealing with some special cases */
public class SignalWALEntry extends WALEntry {
  private final SignalType signalType;

  public SignalWALEntry(SignalType signalType) {
    this(signalType, false);
  }

  public SignalWALEntry(SignalType signalType, boolean wait) {
    super(Long.MIN_VALUE, new DeletePlan(), wait);
    this.signalType = signalType;
  }

  @Override
  public boolean isSignal() {
    return true;
  }

  public SignalType getSignalType() {
    return signalType;
  }

  public enum SignalType {
    /** signal wal buffer has been closed */
    CLOSE_SIGNAL,
    /** signal wal buffer to roll wal log writer */
    ROLL_WAL_LOG_WRITER_SIGNAL,
  }
}
