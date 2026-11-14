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

package org.apache.iotdb.consensus.raft.core.log;

/**
 * Decides whether slot {@code next} may extend the contiguous frontier ending at {@code prev}. The
 * default only requires the slot to exist; TRaft's optional R2 validator adds partition chaining.
 */
public interface ChainValidator {

  boolean linked(LogView log, long prev, long next);

  /**
   * True if {@link #linked} depends on nothing but the presence of both slots: then replacing the
   * copy a slot holds never moves the contiguous frontier, and the log index can overwrite a slot
   * inside the frontier without recomputing it. False (the default) for a validator that reads the
   * slots' terms or metadata.
   */
  default boolean presenceOnly() {
    return false;
  }

  /** Every held slot links to its predecessor: the frontier is the end of the held prefix. */
  ChainValidator CONTIGUOUS =
      new ChainValidator() {
        @Override
        public boolean linked(LogView log, long prev, long next) {
          return true;
        }

        @Override
        public boolean presenceOnly() {
          return true;
        }

        @Override
        public String toString() {
          return "CONTIGUOUS";
        }
      };
}
