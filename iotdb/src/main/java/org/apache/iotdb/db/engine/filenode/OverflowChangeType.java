/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.filenode;

/**
 * if a file is not changed by overflow, it's in NO_CHANGE;<br>
 * if it's changed and in NO_CHANGE previously, NO_CHANGE-->CHANGED, update file<br>
 * If it's changed and in CHANGED previously, and in merging, CHANGED-->MERGING_CHANGE, update file<br>
 * If it's changed and in CHANGED previously, and not in merging, do nothing<br>
 * After merging, if it's MERGING_CHANGE, MERGING_CHANGE-->CHANGED, otherwise in NO_CHANGE, MERGING_CHANGE-->NO_CHANGE
 */
public enum OverflowChangeType {
  NO_CHANGE, CHANGED, MERGING_CHANGE;

  public short serialize() {
    switch (this) {
      case NO_CHANGE:
        return 0;
      case CHANGED:
        return 1;
      case MERGING_CHANGE:
        return 2;
      default:
        throw new IllegalStateException("Unsupported type");
    }
  }

  public static OverflowChangeType deserialize(short i) {
    switch (i) {
      case 0:
        return NO_CHANGE;
      case 1:
        return CHANGED;
      case 2:
        return MERGING_CHANGE;
      default:
        throw new IllegalArgumentException(
            String.format("Invalid input %d for OverflowChangeType", i));
    }
  }
}