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

package org.apache.iotdb.db.storageengine.dataregion.read.reader.common;

import java.util.Objects;

public class MergeReaderPriority implements Comparable<MergeReaderPriority> {
  final long timestampInFileName;
  final long version;
  final long offset;

  final boolean isSeq;

  public MergeReaderPriority(long timestampInFileName, long version, long offset, boolean isSeq) {
    this.timestampInFileName = timestampInFileName;
    this.version = version;
    this.offset = offset;
    this.isSeq = isSeq;
  }

  @Override
  public int compareTo(MergeReaderPriority o) {
    if (isSeq != o.isSeq) {
      // one is seq and another is unseq, unseq always win
      return isSeq ? -1 : 1;
    } else {
      // both seq or both unseq, using version + timestamp + offset to compare
      if (version != o.version) {
        return version < o.version ? -1 : 1;
      }
      if (timestampInFileName != o.timestampInFileName) {
        return timestampInFileName < o.timestampInFileName ? -1 : 1;
      }
      return Long.compare(offset, o.offset);
    }
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    MergeReaderPriority that = (MergeReaderPriority) object;
    return (this.timestampInFileName == that.timestampInFileName
        && this.version == that.version
        && this.offset == that.offset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, offset);
  }
}
