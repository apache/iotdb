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
 *
 */

package org.apache.iotdb.db.exception.metadata.schemafile;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.rpc.TSStatusCode;

public class SegmentOverflowException extends MetadataException {

  public SegmentOverflowException(int tarIndex) {
    super(
        String.format("Segment overflow  : %d", tarIndex),
        TSStatusCode.SEGMENT_OUT_OF_SPACE.getStatusCode(),
        true);
  }

  public SegmentOverflowException() {
    super("Segment not enough space", TSStatusCode.SEGMENT_OUT_OF_SPACE.getStatusCode(), true);
  }

  public SegmentOverflowException(String key) {
    super(
        String.format("Segment not enough space even after split and compact to insert: %s", key),
        TSStatusCode.SEGMENT_OUT_OF_SPACE.getStatusCode(),
        true);
  }
}
