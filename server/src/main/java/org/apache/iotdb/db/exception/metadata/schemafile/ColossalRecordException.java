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

/**
 * Record is the element stored in {@link
 * org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.ISegment}. Huge record is expected to be
 * supported further.
 */
public class ColossalRecordException extends MetadataException {

  public ColossalRecordException(String key, int size) {
    super(
        String.format(
            "Record of key [%s] is too large for SchemaFile to store, content size:%d", key, size),
        TSStatusCode.OVERSIZE_RECORD.getStatusCode(),
        true);
  }

  public ColossalRecordException(String key) {
    super(
        String.format("Key [%s] is too large to store in a InternalPage as index entry.", key),
        TSStatusCode.OVERSIZE_RECORD.getStatusCode(),
        true);
  }

  public ColossalRecordException(String key, String alias) {
    super(
        String.format("Key-Alias pair (%s, %s) is too large for SchemaFile to store.", key, alias),
        TSStatusCode.OVERSIZE_RECORD.getStatusCode(),
        true);
  }
}
