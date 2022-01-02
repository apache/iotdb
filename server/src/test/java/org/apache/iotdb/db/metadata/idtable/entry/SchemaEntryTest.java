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

package org.apache.iotdb.db.metadata.idtable.entry;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import org.junit.Test;

import static org.apache.iotdb.db.utils.EncodingInferenceUtils.getDefaultEncoding;
import static org.junit.Assert.assertEquals;

public class SchemaEntryTest {
  @Test
  public void schemaEntryBuildTest() throws IllegalPathException {
    for (TSDataType type : TSDataType.values()) {
      // omit vector
      if (type == TSDataType.VECTOR) {
        continue;
      }

      SchemaEntry schemaEntry = new SchemaEntry(type);

      // schema check
      assertEquals(schemaEntry.getTSDataType(), type);
      assertEquals(schemaEntry.getTSEncoding(), getDefaultEncoding(type));
      assertEquals(
          schemaEntry.getCompressionType(),
          TSFileDescriptor.getInstance().getConfig().getCompressor());

      // flush time
      schemaEntry.updateLastedFlushTime(100);
      assertEquals(schemaEntry.getFlushTime(), 100);
      schemaEntry.updateLastedFlushTime(50);
      assertEquals(schemaEntry.getFlushTime(), 100);

      // last cache
      schemaEntry.updateCachedLast(
          new TimeValuePair(100L, new TsPrimitiveType.TsLong(1L)), false, 0L);
      assertEquals(new TsPrimitiveType.TsLong(1L), schemaEntry.getLastValue());
      assertEquals(100L, schemaEntry.getLastTime());

      schemaEntry.updateCachedLast(
          new TimeValuePair(90L, new TsPrimitiveType.TsLong(2L)), false, 0L);
      assertEquals(new TsPrimitiveType.TsLong(1L), schemaEntry.getLastValue());
      assertEquals(100L, schemaEntry.getLastTime());

      schemaEntry.updateCachedLast(
          new TimeValuePair(110L, new TsPrimitiveType.TsLong(2L)), false, 0L);
      assertEquals(new TsPrimitiveType.TsLong(2L), schemaEntry.getLastValue());
      assertEquals(110L, schemaEntry.getLastTime());
    }
  }
}
