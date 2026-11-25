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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.function.tvf.read_tsfile;

import org.apache.iotdb.udf.api.exception.UDFArgumentNotValidException;

import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class TsFileSchemaCollectorTest {

  @Test
  public void testFailedToReadTableSchemaPreservesCause() {
    File tsFile = new File("sample.tsfile");
    Exception cause = new IllegalStateException("Unsupported encoding");
    TsFileSchemaCollector collector = new TsFileSchemaCollector("table", null);

    UDFArgumentNotValidException exception =
        collector.failedToReadTableSchemaException(tsFile, cause);

    assertEquals(
        "Failed to read table schema from TsFile: " + tsFile.getAbsolutePath(),
        exception.getMessage());
    assertSame(cause, exception.getCause());
  }
}
