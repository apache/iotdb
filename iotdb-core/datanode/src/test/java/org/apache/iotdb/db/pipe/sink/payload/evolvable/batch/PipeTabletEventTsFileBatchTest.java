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

package org.apache.iotdb.db.pipe.sink.payload.evolvable.batch;

import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class PipeTabletEventTsFileBatchTest {

  @Test
  public void testFullyPrunedTableModelTabletIsReleasedAndNotRetained() throws Exception {
    final PipeTabletEventTsFileBatch batch =
        new PipeTabletEventTsFileBatch(100_000, 1024 * 1024, (databaseName, tablet) -> null);
    final PipeRawTabletInsertionEvent event =
        new PipeRawTabletInsertionEvent(
            true, "root.db", "db", "root.db", createTablet(), false, "pipe", 1L, null, null, false);

    Assert.assertFalse(batch.onEvent(event));
    Assert.assertTrue(batch.isEmpty());
    Assert.assertTrue(event.isReleased());
    Assert.assertEquals(0, event.getReferenceCount());

    batch.close();
  }

  private static Tablet createTablet() {
    final Tablet tablet =
        new Tablet(
            "sensors",
            Arrays.asList("device", "temperature"),
            Arrays.asList(TSDataType.STRING, TSDataType.DOUBLE),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD),
            1);
    tablet.addTimestamp(0, 1L);
    tablet.addValue(0, 0, "d1");
    tablet.addValue(0, 1, 36.5);
    tablet.setRowSize(1);
    return tablet;
  }
}
