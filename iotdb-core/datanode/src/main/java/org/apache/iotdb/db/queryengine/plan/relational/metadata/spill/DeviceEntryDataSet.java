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

package org.apache.iotdb.db.queryengine.plan.relational.metadata.spill;

import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public interface DeviceEntryDataSet extends AutoCloseable {

  int getEntryCount();

  boolean isSpilled();

  default Set<TSeriesPartitionSlot> getSeriesPartitionSlots() {
    return Set.of();
  }

  DeviceEntryReader openReader() throws IOException;

  default DeviceEntryReader openConsumingReader() throws IOException {
    throw new UnsupportedOperationException(
        DataNodeQueryMessages.EXCEPTION_OPEN_CONSUMING_READER_IS_NOT_SUPPORTED_8B2A59A4);
  }

  default List<DeviceEntry> getInlineEntries() {
    throw new UnsupportedOperationException(
        DataNodeQueryMessages
            .EXCEPTION_ONLY_INMEMORYDEVICEENTRYDATASET_SUPPORTS_GET_INLINE_DEVICE_ENTRIES_07A52CAB);
  }

  @Override
  void close() throws IOException;
}
