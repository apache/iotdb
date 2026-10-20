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

package org.apache.iotdb.pipe.plugin.sink.opcua.server;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.Type;
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;
import org.junit.Test;

import java.time.LocalDate;
import java.time.ZoneId;

import static org.junit.Assert.assertEquals;

public class OpcUaTypeServicesTest {

  @Test
  public void testOpcUaTabletDateUsesSupportedInstantConversion() {
    LocalDate date = LocalDate.of(2026, 9, 9);
    DateTime value =
        (DateTime)
            OpcUaTypeServices.OPC_UA_TABLET_OBJECT_VALUE_GETTER_SERVICE
                .call(Type.fromTsDataType(TSDataType.DATE))
                .get(new LocalDate[] {date}, 0);
    assertEquals(date.atStartOfDay(ZoneId.systemDefault()).toInstant(), value.getJavaInstant());
  }
}
