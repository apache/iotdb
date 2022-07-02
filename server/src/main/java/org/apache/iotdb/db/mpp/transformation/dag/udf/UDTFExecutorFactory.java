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

package org.apache.iotdb.db.mpp.transformation.dag.udf;

import org.apache.iotdb.commons.udf.service.UDFRegistrationService;
import org.apache.iotdb.udf.api.MappableUDTF;
import org.apache.iotdb.udf.api.UDTF;

import java.time.ZoneId;

public class UDTFExecutorFactory {

  public static UDTFExecutor createUDTFExecutor(String functionName, ZoneId zoneId) {
    UDTF udtf = (UDTF) UDFRegistrationService.getInstance().reflect(functionName);
    if (udtf instanceof MappableUDTF) {
      return new MappableUDTFExecutor(functionName, zoneId, udtf);
    } else {
      return new UniversalUDTFExecutor(functionName, zoneId, udtf);
    }
  }
}
