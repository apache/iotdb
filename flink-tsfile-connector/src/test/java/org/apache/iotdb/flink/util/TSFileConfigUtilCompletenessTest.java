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

package org.apache.iotdb.flink.util;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;

import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

/**
 * This test is used to help maintain the {@link
 * org.apache.iotdb.flink.tsfile.util.TSFileConfigUtil}.
 */
public class TSFileConfigUtilCompletenessTest {

  @Test
  public void testTSFileConfigUtilCompleteness() {
    String[] addedSetters = {
      "setBatchSize",
      "setBloomFilterErrorRate",
      "setCompressor",
      "setCoreSitePath",
      "setDeltaBlockSize",
      "setDfsClientFailoverProxyProvider",
      "setDfsHaAutomaticFailoverEnabled",
      "setDfsHaNamenodes",
      "setDfsNameServices",
      "setDftSatisfyRate",
      "setEndian",
      "setFloatPrecision",
      "setFreqType",
      "setGroupSizeInByte",
      "setHdfsIp",
      "setHdfsPort",
      "setHdfsSitePath",
      "setKerberosKeytabFilePath",
      "setKerberosPrincipal",
      "setMaxNumberOfPointsInPage",
      "setMaxDegreeOfIndexNode",
      "setMaxStringLength",
      "setPageCheckSizeThreshold",
      "setPageSizeInByte",
      "setPlaMaxError",
      "setRleBitWidth",
      "setSdtMaxError",
      "setTimeEncoder",
      "setTimeSeriesDataType",
      "setTSFileStorageFs",
      "setUseKerberos",
      "setValueEncoder"
    };
    Set<String> newSetters =
        Arrays.stream(TSFileConfig.class.getMethods())
            .map(Method::getName)
            .filter(s -> s.startsWith("set"))
            .filter(s -> !Arrays.asList(addedSetters).contains(s))
            .collect(Collectors.toSet());
    assertTrue(
        String.format(
            "New setters in TSFileConfig are detected, please add them to "
                + "org.apache.iotdb.flink.tsfile.util.TSFileConfigUtil. The setters need to be added: %s",
            newSetters),
        newSetters.isEmpty());
  }
}
