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

package org.apache.iotdb.db.metadata;

import org.apache.iotdb.db.exception.metadata.IllegalParameterOfPathException;
import org.apache.iotdb.db.schemaengine.schemaregion.utils.MetaFormatUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class MetaFormatUtilsTest {

  private static final String PATH = "root.sg.d.s";

  @Test
  public void testNonNumericSdtProperties() {
    for (String property : new String[] {"compdev", "compmintime", "compmaxtime"}) {
      assertInvalidProperty(property, "invalid");
    }
  }

  @Test
  public void testNegativeSdtProperties() {
    for (String property : new String[] {"compdev", "compmintime", "compmaxtime"}) {
      assertInvalidProperty(property, "-1");
    }
  }

  @Test
  public void testSdtTimeOverflow() {
    for (String property : new String[] {"compmintime", "compmaxtime"}) {
      assertInvalidProperty(property, "9223372036854775808");
      assertInvalidProperty(property, "-9223372036854775809");
    }
  }

  @Test
  public void testInvalidSdtTimeRange() {
    Map<String, String> props = sdtProps();
    props.put("compmintime", "20");
    props.put("compmaxtime", "10");
    IllegalParameterOfPathException exception =
        assertThrows(
            IllegalParameterOfPathException.class,
            () -> MetaFormatUtils.checkTimeseriesProps(PATH, props));

    assertUserParameterError(exception);
    assertTrue(exception.getMessage().contains("compmintime [20]"));
    assertTrue(exception.getMessage().contains("compmaxtime [10]"));
  }

  @Test
  public void testValidSdtTimeBoundsAndDefaults() throws IllegalParameterOfPathException {
    Map<String, String> props = sdtProps();
    MetaFormatUtils.checkTimeseriesProps(PATH, props);
    props.put("compmintime", "0");
    MetaFormatUtils.checkTimeseriesProps(PATH, props);
    props.put("compmaxtime", Long.toString(Long.MAX_VALUE));
    MetaFormatUtils.checkTimeseriesProps(PATH, props);
  }

  private static void assertInvalidProperty(String property, String value) {
    Map<String, String> props = sdtProps();
    props.put(property, value);
    IllegalParameterOfPathException exception =
        assertThrows(
            IllegalParameterOfPathException.class,
            () -> MetaFormatUtils.checkTimeseriesProps(PATH, props));

    assertUserParameterError(exception);
    assertTrue(exception.getMessage().contains(property));
    assertTrue(exception.getMessage().contains("[" + value + "]"));
  }

  private static void assertUserParameterError(IllegalParameterOfPathException exception) {
    assertEquals(TSStatusCode.ILLEGAL_PARAMETER.getStatusCode(), exception.getErrorCode());
    assertTrue(exception.isUserException());
    assertTrue(exception.getMessage().contains(PATH));
  }

  private static Map<String, String> sdtProps() {
    Map<String, String> props = new HashMap<>();
    props.put("loss", "sdt");
    props.put("compdev", "0.01");
    return props;
  }
}
