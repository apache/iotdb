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

package org.apache.iotdb.session;

import static org.apache.iotdb.session.Config.PATH_MATCHER;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.regex.Pattern;
import org.junit.Test;

public class CheckPathValidityTest {

  @Test
  public void testCheckPathValidity() {
    assertTrue(Pattern.matches(PATH_MATCHER, "root.vehicle"));
    assertTrue(Pattern.matches(PATH_MATCHER, "root.123456"));
    assertTrue(Pattern.matches(PATH_MATCHER, "root._1234"));
    assertTrue(Pattern.matches(PATH_MATCHER, "root._vehicle"));
    assertTrue(Pattern.matches(PATH_MATCHER, "root.1234a4"));
    assertTrue(Pattern.matches(PATH_MATCHER, "root.1_2"));
    assertTrue(Pattern.matches(PATH_MATCHER, "root.vehicle.1245.1.2.3"));
    assertTrue(Pattern.matches(PATH_MATCHER, "root.vehicle.1245.\"1.2.3\""));
    assertTrue(Pattern.matches(PATH_MATCHER, "root.vehicle.1245.\'1.2.3\'"));

    assertFalse(Pattern.matches(PATH_MATCHER, "vehicle"));
    assertFalse(Pattern.matches(PATH_MATCHER, "root.\tvehicle"));
    assertFalse(Pattern.matches(PATH_MATCHER, "root.\nvehicle"));
    assertFalse(Pattern.matches(PATH_MATCHER, "root..vehicle"));
    assertFalse(Pattern.matches(PATH_MATCHER, "root.%12345"));
    assertFalse(Pattern.matches(PATH_MATCHER, "root.a{12345}"));
  }
}
