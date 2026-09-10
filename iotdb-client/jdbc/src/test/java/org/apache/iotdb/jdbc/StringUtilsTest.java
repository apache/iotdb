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

package org.apache.iotdb.jdbc;

import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class StringUtilsTest {

  @Test
  public void consistentToStringHandlesNull() {
    assertNull(StringUtils.consistentToString(null));
  }

  @Test
  public void consistentToStringExpandsNormalDecimal() {
    assertEquals("123.45", StringUtils.consistentToString(new BigDecimal("123.45")));
    // Plain-string form, not scientific notation.
    assertEquals("100", StringUtils.consistentToString(new BigDecimal("1E+2")));
  }

  @Test
  public void consistentToStringExpandsUpToLimit() {
    // scale right at the boundary must still be expanded to plain form.
    BigDecimal atLimit = new BigDecimal("1E+" + StringUtils.MAX_PLAIN_STRING_SCALE);
    String out = StringUtils.consistentToString(atLimit);
    assertEquals(StringUtils.MAX_PLAIN_STRING_SCALE + 1, out.length());
    assertTrue(out.startsWith("1"));
  }

  /**
   * Regression test for the BigDecimal DoS: {@code new BigDecimal("1e1000000000")} used to cause
   * {@link BigDecimal#toPlainString()} to allocate a ~1GB string and OOM the JVM. The guard must
   * refuse to expand such values and return the compact scientific form instead.
   */
  @Test
  public void consistentToStringRefusesToExpandHugeScale() {
    BigDecimal malicious = new BigDecimal("1e1000000000");
    String out = StringUtils.consistentToString(malicious);
    // Must be short (scientific notation), never the ~1e9-char plain form.
    assertTrue("expected scientific form, got length " + out.length(), out.length() < 64);
    assertTrue(out.contains("E") || out.contains("e"));
  }

  @Test
  public void consistentToStringRefusesToExpandHugePositiveScale() {
    // Very large positive scale -> also produces a long plain string; must be refused.
    BigDecimal malicious = new BigDecimal("1e-1000000000");
    String out = StringUtils.consistentToString(malicious);
    assertTrue("expected scientific form, got length " + out.length(), out.length() < 64);
  }
}
