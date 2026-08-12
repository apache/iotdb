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

package com.timecho.iotdb.manager.activation;

import org.apache.iotdb.commons.conf.ModuleConfigManager;

import com.google.gson.JsonObject;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class AutoActivationClientTest {

  @Test
  public void testBuildAuthorizationHeader() throws Exception {
    String authorization =
        AutoActivationClient.buildAuthorizationHeader("secret", "1700000000000", "{\"a\":1}");

    Assert.assertEquals(
        "version=2,api_key=\"secret\",timestamp=1700000000000,"
            + "signature=\"Gv7Udlv62tbIHXbzkvk9O3l4BfaA07wL0b/oR1TPKXI=\"",
        authorization);
  }

  @Test
  public void testBuildRequestBodyIncludesCurrentRelease() {
    JsonObject body = AutoActivationClient.buildRequestBody("machine", true, "trace", "span");

    Assert.assertEquals("trace", body.get("traceId").getAsString());
    Assert.assertEquals("span", body.get("spanId").getAsString());
    Assert.assertEquals("machine", body.get("machineCode").getAsString());
    Assert.assertEquals(
        ModuleConfigManager.getInstance().getEdition().getRelease(),
        body.get("release").getAsInt());
    Assert.assertEquals("license_expired", body.get("activationReason").getAsString());
  }

  @Test
  public void testParseResponseRejectsNonObjectJson() {
    try {
      AutoActivationClient.parseResponse(503, "\"service busy\"");
      Assert.fail("Expected IOException");
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("non-object JSON response"));
      Assert.assertTrue(e.getMessage().contains("statusCode=503"));
      Assert.assertTrue(e.getMessage().contains("\"service busy\""));
    }
  }

  @Test
  public void testParseResponseRejectsInvalidJson() {
    try {
      AutoActivationClient.parseResponse(403, "<html>Forbidden</html>");
      Assert.fail("Expected IOException");
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("invalid JSON response"));
      Assert.assertTrue(e.getMessage().contains("statusCode=403"));
      Assert.assertTrue(e.getMessage().contains("<html>Forbidden</html>"));
    }
  }

  @Test
  public void testSanitizeResponseBodyForLogRedactsLicense() {
    String responseBody =
        "{\"code\":0,\"data\":{\"license\":\"secret-license\",\"nested\":{\"signature\":\"abc\"}}}";

    String sanitized = AutoActivationClient.sanitizeResponseBodyForLog(responseBody);

    Assert.assertTrue(sanitized.contains("\"license\":\"[REDACTED]\""));
    Assert.assertTrue(sanitized.contains("\"signature\":\"[REDACTED]\""));
    Assert.assertFalse(sanitized.contains("secret-license"));
    Assert.assertFalse(sanitized.contains("abc"));
  }

  @Test
  public void testSanitizeResponseBodyForLogKeepsPrimitiveResponse() {
    Assert.assertEquals(
        "\"service busy\"", AutoActivationClient.sanitizeResponseBodyForLog("\"service busy\""));
  }
}
