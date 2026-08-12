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

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.ModuleConfigManager;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.bouncycastle.jcajce.provider.digest.SM3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class AutoActivationClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(AutoActivationClient.class);
  private static final Gson GSON = new Gson();
  private static final String LICENSE_SERVER_URL = "https://license.timecho.com";
  private static final String AUTHORIZATION_VERSION = "2";
  private static final char[] HEX_ARRAY = "0123456789abcdef".toCharArray();
  private static final int MAX_RESPONSE_BODY_LOG_LENGTH = 1024;
  private static final String REDACTED_LOG_VALUE = "[REDACTED]";
  private static final String API_KEY_FILE_PROPERTY = "timechodb.auto.activation.api-key-file";
  private static final String API_KEY_FILE_ENV = "TIMECHODB_AUTO_ACTIVATION_API_KEY_FILE";
  private static final long INITIAL_DELAY_MS = 15_000L;
  private static final long RETRY_INTERVAL_MS = 60_000L;
  private static final int MAX_ATTEMPTS = 0;
  private static final int CONNECT_TIMEOUT_MS = 10_000;
  private static final int READ_TIMEOUT_MS = 30_000;

  public static String getConfiguredApiKeyFile() {
    return getConfig(API_KEY_FILE_PROPERTY, API_KEY_FILE_ENV, "");
  }

  public static long getInitialDelayMs() {
    return INITIAL_DELAY_MS;
  }

  public static long getRetryIntervalMs() {
    return RETRY_INTERVAL_MS;
  }

  public static int getMaxAttempts() {
    return MAX_ATTEMPTS;
  }

  public AutoActivationResult requestLicense(
      String apiKey, String machineCode, boolean renew, String traceId) throws IOException {
    String actualTraceId = traceId == null || traceId.isEmpty() ? newTraceId() : traceId;
    String spanId = newSpanId();
    JsonObject body = buildRequestBody(machineCode, renew, actualTraceId, spanId);

    JsonObject response = post("/api/timechodb/auto-activation/request", body, apiKey);
    JsonObject data = getData(response);
    String license = getRequiredString(data, "license");
    return new AutoActivationResult(license);
  }

  static JsonObject buildRequestBody(
      String machineCode, boolean renew, String traceId, String spanId) {
    JsonObject body = new JsonObject();
    body.addProperty("traceId", traceId);
    body.addProperty("spanId", spanId);
    body.addProperty("machineCode", machineCode);
    body.addProperty("release", ModuleConfigManager.getInstance().getEdition().getRelease());
    body.addProperty("renew", renew);
    if (renew) {
      body.addProperty("activationReason", "license_expired");
    }
    return body;
  }

  private JsonObject post(String path, JsonObject body, String apiKey) throws IOException {
    URL url = new URL(LICENSE_SERVER_URL + path);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod("POST");
    connection.setConnectTimeout(CONNECT_TIMEOUT_MS);
    connection.setReadTimeout(READ_TIMEOUT_MS);
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json");
    String payloadJson = GSON.toJson(body);
    connection.setRequestProperty(
        "Authorization",
        buildAuthorizationHeader(apiKey, String.valueOf(System.currentTimeMillis()), payloadJson));
    byte[] payload = payloadJson.getBytes(StandardCharsets.UTF_8);
    try (OutputStream outputStream = connection.getOutputStream()) {
      outputStream.write(payload);
    }
    int statusCode = connection.getResponseCode();
    String responseBody =
        readAll(statusCode >= 400 ? connection.getErrorStream() : connection.getInputStream());
    LOGGER.info(
        "auto activation request response: path={}, statusCode={}, responseBody={}",
        path,
        statusCode,
        sanitizeResponseBodyForLog(responseBody));
    return parseResponse(statusCode, responseBody);
  }

  static JsonObject parseResponse(int statusCode, String responseBody) throws IOException {
    JsonElement responseElement;
    try {
      responseElement = GSON.fromJson(responseBody, JsonElement.class);
    } catch (RuntimeException e) {
      throw new IOException(
          "auto activation request returned invalid JSON response: statusCode="
              + statusCode
              + ", responseBody="
              + abbreviateResponseBody(responseBody),
          e);
    }
    if (responseElement == null || !responseElement.isJsonObject()) {
      throw new IOException(
          "auto activation request returned non-object JSON response: statusCode="
              + statusCode
              + ", responseBody="
              + abbreviateResponseBody(responseBody));
    }
    JsonObject response = responseElement.getAsJsonObject();
    if (statusCode >= 400 || !isSuccessResponse(response)) {
      throw new IOException(
          "auto activation request failed: statusCode="
              + statusCode
              + ", responseBody="
              + abbreviateResponseBody(responseBody));
    }
    return response;
  }

  private static boolean isSuccessResponse(JsonObject response) {
    JsonElement code = response.get("code");
    if (code == null || code.isJsonNull()) {
      return false;
    }
    try {
      return code.getAsInt() == 0;
    } catch (RuntimeException e) {
      return false;
    }
  }

  private static String abbreviateResponseBody(String responseBody) {
    if (responseBody == null) {
      return "";
    }
    if (responseBody.length() <= MAX_RESPONSE_BODY_LOG_LENGTH) {
      return responseBody;
    }
    return responseBody.substring(0, MAX_RESPONSE_BODY_LOG_LENGTH) + "...";
  }

  static String sanitizeResponseBodyForLog(String responseBody) {
    if (responseBody == null || responseBody.isEmpty()) {
      return "";
    }
    try {
      JsonElement responseElement = GSON.fromJson(responseBody, JsonElement.class);
      redactSensitiveFields(responseElement);
      return abbreviateResponseBody(GSON.toJson(responseElement));
    } catch (RuntimeException e) {
      return abbreviateResponseBody(responseBody);
    }
  }

  private static void redactSensitiveFields(JsonElement element) {
    if (element == null || element.isJsonNull()) {
      return;
    }
    if (element.isJsonObject()) {
      for (Map.Entry<String, JsonElement> entry : element.getAsJsonObject().entrySet()) {
        if (isSensitiveResponseField(entry.getKey())) {
          entry.setValue(new JsonPrimitive(REDACTED_LOG_VALUE));
        } else {
          redactSensitiveFields(entry.getValue());
        }
      }
      return;
    }
    if (element.isJsonArray()) {
      for (JsonElement child : element.getAsJsonArray()) {
        redactSensitiveFields(child);
      }
    }
  }

  private static boolean isSensitiveResponseField(String fieldName) {
    return "license".equalsIgnoreCase(fieldName)
        || "apiKey".equalsIgnoreCase(fieldName)
        || "api_key".equalsIgnoreCase(fieldName)
        || "apiKeyId".equalsIgnoreCase(fieldName)
        || "api_key_id".equalsIgnoreCase(fieldName)
        || "signature".equalsIgnoreCase(fieldName);
  }

  static String buildAuthorizationHeader(String apiKey, String timestamp, String requestBody)
      throws IOException {
    try {
      String signature =
          sm3Base64(AUTHORIZATION_VERSION + timestamp + apiKey + sm3Hex(requestBody));
      return "version="
          + AUTHORIZATION_VERSION
          + ",api_key=\""
          + quoteHeaderValue(apiKey)
          + "\",timestamp="
          + timestamp
          + ",signature=\""
          + quoteHeaderValue(signature)
          + "\"";
    } catch (Exception e) {
      throw new IOException("failed to sign auto activation request", e);
    }
  }

  private static String sm3Hex(String value) {
    return toHex(sm3(value));
  }

  private static String sm3Base64(String value) {
    return Base64.getEncoder().encodeToString(sm3(value));
  }

  private static byte[] sm3(String value) {
    SM3.Digest digest = new SM3.Digest();
    return digest.digest(value.getBytes(StandardCharsets.UTF_8));
  }

  private static String toHex(byte[] bytes) {
    char[] hexChars = new char[bytes.length * 2];
    for (int i = 0; i < bytes.length; i++) {
      int value = bytes[i] & 0xFF;
      hexChars[i * 2] = HEX_ARRAY[value >>> 4];
      hexChars[i * 2 + 1] = HEX_ARRAY[value & 0x0F];
    }
    return new String(hexChars);
  }

  private static String quoteHeaderValue(String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  public static String newTraceId() {
    return "TDB-" + UUID.randomUUID();
  }

  private static String newSpanId() {
    return "SPAN-" + UUID.randomUUID();
  }

  private static JsonObject getData(JsonObject response) throws IOException {
    JsonElement data = response.get("data");
    if (data == null || !data.isJsonObject()) {
      throw new IOException("auto activation response misses data: " + response);
    }
    return data.getAsJsonObject();
  }

  private static String getRequiredString(JsonObject object, String field) throws IOException {
    JsonElement element = object.get(field);
    String value = element == null || element.isJsonNull() ? null : element.getAsString();
    if (value == null || value.isEmpty()) {
      throw new IOException("auto activation response misses " + field + ": " + object);
    }
    return value;
  }

  private static String readAll(InputStream inputStream) throws IOException {
    if (inputStream == null) {
      return "";
    }
    StringBuilder builder = new StringBuilder();
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        builder.append(line);
      }
    }
    return builder.toString();
  }

  private static String getConfig(String propertyName, String envName, String defaultValue) {
    String value = System.getProperty(propertyName);
    if (value == null || value.isEmpty()) {
      value = System.getenv(envName);
    }
    if (value == null || value.isEmpty()) {
      value = getFileConfig(propertyName);
    }
    return value == null || value.trim().isEmpty() ? defaultValue : value.trim();
  }

  private static String getFileConfig(String propertyName) {
    try {
      URL propsUrl = ConfigNodeDescriptor.getPropsUrl(CommonConfig.SYSTEM_CONFIG_NAME);
      if (propsUrl == null) {
        return null;
      }
      Properties properties = new Properties();
      try (InputStream inputStream = propsUrl.openStream()) {
        properties.load(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
      }
      return properties.getProperty(propertyName);
    } catch (Exception e) {
      LOGGER.debug("Failed to read auto activation config from system config file", e);
      return null;
    }
  }
}
