/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.rest.protocol.filter;

import org.apache.iotdb.db.conf.rest.IoTDBRestServiceConfig;
import org.apache.iotdb.db.conf.rest.IoTDBRestServiceDescriptor;
import org.apache.iotdb.rest.i18n.RestMessages;
import org.apache.iotdb.rest.protocol.model.ExecutionStatus;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class RequestSizeLimitFilterTest {

  private IoTDBRestServiceConfig config;
  private long originalMaxBodySize;
  private long originalMaxTotalConcurrentRequestBodySize;

  @Before
  public void setUp() {
    config = IoTDBRestServiceDescriptor.getInstance().getConfig();
    originalMaxBodySize = config.getRestMaxRequestBodySizeInBytes();
    originalMaxTotalConcurrentRequestBodySize =
        config.getRestMaxTotalConcurrentRequestBodySizeInBytes();
    RestRequestBodyMemoryManager.resetForTest();
  }

  @After
  public void tearDown() {
    config.setRestMaxRequestBodySizeInBytes(originalMaxBodySize);
    config.setRestMaxTotalConcurrentRequestBodySizeInBytes(
        originalMaxTotalConcurrentRequestBodySize);
    RestRequestBodyMemoryManager.resetForTest();
  }

  @Test
  public void testAbortContentLengthOverLimit() {
    config.setRestMaxRequestBodySizeInBytes(4);
    config.setRestMaxTotalConcurrentRequestBodySizeInBytes(10);
    TestRequestContext context = TestRequestContext.withLength(5);

    new RequestSizeLimitFilter().filter(context.proxy());

    assertPayloadTooLarge(context.abortedResponse(), 4);
  }

  @Test
  public void testRejectStreamOverLimit() throws IOException {
    config.setRestMaxRequestBodySizeInBytes(4);
    config.setRestMaxTotalConcurrentRequestBodySizeInBytes(10);
    TestRequestContext context =
        TestRequestContext.withStream("12345".getBytes(StandardCharsets.UTF_8));

    new RequestSizeLimitFilter().filter(context.proxy());

    assertNull(context.abortedResponse());
    WebApplicationException exception =
        assertThrows(WebApplicationException.class, () -> context.entityStream().readAllBytes());
    assertPayloadTooLarge(exception.getResponse(), 4);
    assertEquals(0, RestRequestBodyMemoryManager.getReservedMemoryInBytes());
  }

  @Test
  public void testAbortContentLengthOverMemoryLimit() {
    config.setRestMaxRequestBodySizeInBytes(10);
    config.setRestMaxTotalConcurrentRequestBodySizeInBytes(4);
    TestRequestContext context = TestRequestContext.withLength(5);

    new RequestSizeLimitFilter().filter(context.proxy());

    assertMemoryQuotaExceeded(context.abortedResponse(), 4);
    assertEquals(0, RestRequestBodyMemoryManager.getReservedMemoryInBytes());
  }

  @Test
  public void testRejectStreamOverMemoryLimit() throws IOException {
    config.setRestMaxRequestBodySizeInBytes(10);
    config.setRestMaxTotalConcurrentRequestBodySizeInBytes(4);
    TestRequestContext context =
        TestRequestContext.withStream("12345".getBytes(StandardCharsets.UTF_8));

    new RequestSizeLimitFilter().filter(context.proxy());

    assertNull(context.abortedResponse());
    WebApplicationException exception =
        assertThrows(WebApplicationException.class, () -> context.entityStream().readAllBytes());
    assertMemoryQuotaExceeded(exception.getResponse(), 4);
    assertEquals(0, RestRequestBodyMemoryManager.getReservedMemoryInBytes());
  }

  @Test
  public void testDisabledMemoryLimitDoesNotReserveMemory() throws IOException {
    config.setRestMaxRequestBodySizeInBytes(10);
    config.setRestMaxTotalConcurrentRequestBodySizeInBytes(-1);
    TestRequestContext context =
        TestRequestContext.withStream("12345".getBytes(StandardCharsets.UTF_8));

    new RequestSizeLimitFilter().filter(context.proxy());

    assertNull(context.abortedResponse());
    assertEquals(
        "12345", new String(context.entityStream().readAllBytes(), StandardCharsets.UTF_8));
    assertEquals(0, RestRequestBodyMemoryManager.getReservedMemoryInBytes());
  }

  @Test
  public void testReleaseMemoryOnResponse() {
    config.setRestMaxRequestBodySizeInBytes(10);
    config.setRestMaxTotalConcurrentRequestBodySizeInBytes(5);
    TestRequestContext context = TestRequestContext.withLength(4);
    RequestSizeLimitFilter filter = new RequestSizeLimitFilter();

    filter.filter(context.proxy());

    assertNull(context.abortedResponse());
    assertEquals(4, RestRequestBodyMemoryManager.getReservedMemoryInBytes());

    new RequestBodyMemoryReleaseFilter().filter(context.proxy(), responseContext());

    assertEquals(0, RestRequestBodyMemoryManager.getReservedMemoryInBytes());
  }

  private static void assertPayloadTooLarge(Response response, long maxBodySize) {
    assertEquals(413, response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMediaType());
    assertTrue(response.getEntity() instanceof ExecutionStatus);

    ExecutionStatus status = (ExecutionStatus) response.getEntity();
    assertEquals(Integer.valueOf(413), status.getCode());
    assertEquals(
        String.format(
            RestMessages
                .MESSAGE_REST_REQUEST_BODY_EXCEEDS_LIMIT_ARG_BYTES_USE_SET_CONFIGURATION_REST_MAX_REQUEST_BODY_SIZE_IN_BYTES_BYTES_TO_INCREASE_IT_424392C6,
            maxBodySize),
        status.getMessage());
  }

  private static void assertMemoryQuotaExceeded(Response response, long memoryLimit) {
    assertEquals(503, response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMediaType());
    assertTrue(response.getEntity() instanceof ExecutionStatus);

    ExecutionStatus status = (ExecutionStatus) response.getEntity();
    assertEquals(Integer.valueOf(503), status.getCode());
    assertEquals(
        String.format(
            RestMessages
                .MESSAGE_REST_REQUEST_BODY_MEMORY_QUOTA_EXCEEDS_LIMIT_ARG_BYTES_USE_SET_CONFIGURATION_REST_MAX_TOTAL_CONCURRENT_REQUEST_BODY_SIZE_IN_BYTES_BYTES_TO_INCREASE_IT_F07B9DDD,
            memoryLimit),
        status.getMessage());
  }

  private static ContainerResponseContext responseContext() {
    return (ContainerResponseContext)
        Proxy.newProxyInstance(
            ContainerResponseContext.class.getClassLoader(),
            new Class<?>[] {ContainerResponseContext.class},
            (proxy, method, args) -> {
              throw new UnsupportedOperationException(method.getName());
            });
  }

  private static class TestRequestContext {

    private final int contentLength;
    private final AtomicReference<InputStream> entityStream;
    private final AtomicReference<Response> abortedResponse = new AtomicReference<>();
    private final Map<String, Object> properties = new HashMap<>();

    private TestRequestContext(int contentLength, InputStream entityStream) {
      this.contentLength = contentLength;
      this.entityStream = new AtomicReference<>(entityStream);
    }

    private static TestRequestContext withLength(int contentLength) {
      return new TestRequestContext(contentLength, InputStream.nullInputStream());
    }

    private static TestRequestContext withStream(byte[] body) {
      return new TestRequestContext(-1, new ByteArrayInputStream(body));
    }

    private ContainerRequestContext proxy() {
      return (ContainerRequestContext)
          Proxy.newProxyInstance(
              ContainerRequestContext.class.getClassLoader(),
              new Class<?>[] {ContainerRequestContext.class},
              (proxy, method, args) -> {
                switch (method.getName()) {
                  case "getLength":
                    return contentLength;
                  case "getEntityStream":
                    return entityStream.get();
                  case "setEntityStream":
                    entityStream.set((InputStream) args[0]);
                    return null;
                  case "abortWith":
                    abortedResponse.set((Response) args[0]);
                    return null;
                  case "getProperty":
                    return properties.get((String) args[0]);
                  case "setProperty":
                    properties.put((String) args[0], args[1]);
                    return null;
                  case "removeProperty":
                    properties.remove((String) args[0]);
                    return null;
                  default:
                    throw new UnsupportedOperationException(method.getName());
                }
              });
    }

    private InputStream entityStream() {
      return entityStream.get();
    }

    private Response abortedResponse() {
      return abortedResponse.get();
    }
  }
}
