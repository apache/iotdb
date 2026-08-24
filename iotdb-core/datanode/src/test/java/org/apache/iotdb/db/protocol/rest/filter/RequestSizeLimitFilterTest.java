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

package org.apache.iotdb.db.protocol.rest.filter;

import org.apache.iotdb.db.conf.rest.IoTDBRestServiceConfig;
import org.apache.iotdb.db.conf.rest.IoTDBRestServiceDescriptor;
import org.apache.iotdb.db.protocol.rest.model.ExecutionStatus;

import org.junit.After;
import org.junit.Assert;
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
    TestRequestContext context = TestRequestContext.withStream("12345");

    new RequestSizeLimitFilter().filter(context.proxy());

    Assert.assertNull(context.abortedResponse());
    try {
      consume(context.entityStream());
      Assert.fail("Expected WebApplicationException");
    } catch (WebApplicationException e) {
      assertPayloadTooLarge(e.getResponse(), 4);
    }
    Assert.assertEquals(0, RestRequestBodyMemoryManager.getReservedMemoryInBytes());
  }

  @Test
  public void testAbortContentLengthOverMemoryLimit() {
    config.setRestMaxRequestBodySizeInBytes(10);
    config.setRestMaxTotalConcurrentRequestBodySizeInBytes(4);
    TestRequestContext context = TestRequestContext.withLength(5);

    new RequestSizeLimitFilter().filter(context.proxy());

    assertMemoryQuotaExceeded(context.abortedResponse(), 4);
    Assert.assertEquals(0, RestRequestBodyMemoryManager.getReservedMemoryInBytes());
  }

  @Test
  public void testRejectConcurrentRequestsOverMemoryLimit() {
    config.setRestMaxRequestBodySizeInBytes(10);
    config.setRestMaxTotalConcurrentRequestBodySizeInBytes(4);
    TestRequestContext firstContext = TestRequestContext.withLength(3);
    TestRequestContext secondContext = TestRequestContext.withLength(2);

    new RequestSizeLimitFilter().filter(firstContext.proxy());
    new RequestSizeLimitFilter().filter(secondContext.proxy());

    Assert.assertNull(firstContext.abortedResponse());
    assertMemoryQuotaExceeded(secondContext.abortedResponse(), 4);
    Assert.assertEquals(3, RestRequestBodyMemoryManager.getReservedMemoryInBytes());

    new RequestBodyMemoryReleaseFilter().filter(firstContext.proxy(), responseContext());

    Assert.assertEquals(0, RestRequestBodyMemoryManager.getReservedMemoryInBytes());
  }

  @Test
  public void testRejectStreamOverMemoryLimit() throws IOException {
    config.setRestMaxRequestBodySizeInBytes(10);
    config.setRestMaxTotalConcurrentRequestBodySizeInBytes(4);
    TestRequestContext context = TestRequestContext.withStream("12345");

    new RequestSizeLimitFilter().filter(context.proxy());

    Assert.assertNull(context.abortedResponse());
    try {
      consume(context.entityStream());
      Assert.fail("Expected WebApplicationException");
    } catch (WebApplicationException e) {
      assertMemoryQuotaExceeded(e.getResponse(), 4);
    }
    Assert.assertEquals(0, RestRequestBodyMemoryManager.getReservedMemoryInBytes());
  }

  @Test
  public void testRejectSkippedStreamOverLimit() throws IOException {
    config.setRestMaxRequestBodySizeInBytes(4);
    config.setRestMaxTotalConcurrentRequestBodySizeInBytes(10);
    TestRequestContext context = TestRequestContext.withStream("12345");

    new RequestSizeLimitFilter().filter(context.proxy());

    try {
      context.entityStream().skip(5);
      Assert.fail("Expected WebApplicationException");
    } catch (WebApplicationException e) {
      assertPayloadTooLarge(e.getResponse(), 4);
    }
    Assert.assertEquals(0, RestRequestBodyMemoryManager.getReservedMemoryInBytes());
  }

  @Test
  public void testReleaseMemoryOnResponse() {
    config.setRestMaxRequestBodySizeInBytes(10);
    config.setRestMaxTotalConcurrentRequestBodySizeInBytes(5);
    TestRequestContext context = TestRequestContext.withLength(4);

    new RequestSizeLimitFilter().filter(context.proxy());

    Assert.assertNull(context.abortedResponse());
    Assert.assertEquals(4, RestRequestBodyMemoryManager.getReservedMemoryInBytes());

    new RequestBodyMemoryReleaseFilter().filter(context.proxy(), responseContext());

    Assert.assertEquals(0, RestRequestBodyMemoryManager.getReservedMemoryInBytes());
  }

  @Test
  public void testDisabledLimitsDoNotWrapStream() {
    config.setRestMaxRequestBodySizeInBytes(-1);
    config.setRestMaxTotalConcurrentRequestBodySizeInBytes(-1);
    TestRequestContext context = TestRequestContext.withStream("12345");
    InputStream originalStream = context.entityStream();

    new RequestSizeLimitFilter().filter(context.proxy());

    Assert.assertSame(originalStream, context.entityStream());
    Assert.assertEquals(0, RestRequestBodyMemoryManager.getReservedMemoryInBytes());
  }

  private static void consume(InputStream inputStream) throws IOException {
    byte[] buffer = new byte[8];
    while (inputStream.read(buffer) != -1) {
      // consume the request body
    }
  }

  private static void assertPayloadTooLarge(Response response, long maxBodySize) {
    Assert.assertEquals(413, response.getStatus());
    Assert.assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMediaType());
    Assert.assertTrue(response.getEntity() instanceof ExecutionStatus);
    ExecutionStatus status = (ExecutionStatus) response.getEntity();
    Assert.assertEquals(Integer.valueOf(413), status.getCode());
    Assert.assertTrue(status.getMessage().contains(Long.toString(maxBodySize)));
  }

  private static void assertMemoryQuotaExceeded(Response response, long memoryLimit) {
    Assert.assertEquals(503, response.getStatus());
    Assert.assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMediaType());
    Assert.assertTrue(response.getEntity() instanceof ExecutionStatus);
    ExecutionStatus status = (ExecutionStatus) response.getEntity();
    Assert.assertEquals(Integer.valueOf(503), status.getCode());
    Assert.assertTrue(status.getMessage().contains(Long.toString(memoryLimit)));
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
      return new TestRequestContext(contentLength, new ByteArrayInputStream(new byte[0]));
    }

    private static TestRequestContext withStream(String body) {
      return new TestRequestContext(
          -1, new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8)));
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
