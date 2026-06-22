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

package org.apache.iotdb.rpc;

import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransportException;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.nio.file.AccessDeniedException;
import java.security.KeyStore;

/**
 * TNonblockingSocket's constructor declares a TTransportException for compatibility, but this code
 * path is not expected to throw one. See https://issues.apache.org/jira/browse/THRIFT-5412.
 */
public class TNonblockingTransportWrapper {

  public static TNonblockingTransport wrap(String host, int port) throws IOException {
    try {
      return new TNonblockingSocket(host, port);
    } catch (TTransportException e) {
      // never happen
      return null;
    }
  }

  public static TNonblockingTransport wrap(String host, int port, int timeout) throws IOException {
    try {
      return new TNonblockingSocket(host, port, timeout);
    } catch (TTransportException e) {
      // never happen
      return null;
    }
  }

  public static TNonblockingTransport wrap(SocketChannel socketChannel) throws IOException {
    try {
      return new TNonblockingSocket(socketChannel);
    } catch (TTransportException e) {
      // never happen
      return null;
    }
  }

  public static TNonblockingTransport wrap(
      String host,
      int port,
      int timeout,
      String keyStore,
      String keyStorePwd,
      String trustStore,
      String trustStorePwd) {
    try {
      SSLContext sslContext = createSSLContext(keyStore, keyStorePwd, trustStore, trustStorePwd);
      return new TNonblockingSSLSocket(host, port, timeout, sslContext);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static SSLContext createSSLContext(
      String keystorePath,
      String keystorePassword,
      String truststorePath,
      String truststorePassword)
      throws Exception {

    KeyManagerFactory kmf = null;
    TrustManagerFactory tmf = null;

    SSLContext ctx = SSLContext.getInstance("TLS");
    if (keystorePath != null && keystorePassword != null) {
      KeyStore keyStore = KeyStore.getInstance("JKS");
      try (FileInputStream fis = new FileInputStream(keystorePath)) {
        keyStore.load(fis, keystorePassword.toCharArray());
      } catch (AccessDeniedException e) {
        throw new AccessDeniedException("Failed to load keystore file");
      } catch (FileNotFoundException e) {
        throw new FileNotFoundException("keystore file not found");
      }
      kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      kmf.init(keyStore, keystorePassword.toCharArray());
    }

    if (truststorePath != null && truststorePassword != null) {
      KeyStore trustStore = KeyStore.getInstance("JKS");
      try (FileInputStream fis = new FileInputStream(truststorePath)) {
        trustStore.load(fis, truststorePassword.toCharArray());
      } catch (AccessDeniedException e) {
        throw new AccessDeniedException("Failed to load truststore file");
      } catch (FileNotFoundException e) {
        throw new FileNotFoundException("truststore file not found");
      }
      tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(trustStore);
    }
    if (kmf != null && tmf != null) {
      ctx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
    } else if (kmf != null) {
      ctx.init(kmf.getKeyManagers(), null, null);
    } else if (tmf != null) {
      ctx.init(null, tmf.getTrustManagers(), null);
    }
    return ctx;
  }

  private TNonblockingTransportWrapper() {}
}
