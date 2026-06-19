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

import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TTransportException;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.Enumeration;
import java.util.Locale;
import java.util.stream.Stream;

public final class RpcSslUtils {

  private static final String DEFAULT_PROTOCOL = "TLS";
  private static final String PKCS12_STORE_TYPE = "PKCS12";
  private static final String JKS_STORE_TYPE = "JKS";

  private static volatile String protocol = DEFAULT_PROTOCOL;

  private RpcSslUtils() {}

  public static void configure(String sslProtocol) {
    protocol = normalizeProtocol(sslProtocol);
  }

  public static TSSLTransportFactory.TSSLTransportParameters createTSSLTransportParameters() {
    return createTSSLTransportParameters(protocol);
  }

  public static TSSLTransportFactory.TSSLTransportParameters createTSSLTransportParameters(
      String sslProtocol) {
    return new TSSLTransportFactory.TSSLTransportParameters(normalizeProtocol(sslProtocol), null);
  }

  public static void setKeyStore(
      TSSLTransportFactory.TSSLTransportParameters params, String keyStorePath, String keyStorePwd)
      throws TTransportException {
    try {
      params.setKeyStore(
          keyStorePath,
          keyStorePwd,
          KeyManagerFactory.getDefaultAlgorithm(),
          detectStoreType(keyStorePath, keyStorePwd));
    } catch (GeneralSecurityException | IOException e) {
      throw new TTransportException(e);
    }
  }

  public static void setTrustStore(
      TSSLTransportFactory.TSSLTransportParameters params,
      String trustStorePath,
      String trustStorePwd)
      throws TTransportException {
    try {
      params.setTrustStore(
          trustStorePath,
          trustStorePwd,
          TrustManagerFactory.getDefaultAlgorithm(),
          detectStoreType(trustStorePath, trustStorePwd));
    } catch (GeneralSecurityException | IOException e) {
      throw new TTransportException(e);
    }
  }

  public static SSLContext createSSLContext(
      String keyStorePath,
      String keyStorePassword,
      String trustStorePath,
      String trustStorePassword)
      throws GeneralSecurityException, IOException {
    return createSSLContext(
        keyStorePath, keyStorePassword, trustStorePath, trustStorePassword, protocol);
  }

  public static SSLContext createSSLContext(
      String keyStorePath,
      String keyStorePassword,
      String trustStorePath,
      String trustStorePassword,
      String sslProtocol)
      throws GeneralSecurityException, IOException {
    SSLContext context = SSLContext.getInstance(normalizeProtocol(sslProtocol));
    KeyManager[] keyManagers =
        hasText(keyStorePath) ? loadKeyManagers(keyStorePath, keyStorePassword) : null;
    TrustManager[] trustManagers =
        hasText(trustStorePath) ? loadTrustManagers(trustStorePath, trustStorePassword) : null;
    context.init(keyManagers, trustManagers, null);
    return context;
  }

  public static KeyManager[] createKeyManagers(String keyStorePath, String keyStorePassword)
      throws GeneralSecurityException, IOException {
    return loadKeyManagers(keyStorePath, keyStorePassword);
  }

  public static TrustManager[] createTrustManagers(String trustStorePath, String trustStorePassword)
      throws GeneralSecurityException, IOException {
    return loadTrustManagers(trustStorePath, trustStorePassword);
  }

  public static String getProtocol() {
    return protocol;
  }

  public static void validateKeyStore(String keyStorePath, String keyStorePassword)
      throws TTransportException {
    validateStore(keyStorePath, keyStorePassword);
  }

  public static void validateTrustStore(String trustStorePath, String trustStorePassword)
      throws TTransportException {
    validateStore(trustStorePath, trustStorePassword);
  }

  private static KeyManager[] loadKeyManagers(String keyStorePath, String keyStorePassword)
      throws GeneralSecurityException, IOException {
    KeyStore keyStore = loadStore(keyStorePath, keyStorePassword);
    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    kmf.init(keyStore, toPassword(keyStorePassword));
    return kmf.getKeyManagers();
  }

  private static TrustManager[] loadTrustManagers(String trustStorePath, String trustStorePassword)
      throws GeneralSecurityException, IOException {
    KeyStore trustStore = loadStore(trustStorePath, trustStorePassword);
    TrustManagerFactory tmf =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(trustStore);
    return tmf.getTrustManagers();
  }

  private static String detectStoreType(String storePath, String storePassword)
      throws GeneralSecurityException, IOException {
    return loadStore(storePath, storePassword).getType();
  }

  private static KeyStore loadStore(String storePath, String storePassword)
      throws GeneralSecurityException, IOException {
    Exception lastException = null;
    for (String storeType : storeTypeCandidates()) {
      try {
        return loadStore(storePath, storePassword, storeType);
      } catch (AccessDeniedException | FileNotFoundException e) {
        throw e;
      } catch (GeneralSecurityException | IOException e) {
        lastException = e;
      }
    }
    if (lastException instanceof GeneralSecurityException) {
      throw (GeneralSecurityException) lastException;
    }
    if (lastException instanceof IOException) {
      throw (IOException) lastException;
    }
    throw new IOException("No supported keystore or truststore type is available");
  }

  private static KeyStore loadStore(String storePath, String storePassword, String storeType)
      throws GeneralSecurityException, IOException {
    KeyStore store = KeyStore.getInstance(storeType);
    try (InputStream inputStream = Files.newInputStream(Path.of(storePath))) {
      store.load(inputStream, toPassword(storePassword));
    } catch (AccessDeniedException e) {
      throw new AccessDeniedException("Failed to load keystore or truststore file");
    } catch (FileNotFoundException | NoSuchFileException e) {
      throw new FileNotFoundException("keystore or truststore file not found: " + storePath);
    }
    return store;
  }

  private static void validateStore(String storePath, String storePassword)
      throws TTransportException {
    try {
      KeyStore store = loadStore(storePath, storePassword);
      Enumeration<String> aliases = store.aliases();
      while (aliases.hasMoreElements()) {
        X509Certificate cert = (X509Certificate) store.getCertificate(aliases.nextElement());
        if (cert != null) {
          cert.checkValidity();
        }
      }
    } catch (Exception e) {
      throw new TTransportException(e);
    }
  }

  private static char[] toPassword(String password) {
    return password == null ? null : password.toCharArray();
  }

  public static String normalizeProtocol(String value) {
    String trimmed = trimToEmpty(value);
    return trimmed.isEmpty() ? DEFAULT_PROTOCOL : trimmed;
  }

  private static String trimToEmpty(String value) {
    return value == null ? "" : value.trim();
  }

  private static boolean hasText(String value) {
    return value != null && !value.trim().isEmpty();
  }

  private static String[] storeTypeCandidates() {
    return Stream.of(KeyStore.getDefaultType(), PKCS12_STORE_TYPE, JKS_STORE_TYPE)
        .map(String::trim)
        .map(s -> s.toUpperCase(Locale.ROOT))
        .filter(s -> !s.isEmpty())
        .distinct()
        .toArray(String[]::new);
  }
}
