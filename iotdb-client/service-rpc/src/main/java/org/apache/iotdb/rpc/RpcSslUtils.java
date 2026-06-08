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
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.file.AccessDeniedException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.Provider;
import java.security.Security;
import java.security.cert.X509Certificate;
import java.util.Enumeration;
import java.util.Locale;
import java.util.stream.Stream;

public final class RpcSslUtils {

  private static final String DEFAULT_PROTOCOL = "TLS";
  private static final String PKCS12_STORE_TYPE = "PKCS12";
  private static final String JKS_STORE_TYPE = "JKS";

  private static volatile SslConfig config = new SslConfig(DEFAULT_PROTOCOL, "");

  private RpcSslUtils() {}

  public static void configure(String protocol, String providerClass) {
    config = newSslConfig(protocol, providerClass);
  }

  public static void ensureProvider(String protocol, String providerClass)
      throws TTransportException {
    ensureProvider(newSslConfig(protocol, providerClass));
  }

  public static TSSLTransportFactory.TSSLTransportParameters createTSSLTransportParameters()
      throws TTransportException {
    SslConfig current = config;
    ensureProvider(current);
    return createTSSLTransportParameters(current);
  }

  public static TSSLTransportFactory.TSSLTransportParameters createTSSLTransportParameters(
      String protocol, String providerClass) throws TTransportException {
    SslConfig current = newSslConfig(protocol, providerClass);
    ensureProvider(current);
    return createTSSLTransportParameters(current);
  }

  private static TSSLTransportFactory.TSSLTransportParameters createTSSLTransportParameters(
      SslConfig current) {
    return new TSSLTransportFactory.TSSLTransportParameters(current.protocol, null);
  }

  public static void setKeyStore(
      TSSLTransportFactory.TSSLTransportParameters params, String keyStorePath, String keyStorePwd)
      throws TTransportException {
    setKeyStore(params, keyStorePath, keyStorePwd, config);
  }

  public static void setKeyStore(
      TSSLTransportFactory.TSSLTransportParameters params,
      String keyStorePath,
      String keyStorePwd,
      String protocol,
      String providerClass)
      throws TTransportException {
    setKeyStore(params, keyStorePath, keyStorePwd, newSslConfig(protocol, providerClass));
  }

  private static void setKeyStore(
      TSSLTransportFactory.TSSLTransportParameters params,
      String keyStorePath,
      String keyStorePwd,
      SslConfig current)
      throws TTransportException {
    try {
      ensureProvider(current);
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
    setTrustStore(params, trustStorePath, trustStorePwd, config);
  }

  public static void setTrustStore(
      TSSLTransportFactory.TSSLTransportParameters params,
      String trustStorePath,
      String trustStorePwd,
      String protocol,
      String providerClass)
      throws TTransportException {
    setTrustStore(params, trustStorePath, trustStorePwd, newSslConfig(protocol, providerClass));
  }

  private static void setTrustStore(
      TSSLTransportFactory.TSSLTransportParameters params,
      String trustStorePath,
      String trustStorePwd,
      SslConfig current)
      throws TTransportException {
    try {
      ensureProvider(current);
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
      throws GeneralSecurityException, IOException, TTransportException {
    return createSSLContext(
        keyStorePath, keyStorePassword, trustStorePath, trustStorePassword, config);
  }

  public static SSLContext createSSLContext(
      String keyStorePath,
      String keyStorePassword,
      String trustStorePath,
      String trustStorePassword,
      String protocol,
      String providerClass)
      throws GeneralSecurityException, IOException, TTransportException {
    return createSSLContext(
        keyStorePath,
        keyStorePassword,
        trustStorePath,
        trustStorePassword,
        newSslConfig(protocol, providerClass));
  }

  private static SSLContext createSSLContext(
      String keyStorePath,
      String keyStorePassword,
      String trustStorePath,
      String trustStorePassword,
      SslConfig current)
      throws GeneralSecurityException, IOException, TTransportException {
    ensureProvider(current);
    SSLContext context = newSSLContext(current);
    KeyManager[] keyManagers =
        hasText(keyStorePath) ? loadKeyManagers(keyStorePath, keyStorePassword) : null;
    TrustManager[] trustManagers =
        hasText(trustStorePath) ? loadTrustManagers(trustStorePath, trustStorePassword) : null;
    context.init(keyManagers, trustManagers, null);
    return context;
  }

  public static KeyManager[] createKeyManagers(String keyStorePath, String keyStorePassword)
      throws GeneralSecurityException, IOException, TTransportException {
    ensureProvider(config);
    return loadKeyManagers(keyStorePath, keyStorePassword);
  }

  public static TrustManager[] createTrustManagers(String trustStorePath, String trustStorePassword)
      throws GeneralSecurityException, IOException, TTransportException {
    ensureProvider(config);
    return loadTrustManagers(trustStorePath, trustStorePassword);
  }

  public static String getProtocol() {
    return config.protocol;
  }

  public static boolean hasConfiguredProvider() {
    return hasText(config.providerClass);
  }

  public static String getSSLContextProviderName() throws TTransportException {
    ensureProvider(config);
    try {
      return newSSLContext(config).getProvider().getName();
    } catch (GeneralSecurityException e) {
      throw new TTransportException("Failed to initialize SSL context", e);
    }
  }

  public static String[] getEnabledCipherSuites() throws TTransportException {
    ensureProvider(config);
    try {
      SSLContext context = newSSLContext(config);
      context.init(null, null, null);
      SSLEngine engine = context.createSSLEngine();
      return engine.getEnabledCipherSuites();
    } catch (GeneralSecurityException e) {
      throw new TTransportException("Failed to initialize SSL context", e);
    }
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

  private static SSLContext newSSLContext(SslConfig current) throws GeneralSecurityException {
    return SSLContext.getInstance(current.protocol);
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
    try (FileInputStream fis = new FileInputStream(storePath)) {
      store.load(fis, toPassword(storePassword));
    } catch (AccessDeniedException e) {
      throw new AccessDeniedException("Failed to load keystore or truststore file");
    } catch (FileNotFoundException e) {
      throw new FileNotFoundException("keystore or truststore file not found: " + storePath);
    }
    return store;
  }

  private static void validateStore(String storePath, String storePassword)
      throws TTransportException {
    try {
      ensureProvider(config);
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

  private static synchronized void ensureProvider(SslConfig current) throws TTransportException {
    if (!hasText(current.providerClass)) {
      return;
    }
    try {
      String[] providerClassNames = splitProviderClasses(current.providerClass);
      for (int i = providerClassNames.length - 1; i >= 0; i--) {
        String providerClassName = providerClassNames[i];
        Class<?> providerClass = Class.forName(providerClassName);
        Constructor<?> constructor = providerClass.getDeclaredConstructor();
        constructor.setAccessible(true);
        Provider provider = (Provider) constructor.newInstance();
        Provider firstProvider = Security.getProviders()[0];
        if (!provider.getName().equals(firstProvider.getName())) {
          Security.removeProvider(provider.getName());
          Security.insertProviderAt(provider, 1);
        }
      }
    } catch (Exception e) {
      throw new TTransportException("Failed to initialize SSL provider", e);
    }
  }

  private static char[] toPassword(String password) {
    return password == null ? null : password.toCharArray();
  }

  private static String trimToDefault(String value, String defaultValue) {
    String trimmed = trimToEmpty(value);
    return trimmed.isEmpty() ? defaultValue : trimmed;
  }

  private static String trimToEmpty(String value) {
    return value == null ? "" : value.trim();
  }

  private static boolean hasText(String value) {
    return value != null && !value.trim().isEmpty();
  }

  private static String[] splitProviderClasses(String providerClasses) {
    return splitCommaSeparated(providerClasses);
  }

  private static String[] splitCommaSeparated(String value) {
    return Stream.of(value.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .toArray(String[]::new);
  }

  private static String[] storeTypeCandidates() {
    return Stream.of(KeyStore.getDefaultType(), PKCS12_STORE_TYPE, JKS_STORE_TYPE)
        .map(String::trim)
        .map(s -> s.toUpperCase(Locale.ROOT))
        .filter(s -> !s.isEmpty())
        .distinct()
        .toArray(String[]::new);
  }

  private static SslConfig newSslConfig(String protocol, String providerClass) {
    return new SslConfig(trimToDefault(protocol, DEFAULT_PROTOCOL), trimToEmpty(providerClass));
  }

  private static final class SslConfig {
    private final String protocol;
    private final String providerClass;

    private SslConfig(String protocol, String providerClass) {
      this.protocol = protocol;
      this.providerClass = providerClass;
    }
  }
}
