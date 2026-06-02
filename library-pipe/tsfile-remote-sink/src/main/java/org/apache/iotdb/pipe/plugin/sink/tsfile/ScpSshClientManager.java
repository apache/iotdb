/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.pipe.plugin.sink.tsfile;

import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.session.ClientSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

class ScpSshClientManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ScpSshClientManager.class);

  private static final long CONNECT_TIMEOUT_MS = 10000L;
  private static final Duration SESSION_EVICTION_INTERVAL = Duration.ofMinutes(1);
  static final int DEFAULT_MAX_SESSIONS_PER_KEY = 16;

  private static volatile SshClient client;
  private static final ConcurrentHashMap<ScpConnectionKey, AtomicInteger> SESSION_POOL_REFERENCES =
      new ConcurrentHashMap<>();
  private static final AtomicInteger GLOBAL_SESSION_POOL_REFERENCES = new AtomicInteger(0);
  private static final GenericKeyedObjectPool<ScpConnectionKey, ClientSession> SESSION_POOL =
      createSessionPool();

  private ScpSshClientManager() {}

  static SshClient getClient() throws IOException {
    SshClient currentClient = client;
    if (currentClient == null || !currentClient.isStarted()) {
      synchronized (ScpSshClientManager.class) {
        currentClient = client;
        if (currentClient == null || !currentClient.isStarted()) {
          currentClient = createClient();
          client = currentClient;
          LOGGER.info("Created static shared SCP SSH client");
        }
      }
    }
    return currentClient;
  }

  private static SshClient createClient() throws IOException {
    try {
      System.setProperty("org.apache.sshd.security.provider.BC.enabled", "false");
      final SshClient sshClient = SshClient.setUpDefaultClient();
      sshClient.start();
      return sshClient;
    } catch (Exception e) {
      throw new IOException("Failed to create shared SCP SSH client", e);
    }
  }

  private static GenericKeyedObjectPool<ScpConnectionKey, ClientSession> createSessionPool() {
    final GenericKeyedObjectPoolConfig<ClientSession> config = new GenericKeyedObjectPoolConfig<>();
    config.setMaxTotal(-1);
    config.setMaxTotalPerKey(DEFAULT_MAX_SESSIONS_PER_KEY);
    config.setMaxIdlePerKey(DEFAULT_MAX_SESSIONS_PER_KEY);
    config.setTestOnBorrow(true);
    config.setTestOnReturn(true);
    config.setTestWhileIdle(true);
    config.setTimeBetweenEvictionRuns(SESSION_EVICTION_INTERVAL);
    config.setBlockWhenExhausted(true);
    return new GenericKeyedObjectPool<>(new ScpSessionFactory(), config);
  }

  static ScpSessionPool acquireSessionPool(
      final String host,
      final int port,
      final String user,
      final String password,
      final long sessionKeepAliveSeconds) {
    final ScpConnectionKey key =
        new ScpConnectionKey(host, port, user, password, sessionKeepAliveSeconds);
    synchronized (ScpSshClientManager.class) {
      SESSION_POOL_REFERENCES.compute(
          key,
          (ignored, referenceCount) -> {
            if (referenceCount == null) {
              LOGGER.info("Created static shared SCP session pool for {}", key);
              return new AtomicInteger(1);
            }
            referenceCount.incrementAndGet();
            return referenceCount;
          });
      GLOBAL_SESSION_POOL_REFERENCES.incrementAndGet();
    }
    return new ScpSessionPool(key);
  }

  static final class ScpSessionPool {

    private final ScpConnectionKey key;
    private final AtomicBoolean released = new AtomicBoolean(false);

    private ScpSessionPool(final ScpConnectionKey key) {
      this.key = key;
    }

    ClientSession borrowSession() throws IOException {
      try {
        return SESSION_POOL.borrowObject(key);
      } catch (final IOException e) {
        throw e;
      } catch (final Exception e) {
        throw new IOException("Failed to borrow SCP session for " + key, e);
      }
    }

    void recycleSession(final ClientSession session) {
      if (session == null) {
        return;
      }
      try {
        SESSION_POOL.returnObject(key, session);
      } catch (final Exception e) {
        closeSessionQuietly(session);
        LOGGER.warn("Failed to return SCP session for {}", key, e);
      }
    }

    void invalidateSession(final ClientSession session) {
      if (session == null) {
        return;
      }
      try {
        SESSION_POOL.invalidateObject(key, session);
      } catch (final Exception e) {
        closeSessionQuietly(session);
        LOGGER.warn("Failed to invalidate SCP session for {}", key, e);
      }
    }

    void invalidate() {
      SESSION_POOL.clear(key);
    }

    void release() {
      if (!released.compareAndSet(false, true)) {
        return;
      }
      synchronized (ScpSshClientManager.class) {
        SESSION_POOL_REFERENCES.computeIfPresent(
            key,
            (ignored, referenceCount) -> {
              if (referenceCount.decrementAndGet() > 0) {
                return referenceCount;
              }
              SESSION_POOL.clear(key);
              LOGGER.info("Closed static shared SCP session pool for {}", key);
              return null;
            });
        if (GLOBAL_SESSION_POOL_REFERENCES.decrementAndGet() == 0) {
          stopClient();
        }
      }
    }
  }

  private static final class ScpSessionFactory
      extends BaseKeyedPooledObjectFactory<ScpConnectionKey, ClientSession> {

    @Override
    public ClientSession create(final ScpConnectionKey key) throws Exception {
      final ClientSession session =
          getClient().connect(key.user, key.host, key.port).verify(CONNECT_TIMEOUT_MS).getSession();
      session.addPasswordIdentity(key.password != null ? key.password : "");
      session.auth().verify(CONNECT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      return session;
    }

    @Override
    public PooledObject<ClientSession> wrap(final ClientSession session) {
      return new DefaultPooledObject<>(session);
    }

    @Override
    public boolean validateObject(
        final ScpConnectionKey key, final PooledObject<ClientSession> pooledSession) {
      final ClientSession session = pooledSession.getObject();
      return session != null
          && session.isOpen()
          && (key.sessionKeepAliveSeconds <= 0
              || pooledSession.getIdleTimeMillis()
                  < TimeUnit.SECONDS.toMillis(key.sessionKeepAliveSeconds));
    }

    @Override
    public void destroyObject(
        final ScpConnectionKey key, final PooledObject<ClientSession> pooledSession) {
      closeSessionQuietly(pooledSession.getObject());
    }
  }

  private static final class ScpConnectionKey {

    private final String host;
    private final int port;
    private final String user;
    private final String password;
    private final long sessionKeepAliveSeconds;

    private ScpConnectionKey(
        final String host,
        final int port,
        final String user,
        final String password,
        final long sessionKeepAliveSeconds) {
      this.host = host;
      this.port = port;
      this.user = user;
      this.password = password;
      this.sessionKeepAliveSeconds = sessionKeepAliveSeconds;
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof ScpConnectionKey)) {
        return false;
      }
      final ScpConnectionKey that = (ScpConnectionKey) obj;
      return port == that.port
          && sessionKeepAliveSeconds == that.sessionKeepAliveSeconds
          && Objects.equals(host, that.host)
          && Objects.equals(user, that.user)
          && Objects.equals(password, that.password);
    }

    @Override
    public int hashCode() {
      return Objects.hash(host, port, user, password, sessionKeepAliveSeconds);
    }

    @Override
    public String toString() {
      return user + "@" + host + ":" + port;
    }
  }

  private static void closeSessionQuietly(final ClientSession session) {
    if (session != null) {
      session.close(true);
    }
  }

  private static void stopClient() {
    synchronized (ScpSshClientManager.class) {
      if (GLOBAL_SESSION_POOL_REFERENCES.get() > 0 || client == null) {
        return;
      }
      try {
        if (client.isStarted()) {
          client.stop();
          LOGGER.info("Stopped static shared SCP SSH client");
        }
      } catch (final Exception e) {
        LOGGER.warn("Failed to stop static shared SCP SSH client", e);
      } finally {
        client = null;
      }
    }
  }
}
