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

package org.apache.iotdb.commons.udf.service;

import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.i18n.CommonMessages;
import org.apache.iotdb.commons.i18n.QueryMessages;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.utils.TestOnly;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class UDFClassLoaderManager implements IService {

  private static final Logger LOGGER = LoggerFactory.getLogger(UDFClassLoaderManager.class);

  private final String libRoot;

  private final ThreadLocal<Deque<QueryScope>> queryScopeStack;

  /**
   * activeClassLoader is used to load all classes under libRoot. libRoot may be updated before the
   * user executes CREATE FUNCTION or after the user executes DROP FUNCTION. Therefore, we need to
   * continuously maintain the activeClassLoader so that the classes it loads are always up-to-date.
   */
  private final AtomicReference<UDFClassLoader> activeClassLoader = new AtomicReference<>();

  public static final class QueryScope {
    private final String queryId;
    private final Set<UDFClassLoader> pinnedClassLoaders =
        Collections.newSetFromMap(new IdentityHashMap<>());
    private int activeActions;
    private boolean finalizeRequested;
    private boolean finalized;

    private QueryScope(String queryId) {
      this.queryId = queryId;
    }
  }

  private UDFClassLoaderManager(String libRoot) {
    this.libRoot = libRoot;
    LOGGER.info(CommonMessages.UDF_LIB_ROOT, libRoot);
    queryScopeStack = ThreadLocal.withInitial(ArrayDeque::new);
  }

  @TestOnly
  private UDFClassLoaderManager() throws IOException {
    this.libRoot = null;
    queryScopeStack = ThreadLocal.withInitial(ArrayDeque::new);
    activeClassLoader.set(new UDFClassLoader(""));
  }

  public QueryScope openQueryScope(String queryId) {
    return new QueryScope(Objects.requireNonNull(queryId));
  }

  /**
   * Bind an existing logical query scope to the current thread. Pins acquired by reflect() remain
   * owned by the scope after this method returns and must be released by finalizeUDFQuery().
   */
  public <T> T withQueryScope(QueryScope queryScope, Supplier<T> action) {
    Objects.requireNonNull(queryScope);
    Objects.requireNonNull(action);
    synchronized (this) {
      if (queryScope.finalizeRequested || queryScope.finalized) {
        throw new IllegalStateException(
            String.format(
                QueryMessages.EXCEPTION_UDF_QUERY_SCOPE_IS_CLOSED_ARG_7381D949,
                queryScope.queryId));
      }
      queryScope.activeActions++;
    }

    Deque<QueryScope> scopeStack = queryScopeStack.get();
    scopeStack.push(queryScope);
    try {
      return action.get();
    } finally {
      scopeStack.pop();
      if (scopeStack.isEmpty()) {
        queryScopeStack.remove();
      }
      synchronized (this) {
        queryScope.activeActions--;
        closeScopeIfPossible(queryScope);
      }
    }
  }

  /**
   * Pin the exact class loader that defined the reflected UDF class. Built-in functions loaded by
   * the parent class loader do not create a pin.
   */
  public void pinClassLoaderForCurrentQuery(Class<?> udfClass) {
    ClassLoader classLoader = udfClass.getClassLoader();
    if (!(classLoader instanceof UDFClassLoader)) {
      return;
    }

    Deque<QueryScope> scopeStack = queryScopeStack.get();
    QueryScope queryScope = scopeStack.peek();
    if (queryScope == null) {
      throw new IllegalStateException(
          String.format(
              QueryMessages.EXCEPTION_UDF_CLASSLOADER_REQUIRES_QUERY_SCOPE_ARG_A39FB39E,
              udfClass.getName()));
    }

    UDFClassLoader udfClassLoader = (UDFClassLoader) classLoader;
    synchronized (this) {
      if (queryScope.finalizeRequested || queryScope.finalized) {
        throw new IllegalStateException(
            String.format(
                QueryMessages
                    .EXCEPTION_FAILED_TO_PIN_UDF_CLASSLOADER_QUERY_SCOPE_ARG_CLASS_ARG_275E1E20,
                queryScope.queryId,
                udfClass.getName()));
      }
      if (queryScope.pinnedClassLoaders.contains(udfClassLoader)) {
        return;
      }
      if (!udfClassLoader.tryAcquire()) {
        throw new IllegalStateException(
            String.format(
                QueryMessages
                    .EXCEPTION_FAILED_TO_PIN_UDF_CLASSLOADER_QUERY_SCOPE_ARG_CLASS_ARG_275E1E20,
                queryScope.queryId,
                udfClass.getName()));
      }
      queryScope.pinnedClassLoaders.add(udfClassLoader);
    }
  }

  public synchronized void finalizeUDFQuery(QueryScope queryScope) {
    if (queryScope == null || queryScope.finalized) {
      return;
    }
    queryScope.finalizeRequested = true;
    closeScopeIfPossible(queryScope);
  }

  private void closeScopeIfPossible(QueryScope queryScope) {
    if (!queryScope.finalizeRequested || queryScope.activeActions > 0 || queryScope.finalized) {
      return;
    }
    for (UDFClassLoader classLoader : queryScope.pinnedClassLoaders) {
      releaseClassLoaderPin(queryScope.queryId, classLoader);
    }
    queryScope.pinnedClassLoaders.clear();
    queryScope.finalized = true;
  }

  private void releaseClassLoaderPin(Object queryId, UDFClassLoader classLoader) {
    try {
      classLoader.release();
    } catch (IOException e) {
      LOGGER.warn(
          CommonMessages.LOG_FAILED_CLOSE_UDFCLASSLOADER_QUERYID_ARG_BECAUSE_ARG_8B1C3739,
          queryId,
          e.toString());
    }
  }

  public UDFClassLoader createUDFClassLoader() throws IOException {
    return new UDFClassLoader(libRoot);
  }

  public void discardUDFClassLoader(UDFClassLoader classLoader) {
    try {
      classLoader.close();
    } catch (IOException e) {
      LOGGER.warn(
          CommonMessages.LOG_FAILED_CLOSE_UDFCLASSLOADER_QUERYID_ARG_BECAUSE_ARG_8B1C3739,
          System.identityHashCode(classLoader),
          e.toString());
    }
  }

  public synchronized void publishUDFClassLoader(
      UDFClassLoader newClassLoader, Runnable publishCatalog) {
    UDFClassLoader deprecatedClassLoader = activeClassLoader.get();
    publishCatalog.run();
    activeClassLoader.set(newClassLoader);
    if (deprecatedClassLoader != null) {
      try {
        deprecatedClassLoader.markAsDeprecated();
      } catch (IOException e) {
        LOGGER.warn(
            CommonMessages.LOG_FAILED_CLOSE_UDFCLASSLOADER_QUERYID_ARG_BECAUSE_ARG_8B1C3739,
            System.identityHashCode(deprecatedClassLoader),
            e.toString());
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // IService
  /////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public void start() throws StartupException {
    try {
      SystemFileFactory.INSTANCE.makeDirIfNecessary(libRoot);
      if (activeClassLoader.get() != null) {
        return;
      }
      UDFClassLoader newClassLoader = createUDFClassLoader();
      synchronized (this) {
        if (activeClassLoader.get() == null) {
          activeClassLoader.set(newClassLoader);
        } else {
          newClassLoader.close();
        }
      }
    } catch (IOException e) {
      throw new StartupException(this.getID().getName(), e.getMessage());
    }
  }

  @Override
  public void stop() {
    // nothing to do
  }

  @Override
  public ServiceType getID() {
    return ServiceType.UDF_CLASSLOADER_MANAGER_SERVICE;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // singleton instance holder
  /////////////////////////////////////////////////////////////////////////////////////////////////

  private static UDFClassLoaderManager INSTANCE = null;

  public static synchronized UDFClassLoaderManager setupAndGetInstance(String libRoot) {

    if (INSTANCE == null) {
      INSTANCE = new UDFClassLoaderManager(libRoot);
    }
    return INSTANCE;
  }

  public static UDFClassLoaderManager getInstance() {
    return INSTANCE;
  }

  @TestOnly
  public static synchronized UDFClassLoaderManager setupAndGetInstance() throws IOException {

    if (INSTANCE == null) {
      INSTANCE = new UDFClassLoaderManager();
    }
    return INSTANCE;
  }
}
