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
package org.apache.iotdb.lsm.engine;

import org.apache.iotdb.lsm.context.applicationcontext.ApplicationContext;
import org.apache.iotdb.lsm.context.applicationcontext.ApplicationContextGenerator;
import org.apache.iotdb.lsm.context.requestcontext.DeleteRequestContext;
import org.apache.iotdb.lsm.context.requestcontext.InsertRequestContext;
import org.apache.iotdb.lsm.context.requestcontext.QueryRequestContext;
import org.apache.iotdb.lsm.context.requestcontext.RequestContext;
import org.apache.iotdb.lsm.levelProcess.ILevelProcessor;
import org.apache.iotdb.lsm.levelProcess.LevelProcessorChain;
import org.apache.iotdb.lsm.manager.DeletionManager;
import org.apache.iotdb.lsm.manager.InsertionManager;
import org.apache.iotdb.lsm.manager.QueryManager;
import org.apache.iotdb.lsm.manager.RecoverManager;
import org.apache.iotdb.lsm.manager.WALManager;
import org.apache.iotdb.lsm.request.IDeletionRequest;
import org.apache.iotdb.lsm.request.IInsertionRequest;
import org.apache.iotdb.lsm.request.IQueryRequest;
import org.apache.iotdb.lsm.request.IRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
 * Build the LSMEngine object
 *
 * @param <T> The type of root memory node handled by this engine
 */
public class LSMEngineBuilder<T> {

  private static final Logger logger = LoggerFactory.getLogger(LSMEngineBuilder.class);

  // The constructed LSMEngine object
  private LSMEngine<T> lsmEngine;

  public LSMEngineBuilder() {
    lsmEngine = new LSMEngine<>();
  }

  /**
   * build WalManager for lsmEngine
   *
   * @param walManager WalManager object
   */
  public LSMEngineBuilder<T> buildWalManager(WALManager walManager) {
    lsmEngine.setWalManager(walManager);
    return this;
  }

  /**
   * build InsertionManager for lsmEngine
   *
   * @param levelProcessChain insert level processors chain
   * @param <R> extends IInsertionRequest
   */
  public <R extends IInsertionRequest> LSMEngineBuilder<T> buildInsertionManager(
      LevelProcessorChain<T, R, InsertRequestContext> levelProcessChain) {
    InsertionManager<T, R> insertionManager = new InsertionManager<>(lsmEngine.getWalManager());
    insertionManager.setLevelProcessorsChain(levelProcessChain);
    buildInsertionManager(insertionManager);
    return this;
  }

  /**
   * build InsertionManager for lsmEngine
   *
   * @param insertionManager InsertionManager object
   * @param <R> extends IInsertionRequest
   */
  public <R extends IInsertionRequest> LSMEngineBuilder<T> buildInsertionManager(
      InsertionManager<T, R> insertionManager) {
    lsmEngine.setInsertionManager(insertionManager);
    return this;
  }

  /**
   * build DeletionManager for lsmEngine
   *
   * @param levelProcessChain delete level processors chain
   * @param <R> extends IDeletionRequest
   */
  public <R extends IDeletionRequest> LSMEngineBuilder<T> buildDeletionManager(
      LevelProcessorChain<T, R, DeleteRequestContext> levelProcessChain) {
    DeletionManager<T, R> deletionManager = new DeletionManager<>(lsmEngine.getWalManager());
    deletionManager.setLevelProcessorsChain(levelProcessChain);
    buildDeletionManager(deletionManager);
    return this;
  }

  /**
   * build DeletionManager for lsmEngine
   *
   * @param deletionManager DeletionManager object
   * @param <R> extends IDeletionRequest
   */
  public <R extends IDeletionRequest> LSMEngineBuilder<T> buildDeletionManager(
      DeletionManager<T, R> deletionManager) {
    lsmEngine.setDeletionManager(deletionManager);
    return this;
  }

  /**
   * build QueryManager for lsmEngine
   *
   * @param levelProcessChain query level processors chain
   * @param <R> extends IQueryRequest
   */
  public <R extends IQueryRequest> LSMEngineBuilder<T> buildQueryManager(
      LevelProcessorChain<T, R, QueryRequestContext> levelProcessChain) {
    QueryManager<T, R> queryManager = new QueryManager<>();
    queryManager.setLevelProcessorsChain(levelProcessChain);
    buildQueryManager(queryManager);
    return this;
  }

  /**
   * build QueryManager for lsmEngine
   *
   * @param queryManager QueryManager object
   * @param <R> extends IQueryRequest
   */
  public <R extends IQueryRequest> LSMEngineBuilder<T> buildQueryManager(
      QueryManager<T, R> queryManager) {
    lsmEngine.setQueryManager(queryManager);
    return this;
  }

  /** build RecoverManager for lsmEngine */
  public LSMEngineBuilder<T> buildRecoverManager() {
    RecoverManager<LSMEngine<T>> recoverManager = new RecoverManager<>(lsmEngine.getWalManager());
    lsmEngine.setRecoverManager(recoverManager);
    return this;
  }

  /**
   * build root memory node for lsmEngine
   *
   * @param rootMemNode root memory node
   */
  public LSMEngineBuilder<T> buildRootMemNode(T rootMemNode) {
    lsmEngine.setRootMemNode(rootMemNode);
    return this;
  }

  /**
   * build level processors from ApplicationContext object
   *
   * @param applicationContext ApplicationContext object
   */
  private LSMEngineBuilder<T> buildLevelProcessors(ApplicationContext applicationContext) {
    LevelProcessorChain<T, IInsertionRequest, InsertRequestContext> insertionLevelProcessChain =
        generateLevelProcessorsChain(applicationContext.getInsertionLevelProcessClass());
    LevelProcessorChain<T, IDeletionRequest, DeleteRequestContext> deletionLevelProcessChain =
        generateLevelProcessorsChain(applicationContext.getDeletionLevelProcessClass());
    LevelProcessorChain<T, IQueryRequest, QueryRequestContext> queryLevelProcessChain =
        generateLevelProcessorsChain(applicationContext.getQueryLevelProcessClass());
    return buildQueryManager(queryLevelProcessChain)
        .buildInsertionManager(insertionLevelProcessChain)
        .buildDeletionManager(deletionLevelProcessChain);
  }

  /**
   * Scan the classes of the package and build level processors based on the class annotations
   *
   * @param packageName package name
   */
  private LSMEngineBuilder<T> buildLevelProcessors(String packageName) {
    try {
      ApplicationContext property =
          ApplicationContextGenerator.GeneratePropertyWithAnnotation(packageName);
      buildLevelProcessors(property);
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
    return this;
  }

  /**
   * build all LSM managers
   *
   * @param applicationContext ApplicationContext object
   * @param walManager WalManager object
   */
  public LSMEngineBuilder<T> buildLSMManagers(
      ApplicationContext applicationContext, WALManager walManager) {
    try {
      buildWalManager(walManager).buildLevelProcessors(applicationContext).buildRecoverManager();
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
    return this;
  }

  /**
   * Scan the classes of the package and build all LSM managers based on the class annotations
   *
   * @param packageName package name
   * @param walManager WalManager object
   */
  public LSMEngineBuilder<T> buildLSMManagers(String packageName, WALManager walManager) {
    try {
      ApplicationContext property =
          ApplicationContextGenerator.GeneratePropertyWithAnnotation(packageName);
      buildLSMManagers(property, walManager);
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
    return this;
  }

  /**
   * Get the built lsmEngine
   *
   * @return LSMEngine object
   */
  public LSMEngine<T> build() {
    return lsmEngine;
  }

  /**
   * generate level processors Chain
   *
   * @param levelProcessorClassNames Save all level processor class names in hierarchical order
   * @param <R> extends IRequest
   * @param <C> extends RequestContext
   * @return level Processors Chain
   */
  private <R extends IRequest, C extends RequestContext>
      LevelProcessorChain<T, R, C> generateLevelProcessorsChain(
          List<String> levelProcessorClassNames) {
    LevelProcessorChain<T, R, C> levelProcessChain = new LevelProcessorChain<>();
    try {
      if (levelProcessorClassNames.size() > 0) {
        ILevelProcessor iLevelProcess =
            levelProcessChain.nextLevel(generateLevelProcessor(levelProcessorClassNames.get(0)));
        for (int i = 1; i < levelProcessorClassNames.size(); i++) {
          iLevelProcess =
              iLevelProcess.nextLevel(generateLevelProcessor(levelProcessorClassNames.get(i)));
        }
      }
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
    return levelProcessChain;
  }

  /**
   * generate level processor
   *
   * @param className level processor class name
   * @param <R> extends IRequest
   * @param <C> extends RequestContext
   * @return level processor
   * @throws ClassNotFoundException
   * @throws NoSuchMethodException
   * @throws InvocationTargetException
   * @throws InstantiationException
   * @throws IllegalAccessException
   */
  private <R extends IRequest, C extends RequestContext>
      ILevelProcessor<T, ?, R, C> generateLevelProcessor(String className)
          throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException,
              InstantiationException, IllegalAccessException {
    Class c = Class.forName(className);
    ILevelProcessor<T, ?, R, C> result =
        (ILevelProcessor<T, ?, R, C>) c.getDeclaredConstructor().newInstance();
    return result;
  }
}
