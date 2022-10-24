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

import org.apache.iotdb.lsm.context.DeleteRequestContext;
import org.apache.iotdb.lsm.context.InsertRequestContext;
import org.apache.iotdb.lsm.context.QueryRequestContext;
import org.apache.iotdb.lsm.context.RequestContext;
import org.apache.iotdb.lsm.levelProcess.ILevelProcessor;
import org.apache.iotdb.lsm.levelProcess.LevelProcessorChain;
import org.apache.iotdb.lsm.manager.DeletionManager;
import org.apache.iotdb.lsm.manager.InsertionManager;
import org.apache.iotdb.lsm.manager.QueryManager;
import org.apache.iotdb.lsm.manager.RecoverManager;
import org.apache.iotdb.lsm.manager.WALManager;
import org.apache.iotdb.lsm.property.Property;
import org.apache.iotdb.lsm.property.PropertyGenerator;
import org.apache.iotdb.lsm.request.IDeletionRequest;
import org.apache.iotdb.lsm.request.IInsertionRequest;
import org.apache.iotdb.lsm.request.IQueryRequest;
import org.apache.iotdb.lsm.request.IRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

public class LSMEngineBuilder<T> {

  private static final Logger logger = LoggerFactory.getLogger(LSMEngineBuilder.class);

  private LSMEngine<T> lsmEngine;

  public LSMEngineBuilder() {
    lsmEngine = new LSMEngine<>();
  }

  public LSMEngineBuilder<T> buildWalManager(WALManager walManager) {
    lsmEngine.setWalManager(walManager);
    return this;
  }

  public <R extends IInsertionRequest> LSMEngineBuilder<T> buildInsertionManager(
      LevelProcessorChain<T, R, InsertRequestContext> levelProcessChain) {
    InsertionManager<T, R> insertionManager = new InsertionManager<>(lsmEngine.getWalManager());
    insertionManager.setLevelProcessChain(levelProcessChain);
    buildInsertionManager(insertionManager);
    return this;
  }

  public <R extends IInsertionRequest> LSMEngineBuilder<T> buildInsertionManager(
      InsertionManager<T, R> insertionManager) {
    lsmEngine.setInsertionManager(insertionManager);
    return this;
  }

  public <R extends IDeletionRequest> LSMEngineBuilder<T> buildDeletionManager(
      LevelProcessorChain<T, R, DeleteRequestContext> levelProcessChain) {
    DeletionManager<T, R> deletionManager = new DeletionManager<>(lsmEngine.getWalManager());
    deletionManager.setLevelProcessChain(levelProcessChain);
    buildDeletionManager(deletionManager);
    return this;
  }

  public <R extends IDeletionRequest> LSMEngineBuilder<T> buildDeletionManager(
      DeletionManager<T, R> deletionManager) {
    lsmEngine.setDeletionManager(deletionManager);
    return this;
  }

  public <R extends IQueryRequest> LSMEngineBuilder<T> buildQueryManager(
      LevelProcessorChain<T, R, QueryRequestContext> levelProcessChain) {
    QueryManager<T, R> queryManager = new QueryManager<>();
    queryManager.setLevelProcessChain(levelProcessChain);
    buildQueryManager(queryManager);
    return this;
  }

  public <R extends IQueryRequest> LSMEngineBuilder<T> buildQueryManager(
      QueryManager<T, R> queryManager) {
    lsmEngine.setQueryManager(queryManager);
    return this;
  }

  public LSMEngineBuilder<T> buildRecoverManager(RecoverManager<LSMEngine<T>> recoverManager) {
    lsmEngine.setRecoverManager(recoverManager);
    return this;
  }

  public LSMEngineBuilder<T> buildRecoverManager() {
    RecoverManager<LSMEngine<T>> recoverManager = new RecoverManager<>(lsmEngine.getWalManager());
    lsmEngine.setRecoverManager(recoverManager);
    return this;
  }

  public LSMEngineBuilder<T> buildRootMemNode(T rootMemNode) {
    lsmEngine.setRootMemNode(rootMemNode);
    return this;
  }

  public LSMEngineBuilder<T> buildLevelProcessors(Property property) {
    LevelProcessorChain<T, IInsertionRequest, InsertRequestContext> insertionLevelProcessChain =
        generateLevelProcessChain(property.getInsertionLevelProcessClass());
    LevelProcessorChain<T, IDeletionRequest, DeleteRequestContext> deletionLevelProcessChain =
        generateLevelProcessChain(property.getDeletionLevelProcessClass());
    LevelProcessorChain<T, IQueryRequest, QueryRequestContext> queryLevelProcessChain =
        generateLevelProcessChain(property.getQueryLevelProcessClass());
    return buildQueryManager(queryLevelProcessChain)
        .buildInsertionManager(insertionLevelProcessChain)
        .buildDeletionManager(deletionLevelProcessChain);
  }

  public LSMEngineBuilder<T> buildLevelProcessors(String packageName) {
    try {
      Property property = PropertyGenerator.GeneratePropertyWithAnnotation(packageName);
      buildLevelProcessors(property);
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
    return this;
  }

  public LSMEngineBuilder<T> buildLSMManagers(Property property, WALManager walManager) {
    try {
      buildWalManager(walManager).buildLevelProcessors(property).buildRecoverManager();
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
    return this;
  }

  public LSMEngineBuilder<T> buildLSMManagers(String packageName, WALManager walManager) {
    try {
      Property property = PropertyGenerator.GeneratePropertyWithAnnotation(packageName);
      buildLSMManagers(property, walManager);
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
    return this;
  }

  public LSMEngine<T> build() {
    return lsmEngine;
  }

  private <R extends IRequest, C extends RequestContext>
      LevelProcessorChain<T, R, C> generateLevelProcessChain(List<String> levelProcessClassNames) {
    LevelProcessorChain<T, R, C> levelProcessChain = new LevelProcessorChain<>();
    try {
      if (levelProcessClassNames.size() > 0) {
        ILevelProcessor iLevelProcess =
            levelProcessChain.nextLevel(generateLevelProcess(levelProcessClassNames.get(0)));
        for (int i = 1; i < levelProcessClassNames.size(); i++) {
          iLevelProcess =
              iLevelProcess.nextLevel(generateLevelProcess(levelProcessClassNames.get(i)));
        }
      }
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
    return levelProcessChain;
  }

  private <O, R extends IRequest, C extends RequestContext>
      ILevelProcessor<T, O, R, C> generateLevelProcess(String className)
          throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException,
              InstantiationException, IllegalAccessException {
    Class c = Class.forName(className);
    ILevelProcessor<T, O, R, C> result =
        (ILevelProcessor<T, O, R, C>) c.getDeclaredConstructor().newInstance();
    return result;
  }
}
