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

import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.Request.DeletionRequest;
import org.apache.iotdb.lsm.context.DeleteRequestContext;
import org.apache.iotdb.lsm.context.InsertRequestContext;
import org.apache.iotdb.lsm.context.QueryRequestContext;
import org.apache.iotdb.lsm.context.RequestContext;
import org.apache.iotdb.lsm.levelProcess.ILevelProcess;
import org.apache.iotdb.lsm.levelProcess.LevelProcessChain;
import org.apache.iotdb.lsm.manager.WALManager;
import org.apache.iotdb.lsm.property.Property;
import org.apache.iotdb.lsm.property.PropertyDescriptor;
import org.apache.iotdb.lsm.request.InsertionRequest;
import org.apache.iotdb.lsm.request.QueryRequest;
import org.apache.iotdb.lsm.request.Request;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

public class LSMEngineDirector<T> {
  private static final Logger logger = LoggerFactory.getLogger(LSMEngineDirector.class);

  LSMEngineBuilder<T> lsmEngineBuilder;

  public LSMEngineDirector(LSMEngineBuilder<T> lsmEngineBuilder) {
    this.lsmEngineBuilder = lsmEngineBuilder;
  }

  public LSMEngineDirector() {
    lsmEngineBuilder = new LSMEngineBuilder<>();
  }

  public LSMEngine<T> getLSMEngine(Property property, WALManager walManager) {
    try {
      LevelProcessChain<T, InsertionRequest, InsertRequestContext> insertionLevelProcessChain =
          generateLevelProcessChain(property.getInsertionLevelProcessClass());
      LevelProcessChain<T, DeletionRequest, DeleteRequestContext> deletionLevelProcessChain =
          generateLevelProcessChain(property.getDeletionLevelProcessClass());
      LevelProcessChain<T, QueryRequest, QueryRequestContext> queryLevelProcessChain =
          generateLevelProcessChain(property.getQueryLevelProcessClass());

      return lsmEngineBuilder
          .buildWalManager(walManager)
          .buildQueryManager(queryLevelProcessChain)
          .buildInsertionManager(insertionLevelProcessChain)
          .buildDeletionManager(deletionLevelProcessChain)
          .buildRecoverManager()
          .builder();

    } catch (Exception e) {
      logger.error(e.getMessage());
    }
    return null;
  }

  public LSMEngine<T> getLSMEngine(String packageName, WALManager walManager) {
    try {
      Property property = PropertyDescriptor.getProperty(packageName);
      return getLSMEngine(property, walManager);
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
    return null;
  }

  private <R extends Request, C extends RequestContext>
      LevelProcessChain<T, R, C> generateLevelProcessChain(List<String> levelProcessClassNames) {
    LevelProcessChain<T, R, C> levelProcessChain = new LevelProcessChain<>();
    try {
      if (levelProcessClassNames.size() > 0) {
        ILevelProcess iLevelProcess =
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

  private <O, R extends Request, C extends RequestContext>
      ILevelProcess<T, O, R, C> generateLevelProcess(String className)
          throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException,
              InstantiationException, IllegalAccessException {
    Class c = Class.forName(className);
    ILevelProcess<T, O, R, C> result =
        (ILevelProcess<T, O, R, C>) c.getDeclaredConstructor().newInstance();
    return result;
  }
}
