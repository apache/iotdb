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
 *
 */
package org.apache.iotdb.db.newsync.sender.service;

import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.newsync.sender.pipe.Pipe;
import org.apache.iotdb.db.newsync.sender.pipe.PipeSink;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SenderService implements IService {
  private Map<String, PipeSink> pipeSinks;
  private List<Pipe> pipes;

  private Pipe runningPipe;

  private static volatile SenderService senderService;

  private SenderService() {
    pipeSinks = new HashMap<>();
    pipes = new ArrayList<>();
  }

  public static SenderService getInstance() {
    if (senderService == null) {
      synchronized (SenderService.class) {
        if (senderService == null) {
          senderService = new SenderService();
        }
      }
    }
    return senderService;
  }

  public synchronized PipeSink getPipeSink(String name) {
    return pipeSinks.getOrDefault(name, null);
  }

  public synchronized boolean isPipeSinkExist(String name) {
    return pipeSinks.containsKey(name);
  }

  public synchronized void addPipeSink(PipeSink pipeSink) {
    pipeSinks.put(pipeSink.getName(), pipeSink);
  }

  public synchronized void dropPipeSink(String name) {
    pipeSinks.remove(name);
  }

  public synchronized List<PipeSink> getAllPipeSink() {
    List<PipeSink> allPipeSinks = new ArrayList<>();
    for (Map.Entry<String, PipeSink> entry : pipeSinks.entrySet()) {
      allPipeSinks.add(entry.getValue());
    }
    return allPipeSinks;
  }

  @Override
  public void start() throws StartupException {}

  @Override
  public void stop() {}

  @Override
  public ServiceType getID() {
    return ServiceType.SENDER_SERVICE;
  }
}
