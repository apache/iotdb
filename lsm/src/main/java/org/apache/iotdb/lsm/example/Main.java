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
package org.apache.iotdb.lsm.example;

import org.apache.iotdb.lsm.context.FlushContext;
import org.apache.iotdb.lsm.context.InsertContext;
import org.apache.iotdb.lsm.levelProcess.FlushLevelProcess;
import org.apache.iotdb.lsm.levelProcess.InsertLevelProcess;
import org.apache.iotdb.lsm.manager.BasicLsmManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Main {
  public static void main(String[] args) {
    MemTableManager memTableManager = new MemTableManager();
    System.out.println("-------------insert--------------");
    insertionExample(memTableManager);
    System.out.println("-------------flush--------------");
    flushExample(memTableManager);
  }

  public static void insertionExample(MemTableManager memTableManager) {

    BasicLsmManager<MemTableManager, InsertContext> baseLsmManager =
        new BasicLsmManager<MemTableManager, InsertContext>().manager(memTableManager);
    baseLsmManager
        .nextLevel(
            new InsertLevelProcess<MemTableManager, MemTable>() {
              @Override
              public List<MemTable> getChildren(MemTableManager memNode, InsertContext context) {
                Integer deviceID = (Integer) context.getValue();
                int maxDeviceID = memNode.getMaxDeviceID();
                List<MemTable> children = new ArrayList<>();
                if (deviceID / 65536 == maxDeviceID / 65536) {
                  children.add(memNode.getWorking());
                } else {
                  children.add(memNode.getImmutables().get(deviceID / 65536));
                }
                return children;
              }

              @Override
              public void insert(MemTableManager memNode, InsertContext context) {
                Integer deviceID = (Integer) context.getValue();
                int maxDeviceID = memNode.getMaxDeviceID();
                if (deviceID / 65536 == maxDeviceID / 65536) {
                  if (memNode.getWorking() == null) {
                    memNode.setWorking(new MemTable());
                  }
                } else if (deviceID > maxDeviceID) {
                  memNode
                      .getImmutables()
                      .put(memNode.getMaxDeviceID() / 65536, memNode.getWorking());
                  memNode.setWorking(new MemTable());
                }
                if (deviceID > maxDeviceID) {
                  memNode.setMaxDeviceID(deviceID);
                }
              }
            })
        .nextLevel(
            new InsertLevelProcess<MemTable, MemGroup>() {
              @Override
              public List<MemGroup> getChildren(MemTable memNode, InsertContext context) {
                String key = (String) context.getKey();
                List<MemGroup> children = new ArrayList<>();
                children.add(memNode.getMap().get(key));
                return children;
              }

              @Override
              public void insert(MemTable memNode, InsertContext context) {
                String key = (String) context.getKey();
                Map<String, MemGroup> map = memNode.getMap();
                if (map.containsKey(key)) return;
                map.put(key, new MemGroup());
              }
            })
        .nextLevel(
            new InsertLevelProcess<MemGroup, MemChunk>() {
              @Override
              public List<MemChunk> getChildren(MemGroup memNode, InsertContext context) {
                String key = (String) context.getKey();
                List<MemChunk> children = new ArrayList<>();
                children.add(memNode.getMap().get(key));
                return children;
              }

              @Override
              public void insert(MemGroup memNode, InsertContext context) {
                String key = (String) context.getKey();
                Map<String, MemChunk> map = memNode.getMap();
                if (map.containsKey(key)) return;
                map.put(key, new MemChunk());
              }
            })
        .nextLevel(
            new InsertLevelProcess<MemChunk, Object>() {
              @Override
              public List<Object> getChildren(MemChunk memNode, InsertContext context) {
                return null;
              }

              @Override
              public void insert(MemChunk memNode, InsertContext context) {
                Integer deviceID = (Integer) context.getValue();
                List<Integer> deviceIDs = memNode.getDeviceIDS();
                deviceIDs.add(deviceID);
              }
            });

    baseLsmManager.process(new InsertContext(1, null, "a", "b"));
    baseLsmManager.process(new InsertContext(2, null, "a", "d"));
    baseLsmManager.process(new InsertContext(3, null, "a", "e"));
    baseLsmManager.process(new InsertContext(4, null, "a", "b"));
    baseLsmManager.process(new InsertContext(5, null, "a1", "b"));
    baseLsmManager.process(new InsertContext(6, null, "a2", "b"));
    baseLsmManager.process(new InsertContext(65535, null, "a", "b"));
    baseLsmManager.process(new InsertContext(65536, null, "a", "b"));
    baseLsmManager.process(new InsertContext(2, null, "a", "d"));
    baseLsmManager.process(new InsertContext(3, null, "a", "e"));
    baseLsmManager.process(new InsertContext(4, null, "a", "b"));
    baseLsmManager.process(new InsertContext(5, null, "a1", "b"));
    baseLsmManager.process(new InsertContext(6, null, "a2", "b"));
    System.out.println(memTableManager);
  }

  public static void flushExample(MemTableManager memTableManager) {
    BasicLsmManager<MemTableManager, FlushContext> flushManager =
        new BasicLsmManager<MemTableManager, FlushContext>().manager(memTableManager);

    flushManager
        .nextLevel(
            new FlushLevelProcess<MemTableManager, MemTable>() {
              @Override
              public void flush(MemTableManager memNode, FlushContext context) {
                System.out.println(memNode + "-->[ level:" + context.getLevel() + " ]");
              }

              @Override
              public List<MemTable> getChildren(MemTableManager memNode, FlushContext context) {
                List<MemTable> memTables = new ArrayList<>();
                memTables.addAll(memNode.getImmutables().values());
                if (memNode.getWorking() != null) memTables.add(memNode.getWorking());
                return memTables;
              }
            })
        .nextLevel(
            new FlushLevelProcess<MemTable, MemGroup>() {
              @Override
              public void flush(MemTable memNode, FlushContext context) {
                System.out.println(memNode + "-->[ level:" + context.getLevel() + " ]");
              }

              @Override
              public List<MemGroup> getChildren(MemTable memNode, FlushContext context) {
                List<MemGroup> memGroups = new ArrayList<>();
                memGroups.addAll(memNode.getMap().values());
                return memGroups;
              }
            })
        .nextLevel(
            new FlushLevelProcess<MemGroup, MemChunk>() {
              @Override
              public void flush(MemGroup memNode, FlushContext context) {
                System.out.println(memNode + "-->[ level:" + context.getLevel() + " ]");
              }

              @Override
              public List<MemChunk> getChildren(MemGroup memNode, FlushContext context) {
                List<MemChunk> memChunk = new ArrayList<>();
                memChunk.addAll(memNode.getMap().values());
                return memChunk;
              }
            })
        .nextLevel(
            new FlushLevelProcess<MemChunk, Object>() {
              @Override
              public void flush(MemChunk memNode, FlushContext context) {
                System.out.println(memNode + "-->[ level:" + context.getLevel() + " ]");
              }

              @Override
              public List<Object> getChildren(MemChunk memNode, FlushContext context) {
                return new ArrayList<>();
              }
            });

    flushManager.process(new FlushContext());
  }
}
