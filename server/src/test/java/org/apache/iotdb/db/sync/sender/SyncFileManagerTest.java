///**
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//package org.apache.iotdb.db.sync.sender;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Map;
//import java.util.Map.Entry;
//import java.util.Random;
//import java.util.Set;
//import org.apache.iotdb.db.sync.sender.conf.Constans;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Test;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//public class SyncFileManagerTest {
//
//  private static final String POST_BACK_DIRECTORY_TEST = Constans.SYNC_SENDER + File.separator;
//  private static final String LAST_FILE_INFO_TEST =
//      POST_BACK_DIRECTORY_TEST + Constans.LAST_LOCAL_FILE_NAME;
//  private static final String SENDER_FILE_PATH_TEST = POST_BACK_DIRECTORY_TEST + "data";
//  private SyncFileManager manager = SyncFileManager.getInstance();
//  private static final Logger logger = LoggerFactory.getLogger(SyncFileManagerTest.class);
//
//  @Before
//  public void setUp() throws IOException, InterruptedException {
//    File file = new File(LAST_FILE_INFO_TEST);
//    if (!file.getParentFile().exists()) {
//      file.getParentFile().mkdirs();
//    }
//    if (!file.exists() && !file.createNewFile()) {
//      logger.error("Can not create new file {}", file.getPath());
//    }
//    file = new File(SENDER_FILE_PATH_TEST);
//    if (!file.exists()) {
//      file.mkdirs();
//    }
//    manager.setCurrentLocalFiles(new HashMap<>());
//  }
//
//  @After
//  public void tearDown() throws InterruptedException {
//    delete(new File(POST_BACK_DIRECTORY_TEST));
//    new File(POST_BACK_DIRECTORY_TEST).delete();
//  }
//
//  public void delete(File file) {
//    if (file.isFile() || file.list().length == 0) {
//      file.delete();
//    } else {
//      File[] files = file.listFiles();
//      for (File f : files) {
//        delete(f);
//        f.delete();
//      }
//    }
//  }
//
//  @Test // It tests two classes : backupNowLocalFileInfo and getLastLocalFileList
//  public void testBackupCurrentLocalFileInfo() throws IOException {
//    Map<String, Set<String>> allFileList = new HashMap<>();
//
//    Random r = new Random(0);
//    for (int i = 0; i < 3; i++) {
//      for (int j = 0; j < 5; j++) {
//        if (!allFileList.containsKey(String.valueOf(i))) {
//          allFileList.put(String.valueOf(i), new HashSet<>());
//        }
//        String rand = String.valueOf(r.nextInt(10000));
//        String fileName =
//            SENDER_FILE_PATH_TEST + File.separator + i + File.separator + rand;
//        File file = new File(fileName);
//        allFileList.get(String.valueOf(i)).add(file.getPath());
//        if (!file.getParentFile().exists()) {
//          file.getParentFile().mkdirs();
//        }
//        if (!file.exists() && !file.createNewFile()) {
//          logger.error("Can not create new file {}", file.getPath());
//        }
//      }
//    }
//    Set<String> lastFileList;
//
//    // lastFileList is empty
//    manager.getLastLocalFileList(LAST_FILE_INFO_TEST);
//    lastFileList = manager.getLastLocalFiles();
//    assert (lastFileList.isEmpty());
//
//    // add some files
//    manager.getCurrentLocalFileList(new String[]{SENDER_FILE_PATH_TEST});
//    manager.backupNowLocalFileInfo(LAST_FILE_INFO_TEST);
//    manager.getLastLocalFileList(LAST_FILE_INFO_TEST);
//    lastFileList = manager.getLastLocalFiles();
//    for (Entry<String, Set<String>> entry : allFileList.entrySet()) {
//      assert (lastFileList.containsAll(entry.getValue()));
//    }
//
//    // add some files and delete some files
//    r = new Random(1);
//    for (int i = 0; i < 3; i++) {
//      for (int j = 0; j < 5; j++) {
//        if (!allFileList.containsKey(String.valueOf(i))) {
//          allFileList.put(String.valueOf(i), new HashSet<>());
//        }
//        String rand = String.valueOf(r.nextInt(10000));
//        String fileName =
//            SENDER_FILE_PATH_TEST + File.separator + i + File.separator + rand;
//        File file = new File(fileName);
//        allFileList.get(String.valueOf(i)).add(file.getPath());
//        if (!file.getParentFile().exists()) {
//          file.getParentFile().mkdirs();
//        }
//        if (!file.exists() && !file.createNewFile()) {
//          logger.error("Can not create new file {}", file.getPath());
//        }
//      }
//    }
//    int count = 0;
//    Map<String, Set<String>> deleteFile = new HashMap<>();
//    for (Entry<String, Set<String>> entry : allFileList.entrySet()) {
//      deleteFile.put(entry.getKey(), new HashSet<>());
//      for (String path : entry.getValue()) {
//        count++;
//        if (count % 3 == 0) {
//          deleteFile.get(entry.getKey()).add(path);
//        }
//      }
//    }
//    for (Entry<String, Set<String>> entry : deleteFile.entrySet()) {
//      for (String path : entry.getValue()) {
//        new File(path).delete();
//        allFileList.get(entry.getKey()).remove(path);
//      }
//    }
//    manager.getCurrentLocalFileList(new String[]{SENDER_FILE_PATH_TEST});
//    manager.backupNowLocalFileInfo(LAST_FILE_INFO_TEST);
//    manager.getLastLocalFileList(LAST_FILE_INFO_TEST);
//    lastFileList = manager.getLastLocalFiles();
//    for (Entry<String, Set<String>> entry : allFileList.entrySet()) {
//      assert (lastFileList.containsAll(entry.getValue()));
//    }
//  }
//
//  @Test
//  public void testGetCurrentLocalFileList() throws IOException {
//    Map<String, Set<String>> allFileList = new HashMap<>();
//    Map<String, Set<String>> fileList;
//
//    // nowLocalList is empty
//    manager.getCurrentLocalFileList(new String[]{SENDER_FILE_PATH_TEST});
//    fileList = manager.getCurrentLocalFiles();
//    assert (isEmpty(fileList));
//
//    // add some files
//    Random r = new Random(0);
//    for (int i = 0; i < 3; i++) {
//      for (int j = 0; j < 5; j++) {
//        if (!allFileList.containsKey(String.valueOf(i))) {
//          allFileList.put(String.valueOf(i), new HashSet<>());
//        }
//        String rand = String.valueOf(r.nextInt(10000));
//        String fileName =
//            SENDER_FILE_PATH_TEST + File.separator + i + File.separator + rand;
//        File file = new File(fileName);
//        allFileList.get(String.valueOf(i)).add(file.getPath());
//        if (!file.getParentFile().exists()) {
//          file.getParentFile().mkdirs();
//        }
//        if (!file.exists() && !file.createNewFile()) {
//          logger.error("Can not create new file {}", file.getPath());
//        }
//      }
//    }
//    manager.getCurrentLocalFileList(new String[]{SENDER_FILE_PATH_TEST});
//    fileList = manager.getCurrentLocalFiles();
//    assert (allFileList.size() == fileList.size());
//    for (Entry<String, Set<String>> entry : fileList.entrySet()) {
//      assert (allFileList.containsKey(entry.getKey()));
//      assert (allFileList.get(entry.getKey()).containsAll(entry.getValue()));
//    }
//
//    // delete some files and add some files
//    int count = 0;
//    Map<String, Set<String>> deleteFile = new HashMap<>();
//    for (Entry<String, Set<String>> entry : allFileList.entrySet()) {
//      deleteFile.put(entry.getKey(), new HashSet<>());
//      for (String path : entry.getValue()) {
//        count++;
//        if (count % 3 == 0) {
//          deleteFile.get(entry.getKey()).add(path);
//        }
//      }
//    }
//    for (Entry<String, Set<String>> entry : deleteFile.entrySet()) {
//      for (String path : entry.getValue()) {
//        new File(path).delete();
//        allFileList.get(entry.getKey()).remove(path);
//      }
//    }
//    r = new Random(1);
//    for (int i = 0; i < 3; i++) {
//      for (int j = 0; j < 5; j++) {
//        if (!allFileList.containsKey(String.valueOf(i))) {
//          allFileList.put(String.valueOf(i), new HashSet<>());
//        }
//        String rand = String.valueOf(r.nextInt(10000));
//        String fileName =
//            SENDER_FILE_PATH_TEST + File.separator + i + File.separator + rand;
//        File file = new File(fileName);
//        allFileList.get(String.valueOf(i)).add(file.getPath());
//        if (!file.getParentFile().exists()) {
//          file.getParentFile().mkdirs();
//        }
//        if (!file.exists() && !file.createNewFile()) {
//          logger.error("Can not create new file {}", file.getPath());
//        }
//      }
//    }
//    manager.setCurrentLocalFiles(new HashMap<>());
//    manager.getCurrentLocalFileList(new String[]{SENDER_FILE_PATH_TEST});
//    fileList = manager.getCurrentLocalFiles();
//    assert (allFileList.size() == fileList.size());
//    for (Entry<String, Set<String>> entry : fileList.entrySet()) {
//      assert (allFileList.containsKey(entry.getKey()));
//      logger.debug("allFileList");
//      for (String a : allFileList.get(entry.getKey())) {
//        logger.debug(a);
//      }
//      logger.debug("FileList");
//      for (String a : entry.getValue()) {
//        logger.debug(a);
//      }
//      assert (allFileList.get(entry.getKey()).containsAll(entry.getValue()));
//    }
//  }
//
//  @Test
//  public void testGetValidFileList() throws IOException {
//    Map<String, Set<String>> allFileList;
//    Map<String, Set<String>> newFileList = new HashMap<>();
//    Map<String, Set<String>> sendingFileList;
//    Set<String> lastLocalList;
//
//    // nowSendingList is empty
//
//    manager.setCurrentLocalFiles(new HashMap<>());
//    manager.getCurrentLocalFileList(new String[]{SENDER_FILE_PATH_TEST});
//    allFileList = manager.getCurrentLocalFiles();
//    manager.getLastLocalFileList(LAST_FILE_INFO_TEST);
//    lastLocalList = manager.getLastLocalFiles();
//    manager.getValidFileList();
//    assert (lastLocalList.isEmpty());
//    assert (isEmpty(allFileList));
//
//    // add some files
//    newFileList.clear();
//    manager.backupNowLocalFileInfo(LAST_FILE_INFO_TEST);
//    Random r = new Random(0);
//    for (int i = 0; i < 3; i++) {
//      for (int j = 0; j < 5; j++) {
//        if (!allFileList.containsKey(String.valueOf(i))) {
//          allFileList.put(String.valueOf(i), new HashSet<>());
//        }
//        if (!newFileList.containsKey(String.valueOf(i))) {
//          newFileList.put(String.valueOf(i), new HashSet<>());
//        }
//        String rand = String.valueOf(r.nextInt(10000));
//        String fileName =
//            SENDER_FILE_PATH_TEST + File.separator + i + File.separator + rand;
//        File file = new File(fileName);
//        allFileList.get(String.valueOf(i)).add(file.getPath());
//        newFileList.get(String.valueOf(i)).add(file.getPath());
//        if (!file.getParentFile().exists()) {
//          file.getParentFile().mkdirs();
//        }
//        if (!file.exists() && !file.createNewFile()) {
//          logger.error("Can not create new file {}", file.getPath());
//        }
//      }
//    }
//    manager.getCurrentLocalFileList(new String[]{SENDER_FILE_PATH_TEST});
//    allFileList = manager.getCurrentLocalFiles();
//    manager.backupNowLocalFileInfo(LAST_FILE_INFO_TEST);
//    manager.getLastLocalFileList(LAST_FILE_INFO_TEST);
//    manager.getValidFileList();
//    sendingFileList = manager.getValidAllFiles();
//    assert (sendingFileList.size() == newFileList.size());
//    for (Entry<String, Set<String>> entry : sendingFileList.entrySet()) {
//      assert (newFileList.containsKey(entry.getKey()));
//      assert (newFileList.get(entry.getKey()).containsAll(entry.getValue()));
//    }
//
//    // delete some files and add some files
//    int count = 0;
//    Map<String, Set<String>> deleteFile = new HashMap<>();
//    for (Entry<String, Set<String>> entry : allFileList.entrySet()) {
//      deleteFile.put(entry.getKey(), new HashSet<>());
//      for (String path : entry.getValue()) {
//        count++;
//        if (count % 3 == 0) {
//          deleteFile.get(entry.getKey()).add(path);
//        }
//      }
//    }
//    for (Entry<String, Set<String>> entry : deleteFile.entrySet()) {
//      for (String path : entry.getValue()) {
//        new File(path).delete();
//        allFileList.get(entry.getKey()).remove(path);
//      }
//    }
//    newFileList.clear();
//    r = new Random(1);
//    for (int i = 0; i < 3; i++) {
//      for (int j = 0; j < 5; j++) {
//        if (!allFileList.containsKey(String.valueOf(i))) {
//          allFileList.put(String.valueOf(i), new HashSet<>());
//        }
//        if (!newFileList.containsKey(String.valueOf(i))) {
//          newFileList.put(String.valueOf(i), new HashSet<>());
//        }
//        String rand = String.valueOf(r.nextInt(10000));
//        String fileName =
//            SENDER_FILE_PATH_TEST + File.separator + i + File.separator + rand;
//        File file = new File(fileName);
//        allFileList.get(String.valueOf(i)).add(file.getPath());
//        newFileList.get(String.valueOf(i)).add(file.getPath());
//        if (!file.getParentFile().exists()) {
//          file.getParentFile().mkdirs();
//        }
//        if (!file.exists() && !file.createNewFile()) {
//          logger.error("Can not create new file {}", file.getPath());
//        }
//      }
//    }
//    manager.getCurrentLocalFileList(new String[]{SENDER_FILE_PATH_TEST});
//    manager.getLastLocalFileList(LAST_FILE_INFO_TEST);
//    manager.getValidFileList();
//    sendingFileList = manager.getValidAllFiles();
//    assert (sendingFileList.size() == newFileList.size());
//    for (Entry<String, Set<String>> entry : sendingFileList.entrySet()) {
//      assert (newFileList.containsKey(entry.getKey()));
//      assert (newFileList.get(entry.getKey()).containsAll(entry.getValue()));
//    }
//  }
//
//  private boolean isEmpty(Map<String, Set<String>> sendingFileList) {
//    for (Entry<String, Set<String>> entry : sendingFileList.entrySet()) {
//      if (!entry.getValue().isEmpty()) {
//        return false;
//      }
//    }
//    return true;
//  }
//}