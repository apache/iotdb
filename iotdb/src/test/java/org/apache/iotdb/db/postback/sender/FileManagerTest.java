/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.postback.sender;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Map.Entry;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.iotdb.db.postback.sender.FileManager;

public class FileManagerTest {

    public static final String POST_BACK_DIRECTORY_TEST = "postback" + File.separator;
    public static final String LAST_FILE_INFO_TEST = POST_BACK_DIRECTORY_TEST + "lastLocalFileList.txt";
    public static final String SENDER_FILE_PATH_TEST = POST_BACK_DIRECTORY_TEST + "data";
    FileManager manager = FileManager.getInstance();

    @Before
    public void setUp() throws Exception {
        Thread.sleep(1000);
        File file = new File(LAST_FILE_INFO_TEST);
        if (!file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }
        if (!file.exists()) {
            file.createNewFile();
        }
        file = new File(SENDER_FILE_PATH_TEST);
        if (!file.exists()) {
            file.mkdirs();
        }
    }

    @After
    public void tearDown() throws Exception {
        Thread.sleep(1000);
        delete(new File(POST_BACK_DIRECTORY_TEST));
        new File(POST_BACK_DIRECTORY_TEST).delete();
    }

    public void delete(File file) {
        if (file.isFile() || file.list().length == 0) {
            file.delete();
        } else {
            File[] files = file.listFiles();
            for (File f : files) {
                delete(f);
                f.delete();
            }
        }
    }

    @Test // It tests two classes : backupNowLocalFileInfo and getLastLocalFileList
    public void testBackupNowLocalFileInfo() throws IOException {
        Map<String, Set<String>> allFileList = new HashMap<>();

        // TODO create some files
        Random r = new Random(0);
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 5; j++) {
                if (!allFileList.containsKey(String.valueOf(i)))
                    allFileList.put(String.valueOf(i), new HashSet<>());
                String rand = String.valueOf(r.nextInt(10000));
                String fileName = SENDER_FILE_PATH_TEST + File.separator + String.valueOf(i) + File.separator + rand;
                File file = new File(fileName);
                allFileList.get(String.valueOf(i)).add(file.getAbsolutePath());
                if (!file.getParentFile().exists()) {
                    file.getParentFile().mkdirs();
                }
                if (!file.exists()) {
                    file.createNewFile();
                }
            }
        }
        Set<String> lastFileList = new HashSet<>();

        // lastFileList is empty
        manager.getLastLocalFileList(LAST_FILE_INFO_TEST);
        lastFileList = manager.getLastLocalFiles();
        assert (lastFileList.isEmpty());

        // add some files
        manager.getNowLocalFileList(new String[] { SENDER_FILE_PATH_TEST });
        manager.backupNowLocalFileInfo(LAST_FILE_INFO_TEST);
        manager.getLastLocalFileList(LAST_FILE_INFO_TEST);
        lastFileList = manager.getLastLocalFiles();
        for (Entry<String, Set<String>> entry : allFileList.entrySet()) {
            assert (lastFileList.containsAll(entry.getValue()));
        }

        // add some files and delete some files
        r = new Random(1);
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 5; j++) {
                if (!allFileList.containsKey(String.valueOf(i)))
                    allFileList.put(String.valueOf(i), new HashSet<>());
                String rand = String.valueOf(r.nextInt(10000));
                String fileName = SENDER_FILE_PATH_TEST + File.separator + String.valueOf(i) + File.separator + rand;
                File file = new File(fileName);
                allFileList.get(String.valueOf(i)).add(file.getAbsolutePath());
                if (!file.getParentFile().exists()) {
                    file.getParentFile().mkdirs();
                }
                if (!file.exists()) {
                    file.createNewFile();
                }
            }
        }
        int count = 0;
        Map<String, Set<String>> deleteFile = new HashMap<>();
        for (Entry<String, Set<String>> entry : allFileList.entrySet()) {
            deleteFile.put(entry.getKey(), new HashSet<>());
            for (String path : entry.getValue()) {
                count++;
                if (count % 3 == 0) {
                    deleteFile.get(entry.getKey()).add(path);
                }
            }
        }
        for (Entry<String, Set<String>> entry : deleteFile.entrySet()) {
            for (String path : entry.getValue()) {
                new File(path).delete();
                allFileList.get(entry.getKey()).remove(path);
            }
        }
        manager.getNowLocalFileList(new String[] { SENDER_FILE_PATH_TEST });
        manager.backupNowLocalFileInfo(LAST_FILE_INFO_TEST);
        manager.getLastLocalFileList(LAST_FILE_INFO_TEST);
        lastFileList = manager.getLastLocalFiles();
        for (Entry<String, Set<String>> entry : allFileList.entrySet()) {
            assert (lastFileList.containsAll(entry.getValue()));
        }
    }

    @Test
    public void testGetNowLocalFileList() throws IOException {
        Map<String, Set<String>> allFileList = new HashMap<>();
        Map<String, Set<String>> fileList = new HashMap<>();

        // nowLocalList is empty
        manager.getNowLocalFileList(new String[] { SENDER_FILE_PATH_TEST });
        fileList = manager.getNowLocalFiles();
        assert (isEmpty(fileList));

        // add some files
        Random r = new Random(0);
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 5; j++) {
                if (!allFileList.containsKey(String.valueOf(i)))
                    allFileList.put(String.valueOf(i), new HashSet<>());
                String rand = String.valueOf(r.nextInt(10000));
                String fileName = SENDER_FILE_PATH_TEST + File.separator + String.valueOf(i) + File.separator + rand;
                File file = new File(fileName);
                allFileList.get(String.valueOf(i)).add(file.getAbsolutePath());
                if (!file.getParentFile().exists()) {
                    file.getParentFile().mkdirs();
                }
                if (!file.exists()) {
                    file.createNewFile();
                }
            }
        }
        manager.getNowLocalFileList(new String[] { SENDER_FILE_PATH_TEST });
        fileList = manager.getNowLocalFiles();
        assert (allFileList.size() == fileList.size());
        for (Entry<String, Set<String>> entry : fileList.entrySet()) {
            assert (allFileList.containsKey(entry.getKey()));
            assert (allFileList.get(entry.getKey()).containsAll(entry.getValue()));
        }

        // delete some files and add some files
        int count = 0;
        Map<String, Set<String>> deleteFile = new HashMap<>();
        for (Entry<String, Set<String>> entry : allFileList.entrySet()) {
            deleteFile.put(entry.getKey(), new HashSet<>());
            for (String path : entry.getValue()) {
                count++;
                if (count % 3 == 0) {
                    deleteFile.get(entry.getKey()).add(path);
                }
            }
        }
        for (Entry<String, Set<String>> entry : deleteFile.entrySet()) {
            for (String path : entry.getValue()) {
                new File(path).delete();
                allFileList.get(entry.getKey()).remove(path);
            }
        }
        r = new Random(1);
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 5; j++) {
                if (!allFileList.containsKey(String.valueOf(i)))
                    allFileList.put(String.valueOf(i), new HashSet<>());
                String rand = String.valueOf(r.nextInt(10000));
                String fileName = SENDER_FILE_PATH_TEST + File.separator + String.valueOf(i) + File.separator + rand;
                File file = new File(fileName);
                allFileList.get(String.valueOf(i)).add(file.getAbsolutePath());
                if (!file.getParentFile().exists()) {
                    file.getParentFile().mkdirs();
                }
                if (!file.exists()) {
                    file.createNewFile();
                }
            }
        }
        manager.setNowLocalFiles(new HashMap<>());
        manager.getNowLocalFileList(new String[] { SENDER_FILE_PATH_TEST });
        fileList = manager.getNowLocalFiles();
        assert (allFileList.size() == fileList.size());
        for (Entry<String, Set<String>> entry : fileList.entrySet()) {
            assert (allFileList.containsKey(entry.getKey()));
            System.out.println("allFileList");
            for (String a : allFileList.get(entry.getKey())) {
                System.out.println(a);
            }
            System.out.println("FileList");
            for (String a : entry.getValue()) {
                System.out.println(a);
            }
            assert (allFileList.get(entry.getKey()).containsAll(entry.getValue()));
        }
    }

    @Test
    public void testGetSendingFileList() throws IOException {
        Map<String, Set<String>> allFileList = new HashMap<>();
        Map<String, Set<String>> newFileList = new HashMap<>();
        Map<String, Set<String>> sendingFileList = new HashMap<>();
        Set<String> lastlocalList = new HashSet<>();

        // nowSendingList is empty

        manager.setNowLocalFiles(new HashMap<>());
        manager.getNowLocalFileList(new String[] { SENDER_FILE_PATH_TEST });
        allFileList = manager.getNowLocalFiles();
        manager.getLastLocalFileList(LAST_FILE_INFO_TEST);
        lastlocalList = manager.getLastLocalFiles();
        manager.getSendingFileList();
        sendingFileList = manager.getSendingFiles();
        assert (lastlocalList.size() == 0);
        assert (isEmpty(allFileList));
        assert (isEmpty(sendingFileList));

        // add some files
        newFileList.clear();
        manager.backupNowLocalFileInfo(LAST_FILE_INFO_TEST);
        Random r = new Random(0);
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 5; j++) {
                if (!allFileList.containsKey(String.valueOf(i)))
                    allFileList.put(String.valueOf(i), new HashSet<>());
                if (!newFileList.containsKey(String.valueOf(i)))
                    newFileList.put(String.valueOf(i), new HashSet<>());
                String rand = String.valueOf(r.nextInt(10000));
                String fileName = SENDER_FILE_PATH_TEST + File.separator + String.valueOf(i) + File.separator + rand;
                File file = new File(fileName);
                allFileList.get(String.valueOf(i)).add(file.getAbsolutePath());
                newFileList.get(String.valueOf(i)).add(file.getAbsolutePath());
                if (!file.getParentFile().exists()) {
                    file.getParentFile().mkdirs();
                }
                if (!file.exists()) {
                    file.createNewFile();
                }
            }
        }
        manager.getNowLocalFileList(new String[] { SENDER_FILE_PATH_TEST });
        allFileList = manager.getNowLocalFiles();
        manager.backupNowLocalFileInfo(LAST_FILE_INFO_TEST);
        manager.getLastLocalFileList(LAST_FILE_INFO_TEST);
        lastlocalList = manager.getLastLocalFiles();
        manager.getSendingFileList();
        sendingFileList = manager.getSendingFiles();
        assert (sendingFileList.size() == newFileList.size());
        for (Entry<String, Set<String>> entry : sendingFileList.entrySet()) {
            assert (newFileList.containsKey(entry.getKey()));
            assert (newFileList.get(entry.getKey()).containsAll(entry.getValue()));
        }

        // delete some files and add some files
        int count = 0;
        Map<String, Set<String>> deleteFile = new HashMap<>();
        for (Entry<String, Set<String>> entry : allFileList.entrySet()) {
            deleteFile.put(entry.getKey(), new HashSet<>());
            for (String path : entry.getValue()) {
                count++;
                if (count % 3 == 0) {
                    deleteFile.get(entry.getKey()).add(path);
                }
            }
        }
        for (Entry<String, Set<String>> entry : deleteFile.entrySet()) {
            for (String path : entry.getValue()) {
                new File(path).delete();
                allFileList.get(entry.getKey()).remove(path);
            }
        }
        newFileList.clear();
        r = new Random(1);
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 5; j++) {
                if (!allFileList.containsKey(String.valueOf(i)))
                    allFileList.put(String.valueOf(i), new HashSet<>());
                if (!newFileList.containsKey(String.valueOf(i)))
                    newFileList.put(String.valueOf(i), new HashSet<>());
                String rand = String.valueOf(r.nextInt(10000));
                String fileName = SENDER_FILE_PATH_TEST + File.separator + String.valueOf(i) + File.separator + rand;
                File file = new File(fileName);
                allFileList.get(String.valueOf(i)).add(file.getAbsolutePath());
                newFileList.get(String.valueOf(i)).add(file.getAbsolutePath());
                if (!file.getParentFile().exists()) {
                    file.getParentFile().mkdirs();
                }
                if (!file.exists()) {
                    file.createNewFile();
                }
            }
        }
        manager.getNowLocalFileList(new String[] { SENDER_FILE_PATH_TEST });
        allFileList = manager.getNowLocalFiles();
        manager.getLastLocalFileList(LAST_FILE_INFO_TEST);
        lastlocalList = manager.getLastLocalFiles();
        manager.getSendingFileList();
        sendingFileList = manager.getSendingFiles();
        assert (sendingFileList.size() == newFileList.size());
        for (Entry<String, Set<String>> entry : sendingFileList.entrySet()) {
            assert (newFileList.containsKey(entry.getKey()));
            assert (newFileList.get(entry.getKey()).containsAll(entry.getValue()));
        }
    }

    private boolean isEmpty(Map<String, Set<String>> sendingFileList) {
        for (Entry<String, Set<String>> entry : sendingFileList.entrySet()) {
            if (entry.getValue().size() != 0)
                return false;
        }
        return true;
    }
}