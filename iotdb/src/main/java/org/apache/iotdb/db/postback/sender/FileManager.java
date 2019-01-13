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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.postback.conf.PostBackSenderConfig;
import org.apache.iotdb.db.postback.conf.PostBackSenderDescriptor;

/**
 * The class is to pick up which files need to postback.
 * 
 * @author lta
 */
public class FileManager {

    private Map<String, Set<String>> sendingFiles = new HashMap<>();
    private Set<String> lastLocalFiles = new HashSet<>();
    private Map<String, Set<String>> nowLocalFiles = new HashMap<>();
    private PostBackSenderConfig postbackConfig = PostBackSenderDescriptor.getInstance().getConfig();
    private IoTDBConfig tsfileConfig = IoTDBDescriptor.getInstance().getConfig();

    private static final Logger LOGGER = LoggerFactory.getLogger(FileManager.class);

    private static class FileManagerHolder {
        private static final FileManager INSTANCE = new FileManager();
    }

    private FileManager() {
    }

    public static final FileManager getInstance() {
        return FileManagerHolder.INSTANCE;
    }

    public void init() {
        sendingFiles.clear();
        lastLocalFiles.clear();
        nowLocalFiles.clear();
        getLastLocalFileList(postbackConfig.lastFileInfo);
        getNowLocalFileList(tsfileConfig.getBufferWriteDirs());
        getSendingFileList();
    }

    public void getSendingFileList() {
        for (Entry<String, Set<String>> entry : nowLocalFiles.entrySet()) {
            for (String path : entry.getValue()) {
                if (!lastLocalFiles.contains(path)) {
                    sendingFiles.get(entry.getKey()).add(path);
                }
            }
        }
        LOGGER.info("IoTDB sender : Sender has got list of sending files.");
        for (Entry<String, Set<String>> entry : sendingFiles.entrySet()) {
            for (String path : entry.getValue()) {
                LOGGER.info(path);
                nowLocalFiles.get(entry.getKey()).remove(path);
            }
        }
    }

    public void getLastLocalFileList(String path) {
        Set<String> fileList = new HashSet<>();
        File file = new File(path);
        try {
            if (!file.exists()) {
                file.createNewFile();
            } else {
                BufferedReader bf = null;
                try {
                    bf = new BufferedReader(new FileReader(file));
                    String fileName = null;
                    while ((fileName = bf.readLine()) != null) {
                        fileList.add(fileName);
                    }
                    bf.close();
                } catch (IOException e) {
                    LOGGER.error(
                            "IoTDB post back sender: cannot get last local file list when reading file {} because {}.",
                            postbackConfig.lastFileInfo, e.getMessage());
                } finally {
                    if (bf != null) {
                        bf.close();
                    }
                }
            }
        } catch (IOException e) {
            LOGGER.error("IoTDB post back sender: cannot get last local file list because {}", e.getMessage());
        }
        lastLocalFiles = fileList;
    }

    public void getNowLocalFileList(String[] paths) {
        for (String path : paths) {
            if (!new File(path).exists()) {
                continue;
            }
            File[] SGs = new File(path).listFiles();
            for (File storageGroup : SGs) {
                if (storageGroup.isDirectory() && !storageGroup.getName().equals("postback")) {
                    if (!nowLocalFiles.containsKey(storageGroup.getName()))
                        nowLocalFiles.put(storageGroup.getName(), new HashSet<String>());
                    if (!sendingFiles.containsKey(storageGroup.getName()))
                        sendingFiles.put(storageGroup.getName(), new HashSet<String>());
                    File[] files = storageGroup.listFiles();
                    for (File file : files) {
                        if (!file.getAbsolutePath().endsWith(".restore")) {
                            if (!new File(file.getAbsolutePath() + ".restore").exists())
                                nowLocalFiles.get(storageGroup.getName()).add(file.getAbsolutePath());
                        }
                    }
                }
            }
        }
    }

    public void backupNowLocalFileInfo(String backupFile) {
        BufferedWriter bufferedWriter = null;
        try {
            bufferedWriter = new BufferedWriter(new FileWriter(backupFile));
            for (Entry<String, Set<String>> entry : nowLocalFiles.entrySet()) {
                for (String file : entry.getValue()) {
                    bufferedWriter.write(file + "\n");
                }
            }
        } catch (IOException e) {
            LOGGER.error("IoTDB post back sender: cannot back up now local file info because {}", e);
        } finally {
            if (bufferedWriter != null) {
                try {
                    bufferedWriter.close();
                } catch (IOException e) {
                    LOGGER.error(
                            "IoTDB post back sender: cannot close stream after backing up now local file info because {}",
                            e);
                }
            }
        }
    }

    public Map<String, Set<String>> getSendingFiles() {
        return sendingFiles;
    }

    public Set<String> getLastLocalFiles() {
        return lastLocalFiles;
    }

    public Map<String, Set<String>> getNowLocalFiles() {
        return nowLocalFiles;
    }

    public void setNowLocalFiles(Map<String, Set<String>> newNowLocalFiles) {
        nowLocalFiles = newNowLocalFiles;
    }
}