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

package org.apache.iotdb.tool.backup;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.tool.data.AbstractDataTool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class IoTDBDataBackTool {
  static Map<String, String> mm = new HashMap<>();
  static Map<String, String> cpmm = new HashMap<>();
  static String type = "";
  static String nodeTypeParam = "";

  static String targetDirParam = "";
  static String targetDataDirParam = "";
  static String targetWalDirParam = "";
  static String remoteDnDataDir = "";

  static AtomicInteger fileCount = new AtomicInteger(0);
  static AtomicInteger targetFileCount = new AtomicInteger(0);
  static AtomicInteger processFileCount = new AtomicInteger(0);
  static final String filename = "backup.log";

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDataTool.class);

  private static final IoTDBDescriptor ioTDBDescriptor = IoTDBDescriptor.getInstance();
  static String sourcePath = System.getProperty(IoTDBConstant.IOTDB_HOME, null);
  static boolean IS_OBJECT_STORAGE = false;
  static String DEFAULT_DN_DATA_DIRS =
      "data"
          + File.separator
          + IoTDBConstant.DN_ROLE
          + File.separator
          + IoTDBConstant.DATA_FOLDER_NAME;
  static String DEFAULT_DN_SYSTEM_DIR =
      "data"
          + File.separator
          + IoTDBConstant.DN_ROLE
          + File.separator
          + IoTDBConstant.SYSTEM_FOLDER_NAME;

  static String DEFAULT_DN_CONSENSUS_DIR =
      "data"
          + File.separator
          + IoTDBConstant.DN_ROLE
          + File.separator
          + IoTDBConstant.CONSENSUS_FOLDER_NAME;
  static String DEFAULT_DN_WAL_DIRS =
      "data"
          + File.separator
          + IoTDBConstant.DN_ROLE
          + File.separator
          + IoTDBConstant.WAL_FOLDER_NAME;

  static String DEFAULT_DN_TRACING_DIR =
      "data" + File.separator + IoTDBConstant.DN_ROLE + File.separator + "tracing";

  static String DEFAULT_CN_SYSTEM_DIR =
      "data"
          + File.separator
          + IoTDBConstant.CN_ROLE
          + File.separator
          + IoTDBConstant.SYSTEM_FOLDER_NAME;

  static String DEFAULT_CN_CONSENSUS_DIR =
      "data"
          + File.separator
          + IoTDBConstant.CN_ROLE
          + File.separator
          + IoTDBConstant.CONSENSUS_FOLDER_NAME;

  private static void argsParse(String[] args) {
    for (int i = 0; i < args.length; i++) {
      if (args[i].equalsIgnoreCase("-quick")) {
        type = "quick";
      } else if (args[i].equalsIgnoreCase("-node") && i + 1 < args.length) {
        nodeTypeParam = args[i + 1];
      } else if (args[i].equalsIgnoreCase("-targetdir") && i + 1 < args.length) {
        targetDirParam = args[i + 1];
      } else if (args[i].equalsIgnoreCase("-targetdatadir") && i + 1 < args.length) {
        targetDataDirParam = args[i + 1];
      } else if (args[i].equalsIgnoreCase("-targetwaldir") && i + 1 < args.length) {
        targetWalDirParam = args[i + 1];
      }
    }
  }

  public static boolean vaildParam(String dnDataDirs, String dnWalDirs) {
    boolean isVaild = true;
    if (type == null || type.trim().length() == 0 || !type.equals("quick")) {
      if (targetDirParam.isEmpty()) {
        LOGGER.error(" -targetdir cannot be empty， The backup folder must be specified");
        isVaild = false;
      } else {
        if (isRelativePath(targetDirParam)) {
          LOGGER.error("-targetdir parameter exception, please use absolute path");
          isVaild = false;
        }
      }

      if (!targetDataDirParam.isEmpty()) {
        if (!matchPattern(targetDataDirParam, dnDataDirs)) {
          LOGGER.error(
              "-targetdatadir parameter exception, the number of original paths does not match the number of specified paths");
          isVaild = false;
        }
        if (targetPathVild(targetDataDirParam)) {
          LOGGER.error("-targetdatadir parameter exception, please use absolute path");
          isVaild = false;
        }
      }

      if (!targetWalDirParam.isEmpty()) {
        if (!matchPattern(targetWalDirParam, dnWalDirs)) {
          LOGGER.error(
              "-targetwaldir parameter exception, the number of original paths does not match the number of specified paths");
          isVaild = false;
        }
        if (targetPathVild(targetWalDirParam)) {
          LOGGER.error("-targetwaldir parameter exception, please use absolute path");
          isVaild = false;
        }
      }
    }
    return isVaild;
  }

  public static void main(String[] args) throws IOException {
    System.setProperty("IOTDB_HOME", System.getenv("IOTDB_HOME"));
    argsParse(args);
    File sourceDir = new File(sourcePath);

    Properties properties = getProperties(CommonConfig.SYSTEM_CONFIG_NAME);
    initDataNodeProperties(properties);
    initConfigNodeProperties(properties);

    StringBuilder targetDirString = new StringBuilder();
    Map<String, String> copyMap = new HashMap<>();
    Map<String, String> dnDataDirsMap = new HashMap<>();
    Map<String, String> cnMapProperties = new HashMap<>();
    Map<String, String> dnMapProperties = new HashMap<>();
    processFileCount.set(readFileData(filename));
    if (type != null && type.equals("quick")) {
      targetDirString
          .append(sourceDir.getParent())
          .append(File.separatorChar)
          .append("iotdb_backup");
      File targetDir = new File(targetDirString.toString());
      if (targetDir.exists()) {
        LOGGER.error("The backup folder already exists:{}", targetDirString);
        System.exit(0);
      }

      if (nodeTypeParam.equalsIgnoreCase("confignode")) {
        if (!targetDir.exists()) {
          targetDir.mkdirs();
        }
        countConfigNodeFile(targetDirString.toString(), copyMap, cnMapProperties);
        countNodeBack(targetDirString.toString(), copyMap);
        for (Map.Entry<String, String> entry : copyMap.entrySet()) {
          countFiles(entry.getKey());
        }

        ioTDBDataBack(copyMap, dnDataDirsMap);
        propertiesFileUpdate(
            targetDirString.toString()
                + File.separatorChar
                + "conf"
                + File.separatorChar
                + CommonConfig.SYSTEM_CONFIG_NAME,
            cnMapProperties);
      } else if (nodeTypeParam.equalsIgnoreCase("datanode")) {
        countDataNodeFile(targetDirString.toString(), copyMap, dnDataDirsMap, dnMapProperties);
        countNodeBack(targetDirString.toString(), copyMap);
        checkQuickMode(dnDataDirsMap);
        if (!targetDir.exists()) {
          targetDir.mkdirs();
        }
        for (Map.Entry<String, String> entry : copyMap.entrySet()) {
          countFiles(entry.getKey());
        }
        for (Map.Entry<String, String> entry : dnDataDirsMap.entrySet()) {
          countFiles(entry.getKey());
        }

        ioTDBDataBack(copyMap, dnDataDirsMap);
        propertiesFileUpdate(
            targetDirString.toString()
                + File.separatorChar
                + "conf"
                + File.separatorChar
                + CommonConfig.SYSTEM_CONFIG_NAME,
            dnMapProperties);
      } else if (nodeTypeParam.equalsIgnoreCase("all") || nodeTypeParam.isEmpty()) {
        countConfigNodeFile(targetDirString.toString(), copyMap, cnMapProperties);
        countDataNodeFile(targetDirString.toString(), copyMap, dnDataDirsMap, dnMapProperties);
        countNodeBack(targetDirString.toString(), copyMap);
        checkQuickMode(dnDataDirsMap);
        if (!targetDir.exists()) {
          targetDir.mkdirs();
        }
        for (Map.Entry<String, String> entry : copyMap.entrySet()) {
          countFiles(entry.getKey());
        }
        for (Map.Entry<String, String> entry : dnDataDirsMap.entrySet()) {
          countFiles(entry.getKey());
        }

        ioTDBDataBack(copyMap, dnDataDirsMap);
        propertiesFileUpdate(
            targetDirString.toString()
                + File.separatorChar
                + "conf"
                + File.separatorChar
                + CommonConfig.SYSTEM_CONFIG_NAME,
            cnMapProperties);
        propertiesFileUpdate(
            targetDirString.toString()
                + File.separatorChar
                + "conf"
                + File.separatorChar
                + CommonConfig.SYSTEM_CONFIG_NAME,
            dnMapProperties);
      }

    } else {
      if (targetDirParam != null && targetDirParam.length() > 0) {
        targetDirString.append(targetDirParam);
        File targetDir = new File(targetDirString.toString());
        if (!targetDir.exists()) {
          targetDir.mkdirs();
        }
      }

      if (nodeTypeParam.equalsIgnoreCase("confignode")) {
        countConfigNodeFile(targetDirString.toString(), copyMap, cnMapProperties);
        countNodeBack(targetDirString.toString(), copyMap);
        for (Map.Entry<String, String> entry : copyMap.entrySet()) {
          countFiles(entry.getKey());
        }
        isDirectoryInsideOrSame(copyMap);
        ioTDBDataBack(copyMap, dnDataDirsMap);
        propertiesFileUpdate(
            targetDirString.toString()
                + File.separatorChar
                + "conf"
                + File.separatorChar
                + CommonConfig.SYSTEM_CONFIG_NAME,
            cnMapProperties);
      } else if (nodeTypeParam.equalsIgnoreCase("datanode")) {
        countNodeBack(targetDirString.toString(), copyMap);
        countDataNodeFile(targetDirString.toString(), copyMap, dnDataDirsMap, dnMapProperties);

        for (Map.Entry<String, String> entry : copyMap.entrySet()) {
          countFiles(entry.getKey());
        }
        for (Map.Entry<String, String> entry : dnDataDirsMap.entrySet()) {
          countFiles(entry.getKey());
        }
        isDirectoryInsideOrSame(copyMap);
        isDirectoryInsideOrSame(dnDataDirsMap);
        ioTDBDataBack(copyMap, dnDataDirsMap);
        propertiesFileUpdate(
            targetDirString.toString()
                + File.separatorChar
                + "conf"
                + File.separatorChar
                + CommonConfig.SYSTEM_CONFIG_NAME,
            dnMapProperties);
      } else if (nodeTypeParam.equalsIgnoreCase("all") || nodeTypeParam.isEmpty()) {
        countNodeBack(targetDirString.toString(), copyMap);
        countConfigNodeFile(targetDirString.toString(), copyMap, cnMapProperties);
        countDataNodeFile(targetDirString.toString(), copyMap, dnDataDirsMap, dnMapProperties);
        for (Map.Entry<String, String> entry : copyMap.entrySet()) {
          countFiles(entry.getKey());
        }
        for (Map.Entry<String, String> entry : dnDataDirsMap.entrySet()) {
          countFiles(entry.getKey());
        }

        isDirectoryInsideOrSame(copyMap);
        isDirectoryInsideOrSame(dnDataDirsMap);

        ioTDBDataBack(copyMap, dnDataDirsMap);
        propertiesFileUpdate(
            targetDirString.toString()
                + File.separatorChar
                + "conf"
                + File.separatorChar
                + CommonConfig.SYSTEM_CONFIG_NAME,
            cnMapProperties);
        propertiesFileUpdate(
            targetDirString.toString()
                + File.separatorChar
                + "conf"
                + File.separatorChar
                + CommonConfig.SYSTEM_CONFIG_NAME,
            dnMapProperties);
      }
    }
    LOGGER.info("all operations are complete");
    delFile(filename);
  }

  private static void checkQuickMode(Map<String, String> dnDataDirsMap) {
    for (Map.Entry<String, String> entry : dnDataDirsMap.entrySet()) {
      File backupDir = new File(entry.getValue());
      if (backupDir.exists()) {
        LOGGER.error("The backup folder already exists:{}", entry.getValue());
        System.exit(0);
      }
    }
  }

  private static void isDirectoryInsideOrSame(Map<String, String> copyMap) {
    for (Map.Entry<String, String> sourceEntry : copyMap.entrySet()) {
      for (Map.Entry<String, String> targetEntry : copyMap.entrySet()) {
        File file = new File(sourceEntry.getKey());
        if (file.exists()) {
          Path targetPath = Paths.get(targetEntry.getValue());
          Path parentPath = Paths.get(sourceEntry.getKey());
          Path normalizedTargetPath = targetPath.normalize();
          Path normalizedParentPath = parentPath.normalize();
          if (normalizedTargetPath.startsWith(normalizedParentPath)
              || normalizedTargetPath.equals(normalizedParentPath)) {
            if (targetDirParam.length() > 0
                && targetDataDirParam.length() > 0
                && targetWalDirParam.length() > 0) {
              LOGGER.error(
                  "The directory to be backed up cannot be in the source directory, please check:{},{},{}",
                  targetDirParam,
                  targetDataDirParam,
                  targetWalDirParam);
              System.exit(0);
            } else if (targetDirParam.length() > 0 && targetDataDirParam.length() > 0) {
              LOGGER.error(
                  "The directory to be backed up cannot be in the source directory, please check:{},{}",
                  targetDirParam,
                  targetDataDirParam);
              System.exit(0);
            } else if (targetDirParam.length() > 0 && targetWalDirParam.length() > 0) {
              LOGGER.error(
                  "The directory to be backed up cannot be in the source directory, please check:{},{}",
                  targetDirParam,
                  targetWalDirParam);
              System.exit(0);
            } else if (targetDirParam.length() > 0) {
              LOGGER.error(
                  "The directory to be backed up cannot be in the source directory, please check:{}",
                  targetDirParam);
              System.exit(0);
            }
          }
        }
      }
    }
  }

  private static void ioTDBDataBack(
      Map<String, String> copyMap, Map<String, String> dnDataDirsMap) {

    for (Map.Entry<String, String> entry : copyMap.entrySet()) {
      File file = new File(entry.getKey());
      if (file.isDirectory()) {
        compareAndcopyDirectory(file, new File(entry.getValue()));
      } else {
        if (file.exists()) {
          File targetFile = new File(entry.getValue());
          try {
            Files.copy(file.toPath(), targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            targetFileCount.incrementAndGet();
          } catch (IOException e) {
            LOGGER.error("copy file error", e);
          }
        }
      }
    }

    for (Map.Entry<String, String> entry : dnDataDirsMap.entrySet()) {
      File file = new File(entry.getKey());
      if (file.isDirectory()) {
        compareAndLinkDirectory(file, new File(entry.getValue()));
      } else {
        if (file.exists()) {
          File targetFile = new File(entry.getValue());
          try {
            Files.copy(file.toPath(), targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            targetFileCount.incrementAndGet();
          } catch (IOException e) {
            LOGGER.error("copy file error", e);
          }
        }
      }
    }
  }

  private static void countNodeBack(String targetDirString, Map<String, String> copyMap) {
    File sourceDir = new File(sourcePath);
    Properties properties = getProperties(CommonConfig.SYSTEM_CONFIG_NAME);
    initDataNodeProperties(properties);
    initConfigNodeProperties(properties);

    copyMap.put(
        sourceDir.getAbsolutePath() + File.separatorChar + ".env",
        targetDirString + File.separatorChar + ".env");
    copyMap.put(
        sourceDir.getAbsolutePath() + File.separatorChar + "conf",
        targetDirString + File.separatorChar + "conf");
    copyMap.put(
        sourceDir.getAbsolutePath() + File.separatorChar + "ext",
        targetDirString + File.separatorChar + "ext");
    copyMap.put(
        sourceDir.getAbsolutePath() + File.separatorChar + "licenses",
        targetDirString + File.separatorChar + "licenses");
    copyMap.put(
        sourceDir.getAbsolutePath() + File.separatorChar + "tools",
        targetDirString + File.separatorChar + "tools");
    copyMap.put(
        sourceDir.getAbsolutePath() + File.separatorChar + "sbin",
        targetDirString + File.separatorChar + "sbin");
    copyMap.put(
        sourceDir.getAbsolutePath() + File.separatorChar + "LICENSE",
        targetDirString + File.separatorChar + "LICENSE");
    copyMap.put(
        sourceDir.getAbsolutePath() + File.separatorChar + "NOTICE",
        targetDirString + File.separatorChar + "NOTICE");
    copyMap.put(
        sourceDir.getAbsolutePath() + File.separatorChar + "README.md",
        targetDirString + File.separatorChar + "README.md");
    copyMap.put(
        sourceDir.getAbsolutePath() + File.separatorChar + "README_ZH.md",
        targetDirString + File.separatorChar + "README_ZH.md");
    copyMap.put(
        sourceDir.getAbsolutePath() + File.separatorChar + File.separatorChar + "RELEASE_NOTES.md",
        targetDirString + File.separatorChar + "RELEASE_NOTES.md");
    copyMap.put(
        sourceDir.getAbsolutePath() + File.separatorChar + "activation",
        targetDirString + File.separatorChar + "activation");
    copyMap.put(
        sourceDir.getAbsolutePath() + File.separatorChar + "lib",
        targetDirString + File.separatorChar + "lib");
  }

  private static void countDataNodeFile(
      String targetDirString,
      Map<String, String> copyMap,
      Map<String, String> dnDataDirsMap,
      Map<String, String> dnMapProperties) {
    Properties dataProperties = getProperties(CommonConfig.SYSTEM_CONFIG_NAME);
    initDataNodeProperties(dataProperties);

    String dnSystemDir = dataProperties.getProperty("dn_system_dir");
    String dnConsensusDir = dataProperties.getProperty("dn_consensus_dir");

    String dnTracingDir = dataProperties.getProperty("dn_tracing_dir");

    String dnWalDirs = dataProperties.getProperty("dn_wal_dirs");
    String dnDataDirs = dataProperties.getProperty("dn_data_dirs");
    String dnDataDirAll = dnDataDirs;
    dnDataDirs = isObjectStorage(dnDataDirs);
    remoteDnDataDir = dnDataDirAll.replaceAll(dnDataDirs, "");
    dnSystemDir = pathHandler(dnSystemDir);
    dnConsensusDir = pathHandler(dnConsensusDir);
    dnTracingDir = pathHandler(dnTracingDir);
    dnWalDirs = pathHandler(dnWalDirs);
    dnDataDirs = pathHandler(dnDataDirs);
    String bakDnSystemDir = targetDirString + File.separatorChar + DEFAULT_DN_SYSTEM_DIR;
    String bakDnConsensusDir = targetDirString + File.separatorChar + DEFAULT_DN_CONSENSUS_DIR;
    String bakDnTracingDir = targetDirString + File.separatorChar + DEFAULT_DN_TRACING_DIR;
    String bakDnWalDirs = targetDirString + File.separatorChar + DEFAULT_DN_WAL_DIRS;
    if (type.equals("quick")) {
      targetDirParam = targetDirString;
      targetWalDirParam = sourceWalCoverTargetWalDirsHandler(dnWalDirs, bakDnWalDirs);
    }
    if (targetWalDirParam.isEmpty()) {
      targetWalDirParam = sourceWalCoverTargetWalDirsHandler(dnWalDirs, bakDnWalDirs);
    } else {
      targetWalDirParam = getCreateDnDataPathString(dnWalDirs, targetWalDirParam, "wal");
    }
    String targetDnDataDirs;
    if (targetDataDirParam.isEmpty()) {
      targetDataDirParam =
          targetDirParam
              + File.separatorChar
              + "data"
              + File.separatorChar
              + "datanode"
              + File.separatorChar
              + "data";
    }
    if (!vaildParam(dnDataDirs, dnWalDirs)) {
      System.exit(0);
    }
    targetDnDataDirs = getCreateDnDataPathString(dnDataDirs, targetDataDirParam, "data");
    Map<String, String> dnSystemDirMap =
        getCreatePathMapping(Objects.requireNonNull(dnSystemDir), bakDnSystemDir, "system");
    dnDataDirsMap.putAll(getCreatePathMapping(dnDataDirs, targetDnDataDirs, "data"));
    Map<String, String> dnConsensusDirMap =
        getCreatePathMapping(
            Objects.requireNonNull(dnConsensusDir), bakDnConsensusDir, "consensus");
    Map<String, String> dnTracingDirMap =
        getCreatePathMapping(dnTracingDir, bakDnTracingDir, "tracing");
    Map<String, String> dnWalDirsMap = getCreatePathMapping(dnWalDirs, targetWalDirParam, "wal");
    copyMap.putAll(dnSystemDirMap);
    copyMap.putAll(dnConsensusDirMap);
    copyMap.putAll(dnWalDirsMap);
    copyMap.putAll(dnTracingDirMap);

    dnMapProperties.put("dn_system_dir", bakDnSystemDir);
    dnMapProperties.put("dn_data_dirs", targetDnDataDirs + remoteDnDataDir);
    dnMapProperties.put("dn_wal_dirs", targetWalDirParam);
    dnMapProperties.put("dn_tracing_dir", bakDnTracingDir);
    dnMapProperties.put("dn_consensus_dir", bakDnConsensusDir);
  }

  private static String isObjectStorage(String dnDataDirs) {
    StringBuilder tmpDnDataDirs = new StringBuilder();
    String[] patternDirs = dnDataDirs.split(";");
    for (int i = 0; i < patternDirs.length; i++) {
      String patternDir = patternDirs[i];
      String[] subPatternDirs = patternDir.split(",");
      for (int c = 0; c < subPatternDirs.length; c++) {
        if (c == subPatternDirs.length - 1 && i == patternDirs.length - 1) {
          if (subPatternDirs[c].equals("OBJECT_STORAGE")) {
            IS_OBJECT_STORAGE = true;
          } else {
            tmpDnDataDirs.append(subPatternDirs[c]);
          }
        } else {
          tmpDnDataDirs.append(subPatternDirs[c]);
          if (subPatternDirs.length > 1 && c < subPatternDirs.length - 1) {
            tmpDnDataDirs.append(",");
          } else if (patternDirs.length > 1 && i < patternDirs.length - 1) {
            tmpDnDataDirs.append(";");
          }
        }
      }
    }
    if (IS_OBJECT_STORAGE) {
      return tmpDnDataDirs.toString().substring(0, tmpDnDataDirs.toString().length() - 1);
    }
    return tmpDnDataDirs.toString();
  }

  private static void countConfigNodeFile(
      String targetDirString, Map<String, String> copyMap, Map<String, String> cnMapProperties) {
    Properties configProperties = getProperties(CommonConfig.SYSTEM_CONFIG_NAME);
    initConfigNodeProperties(configProperties);

    String bakCnSystemDir = targetDirString + File.separatorChar + DEFAULT_CN_SYSTEM_DIR;
    String bakCnConsensusDir = targetDirString + File.separatorChar + DEFAULT_CN_CONSENSUS_DIR;

    String cnSystemDir = configProperties.getProperty("cn_system_dir");
    String cnConsensusDir = configProperties.getProperty("cn_consensus_dir");
    cnSystemDir = pathHandler(cnSystemDir);
    cnConsensusDir = pathHandler(cnConsensusDir);
    Map<String, String> cnSystemDirMap =
        getCreatePathMapping(cnSystemDir, bakCnSystemDir, "system");
    Map<String, String> cnConsensusDirMap =
        getCreatePathMapping(cnConsensusDir, bakCnConsensusDir, "consensus");

    copyMap.putAll(cnSystemDirMap);
    copyMap.putAll(cnConsensusDirMap);

    cnMapProperties.put("cn_system_dir", bakCnSystemDir);
    cnMapProperties.put("cn_consensus_dir", bakCnConsensusDir);
  }

  private static String pathHandler(String pathsList) {
    StringBuilder pathStrb = new StringBuilder();
    String[] pathList = pathsList.split(";");
    for (int t = 0; t < pathList.length; t++) {
      String paths = pathList[t];
      String[] dirList = paths.split(",");
      for (int i = 0; i < dirList.length; i++) {
        if (isRelativePath(dirList[i])) {
          if (i == 0) {
            pathStrb.append(sourcePath).append(File.separatorChar).append(dirList[i]);
          } else {
            pathStrb.append(",");
            pathStrb.append(sourcePath).append(File.separatorChar).append(dirList[i]);
          }
        } else {
          if (i == 0) {
            pathStrb.append(dirList[i]);
          } else {
            pathStrb.append(",");
            pathStrb.append(dirList[i]);
          }
        }
      }
      if (t < pathList.length - 1) {
        pathStrb.append(";");
      }
    }

    return pathStrb.toString();
  }

  private static boolean isRelativePath(String path) {
    Path p = Paths.get(path);
    return !p.isAbsolute();
  }

  public static String sourceWalCoverTargetWalDirsHandler(String dnDirs, String targetDirs) {
    String[] sourcePathList = dnDirs.split(",");
    StringBuilder subTargetDataDir = new StringBuilder();
    for (int i = 0; i < sourcePathList.length; i++) {
      if (i == sourcePathList.length - 1) {
        if (sourcePathList.length == 1) {
          subTargetDataDir.append(targetDirs);
        } else {
          subTargetDataDir
              .append(targetDirs)
              .append(File.separatorChar)
              .append("wal")
              .append(i + 1);
        }

      } else {
        subTargetDataDir
            .append(targetDirs)
            .append(File.separatorChar)
            .append("wal")
            .append(i + 1)
            .append(",");
      }
    }
    return subTargetDataDir.toString();
  }

  public static void propertiesFileUpdate(
      String sourcePropertiesPath, Map<String, String> mapProperties) {
    for (Map.Entry<String, String> entry : mapProperties.entrySet()) {
      propertiesFileUpdate(sourcePropertiesPath, entry.getKey(), entry.getValue());
    }
  }

  public static String getCreateDnDataPathString(
      String resourcePath, String targetPath, String dirType) {

    String[] sourcePathsList = resourcePath.split(";");
    String[] targetPathsList = targetPath.split(";");
    StringBuilder pathStrb = new StringBuilder();
    if (sourcePathsList.length != targetPathsList.length) {
      int num = 1;
      for (int i = 0; i < sourcePathsList.length; i++) {
        String[] sourcePathArray = sourcePathsList[i].split(",");
        for (int j = 0; j < sourcePathArray.length; j++) {
          String newPath = targetPathsList[0];
          if (sourcePathsList.length == 1
              && targetPathsList.length == 1
              && sourcePathArray.length == 1) {
            newPath = newPath + File.separatorChar + dirType;
          } else {
            newPath = newPath + File.separatorChar + dirType + num;
          }
          pathStrb.append(newPath);
          if (j < sourcePathArray.length - 1) {
            pathStrb.append(",");
          }
          num++;
        }
        if (i < sourcePathsList.length - 1) {
          pathStrb.append(";");
        }
      }
    } else {
      int num = 1;
      for (int i = 0; i < sourcePathsList.length; i++) {
        String[] sourcePathArray = sourcePathsList[i].split(",");
        String[] targetPathArray = targetPathsList[i].split(",");
        if (sourcePathArray.length != targetPathArray.length) {
          if (targetPathArray.length == 1) {
            for (int j = 0; j < sourcePathArray.length; j++) {
              String newPath;
              if (sourcePathsList.length == 1
                  && targetPathsList.length == 1
                  && sourcePathArray.length == 1) {
                newPath = targetPathArray[0] + File.separatorChar + dirType;
              } else {
                newPath = targetPathArray[0] + File.separatorChar + dirType + num;
              }
              pathStrb.append(newPath);
              if (j < sourcePathArray.length - 1) {
                pathStrb.append(",");
              }
              num++;
            }
          } else {
            for (int j = 0; j < sourcePathArray.length; j++) {
              pathStrb.append(targetPathArray[j]);
              if (j < sourcePathArray.length - 1) {
                pathStrb.append(",");
              }
            }
          }
        } else {
          for (int j = 0; j < sourcePathArray.length; j++) {
            pathStrb.append(targetPathArray[j]);
            if (j < sourcePathArray.length - 1) {
              pathStrb.append(",");
            }
          }
        }
        if (sourcePathsList.length > 1 && i < sourcePathsList.length - 1) {
          pathStrb.append(";");
        }
      }
    }

    return pathStrb.toString();
  }

  public static Map<String, String> getCreatePathMapping(
      String resourcePath, String targetPath, String dirType) {
    Map<String, String> map = new HashMap<>();
    String[] sourcePathsList = resourcePath.split(";");
    String[] targetPathsList = targetPath.split(";");
    if (sourcePathsList.length != targetPathsList.length) {
      int num = 1;
      for (int i = 0; i < sourcePathsList.length; i++) {
        String[] sourcePathArray = sourcePathsList[i].split(",");
        for (int j = 0; j < sourcePathArray.length; j++) {
          String newPath = targetPathsList[0];
          if (sourcePathsList.length == 1
              && targetPathsList.length == 1
              && sourcePathArray.length == 1) {
            newPath = newPath + File.separatorChar + dirType;
          } else {
            newPath = newPath + File.separatorChar + dirType + num;
          }
          createDirectory(newPath);
          map.put(sourcePathArray[j], newPath);
          num++;
        }
      }
    } else {
      int num = 1;
      for (int i = 0; i < sourcePathsList.length; i++) {
        String[] sourcePathArray = sourcePathsList[i].split(",");
        String[] targetPathArray = targetPathsList[i].split(",");
        if (sourcePathArray.length != targetPathArray.length) {
          if (targetPathArray.length == 1) {
            for (int j = 0; j < sourcePathArray.length; j++) {
              String newPath;
              if (sourcePathsList.length == 1
                  && targetPathsList.length == 1
                  && sourcePathArray.length == 1) {
                newPath = targetPathArray[0] + File.separatorChar + dirType;
              } else {
                newPath = targetPathArray[0] + File.separatorChar + dirType + num;
              }
              createDirectory(newPath);
              map.put(sourcePathArray[j], newPath);
              num++;
            }
          } else {
            for (int j = 0; j < sourcePathArray.length; j++) {
              map.put(sourcePathArray[j], targetPathArray[j]);
            }
          }
        } else {
          for (int j = 0; j < sourcePathArray.length; j++) {
            map.put(sourcePathArray[j], targetPathArray[j]);
          }
        }
      }
    }

    return map;
  }

  public static boolean matchPattern(String input, String pattern) {
    if (input.contains(";") && pattern.contains(";")) {
      String[] inputDirs = input.split(";");
      String[] patternDirs = pattern.split(";");
      if (inputDirs.length != patternDirs.length) {
        return false;
      }
      for (int i = 0; i < inputDirs.length; i++) {
        String inputDir = inputDirs[i];
        String patternDir = patternDirs[i];
        if (!matchDirectory(inputDir, patternDir)) {
          return false;
        }
      }
      return true;
    } else if (pattern.contains(";") && !input.contains(";")) {
      return !input.contains(",");
    } else if (!pattern.contains(";") && input.contains(";")) {
      return false;
    }
    return matchDirectory(input, pattern);
  }

  public static boolean targetPathVild(String pattern) {
    String[] patternDirs = pattern.split(";");
    for (int i = 0; i < patternDirs.length; i++) {
      String patternDir = patternDirs[i];
      String[] subPatternDirs = patternDir.split(",");
      for (String subPatternDir : subPatternDirs) {
        if (isRelativePath(subPatternDir)) {
          return true;
        }
      }
    }
    return false;
  }

  private static boolean matchDirectory(String inputDir, String patternDir) {
    String[] inputLevels = inputDir.split(",");
    String[] patternLevels = patternDir.split(",");
    return inputLevels.length == patternLevels.length || patternLevels.length == 1;
  }

  private static void initDataNodeProperties(Properties properties) {
    if (properties.getProperty("dn_system_dir") == null) {
      properties.setProperty("dn_system_dir", DEFAULT_DN_SYSTEM_DIR);
    }
    if (properties.getProperty("dn_consensus_dir") == null) {
      properties.setProperty("dn_consensus_dir", DEFAULT_DN_CONSENSUS_DIR);
    }
    if (properties.getProperty("dn_data_dirs") == null) {
      properties.setProperty("dn_data_dirs", DEFAULT_DN_DATA_DIRS);
    }
    if (properties.getProperty("dn_tracing_dir") == null) {
      properties.setProperty("dn_tracing_dir", DEFAULT_DN_TRACING_DIR);
    }
    if (properties.getProperty("dn_wal_dirs") == null) {
      properties.setProperty("dn_wal_dirs", DEFAULT_DN_WAL_DIRS);
    }
  }

  private static void initConfigNodeProperties(Properties properties) {

    if (properties.getProperty("cn_system_dir") == null) {
      properties.setProperty("cn_system_dir", DEFAULT_CN_SYSTEM_DIR);
    }
    if (properties.getProperty("cn_consensus_dir") == null) {
      properties.setProperty("cn_consensus_dir", DEFAULT_CN_CONSENSUS_DIR);
    }
  }

  private static Properties getProperties(String configName) {
    URL url = ioTDBDescriptor.getPropsUrl(configName);
    Properties properties = new Properties();
    if (url != null) {
      try (InputStream inputStream = url.openStream()) {
        LOGGER.info("Start to read config file {}", url);
        properties.load(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return properties;
  }

  private static boolean filesAreEqual(Path file1, Path file2) throws IOException {
    long size1 = Files.size(file1);
    long size2 = Files.size(file2);
    return size1 == size2;
  }

  public static void compareAndcopyDirectory(File sourceDirectory, File targetDirectory) {
    try {
      Files.walkFileTree(
          sourceDirectory.toPath(),
          new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                throws IOException {
              Path targetFile =
                  targetDirectory.toPath().resolve(sourceDirectory.toPath().relativize(file));
              if (Files.exists(file)) {
                cpmm.put(file.toFile().getAbsolutePath(), "1");
                targetFileCount.incrementAndGet();
                if (!Files.exists(targetFile) || !filesAreEqual(file, targetFile)) {
                  Files.copy(file, targetFile, StandardCopyOption.REPLACE_EXISTING);
                }
              }
              if (processFileCount.get() > targetFileCount.get()) {
                writeFileData(filename, processFileCount.get());
                LOGGER.info(
                    "total file number:"
                        + fileCount
                        + ",verify the number of files:"
                        + targetFileCount);
              } else {
                writeFileData(filename, targetFileCount.get());
                LOGGER.info(
                    "total file number:" + fileCount + ",backup file number:" + targetFileCount);
              }
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                throws IOException {
              Path targetDir =
                  targetDirectory.toPath().resolve(sourceDirectory.toPath().relativize(dir));
              if (!Files.exists(targetDir)) {
                Files.createDirectories(targetDir);
              }
              return FileVisitResult.CONTINUE;
            }
          });
    } catch (IOException e) {
      LOGGER.error("copy file error {}", sourceDirectory, e);
    }
  }

  public static void createDirectory(String directoryPath) {
    File directory = new File(directoryPath);
    if (!directory.exists()) {
      boolean created = directory.mkdirs();
      if (created) {
        LOGGER.info("Directory created successfully:{}", directoryPath);
      } else {
        LOGGER.error("Failed to create directory:{}", directoryPath);
      }
    }
  }

  public static void compareAndLinkDirectory(File sourceDirectory, File targetDirectory) {
    try {
      Files.walkFileTree(
          sourceDirectory.toPath(),
          new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                throws IOException {
              Path targetFile =
                  targetDirectory.toPath().resolve(sourceDirectory.toPath().relativize(file));
              if (Files.exists(file)) {
                cpmm.put(file.toFile().getAbsolutePath(), "1");
                targetFileCount.incrementAndGet();
                if (!Files.exists(targetFile) || !filesAreEqual(file, targetFile)) {
                  try {
                    Files.createLink(targetFile, file);
                  } catch (UnsupportedOperationException | IOException e) {
                    LOGGER.debug("link file error {}", e);
                    try {
                      Files.copy(file, targetFile, StandardCopyOption.REPLACE_EXISTING);
                    } catch (IOException ex) {
                      targetFileCount.decrementAndGet();
                      LOGGER.error("copy file error {}", ex);
                    }
                  }
                }
              }
              if (processFileCount.get() > targetFileCount.get()) {
                writeFileData(filename, processFileCount.get());
                LOGGER.info(
                    "total file number:"
                        + fileCount
                        + ",verify the number of files:"
                        + targetFileCount);
              } else {
                writeFileData(filename, targetFileCount.get());
                LOGGER.info(
                    "total file number:" + fileCount + ",backup file number:" + targetFileCount);
              }
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                throws IOException {
              Path targetDir =
                  targetDirectory.toPath().resolve(sourceDirectory.toPath().relativize(dir));
              if (!Files.exists(targetDir)) {
                Files.createDirectories(targetDir);
              }
              return FileVisitResult.CONTINUE;
            }
          });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static String formatPathForOS(String path) {
    String os = System.getProperty("os.name").toLowerCase();
    if (os.contains("win")) {
      return path.replace("\\", "\\\\");
    } else {
      return path;
    }
  }

  public static void propertiesFileUpdate(String filePath, String key, String newValue) {
    try {
      newValue = formatPathForOS(newValue);
      FileInputStream fileInputStream = new FileInputStream(filePath);
      List<String> lines;
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(fileInputStream))) {

        lines = new ArrayList<>();
        String line;
        while ((line = reader.readLine()) != null) {
          lines.add(line);
        }
      }
      boolean keyFound = false;

      for (int i = 0; i < lines.size(); i++) {
        String currentLine = lines.get(i);
        currentLine = currentLine.trim();
        if (currentLine.startsWith("#") && currentLine.substring(1).trim().equals(key)) {
        } else if (currentLine.contains("=")) {
          int equalsIndex = currentLine.indexOf("=");
          String propertyKey = currentLine.substring(0, equalsIndex).trim();
          if (propertyKey.equals(key)) {
            lines.set(i, propertyKey + "=" + newValue);
            keyFound = true;
            break;
          }
        }
      }
      if (!keyFound) {
        lines.add(key + "=" + newValue);
      }

      FileOutputStream fileOutputStream = new FileOutputStream(filePath);
      PrintWriter printWriter = new PrintWriter(fileOutputStream);
      for (String fileLine : lines) {
        printWriter.println(fileLine);
      }

      printWriter.close();
      fileOutputStream.close();

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static int countFiles(String directoryPath) {
    countFilesRecursively(new File(directoryPath), fileCount);
    return fileCount.get();
  }

  private static void countFilesRecursively(File file, AtomicInteger fileCount) {

    if (file.isDirectory() && file.exists()) {
      File[] files = file.listFiles();
      if (files != null) {
        for (File subFile : files) {
          countFilesRecursively(subFile, fileCount);
        }
      }
    } else {
      if (file.exists()) {
        mm.put(file.getAbsolutePath(), "1");
        fileCount.incrementAndGet();
      }
    }
  }

  public static int readFileData(String filename) {
    filename = sourcePath + File.separatorChar + "logs" + File.separatorChar + filename;
    createFile(filename);
    try {
      Path filePath = Paths.get(filename);
      List<String> lines = Files.readAllLines(filePath, StandardCharsets.UTF_8);
      if (!lines.isEmpty()) {
        return Integer.parseInt(lines.get(0));
      }
    } catch (IOException e) {
      LOGGER.error("Failed to read data from file: {}", filename, e);
    }
    return 0;
  }

  public static void delFile(String filename) throws IOException {
    filename = sourcePath + File.separatorChar + "logs" + File.separatorChar + filename;
    File file = new File(filename);
    if (file.exists()) {
      Files.delete(file.toPath());
    }
  }

  public static void writeFileData(String filename, int data) {
    filename = sourcePath + File.separatorChar + "logs" + File.separatorChar + filename;
    Path filePath = Paths.get(filename);
    try {
      Files.write(filePath, Integer.toString(data).getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      LOGGER.error("Failed to write data to file: {}", filename, e);
    }
  }

  public static void createFile(String filename) {
    Path filePath = Paths.get(filename);
    if (!Files.exists(filePath)) {
      try {
        Files.createFile(filePath);
      } catch (IOException e) {
        LOGGER.error("Failed to create file: {}", filename, e);
      }
    }
  }
}
