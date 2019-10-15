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

package org.apache.iotdb.tsfile.fileSystem;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HDFSFactory implements FSFactory {

  private static final Logger logger = LoggerFactory.getLogger(HDFSFactory.class);
  private static Class<?> clazz;
  private static Constructor constructorWithPathname;
  private static Constructor constructorWithParentStringAndChild;
  private static Constructor constructorWithParentFileAndChild;
  private static Constructor constructorWithUri;
  private static Method getBufferedReader;
  private static Method getBufferedWriter;
  private static Method getBufferedInputStream;
  private static Method getBufferedOutputStream;
  private static Method listFilesBySuffix;
  private static Method listFilesByPrefix;

  static {
    try {
      clazz = Class.forName("org.apache.iotdb.tsfile.fileSystem.HDFSFile");
      constructorWithPathname = clazz.getConstructor(String.class);
      constructorWithParentStringAndChild = clazz.getConstructor(String.class, String.class);
      constructorWithParentFileAndChild = clazz.getConstructor(File.class, String.class);
      constructorWithUri = clazz.getConstructor(URI.class);
      getBufferedReader = clazz.getMethod("getBufferedReader", String.class, boolean.class);
      getBufferedWriter = clazz.getMethod("getBufferedWriter", String.class, boolean.class);
      getBufferedInputStream = clazz.getMethod("getBufferedInputStream", String.class);
      getBufferedOutputStream = clazz.getMethod("getBufferedOutputStream", String.class);
      listFilesBySuffix = clazz.getMethod("listFilesBySuffix", String.class);
      listFilesByPrefix = clazz.getMethod("listFilesByPrefix", String.class);
    } catch (ClassNotFoundException | NoSuchMethodException e) {
      logger.error(
          "Failed to get Hadoop file system. Please check your dependency of Hadoop module.", e);
    }
  }

  public File getFile(String pathname) {
    try {
      return (File) constructorWithPathname.newInstance(pathname);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      logger.error(
          "Failed to get file: {}. Please check your dependency of Hadoop module.", pathname, e);
      return null;
    }
  }

  public File getFile(String parent, String child) {
    try {
      return (File) constructorWithParentStringAndChild.newInstance(parent, child);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      logger.error(
          "Failed to get file: {}" + File.separator
              + "{}. Please check your dependency of Hadoop module.", parent, child, e);
      return null;
    }
  }

  public File getFile(File parent, String child) {
    try {
      return (File) constructorWithParentFileAndChild.newInstance(parent, child);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      logger.error(
          "Failed to get file: {}" + File.separator
              + "{}. Please check your dependency of Hadoop module.", parent.getAbsolutePath(),
          child, e);
      return null;
    }
  }

  public File getFile(URI uri) {
    try {
      return (File) constructorWithUri.newInstance(uri);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      logger.error(
          "Failed to get file: {}. Please check your dependency of Hadoop module.",
          uri.toString(), e);
      return null;
    }
  }

  public BufferedReader getBufferedReader(String filePath) {
    try {
      return (BufferedReader) getBufferedReader.invoke(clazz.newInstance(), filePath);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      logger.error(
          "Failed to get buffered reader for {}. Please check your dependency of Hadoop module.",
          filePath, e);
      return null;
    }
  }

  public BufferedWriter getBufferedWriter(String filePath, boolean append) {
    try {
      return (BufferedWriter) getBufferedWriter.invoke(clazz.newInstance(), filePath, append);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      logger.error(
          "Failed to get buffered writer for {}. Please check your dependency of Hadoop module.",
          filePath, e);
      return null;
    }
  }

  public BufferedInputStream getBufferedInputStream(String filePath) {
    try {
      return (BufferedInputStream) getBufferedInputStream.invoke(clazz.newInstance(), filePath);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      logger.error(
          "Failed to get buffered input stream for {}. Please check your dependency of Hadoop module.",
          filePath, e);
      return null;
    }
  }

  public BufferedOutputStream getBufferedOutputStream(String filePath) {
    try {
      return (BufferedOutputStream) getBufferedOutputStream.invoke(clazz.newInstance(), filePath);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      logger.error(
          "Failed to get buffered output stream for {}. Please check your dependency of Hadoop module.",
          filePath, e);
      return null;
    }
  }

  public void moveFile(File srcFile, File destFile) {
    boolean rename = srcFile.renameTo(destFile);
    if (!rename) {
      logger.error("Failed to rename file from {} to {}. ", srcFile.getName(),
          destFile.getName());
    }
  }

  public File[] listFilesBySuffix(String fileFolder, String suffix) {
    try {
      return (File[]) listFilesBySuffix.invoke(clazz.newInstance(), fileFolder, suffix);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      logger.error(
          "Failed to list files in {} with SUFFIX {}. Please check your dependency of Hadoop module.",
          fileFolder, suffix, e);
      return null;
    }
  }

  public File[] listFilesByPrefix(String fileFolder, String prefix) {
    try {
      return (File[]) listFilesByPrefix.invoke(clazz.newInstance(), fileFolder, prefix);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      logger.error(
          "Failed to list files in {} with PREFIX {}. Please check your dependency of Hadoop module.",
          fileFolder, prefix, e);
      return null;
    }
  }
}