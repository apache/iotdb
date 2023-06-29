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
package org.apache.iotdb.tsfile.fileSystem.fsFactory;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.fileSystem.FSType;
import org.apache.iotdb.tsfile.utils.FSUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;

public class OSFSFactory implements FSFactory {
  private static final Logger logger = LoggerFactory.getLogger(OSFSFactory.class);

  private Constructor constructorWithPathname;
  private Constructor constructorWithParentStringAndChild;
  private Constructor constructorWithParentFileAndChild;
  private Constructor constructorWithUri;
  private Method getBufferedReader;
  private Method getBufferedWriter;
  private Method getBufferedInputStream;
  private Method getBufferedOutputStream;
  private Method listFilesBySuffix;
  private Method listFilesByPrefix;
  private Method renameTo;
  private Method putFile;
  private Method copyTo;
  private Method deleteObjectsByPrefix;

  public OSFSFactory() {
    try {
      Class<?> clazz =
          Class.forName(TSFileDescriptor.getInstance().getConfig().getObjectStorageFile());
      constructorWithPathname = clazz.getConstructor(String.class);
      constructorWithParentStringAndChild = clazz.getConstructor(String.class, String.class);
      constructorWithParentFileAndChild = clazz.getConstructor(File.class, String.class);
      constructorWithUri = clazz.getConstructor(URI.class);
      getBufferedReader = clazz.getMethod("getBufferedReader");
      getBufferedWriter = clazz.getMethod("getBufferedWriter", boolean.class);
      getBufferedInputStream = clazz.getMethod("getBufferedInputStream");
      getBufferedOutputStream = clazz.getMethod("getBufferedOutputStream");
      listFilesBySuffix = clazz.getMethod("listFilesBySuffix", String.class, String.class);
      listFilesByPrefix = clazz.getMethod("listFilesByPrefix", String.class, String.class);
      renameTo = clazz.getMethod("renameTo", File.class);
      putFile = clazz.getMethod("putFile", File.class);
      copyTo = clazz.getMethod("copyTo", File.class);
      deleteObjectsByPrefix = clazz.getMethod("deleteObjectsByPrefix");
    } catch (ClassNotFoundException | NoSuchMethodException e) {
      logger.error(
          "Failed to get object storage. Please check your dependency of object storage module.",
          e);
    }
  }

  @Override
  public File getFileWithParent(String pathname) {
    try {
      return (File) constructorWithPathname.newInstance(pathname);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      logger.error(
          "Failed to get file: {}. Please check your dependency of object storage module.",
          pathname,
          e);
      return null;
    }
  }

  @Override
  public File getFile(String pathname) {
    try {
      return (File) constructorWithPathname.newInstance(pathname);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      logger.error(
          "Failed to get file: {}. Please check your dependency of Hadoop module.", pathname, e);
      return null;
    }
  }

  @Override
  public File getFile(String parent, String child) {
    try {
      return (File) constructorWithParentStringAndChild.newInstance(parent, child);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      logger.error(
          "Failed to get file: {}. Please check your dependency of Hadoop module.",
          parent + File.separator + child,
          e);
      return null;
    }
  }

  @Override
  public File getFile(File parent, String child) {
    try {
      return (File) constructorWithParentFileAndChild.newInstance(parent, child);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      logger.error(
          "Failed to get file: {}. Please check your dependency of Hadoop module.",
          parent.getAbsolutePath() + File.separator + child,
          e);
      return null;
    }
  }

  @Override
  public File getFile(URI uri) {
    try {
      return (File) constructorWithUri.newInstance(uri);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      logger.error(
          "Failed to get file: {}. Please check your dependency of object storage module.", uri, e);
      return null;
    }
  }

  @Override
  public BufferedReader getBufferedReader(String filePath) {
    try {
      return (BufferedReader)
          getBufferedReader.invoke(constructorWithPathname.newInstance(filePath));
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      logger.error(
          "Failed to get buffered reader for {}. Please check your dependency of object storage module.",
          filePath,
          e);
      return null;
    }
  }

  @Override
  public BufferedWriter getBufferedWriter(String filePath, boolean append) {
    try {
      return (BufferedWriter)
          getBufferedWriter.invoke(constructorWithPathname.newInstance(filePath), append);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      logger.error(
          "Failed to get buffered writer for {}. Please check your dependency of object storage module.",
          filePath,
          e);
      return null;
    }
  }

  @Override
  public BufferedInputStream getBufferedInputStream(String filePath) {
    try {
      return (BufferedInputStream)
          getBufferedInputStream.invoke(constructorWithPathname.newInstance(filePath));
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      logger.error(
          "Failed to get buffered input stream for {}. Please check your dependency of object storage module.",
          filePath,
          e);
      return null;
    }
  }

  @Override
  public BufferedOutputStream getBufferedOutputStream(String filePath) {
    try {
      return (BufferedOutputStream)
          getBufferedOutputStream.invoke(constructorWithPathname.newInstance(filePath));
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      logger.error(
          "Failed to get buffered output stream for {}. Please check your dependency of object storage module.",
          filePath,
          e);
      return null;
    }
  }

  @Override
  public void moveFile(File srcFile, File destFile) throws IOException {
    try {
      renameTo.invoke(srcFile, destFile);
    } catch (InvocationTargetException | IllegalAccessException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void copyFile(File srcFile, File destFile) throws IOException {
    FSType srcType = FSUtils.getFSType(srcFile);
    try {
      if (srcType == FSType.LOCAL) {
        putFile.invoke(destFile, srcFile);
      } else if (srcType == FSType.OBJECT_STORAGE) {
        copyTo.invoke(srcFile, destFile);
      } else {
        throw new IOException(
            String.format(
                "Doesn't support copy file from %s to %s.", srcType, FSType.OBJECT_STORAGE));
      }
    } catch (InvocationTargetException | IllegalAccessException e) {
      throw new IOException(e);
    }
  }

  @Override
  public File[] listFilesBySuffix(String fileFolder, String suffix) {
    try {
      return (File[])
          listFilesBySuffix.invoke(
              constructorWithPathname.newInstance(fileFolder), fileFolder, suffix);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      logger.error(
          "Failed to list files in {} with SUFFIX {}. Please check your dependency of object storage module.",
          fileFolder,
          suffix,
          e);
      return null;
    }
  }

  @Override
  public File[] listFilesByPrefix(String fileFolder, String prefix) {
    try {
      return (File[])
          listFilesByPrefix.invoke(
              constructorWithPathname.newInstance(fileFolder), fileFolder, prefix);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      logger.error(
          "Failed to list files in {} with PREFIX {}. Please check your dependency of object storage module.",
          fileFolder,
          prefix,
          e);
      return null;
    }
  }

  @Override
  public boolean deleteIfExists(File file) {
    return file.delete();
  }

  @Override
  public void deleteDirectory(String dir) throws IOException {
    try {
      deleteObjectsByPrefix.invoke(constructorWithPathname.newInstance(dir));
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      throw new IOException(e);
    }
  }
}
