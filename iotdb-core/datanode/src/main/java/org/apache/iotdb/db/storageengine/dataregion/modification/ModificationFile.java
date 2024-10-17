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

package org.apache.iotdb.db.storageengine.dataregion.modification;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.storageengine.dataregion.modification.v1.ModificationFileV1;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ModificationFile implements AutoCloseable {

  public static final String FILE_SUFFIX = ".mods2";
  public static final String COMPACTION_FILE_SUFFIX = ".compaction.mods2";
  private static final Logger LOGGER = LoggerFactory.getLogger(ModificationFile.class);

  private final File file;
  private FileChannel channel;
  private OutputStream fileOutputStream;
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public ModificationFile(File file) {
    this.file = file;
  }

  @SuppressWarnings("java:S2093") // cannot use try-with-resource, should not close here
  public void write(ModEntry entry) throws IOException {
    lock.writeLock().lock();
    try {
      if (fileOutputStream == null) {
        fileOutputStream = new BufferedOutputStream(Files.newOutputStream(file.toPath(), CREATE, APPEND));
        channel = FileChannel.open(file.toPath(), CREATE, APPEND);
      }
      entry.serialize(fileOutputStream);
      channel.force(false);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public Iterator<ModEntry> getModIterator(long offset) throws IOException {
    return new ModIterator(offset);
  }

  @TestOnly
  public List<ModEntry> getAllMods() throws IOException {
    return getAllMods(0);
  }

  public List<ModEntry> getAllMods(long offset) throws IOException {
    List<ModEntry> allMods = new ArrayList<>();
    getModIterator(offset).forEachRemaining(allMods::add);
    return allMods;
  }

  @Override
  public void close() throws IOException {
    if (fileOutputStream == null) {
      return;
    }

    lock.writeLock().lock();
    try {
      fileOutputStream.close();
      fileOutputStream = null;
      channel.force(true);
      channel.close();
      channel = null;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public File getFile() {
    return file;
  }

  public static String composeFileName(long levelNum, long modFileNum) {
    return levelNum + "-" + modFileNum + FILE_SUFFIX;
  }

  public static long[] parseFileName(String name) {
    name = name.substring(0, name.lastIndexOf(ModificationFile.FILE_SUFFIX));
    String[] split = name.split("-");
    long levelNum = Long.parseLong(split[0]);
    long modNum = Long.parseLong(split[1]);
    return new long[] {levelNum, modNum};
  }

  public long getSize() {
    return file.length();
  }

  public class ModIterator implements Iterator<ModEntry>, AutoCloseable {
    private InputStream inputStream;
    private ModEntry nextEntry;

    public ModIterator(long offset) throws IOException {
      this.inputStream = Files.newInputStream(file.toPath());
      long skipped = inputStream.skip(offset);
      if (skipped != offset) {
        LOGGER.warn("Fail to read Mod file {}, expecting offset {}, actually skipped {}", file, offset, skipped);
      }
    }

    @Override
    public void close() {
      if (inputStream == null) {
        return;
      }

      try {
        inputStream.close();
      } catch (IOException e) {
        LOGGER.info("Cannot close mod file input stream of {}", file, e);
      } finally {
        inputStream = null;
      }
    }

    @Override
    public boolean hasNext() {
      if (inputStream == null) {
        return false;
      }
      if (nextEntry == null) {
        try {
          if (inputStream.available() == 0) {
            close();
            return false;
          }
          nextEntry = ModEntry.createFrom(inputStream);
        } catch (EOFException e) {
          close();
        } catch (IOException e) {
          LOGGER.info("Cannot read mod file input stream of {}", file, e);
          close();
        }
      }

      return nextEntry != null;
    }

    @Override
    public ModEntry next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      ModEntry ret = nextEntry;
      nextEntry = null;
      return ret;
    }
  }

  public boolean exists() {
    return file.exists();
  }

  public void remove() throws IOException {
    close();
    FileUtils.deleteFileOrDirectory(file);
  }

  public static ModificationFile getNormalMods(TsFileResource tsFileResource) {
    return new ModificationFile(new File(tsFileResource.getTsFilePath() + FILE_SUFFIX));
  }

  public static File getNormalMods(File tsFile) {
    return new File(tsFile.getPath() + FILE_SUFFIX);
  }

  public static ModificationFile getCompactionMods(TsFileResource tsFileResource) {
    return new ModificationFile(
        new File(tsFileResource.getTsFilePath() + COMPACTION_FILE_SUFFIX));
  }

  public static File getCompactionMods(File tsFile) {
    return new File(tsFile.getPath() + COMPACTION_FILE_SUFFIX);
  }

  public void truncate(long size) throws IOException {
    if (channel != null) {
      channel.truncate(size);
    }
  }

  @Override
  public String toString() {
    return "ModificationFile{" +
        "file=" + file +
        '}';
  }
}
