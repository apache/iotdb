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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.service.metrics.FileMetrics;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static org.apache.iotdb.db.utils.ModificationUtils.sortAndMerge;

public class ModificationFile implements AutoCloseable {

  public static final String FILE_SUFFIX = ".mods2";
  public static final String COMPACTION_FILE_SUFFIX = ".compaction.mods2";
  public static final String COMPACT_SUFFIX = ".settle";
  private static final Logger LOGGER = LoggerFactory.getLogger(ModificationFile.class);

  private final File file;
  private FileChannel channel;
  private OutputStream fileOutputStream;
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private static final long COMPACT_THRESHOLD = 1024 * 1024L;
  private boolean hasCompacted = false;
  private boolean fileExists = false;

  public ModificationFile(String filePath) {
    this(new File(filePath));
  }

  public ModificationFile(File file) {
    this.file = file;
    fileExists = file.length() > 0;
    if (fileExists) {
      FileMetrics.getInstance().increaseModFileNum(1);
      FileMetrics.getInstance().increaseModFileSize(file.length());
    }
  }

  @SuppressWarnings("java:S2093") // cannot use try-with-resource, should not close here
  public void write(ModEntry entry) throws IOException {
    lock.writeLock().lock();
    long size = 0;
    try {
      if (fileOutputStream == null) {
        fileOutputStream =
            new BufferedOutputStream(Files.newOutputStream(file.toPath(), CREATE, APPEND));
        channel = FileChannel.open(file.toPath(), CREATE, APPEND);
      }
      size += entry.serialize(fileOutputStream);
      fileOutputStream.flush();
    } finally {
      lock.writeLock().unlock();
    }
    if (!fileExists) {
      fileExists = true;
      FileMetrics.getInstance().increaseModFileNum(1);
    }
    FileMetrics.getInstance().increaseModFileSize(size);
  }

  @SuppressWarnings("java:S2093") // cannot use try-with-resource, should not close here
  public void write(Collection<? extends ModEntry> entries) throws IOException {
    lock.writeLock().lock();
    long size = 0;
    try {
      if (fileOutputStream == null) {
        fileOutputStream =
            new BufferedOutputStream(Files.newOutputStream(file.toPath(), CREATE, APPEND));
        channel = FileChannel.open(file.toPath(), CREATE, APPEND);
      }
      for (ModEntry entry : entries) {
        size += entry.serialize(fileOutputStream);
      }
      fileOutputStream.flush();
    } finally {
      lock.writeLock().unlock();
    }
    if (!fileExists) {
      FileMetrics.getInstance().increaseModFileNum(1);
      fileExists = true;
    }
    FileMetrics.getInstance().increaseModFileSize(size);
  }

  public Iterator<ModEntry> getModIterator(long offset) throws IOException {
    return new ModIterator(offset);
  }

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

  public long getFileLength() {
    lock.readLock().lock();
    try {
      return file.length();
    } finally {
      lock.readLock().unlock();
    }
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

  public class ModIterator implements Iterator<ModEntry>, AutoCloseable {
    private InputStream inputStream;
    private ModEntry nextEntry;

    public ModIterator(long offset) throws IOException {
      if (!fileExists) {
        return;
      }
      this.inputStream = new BufferedInputStream(Files.newInputStream(file.toPath()), 64 * 1024);
      long skipped = inputStream.skip(offset);
      if (skipped != offset) {
        LOGGER.warn(
            "Fail to read Mod file {}, expecting offset {}, actually skipped {}",
            file,
            offset,
            skipped);
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
    return fileExists;
  }

  public void remove() throws IOException {
    close();
    FileUtils.deleteFileOrDirectory(file);
    FileMetrics.getInstance().decreaseModFileNum(1);
    FileMetrics.getInstance().decreaseModFileSize(getFileLength());
    fileExists = false;
  }

  public static ModificationFile getExclusiveMods(TsFileResource tsFileResource) {
    return new ModificationFile(new File(tsFileResource.getTsFilePath() + FILE_SUFFIX));
  }

  public static File getExclusiveMods(File tsFile) {
    return new File(tsFile.getPath() + FILE_SUFFIX);
  }

  public static ModificationFile getCompactionMods(TsFileResource tsFileResource) {
    return new ModificationFile(new File(tsFileResource.getTsFilePath() + COMPACTION_FILE_SUFFIX));
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
    return "ModificationFile{" + "file=" + file + '}';
  }

  public void compact() throws IOException {
    long originFileSize = getFileLength();
    if (originFileSize > COMPACT_THRESHOLD && !hasCompacted) {
      try {
        Map<PartialPath, List<ModEntry>> pathModificationMap =
            getAllMods().stream().collect(Collectors.groupingBy(ModEntry::keyOfPatternTree));
        String newModsFileName = getFile().getPath() + COMPACT_SUFFIX;
        List<ModEntry> allSettledModifications = new ArrayList<>();
        try (ModificationFile compactedModificationFile = new ModificationFile(newModsFileName)) {
          Set<Entry<PartialPath, List<ModEntry>>> modificationsEntrySet =
              pathModificationMap.entrySet();
          for (Map.Entry<PartialPath, List<ModEntry>> modificationEntry : modificationsEntrySet) {
            List<ModEntry> settledModifications = sortAndMerge(modificationEntry.getValue());
            compactedModificationFile.write(settledModifications);
            allSettledModifications.addAll(settledModifications);
          }
        } catch (IOException e) {
          LOGGER.error("compact mods file exception of {}", file, e);
        }
        // remove origin mods file
        this.remove();
        fileExists = true;
        // rename new mods file to origin name
        Files.move(new File(newModsFileName).toPath(), file.toPath());
        LOGGER.info("{} settle successful", file);

        if (getFileLength() > COMPACT_THRESHOLD) {
          LOGGER.warn(
              "After the mod file is settled, the file size is still greater than 1M,the size of the file before settle is {},after settled the file size is {}",
              originFileSize,
              getFileLength());
        }
      } catch (IOException e) {
        LOGGER.error("remove origin file or rename new mods file error.", e);
      }
      hasCompacted = true;
    }
  }
}
