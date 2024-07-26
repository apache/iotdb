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

import org.apache.iotdb.db.storageengine.dataregion.modification.io.LocalTextModificationAccessor;
import org.apache.iotdb.db.storageengine.dataregion.modification.io.ModificationReader;
import org.apache.iotdb.db.storageengine.dataregion.modification.io.ModificationWriter;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * ModificationFile stores the Modifications of a TsFile or unseq file in another file in the same
 * directory. Methods in this class are highly synchronized for concurrency safety.
 */
public class ModificationFile implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(ModificationFile.class);
  public static final String FILE_SUFFIX = ".mods";
  public static final String COMPACT_SUFFIX = ".settle";
  public static final String COMPACTION_FILE_SUFFIX = ".compaction.mods";

  // whether to verify the last line, it may be incomplete in extreme cases
  private boolean needVerify = true;

  private final ModificationWriter writer;
  private final ModificationReader reader;
  private String filePath;
  private final SecureRandom random = new SecureRandom();

  private static final long COMPACT_THRESHOLD = 1024 * 1024L;

  private boolean hasCompacted = false;

  /**
   * Construct a ModificationFile using a file as its storage.
   *
   * @param filePath the path of the storage file.
   */
  public ModificationFile(String filePath) {
    LocalTextModificationAccessor accessor = new LocalTextModificationAccessor(filePath);
    this.writer = accessor;
    this.reader = accessor;
    this.filePath = filePath;
  }

  /** Release resources such as streams and caches. */
  @Override
  public void close() throws IOException {
    synchronized (this) {
      writer.close();
    }
  }

  /**
   * Write a modification in this file. The modification will first be written to the persistent
   * store then the memory cache.
   *
   * @param mod the modification to be written.
   * @throws IOException if IOException is thrown when writing the modification to the store.
   */
  public void write(Modification mod) throws IOException {
    synchronized (this) {
      if (needVerify && new File(filePath).exists()) {
        writer.mayTruncateLastLine();
        needVerify = false;
      }
      writer.write(mod);
    }
  }

  /**
   * Write a modification in this file. The modification will first be written to the persistent
   * store then the memory cache. Notice that this method does not synchronize to physical disk
   * after
   *
   * @param mod the modification to be written.
   * @throws IOException if IOException is thrown when writing the modification to the store.
   */
  public void writeWithoutSync(Modification mod) throws IOException {
    synchronized (this) {
      if (needVerify && new File(filePath).exists()) {
        writer.mayTruncateLastLine();
        needVerify = false;
      }
      writer.writeWithOutSync(mod);
    }
  }

  @GuardedBy("TsFileResource-WriteLock")
  public void truncate(long size) {
    writer.truncate(size);
  }

  /**
   * Get all modifications stored in this file.
   *
   * @return an ArrayList of modifications.
   */
  public Collection<Modification> getModifications() {
    synchronized (this) {
      return reader.read();
    }
  }

  public Iterable<Modification> getModificationsIter() {
    return reader::getModificationIterator;
  }

  public String getFilePath() {
    return filePath;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  public void remove() throws IOException {
    close();
    boolean deleted = FSFactoryProducer.getFSFactory().getFile(filePath).delete();
    if (!deleted) {
      logger.warn("Delete ModificationFile {} failed.", filePath);
    }
  }

  public boolean exists() {
    return new File(filePath).exists();
  }

  /**
   * Create a hardlink for the modification file. The hardlink with have a suffix like
   * ".{sysTime}_{randomLong}"
   *
   * @return a new ModificationFile with its path changed to the hardlink, or null if the origin
   *     file does not exist or the hardlink cannot be created.
   */
  public ModificationFile createHardlink() {
    if (!exists()) {
      return null;
    }

    while (true) {
      String hardlinkSuffix =
          TsFileConstant.PATH_SEPARATOR + System.currentTimeMillis() + "_" + random.nextLong();
      File hardlink = new File(filePath + hardlinkSuffix);

      try {
        Files.createLink(Paths.get(hardlink.getAbsolutePath()), Paths.get(filePath));
        return new ModificationFile(hardlink.getAbsolutePath());
      } catch (FileAlreadyExistsException e) {
        // retry a different name if the file is already created
      } catch (IOException e) {
        logger.error("Cannot create hardlink for {}", filePath, e);
        return null;
      }
    }
  }

  public static ModificationFile getNormalMods(TsFileResource tsFileResource) {
    return new ModificationFile(tsFileResource.getTsFilePath() + ModificationFile.FILE_SUFFIX);
  }

  public static ModificationFile getCompactionMods(TsFileResource tsFileResource) {
    return new ModificationFile(
        tsFileResource.getTsFilePath() + ModificationFile.COMPACTION_FILE_SUFFIX);
  }

  public long getSize() {
    File file = new File(filePath);
    if (file.exists()) {
      return file.length();
    } else {
      return 0;
    }
  }

  public void compact() {
    long originFileSize = getSize();
    if (originFileSize > COMPACT_THRESHOLD && !hasCompacted) {
      Map<String, List<Modification>> pathModificationMap =
          getModifications().stream().collect(Collectors.groupingBy(Modification::getPathString));
      String newModsFileName = filePath + COMPACT_SUFFIX;
      List<Modification> allSettledModifications = new ArrayList<>();
      try (ModificationFile compactedModificationFile = new ModificationFile(newModsFileName)) {
        Set<Map.Entry<String, List<Modification>>> modificationsEntrySet =
            pathModificationMap.entrySet();
        for (Map.Entry<String, List<Modification>> modificationEntry : modificationsEntrySet) {
          List<Modification> settledModifications = sortAndMerge(modificationEntry.getValue());
          for (Modification settledModification : settledModifications) {
            compactedModificationFile.write(settledModification);
          }
          allSettledModifications.addAll(settledModifications);
        }
      } catch (IOException e) {
        logger.error("compact mods file exception of {}", filePath, e);
      }

      try {
        // remove origin mods file
        this.remove();
        // rename new mods file to origin name
        Files.move(new File(newModsFileName).toPath(), new File(filePath).toPath());
        logger.info("{} settle successful", filePath);

        if (getSize() > COMPACT_THRESHOLD) {
          logger.warn(
              "After the mod file is settled, the file size is still greater than 1M,the size of the file before settle is {},after settled the file size is {}",
              originFileSize,
              getSize());
        }
      } catch (IOException e) {
        logger.error("remove origin file or rename new mods file error.", e);
      }
      hasCompacted = true;
    }
  }

  public static List<Modification> sortAndMerge(List<Modification> modifications) {
    modifications.sort(
        (o1, o2) -> {
          if (!o1.getType().equals(o2.getType())) {
            return o1.getType().compareTo(o2.getType());
          } else if (!o1.getPath().equals(o2.getPath())) {
            return o1.getPath().compareTo(o2.getPath());
          } else if (o1.getFileOffset() != o2.getFileOffset()) {
            return (int) (o1.getFileOffset() - o2.getFileOffset());
          } else {
            if (o1.getType() == Modification.Type.DELETION) {
              Deletion del1 = (Deletion) o1;
              Deletion del2 = (Deletion) o2;
              return del1.getTimeRange().compareTo(del2.getTimeRange());
            }
            throw new IllegalArgumentException();
          }
        });
    List<Modification> result = new ArrayList<>();
    if (!modifications.isEmpty()) {
      Deletion current = ((Deletion) modifications.get(0)).clone();
      for (int i = 1; i < modifications.size(); i++) {
        Deletion del = (Deletion) modifications.get(i);
        if (current.intersects(del)) {
          current.merge(del);
        } else {
          result.add(current);
          current = del.clone();
        }
      }
      result.add(current);
    }
    return result;
  }
}
