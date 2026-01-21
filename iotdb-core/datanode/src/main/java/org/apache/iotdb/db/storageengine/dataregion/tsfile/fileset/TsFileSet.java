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

package org.apache.iotdb.db.storageengine.dataregion.tsfile.fileset;

import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.evolution.EvolvedSchema;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.evolution.EvolvedSchemaCache;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.evolution.SchemaEvolution;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.evolution.SchemaEvolutionFile;

import org.apache.tsfile.external.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** TsFileSet represents a set of TsFiles in a time partition whose version <= endVersion. */
public class TsFileSet implements Comparable<TsFileSet> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileSet.class);
  public static final String FILE_SET_DIR_NAME = "filesets";

  private final long endVersion;
  private final File fileSetDir;
  private final ReentrantReadWriteLock lock;
  private SchemaEvolutionFile schemaEvolutionFile;

  public TsFileSet(long endVersion, String fileSetsDir, boolean recover) {
    this.endVersion = endVersion;
    this.fileSetDir = new File(fileSetsDir + File.separator + endVersion);
    this.lock = new ReentrantReadWriteLock();

    if (recover) {
      recover();
    } else {
      //noinspection ResultOfMethodCallIgnored
      fileSetDir.mkdirs();
    }

    if (schemaEvolutionFile == null) {
      schemaEvolutionFile =
          new SchemaEvolutionFile(
              fileSetDir + File.separator + 0 + SchemaEvolutionFile.FILE_SUFFIX);
    }
  }

  private void recover() {
    File[] files = fileSetDir.listFiles();
    if (files != null) {
      for (File file : files) {
        if (file.getName().endsWith(SchemaEvolutionFile.FILE_SUFFIX)) {
          schemaEvolutionFile = new SchemaEvolutionFile(file.getAbsolutePath());
        }
      }
    }
  }

  public void appendSchemaEvolution(Collection<SchemaEvolution> schemaEvolutions)
      throws IOException {
    writeLock();
    try {
      schemaEvolutionFile.append(schemaEvolutions);
      EvolvedSchemaCache.getInstance().invalidate(this);
    } finally {
      writeUnlock();
    }
  }

  public EvolvedSchema readEvolvedSchema() throws IOException {
    readLock();
    try {
      return EvolvedSchemaCache.getInstance()
          .computeIfAbsent(
              this,
              () -> {
                try {
                  return schemaEvolutionFile.readAsSchema();
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              });
    } finally {
      readUnlock();
    }
  }

  @Override
  public int compareTo(TsFileSet o) {
    return Long.compare(endVersion, o.endVersion);
  }

  public void writeLock() {
    lock.writeLock().lock();
  }

  public void readLock() {
    lock.readLock().lock();
  }

  public void writeUnlock() {
    lock.writeLock().unlock();
  }

  public void readUnlock() {
    lock.readLock().unlock();
  }

  public long getEndVersion() {
    return endVersion;
  }

  @Override
  public String toString() {
    return "TsFileSet{" + "endVersion=" + endVersion + ", fileSetDir=" + fileSetDir + '}';
  }

  public void remove() {
    FileUtils.deleteQuietly(fileSetDir);
  }

  public boolean contains(TsFileResource tsFileResource) {
    return tsFileResource.getVersion() <= endVersion;
  }

  public static EvolvedSchema getMergedEvolvedSchema(List<TsFileSet> tsFileSetList) {
    List<EvolvedSchema> list = new ArrayList<>();
    for (TsFileSet fileSet : tsFileSetList) {
      try {
        EvolvedSchema readEvolvedSchema = fileSet.readEvolvedSchema();
        list.add(readEvolvedSchema);
      } catch (IOException e) {
        LOGGER.warn("Cannot read evolved schema from {}, skipping it", fileSet);
      }
    }

    return EvolvedSchema.merge(list.toArray(new EvolvedSchema[0]));
  }
}
