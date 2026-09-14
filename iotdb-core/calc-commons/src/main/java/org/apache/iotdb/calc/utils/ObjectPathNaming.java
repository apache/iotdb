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

package org.apache.iotdb.calc.utils;

/**
 * OBJECT {@code .bin} filename helpers.
 *
 * <ul>
 *   <li>in-progress: {@code {time}.bin.tmp}
 *   <li>backup: {@code {time}.bin.back}
 *   <li>versioned: {@code {time}_{tsFileVersion}.bin}
 *   <li>legacy: {@code {time}.bin}
 * </ul>
 */
public final class ObjectPathNaming {

  /** Version value for legacy {@code {time}.bin} (and tmp/back) names. */
  public static final long LEGACY_VERSION = -1L;

  private ObjectPathNaming() {}

  /**
   * Parse the object timestamp from a filename. Returns {@code -1} if the name is not an object
   * candidate or the time is not numeric.
   */
  public static long parseTime(String fileName) {
    String stem = stemWithoutSuffix(fileName);
    if (stem == null) {
      return -1L;
    }
    int underscore = stem.indexOf('_');
    String timePart = underscore < 0 ? stem : stem.substring(0, underscore);
    try {
      return Long.parseLong(timePart);
    } catch (NumberFormatException e) {
      return -1L;
    }
  }

  /**
   * Parse the TsFile version from a filename. Returns {@link #LEGACY_VERSION} for {@code
   * {time}.bin} / tmp / back, or when the name is not parseable as a versioned bin.
   */
  public static long parseVersion(String fileName) {
    String stem = stemWithoutSuffix(fileName);
    if (stem == null) {
      return LEGACY_VERSION;
    }
    int underscore = stem.indexOf('_');
    if (underscore < 0 || underscore == stem.length() - 1) {
      return LEGACY_VERSION;
    }
    try {
      return Long.parseLong(stem.substring(underscore + 1));
    } catch (NumberFormatException e) {
      return LEGACY_VERSION;
    }
  }

  public static boolean isLegacyBin(String fileName) {
    return ObjectTypeUtils.isObjectCandidate(fileName) && parseVersion(fileName) == LEGACY_VERSION;
  }

  public static String toVersionedFileName(long time, long tsFileVersion) {
    return time + "_" + tsFileVersion + ObjectTypeUtils.OBJECT_FILE_SUFFIX;
  }

  public static String toLegacyFileName(long time) {
    return time + ObjectTypeUtils.OBJECT_FILE_SUFFIX;
  }

  public static String toTempFileName(long time) {
    return toLegacyFileName(time) + ObjectTypeUtils.OBJECT_TEMP_FILE_SUFFIX;
  }

  public static String toBackFileName(long time) {
    return toLegacyFileName(time) + ObjectTypeUtils.OBJECT_BACK_FILE_SUFFIX;
  }

  /** Replace the filename of {@code relativePath} with {@code {time}_{version}.bin}. */
  public static String withTsFileVersion(String relativePath, long time, long tsFileVersion) {
    return siblingRelativePath(relativePath, toVersionedFileName(time, tsFileVersion));
  }

  /** {@code {parent}/{time}.bin.tmp} for chunked writes (tmp never carries a version). */
  public static String toTempRelativePath(String relativePath, long time) {
    return siblingRelativePath(relativePath, toTempFileName(time));
  }

  public static String toBackRelativePath(String relativePath, long time) {
    return siblingRelativePath(relativePath, toBackFileName(time));
  }

  public static String toLegacyRelativePath(String relativePath, long time) {
    return siblingRelativePath(relativePath, toLegacyFileName(time));
  }

  /**
   * Last path segment of a local path or {@code os://} key. {@code OSFile.getName()} is the full
   * key.
   */
  public static String baseFileName(String path) {
    if (path == null || path.isEmpty()) {
      return "";
    }
    int slash = Math.max(path.lastIndexOf('/'), path.lastIndexOf('\\'));
    return slash >= 0 ? path.substring(slash + 1) : path;
  }

  /**
   * Relative path of {@code filePath} under {@code rootPath}. Uses string prefix stripping so it
   * works for {@code OSFile}, which does not implement {@code Path}.
   */
  public static String relativize(String rootPath, String filePath) {
    if (rootPath == null || filePath == null) {
      return filePath == null ? "" : filePath;
    }
    if (filePath.startsWith(rootPath)) {
      String rel = filePath.substring(rootPath.length());
      while (rel.startsWith("/") || rel.startsWith("\\")) {
        rel = rel.substring(1);
      }
      return rel;
    }
    return filePath;
  }

  private static String siblingRelativePath(String relativePath, String siblingFileName) {
    int separator = Math.max(relativePath.lastIndexOf('/'), relativePath.lastIndexOf('\\'));
    return separator < 0
        ? siblingFileName
        : relativePath.substring(0, separator + 1) + siblingFileName;
  }

  /**
   * Strip {@code .tmp}/{@code .back} then {@code .bin}. Returns the remaining stem, or null if the
   * name is not an object candidate.
   */
  private static String stemWithoutSuffix(String fileName) {
    if (fileName == null || !ObjectTypeUtils.isObjectCandidate(fileName)) {
      return null;
    }
    String stem = fileName;
    if (stem.endsWith(ObjectTypeUtils.OBJECT_TEMP_FILE_SUFFIX)) {
      stem = stem.substring(0, stem.length() - ObjectTypeUtils.OBJECT_TEMP_FILE_SUFFIX.length());
    } else if (stem.endsWith(ObjectTypeUtils.OBJECT_BACK_FILE_SUFFIX)) {
      stem = stem.substring(0, stem.length() - ObjectTypeUtils.OBJECT_BACK_FILE_SUFFIX.length());
    }
    if (!stem.endsWith(ObjectTypeUtils.OBJECT_FILE_SUFFIX)) {
      return null;
    }
    return stem.substring(0, stem.length() - ObjectTypeUtils.OBJECT_FILE_SUFFIX.length());
  }
}
