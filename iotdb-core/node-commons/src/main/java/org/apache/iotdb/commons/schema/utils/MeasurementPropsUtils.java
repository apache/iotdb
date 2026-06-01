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

package org.apache.iotdb.commons.schema.utils;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for handling measurement properties related to alias series (renaming) feature.
 * This class provides methods to check and manipulate properties like IS_RENAMED, ALIAS_PATH,
 * ORIGINAL_PATH, IS_RENAMING, and INVALID in a centralized and readable way.
 */
public class MeasurementPropsUtils {

  private MeasurementPropsUtils() {
    // Utility class, forbid instantiation
  }

  // Props keys for alias series feature
  /** Key for IS_RENAMED property. Indicates this is an alias series (renaming completed). */
  public static final String IS_RENAMED_KEY = "IS_RENAMED";

  /**
   * Key for IS_RENAMING property. Indicates this measurement is currently being renamed (for
   * non-procedure alter operations).
   */
  public static final String IS_RENAMING_KEY = "IS_RENAMING";

  /** Key for INVALID property. If this key exists, it indicates the measurement is invalid. */
  public static final String INVALID_KEY = "INVALID";

  /**
   * Key for ORIGINAL_PATH property. If this key exists, it indicates this is an alias series
   * pointing to the physical path.
   */
  public static final String ORIGINAL_PATH_KEY = "ORIGINAL_PATH";

  /**
   * Key for ALIAS_PATH property. If this key exists, it indicates this physical series has an alias
   * pointing to it.
   */
  public static final String ALIAS_PATH_KEY = "ALIAS_PATH";

  /**
   * Key for ORIGINAL_PATH_IS_ALIGNED property. Indicates whether the original path (ORIGINAL_PATH)
   * is aligned.
   */
  public static final String ORIGINAL_PATH_IS_ALIGNED_KEY = "ORIGINAL_PATH_IS_ALIGNED";

  /**
   * Key for QUERY_GENERATED_INVALID_SERIES property. Indicates this physical series is synthesized
   * from an alias series during the current query/statement flow and does not represent a real
   * invalid series stored in schema.
   */
  public static final String QUERY_GENERATED_INVALID_SERIES_KEY = "QUERY_GENERATED_INVALID_SERIES";

  /**
   * Check if the measurement is renamed (alias series) based on props.
   *
   * @param props the properties map, can be null
   * @return true if IS_RENAMED_KEY exists and is "true", false otherwise
   */
  public static boolean isRenamed(Map<String, String> props) {
    if (props == null) {
      return false;
    }
    String value = props.get(IS_RENAMED_KEY);
    return Boolean.parseBoolean(value);
  }

  /**
   * Set the IS_RENAMED property in props.
   *
   * @param props the properties map, must not be null
   * @param isRenamed true to set IS_RENAMED to "true", false to remove the key
   */
  public static void setIsRenamed(Map<String, String> props, boolean isRenamed) {
    if (isRenamed) {
      props.put(IS_RENAMED_KEY, "true");
    } else {
      props.remove(IS_RENAMED_KEY);
    }
  }

  /**
   * Check if the measurement is currently being renamed based on props.
   *
   * @param props the properties map, can be null
   * @return true if IS_RENAMING_KEY exists and is "true", false otherwise
   */
  public static boolean isRenaming(Map<String, String> props) {
    if (props == null) {
      return false;
    }
    String value = props.get(IS_RENAMING_KEY);
    return Boolean.parseBoolean(value);
  }

  /**
   * Set the IS_RENAMING property in props.
   *
   * @param props the properties map, must not be null
   * @param isRenaming true to set IS_RENAMING to "true", false to remove the key
   */
  public static void setIsRenaming(Map<String, String> props, boolean isRenaming) {
    if (isRenaming) {
      props.put(IS_RENAMING_KEY, "true");
    } else {
      props.remove(IS_RENAMING_KEY);
    }
  }

  /**
   * Check if the measurement is invalid based on props.
   *
   * @param props the properties map, can be null
   * @return true if INVALID_KEY exists and is "true", false otherwise
   */
  public static boolean isInvalid(Map<String, String> props) {
    if (props == null) {
      return false;
    }
    String value = props.get(INVALID_KEY);
    return Boolean.parseBoolean(value);
  }

  /**
   * Set the INVALID property in props.
   *
   * @param props the properties map, must not be null
   * @param isInvalid true to set INVALID to "true", false to remove the key
   */
  public static void setInvalid(Map<String, String> props, boolean isInvalid) {
    if (isInvalid) {
      props.put(INVALID_KEY, "true");
    } else {
      props.remove(INVALID_KEY);
    }
  }

  /**
   * Get the alias path from props.
   *
   * @param props the properties map, can be null
   * @return the PartialPath if ALIAS_PATH_KEY exists, null otherwise
   */
  public static PartialPath getAliasPath(Map<String, String> props) {
    if (props == null) {
      return null;
    }
    String pathString = props.get(ALIAS_PATH_KEY);
    if (pathString == null) {
      return null;
    }
    try {
      return new MeasurementPath(pathString);
    } catch (IllegalPathException e) {
      return null;
    }
  }

  /**
   * Get the alias path as string from props.
   *
   * @param props the properties map, can be null
   * @return the alias path string if ALIAS_PATH_KEY exists, null otherwise
   */
  public static String getAliasPathString(Map<String, String> props) {
    if (props == null) {
      return null;
    }
    return props.get(ALIAS_PATH_KEY);
  }

  /**
   * Set the ALIAS_PATH property in props.
   *
   * @param props the properties map, must not be null
   * @param aliasPath the alias path to set, null to remove the key
   */
  public static void setAliasPath(Map<String, String> props, PartialPath aliasPath) {
    if (aliasPath == null) {
      props.remove(ALIAS_PATH_KEY);
    } else {
      props.put(ALIAS_PATH_KEY, aliasPath.getFullPath());
    }
  }

  /**
   * Set the ALIAS_PATH property in props using a string path.
   *
   * @param props the properties map, must not be null
   * @param aliasPathString the alias path string to set, null to remove the key
   */
  public static void setAliasPathString(Map<String, String> props, String aliasPathString) {
    if (aliasPathString == null) {
      props.remove(ALIAS_PATH_KEY);
    } else {
      props.put(ALIAS_PATH_KEY, aliasPathString);
    }
  }

  /**
   * Get the original path from props.
   *
   * @param props the properties map, can be null
   * @return the PartialPath if ORIGINAL_PATH_KEY exists, null otherwise
   */
  public static PartialPath getOriginalPath(Map<String, String> props) {
    if (props == null) {
      return null;
    }
    String pathString = props.get(ORIGINAL_PATH_KEY);
    if (pathString == null) {
      return null;
    }
    try {
      return new MeasurementPath(pathString);
    } catch (IllegalPathException e) {
      return null;
    }
  }

  /**
   * Get the original path as string from props.
   *
   * @param props the properties map, can be null
   * @return the original path string if ORIGINAL_PATH_KEY exists, null otherwise
   */
  public static String getOriginalPathString(Map<String, String> props) {
    if (props == null) {
      return null;
    }
    return props.get(ORIGINAL_PATH_KEY);
  }

  /**
   * Set the ORIGINAL_PATH property in props.
   *
   * @param props the properties map, must not be null
   * @param originalPath the original path to set, null to remove the key
   */
  public static void setOriginalPath(Map<String, String> props, PartialPath originalPath) {
    if (originalPath == null) {
      props.remove(ORIGINAL_PATH_KEY);
    } else {
      props.put(ORIGINAL_PATH_KEY, originalPath.getFullPath());
    }
  }

  /**
   * Set the ORIGINAL_PATH property in props using a string path.
   *
   * @param props the properties map, must not be null
   * @param originalPathString the original path string to set, null to remove the key
   */
  public static void setOriginalPathString(Map<String, String> props, String originalPathString) {
    if (originalPathString == null) {
      props.remove(ORIGINAL_PATH_KEY);
    } else {
      props.put(ORIGINAL_PATH_KEY, originalPathString);
    }
  }

  /**
   * Check whether the measurement schema is a query-generated transient physical series.
   *
   * @param props the properties map, can be null
   * @return true if QUERY_GENERATED_INVALID_SERIES_KEY exists and is "true"
   */
  public static boolean isQueryGeneratedInvalidSeries(Map<String, String> props) {
    if (props == null) {
      return false;
    }
    return Boolean.parseBoolean(props.get(QUERY_GENERATED_INVALID_SERIES_KEY));
  }

  /**
   * Set the QUERY_GENERATED_INVALID_SERIES property in props.
   *
   * @param props the properties map, must not be null
   * @param isQueryGeneratedInvalidSeries true to set the key, false to remove it
   */
  public static void setQueryGeneratedInvalidSeries(
      Map<String, String> props, boolean isQueryGeneratedInvalidSeries) {
    if (isQueryGeneratedInvalidSeries) {
      props.put(QUERY_GENERATED_INVALID_SERIES_KEY, "true");
    } else {
      props.remove(QUERY_GENERATED_INVALID_SERIES_KEY);
    }
  }

  /**
   * Get whether the original path is aligned from props.
   *
   * @param props the properties map, can be null
   * @return true if ORIGINAL_PATH_IS_ALIGNED_KEY exists and is "true", false otherwise
   */
  public static boolean isOriginalPathAligned(Map<String, String> props) {
    if (props == null) {
      return false;
    }
    String value = props.get(ORIGINAL_PATH_IS_ALIGNED_KEY);
    return Boolean.parseBoolean(value);
  }

  /**
   * Set the ORIGINAL_PATH_IS_ALIGNED property in props.
   *
   * @param props the properties map, must not be null
   * @param isAligned true to set ORIGINAL_PATH_IS_ALIGNED to "true", false to remove the key
   */
  public static void setOriginalPathIsAligned(Map<String, String> props, boolean isAligned) {
    if (isAligned) {
      props.put(ORIGINAL_PATH_IS_ALIGNED_KEY, "true");
    } else {
      props.remove(ORIGINAL_PATH_IS_ALIGNED_KEY);
    }
  }

  /**
   * Check if a key is a measurement internal property key (should not be exposed to users).
   *
   * @param key the key to check
   * @return true if the key is an internal property key
   */
  public static boolean isInternalPropertyKey(String key) {
    return IS_RENAMED_KEY.equals(key)
        || IS_RENAMING_KEY.equals(key)
        || ALIAS_PATH_KEY.equals(key)
        || ORIGINAL_PATH_KEY.equals(key)
        || ORIGINAL_PATH_IS_ALIGNED_KEY.equals(key)
        || INVALID_KEY.equals(key)
        || QUERY_GENERATED_INVALID_SERIES_KEY.equals(key);
  }

  /**
   * Remove all internal property keys from props.
   *
   * @param props the properties map, can be null
   */
  public static void removeInternalProperties(Map<String, String> props) {
    if (props == null) {
      return;
    }
    props.remove(IS_RENAMED_KEY);
    props.remove(IS_RENAMING_KEY);
    props.remove(ALIAS_PATH_KEY);
    props.remove(ORIGINAL_PATH_KEY);
    props.remove(ORIGINAL_PATH_IS_ALIGNED_KEY);
    props.remove(INVALID_KEY);
    props.remove(QUERY_GENERATED_INVALID_SERIES_KEY);
  }

  /**
   * Build props map for alias series with necessary properties.
   *
   * @param originalProps the original properties map from time series info, can be null
   * @param physicalPath the physical path that this alias series points to
   * @return a new map containing alias series properties
   */
  public static Map<String, String> buildAliasSeriesProps(
      Map<String, String> originalProps, PartialPath physicalPath) {
    Map<String, String> props = new HashMap<>();
    // Copy original props if any
    if (originalProps != null && !originalProps.isEmpty()) {
      props.putAll(originalProps);
    }
    // Preserve ORIGINAL_PATH_IS_ALIGNED from originalProps if it exists
    if (originalProps != null) {
      boolean isOriginalPathAligned = isOriginalPathAligned(originalProps);
      setOriginalPathIsAligned(props, isOriginalPathAligned);
    }
    // Add alias series properties
    setIsRenamed(props, true);
    setOriginalPath(props, physicalPath);
    return props;
  }

  /**
   * Build props for a physical series path synthesized from an alias series during query/write
   * planning. The synthesized path should preserve the alias mapping while remaining
   * distinguishable from a real invalid series stored in schema.
   *
   * @param aliasProps the alias series props, can be null
   * @param aliasPath the alias path pointing to the physical series
   * @return a new props map representing the transient physical series
   */
  public static Map<String, String> buildQueryGeneratedPhysicalSeriesProps(
      Map<String, String> aliasProps, PartialPath aliasPath) {
    Map<String, String> props = new HashMap<>();
    if (aliasProps != null && !aliasProps.isEmpty()) {
      props.putAll(aliasProps);
      removeInternalProperties(props);
    }
    setAliasPath(props, aliasPath);
    setQueryGeneratedInvalidSeries(props, true);
    return props;
  }

  /**
   * Build props for the cached physical series entry corresponding to a query-generated transient
   * physical series.
   *
   * @param queryGeneratedProps the transient query-generated props, can be null
   * @return a new props map representing the cached invalid physical series
   */
  public static Map<String, String> buildCachedInvalidPhysicalSeriesProps(
      Map<String, String> queryGeneratedProps) {
    Map<String, String> props = new HashMap<>();
    if (queryGeneratedProps != null && !queryGeneratedProps.isEmpty()) {
      props.putAll(queryGeneratedProps);
    }
    String aliasPath = getAliasPathString(props);
    removeInternalProperties(props);
    setAliasPathString(props, aliasPath);
    setInvalid(props, true);
    return props;
  }
}
