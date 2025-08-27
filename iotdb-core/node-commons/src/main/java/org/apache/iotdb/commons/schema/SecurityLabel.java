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

package org.apache.iotdb.commons.schema;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * SecurityLabel represents a collection of key-value pairs used for database security labeling.
 * This class is used to implement Label-Based Access Control (LBAC) functionality.
 */
public class SecurityLabel {

  /** Maximum number of label pairs allowed */
  public static final int MAX_LABEL_PAIRS = 20;

  /** Maximum length of label key */
  public static final int MAX_KEY_LENGTH = 64;

  /** Maximum length of label value */
  public static final int MAX_VALUE_LENGTH = 256;

  /** Maximum total length of all labels serialized as string */
  public static final int MAX_TOTAL_LENGTH = 1024;

  /** Storage for label key-value pairs */
  private final Map<String, String> labels;

  /** Default constructor creates empty SecurityLabel */
  public SecurityLabel() {
    this.labels = new HashMap<>();
  }

  /** Constructor with initial labels map */
  public SecurityLabel(Map<String, String> labels) {
    this.labels = new HashMap<>();
    if (labels != null) {
      for (Map.Entry<String, String> entry : labels.entrySet()) {
        setLabel(entry.getKey(), entry.getValue());
      }
    }
  }

  /**
   * Adds or updates a label with the given key and value.
   *
   * @param key The label key, must match pattern [a-zA-Z][a-zA-Z0-9_]*
   * @param value The label value, can be string or number
   * @throws IllegalArgumentException if key or value is invalid
   */
  public void setLabel(String key, String value) {
    validateKey(key);
    validateValue(value);

    // Check total number of pairs
    if (!labels.containsKey(key) && labels.size() >= MAX_LABEL_PAIRS) {
      throw new IllegalArgumentException(
          String.format("Cannot exceed %d label pairs", MAX_LABEL_PAIRS));
    }

    // Check total length
    Map<String, String> tempLabels = new HashMap<>(labels);
    tempLabels.put(key, value);
    validateTotalLength(tempLabels);

    labels.put(key, value);
  }

  /**
   * Removes a label with the given key.
   *
   * @param key The label key to remove
   * @return The previous value associated with the key, or null if not found
   */
  public String removeLabel(String key) {
    return labels.remove(key);
  }

  /**
   * Gets the value for the given key.
   *
   * @param key The label key
   * @return The label value, or null if not found
   */
  public String getLabel(String key) {
    return labels.get(key);
  }

  /**
   * Gets all labels as an unmodifiable map.
   *
   * @return Unmodifiable map of all labels
   */
  public Map<String, String> getLabels() {
    return Collections.unmodifiableMap(labels);
  }

  /**
   * Checks if this SecurityLabel contains any labels.
   *
   * @return true if no labels are present, false otherwise
   */
  public boolean isEmpty() {
    return labels.isEmpty();
  }

  /**
   * Gets the number of label pairs.
   *
   * @return Number of label pairs
   */
  public int size() {
    return labels.size();
  }

  /** Clears all labels. */
  public void clear() {
    labels.clear();
  }

  /**
   * Validates the label key format. Key must be 1-64 characters, start with letter, contain only
   * letters, numbers, and underscore.
   *
   * @param key The key to validate
   * @throws IllegalArgumentException if key is invalid
   */
  private void validateKey(String key) {
    if (key == null || key.isEmpty()) {
      throw new IllegalArgumentException("Label key cannot be null or empty");
    }

    if (key.length() > MAX_KEY_LENGTH) {
      throw new IllegalArgumentException(
          String.format("Label key length cannot exceed %d characters", MAX_KEY_LENGTH));
    }

    if (!key.matches("^[a-zA-Z][a-zA-Z0-9_]*$")) {
      throw new IllegalArgumentException(
          "Label key must start with letter and contain only letters, numbers and underscore");
    }
  }

  /**
   * Validates the label value format. Value cannot be null or empty and cannot exceed maximum
   * length.
   *
   * @param value The value to validate
   * @throws IllegalArgumentException if value is invalid
   */
  private void validateValue(String value) {
    if (value == null || value.isEmpty()) {
      throw new IllegalArgumentException("Label value cannot be null or empty");
    }

    if (value.length() > MAX_VALUE_LENGTH) {
      throw new IllegalArgumentException(
          String.format("Label value length cannot exceed %d characters", MAX_VALUE_LENGTH));
    }
  }

  /**
   * Validates that the total serialized length doesn't exceed the limit.
   *
   * @param labelsToCheck The labels map to check
   * @throws IllegalArgumentException if total length exceeds limit
   */
  private void validateTotalLength(Map<String, String> labelsToCheck) {
    int totalLength = 0;
    for (Map.Entry<String, String> entry : labelsToCheck.entrySet()) {
      // Calculate as "key='value'," format
      totalLength += entry.getKey().length() + entry.getValue().length() + 4; // key + value + ='',
    }

    if (totalLength > MAX_TOTAL_LENGTH) {
      throw new IllegalArgumentException(
          String.format(
              "Total security label length cannot exceed %d characters", MAX_TOTAL_LENGTH));
    }
  }

  /**
   * Validates that the SecurityLabel is not empty (must have at least one label).
   *
   * @throws IllegalArgumentException if SecurityLabel is empty
   */
  public void validateNotEmpty() {
    if (isEmpty()) {
      throw new IllegalArgumentException(
          "Security label cannot be empty. At least one label is required.");
    }
  }

  /**
   * Serializes this SecurityLabel to a ByteBuffer.
   *
   * @param buffer The ByteBuffer to write to
   */
  public void serialize(ByteBuffer buffer) {
    ReadWriteIOUtils.write(labels.size(), buffer);
    for (Map.Entry<String, String> entry : labels.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), buffer);
      ReadWriteIOUtils.write(entry.getValue(), buffer);
    }
  }

  /**
   * Serializes this SecurityLabel to a DataOutputStream.
   *
   * @param stream The DataOutputStream to write to
   * @throws IOException if I/O error occurs
   */
  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(labels.size(), stream);
    for (Map.Entry<String, String> entry : labels.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), stream);
      ReadWriteIOUtils.write(entry.getValue(), stream);
    }
  }

  /**
   * Deserializes a SecurityLabel from a ByteBuffer.
   *
   * @param buffer The ByteBuffer to read from
   * @return The deserialized SecurityLabel
   */
  public static SecurityLabel deserialize(ByteBuffer buffer) {
    SecurityLabel securityLabel = new SecurityLabel();
    int size = ReadWriteIOUtils.readInt(buffer);

    for (int i = 0; i < size; i++) {
      String key = ReadWriteIOUtils.readString(buffer);
      String value = ReadWriteIOUtils.readString(buffer);
      securityLabel.labels.put(key, value); // Direct put to avoid validation during deserialization
    }

    return securityLabel;
  }

  /**
   * Creates a copy of this SecurityLabel.
   *
   * @return A new SecurityLabel with the same labels
   */
  public SecurityLabel copy() {
    return new SecurityLabel(labels);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SecurityLabel)) {
      return false;
    }
    SecurityLabel that = (SecurityLabel) o;
    return Objects.equals(labels, that.labels);
  }

  @Override
  public int hashCode() {
    return Objects.hash(labels);
  }

  @Override
  public String toString() {
    if (labels.isEmpty()) {
      return "SecurityLabel{}";
    }

    StringBuilder sb = new StringBuilder("SecurityLabel{");
    boolean first = true;
    for (Map.Entry<String, String> entry : labels.entrySet()) {
      if (!first) {
        sb.append(", ");
      }
      sb.append(entry.getKey()).append("='").append(entry.getValue()).append("'");
      first = false;
    }
    sb.append("}");
    return sb.toString();
  }
}
