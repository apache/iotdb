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
package org.apache.iotdb.os.fileSystem;

import org.apache.iotdb.os.utils.ObjectStorageConstant;

import java.net.URI;

import static org.apache.iotdb.os.utils.ObjectStorageConstant.FILE_SEPARATOR;

/** The OSURI format is os://{bucket}/{key} */
public class OSURI {
  public static final String SCHEME = "os";
  public static final String OS_PREFIX = SCHEME + "://";
  private final URI uri;

  public OSURI(String bucket, String key) {
    this(OS_PREFIX + bucket + FILE_SEPARATOR + key);
  }

  public OSURI(String path) {
    this(URI.create(path));
  }

  public OSURI(URI uri) {
    if (!uri.getScheme().equals(SCHEME)) {
      throw new IllegalArgumentException();
    }
    this.uri = uri;
  }

  public URI getURI() {
    return uri;
  }

  public String getBucket() {
    return uri.getAuthority();
  }

  public String getKey() {
    return uri.getPath().substring(ObjectStorageConstant.FILE_SEPARATOR.length());
  }

  public String toString() {
    return uri.toString();
  }

  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    OSURI other = (OSURI) obj;
    return uri.equals(other.uri);
  }

  public int hashCode() {
    return uri.hashCode();
  }
}
