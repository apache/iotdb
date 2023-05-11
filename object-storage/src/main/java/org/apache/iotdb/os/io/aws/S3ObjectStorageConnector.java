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

package org.apache.iotdb.os.io.aws;

import org.apache.iotdb.os.exception.ObjectStorageException;
import org.apache.iotdb.os.fileSystem.OSURI;
import org.apache.iotdb.os.io.ObjectStorageConnector;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.File;

public class S3ObjectStorageConnector implements ObjectStorageConnector {
  private S3Client s3Client;

  public S3ObjectStorageConnector() {
    s3Client =
        S3Client.builder()
            .region(Region.of(AWSS3Config.getRegion()))
            .credentialsProvider(AWSS3Config.getCredentialProvider())
            .build();
  }

  @Override
  public boolean doesObjectExist(OSURI osUri) throws ObjectStorageException {
    try {
      HeadObjectRequest objectRequest =
          HeadObjectRequest.builder().key(osUri.getKey()).bucket(osUri.getBucket()).build();
      s3Client.headObject(objectRequest);
    } catch (NoSuchKeyException e) {
      return false;
    } catch (S3Exception e) {
      throw new ObjectStorageException(e);
    }
    return true;
  }

  @Override
  public void write(String sourceFile, String containerName, String targetFileName)
      throws ObjectStorageException {
    try {
      PutObjectRequest putOb =
          PutObjectRequest.builder()
              .bucket(AWSS3Config.getBucketName())
              .key(targetFileName)
              .build();
      s3Client.putObject(putOb, RequestBody.fromFile(new File(sourceFile)));
    } catch (S3Exception e) {
      throw new ObjectStorageException(e);
    }
  }

  public void close() {
    s3Client.close();
  }
}
