/**
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
package org.apache.iotdb.db.qp.physical.transfer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator;
import org.apache.iotdb.db.qp.logical.sys.MetadataOperator;
import org.apache.iotdb.db.qp.logical.sys.PropertyOperator;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.UpdatePlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadDataPlan;
import org.apache.iotdb.db.qp.physical.sys.MetadataPlan;
import org.apache.iotdb.db.qp.physical.sys.PropertyPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public class CodecInstances {

  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final int NULL_VALUE_LEN = -1;

  private CodecInstances() {
  }


  /**
   * Put a string to ByteBuffer, first put a int which represents the bytes len of string, then put
   * the bytes of string to ByteBuffer, string may be null
   *
   * @param buffer Target ByteBuffer to put a string value
   * @param value String value to be put
   */
  static void putString(ByteBuffer buffer, String value) {
    if(value == null){
      buffer.putInt(NULL_VALUE_LEN);
    }else{
      ReadWriteIOUtils.write(value, buffer);
    }
  }

  /**
   * Read a string value from ByteBuffer, first read a int that represents the bytes len of string,
   * then read bytes len value, finally transfer bytes to string, string may be null
   *
   * @param buffer ByteBuffer to be read
   * @return string value
   */
  static String readString(ByteBuffer buffer) {
    int valueLen = buffer.getInt();
    if (valueLen == NULL_VALUE_LEN) {
      return null;
    }
    return ReadWriteIOUtils.readStringWithoutLength(buffer, valueLen);
  }

  /**
   * Check if ByteBuffer is initialized and put type
   */
  static void checkBufferAndPutType(ThreadLocal<ByteBuffer> localBuffer, int type) {
    if (localBuffer.get() == null) {
      localBuffer.set(ByteBuffer.allocate(config.getMaxLogEntrySize()));
    }

    ByteBuffer buffer = localBuffer.get();
    buffer.clear();
    buffer.put((byte) type);
  }
  
  static final Codec<DeletePlan> deletePlanCodec = new Codec<DeletePlan>() {
    ThreadLocal<ByteBuffer> localBuffer = new ThreadLocal<>();

    @Override
    public byte[] encode(DeletePlan t) {
      if (localBuffer.get() == null) {
        localBuffer.set(ByteBuffer.allocate(config.getMaxLogEntrySize()));
      }

      int type = SystemLogOperator.DELETE;
      ByteBuffer buffer = localBuffer.get();
      buffer.clear();
      buffer.put((byte) type);
      buffer.putLong(t.getDeleteTime());
      putString(buffer, t.getPaths().get(0).getFullPath());

      return Arrays.copyOfRange(buffer.array(), 0, buffer.position());
    }

    @Override
    public DeletePlan decode(byte[] bytes) throws IOException {
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      buffer.get(); // read  and skip an int representing "type".
      long time = buffer.getLong();

      String path = readString(buffer);

      return new DeletePlan(time, new Path(path));
    }
  };

  static final Codec<UpdatePlan> updatePlanCodec = new Codec<UpdatePlan>() {
    ThreadLocal<ByteBuffer> localBuffer = new ThreadLocal<>();

    @Override
    public byte[] encode(UpdatePlan updatePlan) {
      int type = SystemLogOperator.UPDATE;
      checkBufferAndPutType(localBuffer, type);
      ByteBuffer buffer = localBuffer.get();
      buffer.putInt(updatePlan.getIntervals().size());
      for (Pair<Long, Long> pair : updatePlan.getIntervals()) {
        buffer.putLong(pair.left);
        buffer.putLong(pair.right);
      }

      putString(buffer, updatePlan.getValue());
      putString(buffer, updatePlan.getPath().getFullPath());

      return Arrays.copyOfRange(buffer.array(), 0, buffer.position());
    }

    @Override
    public UpdatePlan decode(byte[] bytes) throws IOException {
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      buffer.get(); // read and skip an int representing "type"

      int timeListBytesLength = buffer.getInt();
      List<Pair<Long, Long>> timeArrayList = new ArrayList<>(timeListBytesLength);
      for (int i = 0; i < timeListBytesLength; i++) {
        long startTime = buffer.getLong();
        long endTime = buffer.getLong();
        timeArrayList.add(new Pair<>(startTime, endTime));
      }

      String value = readString(buffer);
      String path = readString(buffer);

      return new UpdatePlan(timeArrayList, value, new Path(path));
    }
  };

  static final Codec<InsertPlan> multiInsertPlanCodec = new Codec<InsertPlan>() {
    ThreadLocal<ByteBuffer> localBuffer = new ThreadLocal<>();

    @Override
    public byte[] encode(InsertPlan plan) {
      int type = SystemLogOperator.INSERT;
      checkBufferAndPutType(localBuffer, type);
      ByteBuffer buffer = localBuffer.get();
      buffer.put((byte) plan.getInsertType());
      buffer.putLong(plan.getTime());

      putString(buffer, plan.getDeviceId());

      String[] measurementList = plan.getMeasurements();
      buffer.putInt(measurementList.length);
      for (String m : measurementList) {
        putString(buffer, m);
      }

      String[] valueList = plan.getValues();
      buffer.putInt(valueList.length);
      for (String m : valueList) {
        putString(buffer, m);
      }

      return Arrays.copyOfRange(buffer.array(), 0, buffer.position());
    }

    @Override
    public InsertPlan decode(byte[] bytes) throws IOException {
      ByteBuffer buffer = ByteBuffer.wrap(bytes);

      buffer.get(); // read and skip an int representing "type"
      int insertType = buffer.get();
      long time = buffer.getLong();

      String device = readString(buffer);

      int mmListLength = buffer.getInt();
      String[] measurements = new String[mmListLength];
      for (int i = 0; i < mmListLength; i++) {
        measurements[i] = readString(buffer);
      }

      int valueListLength = buffer.getInt();
      String[] values = new String[valueListLength];
      for (int i = 0; i < valueListLength; i++) {
        values[i] = readString(buffer);
      }

      InsertPlan ans = new InsertPlan(device, time, measurements, values);
      ans.setInsertType(insertType);
      return ans;
    }
  };

  static final Codec<MetadataPlan> metadataPlanCodec = new Codec<MetadataPlan>() {
    ThreadLocal<ByteBuffer> localBuffer = new ThreadLocal<>();

    @Override
    public byte[] encode(MetadataPlan plan) {
      int type = SystemLogOperator.METADATA;
      checkBufferAndPutType(localBuffer, type);
      ByteBuffer buffer = localBuffer.get();

      MetadataOperator.NamespaceType namespaceType = plan.getNamespaceType();
      if (namespaceType != null) {
        buffer.put((byte) plan.getNamespaceType().serialize());
      } else {
        buffer.put((byte) NULL_VALUE_LEN);
      }

      TSDataType dataType = plan.getDataType();
      if (dataType != null) {
        buffer.put((byte) plan.getDataType().serialize());
      } else {
        buffer.put((byte) NULL_VALUE_LEN);
      }

      CompressionType compressionType = plan.getCompressor();
      if (compressionType != null) {
        buffer.put((byte) plan.getCompressor().serialize());
      } else {
        buffer.put((byte) NULL_VALUE_LEN);
      }

      TSEncoding tsEncoding = plan.getEncoding();
      if (tsEncoding != null) {
        buffer.put((byte) plan.getEncoding().serialize());
      } else {
        buffer.put((byte) NULL_VALUE_LEN);
      }

      String path = plan.getPath().toString();
      putString(buffer, path);

      List<Path> deletePathList = plan.getDeletePathList();
      if (deletePathList == null) {
        buffer.putInt(NULL_VALUE_LEN);
      } else {
        buffer.putInt(deletePathList.size());
        for (Path deletePath : deletePathList) {
          putString(buffer, deletePath.toString());
        }
      }

      Map<String, String> props = plan.getProps();
      if (props != null) {
        buffer.putInt(props.size());
        for (Entry<String, String> entry : props.entrySet()) {
          putString(buffer, entry.getKey());
          putString(buffer, entry.getValue());
        }
      } else {
        buffer.putInt(NULL_VALUE_LEN);
      }

      return Arrays.copyOfRange(buffer.array(), 0, buffer.position());
    }

    @Override
    public MetadataPlan decode(byte[] bytes) throws IOException {
      ByteBuffer buffer = ByteBuffer.wrap(bytes);

      buffer.get(); // read and skip an int representing "type"

      byte namespaceTypeByte = buffer.get();
      MetadataOperator.NamespaceType namespaceType = null;
      if (namespaceTypeByte != NULL_VALUE_LEN) {
        namespaceType = MetadataOperator.NamespaceType
            .deserialize(namespaceTypeByte);
      }

      byte dataTypeByte = buffer.get();
      TSDataType dataType = null;
      if (dataTypeByte != NULL_VALUE_LEN) {
        dataType = TSDataType.deserialize(dataTypeByte);
      }

      byte compressorByte = buffer.get();
      CompressionType compressor = null;
      if (compressorByte != NULL_VALUE_LEN) {
        compressor = CompressionType.deserialize(compressorByte);
      }

      byte encodingByte = buffer.get();
      TSEncoding encoding = null;
      if (compressorByte != NULL_VALUE_LEN) {
        encoding = TSEncoding.deserialize(encodingByte);
      }

      String path = readString(buffer);
      int pathListLen = buffer.getInt();
      List<Path> deletePathList = null;
      if (pathListLen != NULL_VALUE_LEN) {
        deletePathList = new ArrayList<>(pathListLen);
        for (int i = 0; i < pathListLen; i++) {
          deletePathList.add(new Path(readString(buffer)));
        }
      }

      int propsLen = buffer.getInt();
      Map<String, String> props = null;
      if (propsLen != NULL_VALUE_LEN) {
        props = new HashMap<>(propsLen);
        for (int i = 0; i < propsLen; i++) {
          props.put(readString(buffer), readString(buffer));
        }
      }

      return new MetadataPlan(namespaceType, new Path(path), dataType, compressor, encoding, props,
          deletePathList);
    }
  };

  static final Codec<AuthorPlan> authorPlanCodec = new Codec<AuthorPlan>() {
    ThreadLocal<ByteBuffer> localBuffer = new ThreadLocal<>();

    @Override
    public byte[] encode(AuthorPlan plan) {
      int type = SystemLogOperator.AUTHOR;
      checkBufferAndPutType(localBuffer, type);
      ByteBuffer buffer = localBuffer.get();

      int authorType = plan.getAuthorType().serialize();
      buffer.put((byte) authorType);

      putString(buffer, plan.getUserName());
      putString(buffer, plan.getRoleName());
      putString(buffer, plan.getPassword());
      putString(buffer, plan.getNewPassword());
      putString(buffer, plan.getNodeName().toString());

      Set<Integer> permissions = plan.getPermissions();
      if (permissions == null) {
        buffer.putInt(NULL_VALUE_LEN);
      } else {
        buffer.putInt(permissions.size());
        for (int permission : permissions) {
          buffer.putInt(permission);
        }
      }
      return Arrays.copyOfRange(buffer.array(), 0, buffer.position());
    }

    @Override
    public AuthorPlan decode(byte[] bytes) throws IOException {
      ByteBuffer buffer = ByteBuffer.wrap(bytes);

      buffer.get(); // read and skip an int representing "type"

      AuthorOperator.AuthorType authorType = AuthorOperator.AuthorType.deserialize(buffer.get());
      String userName = readString(buffer);
      String roleName = readString(buffer);
      String password = readString(buffer);
      String newPassword = readString(buffer);
      Path nodeName = new Path(readString(buffer));
      Set<Integer> permissions = null;
      int permissionListLen = buffer.getInt();
      if (permissionListLen != NULL_VALUE_LEN) {
        permissions = new HashSet<>(permissionListLen);
        for (int i = 0; i < permissionListLen; i++) {
          permissions.add(buffer.getInt());
        }
      }
      AuthorPlan authorPlan = null;
      try {
        authorPlan = new AuthorPlan(authorType, userName, roleName, password, newPassword,
            null, nodeName);
      } catch (AuthException e) {
        /** This AuthException will never be caught if authorization parameter is null **/
      }
      authorPlan.setPermissions(permissions);
      return authorPlan;
    }
  };

  static final Codec<LoadDataPlan> loadDataPlanCodec = new Codec<LoadDataPlan>() {
    ThreadLocal<ByteBuffer> localBuffer = new ThreadLocal<>();

    @Override
    public byte[] encode(LoadDataPlan plan) {
      int type = SystemLogOperator.LOADDATA;
      checkBufferAndPutType(localBuffer, type);
      ByteBuffer buffer = localBuffer.get();

      putString(buffer, plan.getInputFilePath());
      putString(buffer, plan.getMeasureType());

      return Arrays.copyOfRange(buffer.array(), 0, buffer.position());
    }

    @Override
    public LoadDataPlan decode(byte[] bytes) throws IOException {
      ByteBuffer buffer = ByteBuffer.wrap(bytes);

      buffer.get(); // read and skip an int representing "type"

      String inputFilePath = readString(buffer);
      String measureType = readString(buffer);
      return new LoadDataPlan(inputFilePath, measureType);
    }
  };

  static final Codec<PropertyPlan> propertyPlanCodec = new Codec<PropertyPlan>() {
    ThreadLocal<ByteBuffer> localBuffer = new ThreadLocal<>();

    @Override
    public byte[] encode(PropertyPlan plan) {
      int type = SystemLogOperator.PROPERTY;
      checkBufferAndPutType(localBuffer, type);
      ByteBuffer buffer = localBuffer.get();

      int propertyType = plan.getPropertyType().serialize();
      buffer.put((byte) propertyType);

      Path metadataPath = plan.getMetadataPath();
      Path propertyPath = plan.getPropertyPath();
      putString(buffer, metadataPath == null ? null : metadataPath.toString());
      putString(buffer, propertyPath == null ? null : propertyPath.toString());

      return Arrays.copyOfRange(buffer.array(), 0, buffer.position());
    }

    @Override
    public PropertyPlan decode(byte[] bytes) throws IOException {
      ByteBuffer buffer = ByteBuffer.wrap(bytes);

      buffer.get(); // read and skip an int representing "type"

      PropertyOperator.PropertyType propertyType = PropertyOperator.PropertyType
          .deserialize(buffer.get());
      String metadataPath = readString(buffer);
      String propertyPath = readString(buffer);
      return new PropertyPlan(propertyType, propertyPath == null ? null : new Path(propertyPath),
          metadataPath == null ? null : new Path(metadataPath));
    }
  };
}
