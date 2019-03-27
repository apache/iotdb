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
package org.apache.iotdb.db.writelog.transfer;

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
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.UpdatePlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadDataPlan;
import org.apache.iotdb.db.qp.physical.sys.MetadataPlan;
import org.apache.iotdb.db.utils.ByteBufferUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;

public class CodecInstances {

  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private CodecInstances() {
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
      ByteBufferUtils.putString(buffer, t.getPaths().get(0).getFullPath());

      return Arrays.copyOfRange(buffer.array(), 0, buffer.position());
    }

    @Override
    public DeletePlan decode(byte[] bytes) throws IOException {
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      buffer.get(); // read  and skip an int representing "type".
      long time = buffer.getLong();

      String path = ByteBufferUtils.readString(buffer);

      return new DeletePlan(time, new Path(path));
    }
  };

  static final Codec<UpdatePlan> updatePlanCodec = new Codec<UpdatePlan>() {
    ThreadLocal<ByteBuffer> localBuffer = new ThreadLocal<>();

    @Override
    public byte[] encode(UpdatePlan updatePlan) {
      int type = SystemLogOperator.UPDATE;
      if (localBuffer.get() == null) {
        localBuffer.set(ByteBuffer.allocate(config.getMaxLogEntrySize()));
      }

      ByteBuffer buffer = localBuffer.get();
      buffer.clear();
      buffer.put((byte) type);
      buffer.putInt(updatePlan.getIntervals().size());
      for (Pair<Long, Long> pair : updatePlan.getIntervals()) {
        buffer.putLong(pair.left);
        buffer.putLong(pair.right);
      }

      ByteBufferUtils.putString(buffer, updatePlan.getValue());
      ByteBufferUtils.putString(buffer, updatePlan.getPath().getFullPath());

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

      String value = ByteBufferUtils.readString(buffer);
      String path = ByteBufferUtils.readString(buffer);

      return new UpdatePlan(timeArrayList, value, new Path(path));
    }
  };

  static final Codec<InsertPlan> multiInsertPlanCodec = new Codec<InsertPlan>() {
    ThreadLocal<ByteBuffer> localBuffer = new ThreadLocal<>();

    @Override
    public byte[] encode(InsertPlan plan) {
      int type = SystemLogOperator.INSERT;
      if (localBuffer.get() == null) {
        localBuffer.set(ByteBuffer.allocate(config.getMaxLogEntrySize()));
      }
      ByteBuffer buffer = localBuffer.get();
      buffer.clear();
      buffer.put((byte) type);
      buffer.put((byte) plan.getInsertType());
      buffer.putLong(plan.getTime());

      ByteBufferUtils.putString(buffer, plan.getDeviceId());

      List<String> measurementList = plan.getMeasurements();
      buffer.putInt(measurementList.size());
      for (String m : measurementList) {
        ByteBufferUtils.putString(buffer, m);
      }

      List<String> valueList = plan.getValues();
      buffer.putInt(valueList.size());
      for (String m : valueList) {
        ByteBufferUtils.putString(buffer, m);
      }

      return Arrays.copyOfRange(buffer.array(), 0, buffer.position());
    }

    @Override
    public InsertPlan decode(byte[] bytes) throws IOException {
      ByteBuffer buffer = ByteBuffer.wrap(bytes);

      buffer.get(); // read and skip an int representing "type"
      int insertType = buffer.get();
      long time = buffer.getLong();

      String device = ByteBufferUtils.readString(buffer);

      int mmListLength = buffer.getInt();
      List<String> measurementsList = new ArrayList<>(mmListLength);
      for (int i = 0; i < mmListLength; i++) {
        measurementsList.add(ByteBufferUtils.readString(buffer));
      }

      int valueListLength = buffer.getInt();
      List<String> valuesList = new ArrayList<>(valueListLength);
      for (int i = 0; i < valueListLength; i++) {
        valuesList.add(ByteBufferUtils.readString(buffer));
      }

      InsertPlan ans = new InsertPlan(device, time, measurementsList, valuesList);
      ans.setInsertType(insertType);
      return ans;
    }
  };

  static final Codec<MetadataPlan> metadataPlanCodec = new Codec<MetadataPlan>() {
    ThreadLocal<ByteBuffer> localBuffer = new ThreadLocal<>();

    @Override
    public byte[] encode(MetadataPlan plan) {
      int type = SystemLogOperator.METADATA;
      if (localBuffer.get() == null) {
        localBuffer.set(ByteBuffer.allocate(config.getMaxLogEntrySize()));
      }
      ByteBuffer buffer = localBuffer.get();
      buffer.clear();
      buffer.put((byte) type);
      buffer.put((byte) plan.getNamespaceType().serialize());
      buffer.put((byte) plan.getDataType().serialize());
      buffer.put((byte) plan.getCompressor().serialize());
      buffer.put((byte) plan.getEncoding().serialize());

      String path = plan.getPath().toString();
      ByteBufferUtils.putString(buffer, path);

      List<Path> deletePathList = plan.getDeletePathList();
      if (deletePathList == null) {
        buffer.putInt(-1);
      } else {
        buffer.putInt(deletePathList.size());
        for (Path deletePath : deletePathList) {
          ByteBufferUtils.putString(buffer, deletePath.toString());
        }
      }

      Map<String, String> props = plan.getProps();
      buffer.putInt(props.size());
      for (Entry<String, String> entry : props.entrySet()) {
        ByteBufferUtils.putString(buffer, entry.getKey());
        ByteBufferUtils.putString(buffer, entry.getValue());
      }

      return Arrays.copyOfRange(buffer.array(), 0, buffer.position());
    }

    @Override
    public MetadataPlan decode(byte[] bytes) throws IOException {
      ByteBuffer buffer = ByteBuffer.wrap(bytes);

      buffer.get(); // read and skip an int representing "type"

      MetadataOperator.NamespaceType namespaceType = MetadataOperator.NamespaceType
          .deserialize(buffer.get());
      TSDataType dataType = TSDataType.deserialize(buffer.get());
      CompressionType compressor = CompressionType.deserialize(buffer.get());
      TSEncoding encoding = TSEncoding.deserialize(buffer.get());

      String path = ByteBufferUtils.readString(buffer);
      int pathListLen = buffer.getInt();
      List<Path> deletePathList = null;
      if (pathListLen != -1) {
        deletePathList = new ArrayList<>(pathListLen);
        for (int i = 0; i < pathListLen; i++) {
          deletePathList.add(new Path(ByteBufferUtils.readString(buffer)));
        }
      }

      int propsLen = buffer.getInt();
      Map<String, String> props = new HashMap<>(propsLen);
      for (int i = 0; i < propsLen; i++) {
        props.put(ByteBufferUtils.readString(buffer), ByteBufferUtils.readString(buffer));
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
      if (localBuffer.get() == null) {
        localBuffer.set(ByteBuffer.allocate(config.getMaxLogEntrySize()));
      }
      ByteBuffer buffer = localBuffer.get();
      buffer.clear();
      buffer.put((byte) type);

      int authorType = plan.getAuthorType().serialize();
      buffer.put((byte)authorType);

      ByteBufferUtils.putString(buffer, plan.getUserName());
      ByteBufferUtils.putString(buffer, plan.getRoleName());
      ByteBufferUtils.putString(buffer, plan.getPassword());
      ByteBufferUtils.putString(buffer, plan.getNewPassword());
      ByteBufferUtils.putString(buffer, plan.getNodeName().toString());

      Set<Integer> permissions = plan.getPermissions();
      if (permissions == null) {
        buffer.putInt(-1);
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
      String userName = ByteBufferUtils.readString(buffer);
      String roleName = ByteBufferUtils.readString(buffer);
      String password = ByteBufferUtils.readString(buffer);
      String newPassword = ByteBufferUtils.readString(buffer);
      Path nodeName = new Path(ByteBufferUtils.readString(buffer));
      Set<Integer> permissions = null;
      int permissionListLen = buffer.getInt();
      if (permissionListLen != -1) {
        permissions = new HashSet<>(permissionListLen);
        for(int i =0 ; i < permissionListLen; i ++) {
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
      if (localBuffer.get() == null) {
        localBuffer.set(ByteBuffer.allocate(config.getMaxLogEntrySize()));
      }
      ByteBuffer buffer = localBuffer.get();
      buffer.clear();
      buffer.put((byte) type);

      ByteBufferUtils.putString(buffer, plan.getInputFilePath());
      ByteBufferUtils.putString(buffer, plan.getMeasureType());

      return Arrays.copyOfRange(buffer.array(), 0, buffer.position());
    }

    @Override
    public LoadDataPlan decode(byte[] bytes) throws IOException {
      ByteBuffer buffer = ByteBuffer.wrap(bytes);

      buffer.get(); // read and skip an int representing "type"

      String inputFilePath = ByteBufferUtils.readString(buffer);
      String measureType = ByteBufferUtils.readString(buffer);
      return new LoadDataPlan(inputFilePath, measureType);
    }
  };

}
