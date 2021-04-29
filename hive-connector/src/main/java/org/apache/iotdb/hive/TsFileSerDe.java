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
package org.apache.iotdb.hive;

import org.apache.iotdb.hadoop.tsfile.record.HDFSTSRecord;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StringInternUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.apache.iotdb.hadoop.tsfile.TSFInputFormat.READ_DELTAOBJECTS;
import static org.apache.iotdb.hadoop.tsfile.TSFInputFormat.READ_MEASUREMENTID;

public class TsFileSerDe extends AbstractSerDe {

  private static final Logger logger = LoggerFactory.getLogger(TsFileSerDe.class);

  public static final String DEVICE_ID = "device_id";

  private List<String> columnNames;
  private List<TypeInfo> columnTypes;
  private TsFileDeserializer tsFileDeserializer = null;
  private ObjectInspector oi;
  private String deviceId;

  @Override
  public void initialize(@Nullable Configuration conf, Properties tbl) throws SerDeException {

    final String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
    final String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
    final String columnNameDelimiter =
        tbl.containsKey(serdeConstants.COLUMN_NAME_DELIMITER)
            ? tbl.getProperty(serdeConstants.COLUMN_NAME_DELIMITER)
            : String.valueOf(SerDeUtils.COMMA);

    deviceId = tbl.getProperty(DEVICE_ID);

    if (columnNameProperty == null
        || columnNameProperty.isEmpty()
        || columnTypeProperty == null
        || columnTypeProperty.isEmpty()) {
      columnNames = Collections.emptyList();
      columnTypes = Collections.emptyList();
    } else {
      columnNames =
          StringInternUtils.internStringsInList(
              Arrays.asList(columnNameProperty.split(columnNameDelimiter)));
      columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    }

    // Check column and types equals
    if (columnTypes.size() != columnNames.size()) {
      throw new TsFileSerDeException("len(columnNames) != len(columnTypes)");
    }

    if (conf != null) {
      conf.set(READ_DELTAOBJECTS, deviceId);
      conf.set(READ_MEASUREMENTID, columnNames.get(1));
    }

    oi = createObjectInspector();
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    // Not support yet
    return HDFSTSRecord.class;
  }

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) {
    // Not support yet
    return null;
  }

  @Override
  public SerDeStats getSerDeStats() {
    // Not support yet
    return null;
  }

  @Override
  public Object deserialize(Writable blob) throws SerDeException {
    return getDeserializer().deserialize(columnNames, columnTypes, blob, deviceId);
  }

  @Override
  public ObjectInspector getObjectInspector() {
    return oi;
  }

  private ObjectInspector createObjectInspector() throws TsFileSerDeException {
    List<ObjectInspector> columnOIs = new ArrayList<>(columnNames.size());

    // At this point we've verified the types are correct.
    for (int i = 0; i < columnNames.size(); i++) {
      columnOIs.add(i, createObjectInspectorWorker(columnTypes.get(i)));
    }
    return ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs);
  }

  private ObjectInspector createObjectInspectorWorker(TypeInfo ti) throws TsFileSerDeException {
    if (!supportedCategories(ti)) {
      throw new TsFileSerDeException("Don't yet support this type: " + ti);
    }
    ObjectInspector result;
    switch (ti.getCategory()) {
      case PRIMITIVE:
        PrimitiveTypeInfo pti = (PrimitiveTypeInfo) ti;
        result = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(pti);
        break;
        // these types is not supported in TsFile
      case LIST:
      case MAP:
      case STRUCT:
      case UNION:
        throw new TsFileSerDeException("The type is not supported in TsFile: " + ti);
      default:
        throw new TsFileSerDeException("No Hive categories matched: " + ti);
    }
    return result;
  }

  private boolean supportedCategories(TypeInfo ti) {
    final ObjectInspector.Category c = ti.getCategory();
    return c.equals(ObjectInspector.Category.PRIMITIVE);
  }

  private TsFileDeserializer getDeserializer() {
    if (tsFileDeserializer == null) {
      tsFileDeserializer = new TsFileDeserializer();
    }

    return tsFileDeserializer;
  }
}
