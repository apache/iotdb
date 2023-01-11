/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.mpp.execution.operator.schema.source;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.read.SchemaRegionReadPlanFactory;
import org.apache.iotdb.db.metadata.query.info.IDeviceSchemaInfo;
import org.apache.iotdb.db.metadata.query.reader.ISchemaReader;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import java.util.Iterator;
import java.util.List;

public class PathsUsingTemplateSource implements ISchemaSource<IDeviceSchemaInfo> {

  private final List<PartialPath> pathPatternList;

  private final int templateId;

  PathsUsingTemplateSource(List<PartialPath> pathPatternList, int templateId) {
    this.pathPatternList = pathPatternList;
    this.templateId = templateId;
  }

  @Override
  public ISchemaReader<IDeviceSchemaInfo> getSchemaReader(ISchemaRegion schemaRegion) {
    return new DevicesUsingTemplateReader(pathPatternList.iterator(), schemaRegion);
  }

  @Override
  public List<ColumnHeader> getInfoQueryColumnHeaders() {
    return ColumnHeaderConstant.showPathsUsingTemplateHeaders;
  }

  @Override
  public void transformToTsBlockColumns(
      IDeviceSchemaInfo device, TsBlockBuilder builder, String database) {
    builder.getTimeColumnBuilder().writeLong(0L);
    builder.getColumnBuilder(0).writeBinary(new Binary(device.getFullPath()));
    builder.declarePosition();
  }

  private class DevicesUsingTemplateReader implements ISchemaReader<IDeviceSchemaInfo> {

    final Iterator<PartialPath> pathPatternIterator;

    final ISchemaRegion schemaRegion;

    private Throwable throwable;

    ISchemaReader<IDeviceSchemaInfo> currentDeviceReader;

    DevicesUsingTemplateReader(
        Iterator<PartialPath> pathPatternIterator, ISchemaRegion schemaRegion) {
      this.pathPatternIterator = pathPatternIterator;
      this.schemaRegion = schemaRegion;
      this.throwable = null;
    }

    @Override
    public void close() throws Exception {
      if (currentDeviceReader != null) {
        currentDeviceReader.close();
      }
    }

    @Override
    public boolean hasNext() {
      try {
        if (throwable != null) {
          return false;
        }
        if (currentDeviceReader != null) {
          if (currentDeviceReader.hasNext()) {
            return true;
          } else {
            currentDeviceReader.close();
            if (!currentDeviceReader.isSuccess()) {
              throwable = currentDeviceReader.getFailure();
              return false;
            }
          }
        }

        while (pathPatternIterator.hasNext()) {
          currentDeviceReader =
              schemaRegion.getDeviceReader(
                  SchemaRegionReadPlanFactory.getShowDevicesPlan(
                      pathPatternIterator.next(), 0, 0, false, templateId));
          if (currentDeviceReader.hasNext()) {
            return true;
          } else {
            currentDeviceReader.close();
          }
        }
        return false;
      } catch (Exception e) {
        throw new RuntimeException(e.getMessage(), e);
      }
    }

    @Override
    public IDeviceSchemaInfo next() {
      return currentDeviceReader.next();
    }

    @Override
    public boolean isSuccess() {
      return throwable == null && (currentDeviceReader == null || currentDeviceReader.isSuccess());
    }

    @Override
    public Throwable getFailure() {
      if (throwable != null) {
        return throwable;
      } else if (currentDeviceReader != null) {
        return currentDeviceReader.getFailure();
      } else {
        return null;
      }
    }
  }
}
