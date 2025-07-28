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

package org.apache.iotdb.db.queryengine.execution.operator.schema.source;

import org.apache.iotdb.commons.exception.runtime.SchemaExecutionException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.schemaengine.schemaregion.read.req.SchemaRegionReadPlanFactory;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.IDeviceSchemaInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.reader.ISchemaReader;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class PathsUsingTemplateSource implements ISchemaSource<IDeviceSchemaInfo> {

  private final List<PartialPath> pathPatternList;
  private final PathPatternTree scope;

  private final int templateId;

  PathsUsingTemplateSource(
      List<PartialPath> pathPatternList, int templateId, PathPatternTree scope) {
    this.pathPatternList = pathPatternList;
    this.templateId = templateId;
    this.scope = scope;
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
    builder
        .getColumnBuilder(0)
        .writeBinary(new Binary(device.getFullPath(), TSFileConfig.STRING_CHARSET));
    builder.declarePosition();
  }

  @Override
  public boolean hasSchemaStatistic(ISchemaRegion schemaRegion) {
    return false;
  }

  @Override
  public long getSchemaStatistic(ISchemaRegion schemaRegion) {
    return schemaRegion.getSchemaRegionStatistics().getTemplateActivatedNumber();
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
    public ListenableFuture<?> isBlocked() {
      return NOT_BLOCKED;
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
                      pathPatternIterator.next(), 0, 0, false, templateId, scope));
          if (currentDeviceReader.hasNext()) {
            return true;
          } else {
            currentDeviceReader.close();
          }
        }
        return false;
      } catch (Exception e) {
        throw new SchemaExecutionException(e.getMessage(), e);
      }
    }

    @Override
    public IDeviceSchemaInfo next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
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
