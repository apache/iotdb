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

package org.apache.iotdb.db.pipe.metric.overview;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.pipe.resource.memory.PipeDynamicMemoryBlock;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlockType;
import org.apache.iotdb.db.pipe.resource.memory.PipeModelFixedMemoryBlock;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

public class PipeModelFixedMemoryBlockMetrics {

  private final PipeModelFixedMemoryBlock fixedMemoryBlock;

  private final PipeMemoryBlockType memoryBlockType;

  private AbstractMetricService metricService;

  public PipeModelFixedMemoryBlockMetrics(
      PipeMemoryBlockType type, PipeModelFixedMemoryBlock fixedMemoryBlock) {
    this.memoryBlockType = type;
    this.fixedMemoryBlock = fixedMemoryBlock;
  }

  public void registerDynamicMemoryBlockGauge(PipeDynamicMemoryBlock dynamicMemoryBlock) {
    if (dynamicMemoryBlock == null) {
      return;
    }

    metricService.createAutoGauge(
        Metric.PIPE_MODEL_DYNAMIC_MEMORY_BLOCK_MEMORY_SIZE.toString(),
        MetricLevel.IMPORTANT,
        dynamicMemoryBlock,
        PipeDynamicMemoryBlock::getMemoryUsageInBytes,
        Tag.TYPE.toString(),
        memoryBlockType.name(),
        Tag.NAME.toString(),
        String.valueOf(dynamicMemoryBlock.getId()));

    metricService.createAutoGauge(
        Metric.PIPE_MODEL_DYNAMIC_MEMORY_BLOCK_MEMORY_EFFICIENCY.toString(),
        MetricLevel.IMPORTANT,
        dynamicMemoryBlock,
        fixedMemoryBlock::calculateDeficitRatio,
        Tag.TYPE.toString(),
        memoryBlockType.name(),
        Tag.NAME.toString(),
        String.valueOf(dynamicMemoryBlock.getId()));
  }

  public void deregisterDynamicMemoryBlockGauge(PipeDynamicMemoryBlock dynamicMemoryBlock) {
    if (metricService == null) {
      return;
    }

    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PIPE_MODEL_DYNAMIC_MEMORY_BLOCK_MEMORY_SIZE.toString(),
        Tag.TYPE.toString(),
        memoryBlockType.name(),
        Tag.NAME.toString(),
        String.valueOf(dynamicMemoryBlock.getId()));

    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PIPE_MODEL_DYNAMIC_MEMORY_BLOCK_MEMORY_EFFICIENCY.toString(),
        Tag.TYPE.toString(),
        memoryBlockType.name(),
        Tag.NAME.toString(),
        String.valueOf(dynamicMemoryBlock.getId()));
  }

  public void register(final AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.PIPE_FIXED_MEMORY_TOTAL_MEMORY.toString(),
        MetricLevel.IMPORTANT,
        fixedMemoryBlock,
        PipeModelFixedMemoryBlock::getMemoryUsageInBytes,
        Tag.NAME.toString(),
        memoryBlockType.name());

    metricService.createAutoGauge(
        Metric.PIPE_FIXED_MEMORY_ALLOCATED_MEMORY.toString(),
        MetricLevel.IMPORTANT,
        fixedMemoryBlock,
        PipeModelFixedMemoryBlock::getMemoryUsageInBytes,
        Tag.NAME.toString(),
        memoryBlockType.name());

    this.metricService = metricService;

    fixedMemoryBlock.getMemoryBlocks().forEach(this::registerDynamicMemoryBlockGauge);
  }

  public void deregister() {
    if (metricService == null) {
      return;
    }

    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PIPE_FIXED_MEMORY_TOTAL_MEMORY.toString(),
        Tag.NAME.toString(),
        memoryBlockType.name());

    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PIPE_FIXED_MEMORY_ALLOCATED_MEMORY.toString(),
        Tag.NAME.toString(),
        memoryBlockType.name());

    fixedMemoryBlock.getMemoryBlocks().forEach(this::deregisterDynamicMemoryBlockGauge);
  }
}
