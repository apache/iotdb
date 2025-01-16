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

package org.apache.iotdb.pipe.api;

/**
 * {@link PipePlugin}
 *
 * <p>{@link PipePlugin} represents a customizable component that can serve as a data extraction
 * plugin, data processing plugin, or data sending plugin within a pipeline framework.
 *
 * <p>Developers can implement different plugin functionalities according to specific requirements,
 * such as collecting data from various sources, transforming the data, or forwarding the data to
 * external systems.
 *
 * <p>Usage Model:
 *
 * <ul>
 *   <li>By default, a {@link PipePlugin} can operate under a tree model only, if no model
 *       annotation under {@link org.apache.iotdb.pipe.api.annotation} is specified.
 *   <li>To extend its applicability, a {@link PipePlugin} can be configured to support table model,
 *       or both tree and table models concurrently.
 * </ul>
 *
 * <p>Lifecycle:
 *
 * <ul>
 *   <li>When the pipeline framework loads, the plugin's configuration is parsed and validated.
 *   <li>As part of the setup, methods can be provided to prepare connections or resources required
 *       by the plugin (e.g., reading external configurations, establishing data routes).
 *   <li>During data processing, the plugin performs its core functionality (extraction,
 *       transformation, or sending).
 *   <li>When the pipeline is stopped or destroyed, any allocated resources must be released
 *       accurately, and {@link #close()} will be invoked to ensure a clean shutdown.
 * </ul>
 *
 * <p>Example: {@link org.apache.iotdb.CountPointProcessor}
 *
 * <p>Implementations of {@link PipePlugin} should follow best practices for resource management and
 * gracefully handle exceptions, especially when running in long-lived or continuously operating
 * environments.
 */
public interface PipePlugin extends AutoCloseable {}
