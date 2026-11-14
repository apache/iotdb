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

package org.apache.iotdb.consensus.raft.server;

import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.request.RequestSemantics;
import org.apache.iotdb.consensus.common.request.RequestSemanticsProvider;
import org.apache.iotdb.consensus.raft.core.ingest.RequestMeta;

/** Extracts what the leader needs to know about a request before it becomes an entry. */
public interface RequestPreprocessor {

  RequestMeta preprocess(IConsensusRequest request);

  RequestPreprocessor OPAQUE = request -> RequestMeta.OPAQUE;

  // Observation metadata only: standard Raft still uses its unchanged log/commit policy.
  RequestPreprocessor REQUEST_SEMANTICS =
      request -> {
        RequestSemantics semantics =
            request instanceof RequestSemanticsProvider
                ? ((RequestSemanticsProvider) request).getRequestSemantics()
                : RequestSemantics.barrier();
        return semantics.getKind() == RequestSemantics.Kind.DATA
            ? RequestMeta.timed(semantics.getTimestamp())
            : RequestMeta.OPAQUE;
      };
}
