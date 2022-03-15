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
package org.apache.iotdb.db.query.mpp.exec;

import com.google.common.util.concurrent.ListenableFuture;

import java.io.Closeable;
import io.airlift.units.Duration;

/**
 * ExecutableFragmentInstance encapsulates some methods which are necessary for execution scheduler to run a fragment instance
 */
public interface ExecFragmentInstance extends Closeable {

    /**
     * Used to judge whether this fragment instance has any more data to process
     *
     * @return true if the FragmentInstance is done, otherwise false.
     */
    boolean isFinished();

    /**
     * run the fragment instance for {@param duration} time slice, the time of this run is likely not to be equal to {@param duration},
     * the actual run time should be calculated by the caller
     *
     * @param duration how long should this fragment instance run
     * @return the returned ListenableFuture<Void> is used to represent status of this processing
     *         if isDone() return true, meaning that this fragment instance is not blocked and is ready for next processing
     *         otherwise, meaning that this fragment instance is blocked and not ready for next processing.
     */
    ListenableFuture<Void> processFor(Duration duration);

    /**
     * @return the information about this Fragment Instance in String format
     */
    String getInfo();

    /**
     * clear resource used by this fragment instance
     */
    @Override
    void close();
}
