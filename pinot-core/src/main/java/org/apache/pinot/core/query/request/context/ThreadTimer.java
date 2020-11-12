/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.query.request.context;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ThreadTimer {
  private static final ThreadMXBean MX_BEAN = ManagementFactory.getThreadMXBean();
  private static final boolean IS_THREAD_CPU_TIME_SUPPORTED = MX_BEAN.isThreadCpuTimeSupported();
  private static final long NANOS_IN_MILLIS = 1;
  private static final Logger LOGGER = LoggerFactory.getLogger(ThreadTimer.class);

  private long _startTimeMs = -1;
  private long _endTimeMs = -1;

  static {
    LOGGER.info("Thread cpu time measurement supported: {}", IS_THREAD_CPU_TIME_SUPPORTED);
  }

  public ThreadTimer() {
  }

  public void start() {
    if (IS_THREAD_CPU_TIME_SUPPORTED) {
      _startTimeMs = MX_BEAN.getCurrentThreadCpuTime()/NANOS_IN_MILLIS;
    }
  }

  public void stop() {
    if (IS_THREAD_CPU_TIME_SUPPORTED) {
      _endTimeMs = MX_BEAN.getCurrentThreadCpuTime()/NANOS_IN_MILLIS;
    }
  }

  public long getThreadTime() {
    if (_startTimeMs == -1 || _endTimeMs == -1) {
      return 0;
    }
    return _endTimeMs - _startTimeMs;
  }
}
