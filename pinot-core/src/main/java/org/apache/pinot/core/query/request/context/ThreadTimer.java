package org.apache.pinot.core.query.request.context;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ThreadTimer {
  private static final ThreadMXBean MX_BEAN = ManagementFactory.getThreadMXBean();
  private static final boolean IS_THREAD_CPU_TIME_SUPPORTED = MX_BEAN.isThreadCpuTimeSupported();
  private static final long NANOS_IN_MILLIS = 1_000_000;
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
      _startTimeMs = MX_BEAN.getCurrentThreadUserTime()/NANOS_IN_MILLIS;
    }
  }

  public void stop() {
    if (IS_THREAD_CPU_TIME_SUPPORTED) {
      _endTimeMs = MX_BEAN.getCurrentThreadUserTime()/NANOS_IN_MILLIS;
    }
  }

  public long getThreadTime() {
    if (_startTimeMs == -1 || _endTimeMs == -1) {
      return 0;
    }
    return _endTimeMs - _startTimeMs;
  }
}
