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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.history.logging.ats.v2;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.client.api.TimelineV2Client;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.app.rm.TaskSchedulerManager;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.events.DAGRecoveredEvent;
import org.apache.tez.dag.history.events.DAGSubmittedEvent;
import org.apache.tez.dag.history.logging.EntityTypes;
import org.apache.tez.dag.history.logging.HistoryLoggingService;
import org.apache.tez.dag.records.TezDAGID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Writes Tez history events to YARN Timeline Service v2.
 *
 * <p>Unlike the ATSv1 plugins, the client only learns where to write once the AM's AMRMClient
 * receives an allocate response carrying the collector address, so the client is handed to the
 * YARN task scheduler in {@link #serviceStart()}. Until the first heartbeat delivers that address
 * the Hadoop client blocks each publish for up to
 * {@code yarn.timeline-service.client.max-retries} times
 * {@code yarn.timeline-service.client.retry-interval-ms}; no retry is layered on top of that here.
 */
public class ATSV2HistoryLoggingService extends HistoryLoggingService {

  private static final Logger LOG = LoggerFactory.getLogger(ATSV2HistoryLoggingService.class);

  private static final String ATS_V2_HISTORY_LOGGING_SERVICE_CLASS_NAME =
      "org.apache.tez.dag.history.logging.ats.v2.ATSV2HistoryLoggingService";

  @VisibleForTesting
  LinkedBlockingQueue<DAGHistoryEvent> eventQueue;

  @VisibleForTesting
  TimelineV2Client timelineClient;

  @VisibleForTesting
  boolean historyLoggingEnabled = true;

  @VisibleForTesting
  volatile long droppedEventCount = 0;

  private HistoryEventTimelineV2Conversion conversion;
  private Thread eventHandlingThread;
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private final Object lock = new Object();
  private final HashSet<TezDAGID> skippedDAGs = new HashSet<>();

  private long maxTimeToWaitOnShutdown;
  private boolean waitForeverOnShutdown = false;
  private int maxEventsPerBatch;
  private long maxPollingTimeMillis;
  private boolean asyncEnabled;
  private boolean subAppWriteEnabled;

  public ATSV2HistoryLoggingService() {
    super(ATSV2HistoryLoggingService.class.getName());
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    historyLoggingEnabled = conf.getBoolean(TezConfiguration.TEZ_AM_HISTORY_LOGGING_ENABLED,
        TezConfiguration.TEZ_AM_HISTORY_LOGGING_ENABLED_DEFAULT);
    if (!historyLoggingEnabled) {
      LOG.info("ATSV2Service: History Logging disabled. "
          + TezConfiguration.TEZ_AM_HISTORY_LOGGING_ENABLED + " set to false");
      return;
    }

    if (!YarnConfiguration.timelineServiceV2Enabled(conf)) {
      historyLoggingEnabled = false;
      LOG.warn("{} is disabled because it requires Timeline Service v2, but {} is set to {}",
          ATS_V2_HISTORY_LOGGING_SERVICE_CLASS_NAME, YarnConfiguration.TIMELINE_SERVICE_VERSION,
          YarnConfiguration.getTimelineServiceVersion(conf));
      return;
    }

    // the queue is built here rather than in serviceStart because the AM emits AppLaunched and
    // AMLaunched while still initializing, before any service has been started
    int queueSize = conf.getInt(TezConfiguration.TEZ_HISTORY_LOGGING_PROTO_QUEUE_SIZE,
        TezConfiguration.TEZ_HISTORY_LOGGING_PROTO_QUEUE_SIZE_DEFAULT);
    eventQueue = new LinkedBlockingQueue<>(queueSize);

    maxTimeToWaitOnShutdown = conf.getLong(
        TezConfiguration.YARN_ATS_EVENT_FLUSH_TIMEOUT_MILLIS,
        TezConfiguration.YARN_ATS_EVENT_FLUSH_TIMEOUT_MILLIS_DEFAULT);
    waitForeverOnShutdown = maxTimeToWaitOnShutdown < 0;
    maxEventsPerBatch = conf.getInt(
        TezConfiguration.YARN_ATS_MAX_EVENTS_PER_BATCH,
        TezConfiguration.YARN_ATS_MAX_EVENTS_PER_BATCH_DEFAULT);
    maxPollingTimeMillis = conf.getInt(
        TezConfiguration.YARN_ATS_MAX_POLLING_TIME_PER_EVENT,
        TezConfiguration.YARN_ATS_MAX_POLLING_TIME_PER_EVENT_DEFAULT);
    asyncEnabled = conf.getBoolean(TezConfiguration.YARN_ATS_V2_ASYNC_ENABLED,
        TezConfiguration.YARN_ATS_V2_ASYNC_ENABLED_DEFAULT);
    subAppWriteEnabled = conf.getBoolean(TezConfiguration.YARN_ATS_V2_SUBAPP_WRITE,
        TezConfiguration.YARN_ATS_V2_SUBAPP_WRITE_DEFAULT);

    conversion = new HistoryEventTimelineV2Conversion(
        conf.getInt(TezConfiguration.YARN_ATS_V2_CONFIG_PUBLISH_SIZE_BYTES,
            TezConfiguration.YARN_ATS_V2_CONFIG_PUBLISH_SIZE_BYTES_DEFAULT),
        conf.getBoolean(TezConfiguration.YARN_ATS_V2_COUNTERS_AS_METRICS,
            TezConfiguration.YARN_ATS_V2_COUNTERS_AS_METRICS_DEFAULT));

    timelineClient = TimelineV2Client.createTimelineClient(appContext.getApplicationID());
    timelineClient.init(conf);

    LOG.info("Initializing {} for {} with queueSize={}, maxEventsPerBatch={},"
        + " maxPollingTime(ms)={}, waitTimeForShutdown(ms)={}, async={}, subAppWrite={}",
        ATSV2HistoryLoggingService.class.getSimpleName(), appContext.getApplicationID(), queueSize,
        maxEventsPerBatch, maxPollingTimeMillis, maxTimeToWaitOnShutdown, asyncEnabled,
        subAppWriteEnabled);
  }

  @Override
  public void serviceStart() {
    if (!historyLoggingEnabled || timelineClient == null) {
      return;
    }

    timelineClient.start();
    registerTimelineClient();

    eventHandlingThread = new Thread(new Runnable() {
      @Override
      public void run() {
        List<DAGHistoryEvent> events = new LinkedList<>();
        boolean interrupted = false;
        while (!stopped.get() && !Thread.currentThread().isInterrupted() && !interrupted) {
          synchronized (lock) {
            try {
              getEventBatch(events);
            } catch (InterruptedException e) {
              // finish processing the current batch and then return
              interrupted = true;
            }
            if (events.isEmpty()) {
              continue;
            }
            try {
              handleEvents(events, asyncEnabled);
            } catch (Exception e) {
              LOG.warn("Error handling events", e);
            }
          }
        }
      }
    }, "HistoryEventHandlingThread");
    eventHandlingThread.start();
  }

  @Override
  public void serviceStop() {
    LOG.info("Stopping ATSV2Service, eventQueueBacklog={}",
        eventQueue == null ? 0 : eventQueue.size());
    stopped.set(true);
    if (eventHandlingThread != null) {
      eventHandlingThread.interrupt();
    }
    if (eventQueue != null) {
      drainEventQueue();
    }
    if (timelineClient != null) {
      // strictly last: a stopped client rejects every further write
      timelineClient.stop();
    }
    if (droppedEventCount > 0) {
      LOG.warn("Dropped {} history events because the event queue was full", droppedEventCount);
    }
  }

  @Override
  public void handle(DAGHistoryEvent event) {
    if (!historyLoggingEnabled || timelineClient == null) {
      return;
    }
    // never block: this runs on the AM's central dispatcher thread
    if (!eventQueue.offer(event)) {
      if (droppedEventCount++ % 1000 == 0) {
        LOG.warn("Event queue full, dropping history event, eventType={}, droppedEventCount={}",
            event.getHistoryEvent().getEventType(), droppedEventCount);
      }
    }
  }

  private void registerTimelineClient() {
    Object taskSchedulerManager = appContext.getTaskScheduler();
    if (!(taskSchedulerManager instanceof TaskSchedulerManager)) {
      LOG.warn("No task scheduler manager available, history will not reach ATSv2");
      return;
    }
    ((TaskSchedulerManager) taskSchedulerManager).registerTimelineV2Client(timelineClient);
  }

  private void drainEventQueue() {
    synchronized (lock) {
      if (eventQueue.isEmpty()) {
        return;
      }
      LOG.warn("ATSV2Service being stopped, eventQueueBacklog={}, maxTimeLeftToFlush={},"
          + " waitForever={}", eventQueue.size(), maxTimeToWaitOnShutdown, waitForeverOnShutdown);
      long endTime = appContext.getClock().getTime() + maxTimeToWaitOnShutdown;
      List<DAGHistoryEvent> events = new LinkedList<>();
      while (waitForeverOnShutdown || endTime >= appContext.getClock().getTime()) {
        try {
          getEventBatch(events);
        } catch (InterruptedException e) {
          LOG.info("ATSV2Service interrupted while shutting down, eventQueueBacklog={}",
              eventQueue.size());
        }
        if (events.isEmpty()) {
          LOG.info("Event queue empty, stopping ATSV2Service");
          break;
        }
        try {
          // synchronous: the async dispatcher discards whatever it still holds when the client is
          // stopped, and a dead collector would otherwise stall every batch for the full client
          // retry window
          handleEvents(events, false);
        } catch (Exception e) {
          LOG.warn("Error handling events while stopping, abandoning the remaining backlog", e);
          break;
        }
      }
    }
    if (!eventQueue.isEmpty()) {
      LOG.warn("Did not finish flushing eventQueue before stopping ATSV2Service,"
          + " eventQueueBacklog={}", eventQueue.size());
    }
  }

  private void getEventBatch(List<DAGHistoryEvent> events) throws InterruptedException {
    events.clear();
    int counter = 0;
    while (counter < maxEventsPerBatch) {
      DAGHistoryEvent event = eventQueue.poll(maxPollingTimeMillis, TimeUnit.MILLISECONDS);
      if (event == null) {
        break;
      }
      if (!isValidEvent(event)) {
        continue;
      }
      ++counter;
      events.add(event);
      if (event.getHistoryEvent().getEventType() == HistoryEventType.DAG_SUBMITTED) {
        // special cased as it might be a large payload
        break;
      }
    }
  }

  private boolean isValidEvent(DAGHistoryEvent event) {
    HistoryEventType eventType = event.getHistoryEvent().getEventType();
    TezDAGID dagId = event.getDAGID();

    if (eventType == HistoryEventType.DAG_SUBMITTED) {
      DAGSubmittedEvent dagSubmittedEvent = (DAGSubmittedEvent) event.getHistoryEvent();
      String dagName = dagSubmittedEvent.getDAGName();
      if ((dagName != null && dagName.startsWith(TezConstants.TEZ_PREWARM_DAG_NAME_PREFIX))
          || !dagSubmittedEvent.isHistoryLoggingEnabled()) {
        skippedDAGs.add(dagId);
        return false;
      }
    } else if (eventType == HistoryEventType.DAG_RECOVERED) {
      DAGRecoveredEvent dagRecoveredEvent = (DAGRecoveredEvent) event.getHistoryEvent();
      if (!dagRecoveredEvent.isHistoryLoggingEnabled()) {
        skippedDAGs.add(dagRecoveredEvent.getDagID());
        return false;
      }
    } else if (eventType == HistoryEventType.DAG_FINISHED) {
      // skippedDAGs is bounded by keeping only in-flight DAGs
      if (skippedDAGs.remove(dagId)) {
        return false;
      }
    }

    return dagId == null || !skippedDAGs.contains(dagId);
  }

  private void handleEvents(List<DAGHistoryEvent> events, boolean async) throws Exception {
    for (DAGHistoryEvent event : events) {
      HistoryEventType eventType = event.getHistoryEvent().getEventType();
      // a terminal event is published synchronously so it cannot be lost to the async dispatcher
      boolean publishAsync = async && eventType != HistoryEventType.DAG_FINISHED;
      for (TimelineEntity entity : conversion.convertToTimelineEntities(event.getHistoryEvent())) {
        publish(entity, publishAsync);
      }
    }
  }

  private void publish(TimelineEntity entity, boolean async) throws Exception {
    if (async) {
      timelineClient.putEntitiesAsync(entity);
    } else {
      timelineClient.putEntities(entity);
    }
    if (!subAppWriteEnabled || !EntityTypes.TEZ_DAG_ID.name().equals(entity.getType())) {
      return;
    }
    // timeline v2 scopes entity queries by application; the sub-application table is the only
    // place a cross-application listing of DAGs can come from
    if (async) {
      timelineClient.putSubAppEntitiesAsync(entity);
    } else {
      timelineClient.putSubAppEntities(entity);
    }
  }
}
