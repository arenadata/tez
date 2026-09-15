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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

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

  /** Consecutive fully failed batches after which the shutdown flush gives up. */
  private static final int MAX_CONSECUTIVE_DRAIN_FAILURES = 3;
  /** Empty batches tolerated during the shutdown flush while the queue is still not empty. */
  private static final int MAX_EMPTY_DRAIN_BATCHES = 3;

  @VisibleForTesting
  LinkedBlockingQueue<DAGHistoryEvent> eventQueue;

  @VisibleForTesting
  TimelineV2Client timelineClient;

  @VisibleForTesting
  boolean historyLoggingEnabled = true;

  @VisibleForTesting
  final AtomicLong droppedEventCount = new AtomicLong();

  private HistoryEventTimelineV2Conversion conversion;
  private Thread eventHandlingThread;
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  @VisibleForTesting
  volatile boolean started = false;
  private final Object lock = new Object();
  // written from the AM dispatcher thread in handle(), read there too; concurrent because
  // handleCriticalEvent can deliver events from another thread
  private final Set<TezDAGID> skippedDAGs = ConcurrentHashMap.newKeySet();

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
      logTimelineV2Unavailable(conf);
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

  private void logTimelineV2Unavailable(Configuration conf) {
    String serviceClassName = ATSV2HistoryLoggingService.class.getName();
    if (!YarnConfiguration.timelineServiceEnabled(conf)) {
      LOG.warn("{} is disabled due to Timeline Service being disabled, {} set to false",
          serviceClassName, YarnConfiguration.TIMELINE_SERVICE_ENABLED);
      return;
    }
    // timelineServiceV2Enabled resolves through yarn.timeline-service.versions when it is set
    String versions = conf.get(YarnConfiguration.TIMELINE_SERVICE_VERSIONS);
    LOG.warn("{} is disabled because it requires Timeline Service v2, but the configured version"
        + " is {}", serviceClassName,
        versions != null ? versions : YarnConfiguration.getTimelineServiceVersion(conf));
  }

  @Override
  public void serviceStart() {
    if (!historyLoggingEnabled) {
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
            handleEvents(events, asyncEnabled);
          }
        }
      }
    }, "HistoryEventHandlingThread");
    eventHandlingThread.start();
    started = true;
  }

  @Override
  public void serviceStop() {
    LOG.info("Stopping ATSV2Service, eventQueueBacklog={}",
        eventQueue == null ? 0 : eventQueue.size());
    stopped.set(true);
    if (eventHandlingThread != null) {
      eventHandlingThread.interrupt();
    }
    if (started) {
      drainEventQueue();
      // strictly last: a stopped client rejects every further write
      timelineClient.stop();
    } else if (eventQueue != null && !eventQueue.isEmpty()) {
      // the client only owns its dispatcher threads once started; publishing or stopping it
      // before that dereferences a null executor inside Hadoop
      LOG.warn("ATSV2Service stopped before it started, dropping {} history events",
          eventQueue.size());
    }
    long dropped = droppedEventCount.get();
    if (dropped > 0) {
      LOG.warn("Dropped {} history events because the event queue was full", dropped);
    }
  }

  @Override
  public void handle(DAGHistoryEvent event) {
    if (!historyLoggingEnabled || stopped.get()) {
      return;
    }
    // filtered here rather than on the handling thread so that a full queue cannot defeat it: a
    // dropped DAG_SUBMITTED would otherwise leave the DAG out of skippedDAGs and silently
    // re-enable logging for a DAG that opted out
    if (!isValidEvent(event)) {
      return;
    }
    // never block: this runs on the AM's central dispatcher thread
    if (!eventQueue.offer(event)) {
      long dropped = droppedEventCount.incrementAndGet();
      if (dropped % 1000 == 1) {
        LOG.warn("Event queue full, dropping history event, eventType={}, droppedEventCount={}",
            event.getHistoryEvent().getEventType(), dropped);
      }
    }
  }

  private void registerTimelineClient() {
    TaskSchedulerManager taskSchedulerManager = appContext.getTaskScheduler();
    if (taskSchedulerManager == null) {
      LOG.warn("No task scheduler manager available, history will not reach ATSv2");
      return;
    }
    taskSchedulerManager.registerTimelineV2Client(timelineClient);
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
      int consecutiveFailures = 0;
      int emptyBatches = 0;
      while (waitForeverOnShutdown || endTime >= appContext.getClock().getTime()) {
        try {
          getEventBatch(events);
        } catch (InterruptedException e) {
          LOG.info("ATSV2Service interrupted while shutting down, eventQueueBacklog={}",
              eventQueue.size());
        }
        if (events.isEmpty()) {
          if (eventQueue.isEmpty()) {
            LOG.info("Event queue empty, stopping ATSV2Service");
            break;
          }
          // an interrupted poll returns an empty batch while the backlog is still there; the
          // exception cleared the interrupt flag, so the next poll can succeed
          if (++emptyBatches > MAX_EMPTY_DRAIN_BATCHES) {
            LOG.warn("Could not read the ATSv2 backlog, eventQueueBacklog={}", eventQueue.size());
            break;
          }
          continue;
        }
        emptyBatches = 0;
        // synchronous: the async dispatcher discards whatever it still holds when the client is
        // stopped
        if (handleEvents(events, false)) {
          consecutiveFailures = 0;
        } else if (++consecutiveFailures >= MAX_CONSECUTIVE_DRAIN_FAILURES) {
          LOG.warn("Abandoning the ATSv2 backlog after {} consecutive failed batches,"
              + " eventQueueBacklog={}", consecutiveFailures, eventQueue.size());
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

  /**
   * Converts a batch of events and publishes it in at most four calls, so that one HTTP round trip
   * carries the whole batch rather than one entity.
   *
   * @return whether everything in the batch reached the collector
   */
  private boolean handleEvents(List<DAGHistoryEvent> events, boolean async) {
    List<TimelineEntity> asyncEntities = new ArrayList<>();
    List<TimelineEntity> syncEntities = new ArrayList<>();
    List<TimelineEntity> asyncSubAppEntities = new ArrayList<>();
    List<TimelineEntity> syncSubAppEntities = new ArrayList<>();
    boolean published = true;

    for (DAGHistoryEvent event : events) {
      HistoryEventType eventType = event.getHistoryEvent().getEventType();
      // a terminal event is published synchronously so it cannot be lost to the async dispatcher
      boolean publishAsync = async && eventType != HistoryEventType.DAG_FINISHED;
      List<TimelineEntity> entities;
      try {
        entities = conversion.convertToTimelineEntities(event.getHistoryEvent());
      } catch (Exception e) {
        LOG.warn("Could not convert a history event, eventType={}", eventType, e);
        published = false;
        continue;
      }
      for (TimelineEntity entity : entities) {
        (publishAsync ? asyncEntities : syncEntities).add(entity);
        if (isSubAppEntity(entity)) {
          (publishAsync ? asyncSubAppEntities : syncSubAppEntities).add(entity);
        }
      }
    }

    published &= publish(asyncEntities, true, false);
    published &= publish(syncEntities, false, false);
    // published independently of the entity-table write so that one failure does not drop the DAG
    // from the cross-application listing as well
    published &= publish(asyncSubAppEntities, true, true);
    published &= publish(syncSubAppEntities, false, true);
    return published;
  }

  /**
   * Timeline v2 scopes entity queries by application, so DAG entities are mirrored into the
   * sub-application table, the only place a cross-application listing of DAGs can come from. The
   * configuration chunks share the DAG entity's identity but carry no events; that listing does
   * not need them.
   */
  private boolean isSubAppEntity(TimelineEntity entity) {
    return subAppWriteEnabled
        && EntityTypes.TEZ_DAG_ID.name().equals(entity.getType())
        && !entity.getEvents().isEmpty();
  }

  private boolean publish(List<TimelineEntity> entities, boolean async, boolean subApp) {
    if (entities.isEmpty()) {
      return true;
    }
    TimelineEntity[] batch = entities.toArray(new TimelineEntity[0]);
    try {
      if (subApp) {
        if (async) {
          timelineClient.putSubAppEntitiesAsync(batch);
        } else {
          timelineClient.putSubAppEntities(batch);
        }
      } else {
        if (async) {
          timelineClient.putEntitiesAsync(batch);
        } else {
          timelineClient.putEntities(batch);
        }
      }
      return true;
    } catch (Exception e) {
      LOG.warn("Could not publish {} timeline entities, subApp={}, async={}",
          batch.length, subApp, async, e);
      return false;
    }
  }
}
