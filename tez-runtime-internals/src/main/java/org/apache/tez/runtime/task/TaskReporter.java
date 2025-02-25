/**
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

package org.apache.tez.runtime.task;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.tez.common.GuavaShim;
import org.apache.tez.common.TezTaskUmbilicalProtocol;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.RuntimeTask;
import org.apache.tez.runtime.api.*;
import org.apache.tez.runtime.api.events.TaskAttemptCompletedEvent;
import org.apache.tez.runtime.api.events.TaskAttemptFailedEvent;
import org.apache.tez.runtime.api.events.TaskAttemptKilledEvent;
import org.apache.tez.runtime.api.events.TaskStatusUpdateEvent;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.TaskStatistics;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.runtime.api.impl.TezHeartbeatRequest;
import org.apache.tez.runtime.api.impl.TezHeartbeatResponse;
import org.apache.tez.runtime.api.impl.EventMetaData.EventProducerConsumerType;
import org.apache.tez.runtime.internals.api.TaskReporterInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Responsible for communication between tasks running in a Container and the ApplicationMaster.
 * Takes care of sending heartbeats (regular and OOB) to the AM - to send generated events, and to
 * retrieve events specific to this task.
 *
 */
public class TaskReporter implements TaskReporterInterface {

  private static final Logger LOG = LoggerFactory.getLogger(TaskReporter.class);

  private final TezTaskUmbilicalProtocol umbilical;
  private final long pollInterval;
  private final long sendCounterInterval;
  private final int maxEventsToGet;
  private final AtomicLong requestCounter;
  private final String containerIdStr;

  private final ListeningExecutorService heartbeatExecutor;

  @VisibleForTesting
  HeartbeatCallable currentCallable;

  public TaskReporter(TezTaskUmbilicalProtocol umbilical, long amPollInterval,
      long sendCounterInterval, int maxEventsToGet, AtomicLong requestCounter, String containerIdStr) {
    this.umbilical = umbilical;
    this.pollInterval = amPollInterval;
    this.sendCounterInterval = sendCounterInterval;
    this.maxEventsToGet = maxEventsToGet;
    this.requestCounter = requestCounter;
    this.containerIdStr = containerIdStr;
    ExecutorService executor = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder()
        .setDaemon(true).setNameFormat("TaskHeartbeatThread").build());
    heartbeatExecutor = MoreExecutors.listeningDecorator(executor);
  }

  /**
   * Register a task to be tracked. Heartbeats will be sent out for this task to fetch events, etc.
   */
  @Override
  public synchronized void registerTask(RuntimeTask task,
      ErrorReporter errorReporter) {
    currentCallable = new HeartbeatCallable(task, umbilical, pollInterval, sendCounterInterval,
        maxEventsToGet, requestCounter, containerIdStr);
    ListenableFuture<Boolean> future = heartbeatExecutor.submit(currentCallable);
    Futures.addCallback(future, new HeartbeatCallback(errorReporter), GuavaShim.directExecutor());
  }

  /**
   * This method should always be invoked before setting up heartbeats for another task running in
   * the same container.
   */
  @Override
  public synchronized void unregisterTask(TezTaskAttemptID taskAttemptID) {
    currentCallable.markComplete();
    currentCallable = null;
  }

  @Override
  public void shutdown() {
    heartbeatExecutor.shutdownNow();
  }

  protected boolean isShuttingDown() {
    return ShutdownHookManager.get().isShutdownInProgress();
  }

  @VisibleForTesting
  static class HeartbeatCallable implements Callable<Boolean> {

    private static final int LOG_COUNTER_START_INTERVAL = 5000; // 5 seconds
    private static final float LOG_COUNTER_BACKOFF = 1.3f;
    private static final int HEAP_MEMORY_USAGE_UPDATE_INTERVAL = 5000; // 5 seconds

    private final RuntimeTask task;
    private final EventMetaData updateEventMetadata;

    private final TezTaskUmbilicalProtocol umbilical;

    private final long pollInterval;
    private final long sendCounterInterval;
    private final int maxEventsToGet;
    private final String containerIdStr;

    private final AtomicLong requestCounter;

    private final AtomicBoolean finalEventQueued = new AtomicBoolean(false);
    private final AtomicBoolean askedToDie = new AtomicBoolean(false);

    private LinkedBlockingQueue<TezEvent> eventsToSend = new LinkedBlockingQueue<TezEvent>();

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();

    private final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    private long usedMemory = 0;
    private long heapMemoryUsageUpdatedTime = System.currentTimeMillis() - HEAP_MEMORY_USAGE_UPDATE_INTERVAL;

    /*
     * Keeps track of regular timed heartbeats. Is primarily used as a timing mechanism to send /
     * log counters.
     */
    private AtomicInteger nonOobHeartbeatCounter = new AtomicInteger(0);
    private int nextHeartbeatNumToLog = 0;
    /*
     * Tracks the last non-OOB heartbeat number at which counters were sent to the AM.
     */
    private int prevCounterSendHeartbeatNum = 0;

    public HeartbeatCallable(RuntimeTask task,
        TezTaskUmbilicalProtocol umbilical, long amPollInterval, long sendCounterInterval,
        int maxEventsToGet, AtomicLong requestCounter, String containerIdStr) {

      this.pollInterval = amPollInterval;
      this.sendCounterInterval = sendCounterInterval;
      this.maxEventsToGet = maxEventsToGet;
      this.requestCounter = requestCounter;
      this.containerIdStr = containerIdStr;

      this.task = task;
      this.umbilical = umbilical;
      this.updateEventMetadata = new EventMetaData(EventProducerConsumerType.SYSTEM,
          task.getVertexName(), "", task.getTaskAttemptID());

      nextHeartbeatNumToLog = (Math.max(1,
          (int) (LOG_COUNTER_START_INTERVAL / (amPollInterval == 0 ? 0.000001f
              : (float) amPollInterval))));
    }

    @Override
    public Boolean call() throws Exception {
      // Heartbeat only for active tasks. Errors, etc will be reported directly.
      while (!task.isTaskDone() && !task.wasErrorReported()) {
        ResponseWrapper response = heartbeat(null);

        if (response.shouldDie) {
          // AM sent a shouldDie=true
          LOG.info("Asked to die via task heartbeat");
          return false;
        } else {
          if (response.numEvents < maxEventsToGet) {
            // Wait before sending another heartbeat. Otherwise consider as an OOB heartbeat
            lock.lock();
            try {
              boolean interrupted = condition.await(pollInterval, TimeUnit.MILLISECONDS);
              if (!interrupted) {
                nonOobHeartbeatCounter.incrementAndGet();
              }
            } finally {
              lock.unlock();
            }
          }
        }
      }
      int pendingEventCount = eventsToSend.size();
      if (pendingEventCount > 0) {
        // This is OK because the pending events will be sent via the succeeded/failed messages.
        // TaskDone is set before taskSucceeded / taskTerminated are sent out - which is what causes the
        // thread to exit.
        LOG.warn("Exiting TaskReporter thread with pending queue size=" + pendingEventCount);
      }
      return true;
    }

    /**
     * @param eventsArg
     * @return
     * @throws IOException
     *           indicates an RPC communication failure.
     * @throws TezException
     *           indicates an exception somewhere in the AM.
     */
    private synchronized ResponseWrapper heartbeat(Collection<TezEvent> eventsArg) throws IOException,
        TezException {

      if (eventsArg != null) {
        eventsToSend.addAll(eventsArg);
      }

      TezEvent updateEvent = null;
      List<TezEvent> events = new ArrayList<TezEvent>();
      eventsToSend.drainTo(events);

      if (!task.isTaskDone() && !task.wasErrorReported()) {
        boolean sendCounters = false;
        /**
         * Increasing the heartbeat interval can delay the delivery of events. Sending just updated
         * records would save CPU in DAG AM, but certain counters are updated very frequently. Until
         * real time decisions are made based on these counters, it can be sent once per second.
         */
        // Not completely accurate, since OOB heartbeats could go out.
        if ((nonOobHeartbeatCounter.get() - prevCounterSendHeartbeatNum) * pollInterval >= sendCounterInterval) {
          sendCounters = true;
          prevCounterSendHeartbeatNum = nonOobHeartbeatCounter.get();
        }
        updateEvent = new TezEvent(getStatusUpdateEvent(sendCounters), updateEventMetadata);
        events.add(updateEvent);
      }

      long requestId = requestCounter.incrementAndGet();
      int fromEventId = task.getNextFromEventId();
      int fromPreRoutedEventId = task.getNextPreRoutedEventId();
      int maxEvents = Math.min(maxEventsToGet, task.getMaxEventsToHandle());
      TezHeartbeatRequest request = new TezHeartbeatRequest(requestId, events, fromPreRoutedEventId,
          containerIdStr, task.getTaskAttemptID(), fromEventId, maxEvents, getUsedMemory());
      LOG.debug("Sending heartbeat to AM, request={}", request);

      maybeLogCounters();

      TezHeartbeatResponse response = umbilical.heartbeat(request);
      LOG.debug("Received heartbeat response from AM, response={}", response);

      if (response.shouldDie()) {
        LOG.info("Received should die response from AM");
        askedToDie.set(true);
        return new ResponseWrapper(true, 1);
      }
      if (response.getLastRequestId() != requestId) {
        throw new TezException("AM and Task out of sync" + ", responseReqId="
            + response.getLastRequestId() + ", expectedReqId=" + requestId);
      }

      // The same umbilical is used by multiple tasks. Problematic in the case where multiple tasks
      // are running using the same umbilical.
      int numEventsReceived = 0;
      if (task.isTaskDone() || task.wasErrorReported()) {
        if (response.getEvents() != null && !response.getEvents().isEmpty()) {
          LOG.info("Current task already complete, Ignoring all events in"
              + " heartbeat response, eventCount=" + response.getEvents().size());
        }
      } else {
        task.setNextFromEventId(response.getNextFromEventId());
        task.setNextPreRoutedEventId(response.getNextPreRoutedEventId());
        if (response.getEvents() != null && !response.getEvents().isEmpty()) {
          LOG.info("Routing events from heartbeat response to task" + ", currentTaskAttemptId="
              + task.getTaskAttemptID() + ", eventCount=" + response.getEvents().size()
              + " fromEventId=" + fromEventId
              + " nextFromEventId=" + response.getNextFromEventId());
          // This should ideally happen in a separate thread
          numEventsReceived = response.getEvents().size();
          task.handleEvents(response.getEvents());
        }
      }
      return new ResponseWrapper(false, numEventsReceived);
    }

    private long getUsedMemory() {
      long now = System.currentTimeMillis();
      if (now - heapMemoryUsageUpdatedTime > HEAP_MEMORY_USAGE_UPDATE_INTERVAL) {
        usedMemory = memoryMXBean.getHeapMemoryUsage().getUsed();
        heapMemoryUsageUpdatedTime = now;
      }
      return usedMemory;
    }

    public void markComplete() {
      // Notify to clear pending events, if any.
      lock.lock();
      try {
        condition.signal();
      } finally {
        lock.unlock();
      }
    }

    private void maybeLogCounters() {
      if (LOG.isDebugEnabled()) {
        if (nonOobHeartbeatCounter.get() == nextHeartbeatNumToLog) {
          LOG.debug("Counters: " + task.getCounters().toShortString());
          nextHeartbeatNumToLog = (int) (nextHeartbeatNumToLog * (LOG_COUNTER_BACKOFF));
        }
      }
    }

    /**
     * Sends out final events for task success.
     * @param taskAttemptID
     * @return
     * @throws IOException
     *           indicates an RPC communication failure.
     * @throws TezException
     *           indicates an exception somewhere in the AM.
     */
    private boolean taskSucceeded(TezTaskAttemptID taskAttemptID) throws IOException, TezException {
      // Ensure only one final event is ever sent.
      if (!finalEventQueued.getAndSet(true)) {
        TezEvent statusUpdateEvent = new TezEvent(getStatusUpdateEvent(true), updateEventMetadata);
        TezEvent taskCompletedEvent = new TezEvent(new TaskAttemptCompletedEvent(),
            updateEventMetadata);
        return !heartbeat(Lists.newArrayList(statusUpdateEvent, taskCompletedEvent)).shouldDie;
      } else {
        LOG.warn("A final task state event has already been sent. Not sending again");
        return askedToDie.get();
      }
    }

    @VisibleForTesting
    TaskStatusUpdateEvent getStatusUpdateEvent(boolean sendCounters) {
      TezCounters counters = null;
      TaskStatistics stats = null;
      float progress = 0;
      boolean progressNotified = false;
      if (task.hasInitialized()) {
        progress = task.getProgress();
        progressNotified = task.getAndClearProgressNotification();
        if (sendCounters) {
          // send these potentially large objects at longer intervals to avoid overloading the AM
          counters = task.getCounters();
          stats = task.getTaskStatistics();
        }
      }
      return new TaskStatusUpdateEvent(counters, progress, stats, progressNotified);
    }

    /**
     * Sends out final events for task failure.
     * @param taskAttemptID
     * @param isKilled
     * @param taskFailureType
     * @param t
     * @param diagnostics
     * @param srcMeta
     * @return
     * @throws IOException
     *           indicates an RPC communication failure.
     * @throws TezException
     *           indicates an exception somewhere in the AM.
     */
    private boolean taskTerminated(TezTaskAttemptID taskAttemptID, boolean isKilled, TaskFailureType taskFailureType,
                                   Throwable t, String diagnostics,
                                   EventMetaData srcMeta) throws IOException, TezException {
      // Ensure only one final event is ever sent.
      if (!finalEventQueued.getAndSet(true)) {
        List<TezEvent> tezEvents = new ArrayList<TezEvent>();
        if (diagnostics == null) {
          diagnostics = "Node: " + InetAddress.getLocalHost() + " : " + ExceptionUtils.getStackTrace(t);
        } else {
          diagnostics =
              "Node: " + InetAddress.getLocalHost() + " : " + diagnostics + ":" + ExceptionUtils.getStackTrace(t);
        }
        if (isKilled) {
          tezEvents.add(new TezEvent(new TaskAttemptKilledEvent(diagnostics),
              srcMeta == null ? updateEventMetadata : srcMeta));
        } else {
          tezEvents.add(new TezEvent(new TaskAttemptFailedEvent(diagnostics,
              taskFailureType),
              srcMeta == null ? updateEventMetadata : srcMeta));
        }
        try {
          tezEvents.add(new TezEvent(getStatusUpdateEvent(true), updateEventMetadata));
        } catch (Exception e) {
          // Counter may exceed limitation
          LOG.warn("Error when get constructing TaskStatusUpdateEvent. Not sending it out");
        }
        return !heartbeat(tezEvents).shouldDie;
      } else {
        LOG.warn("A final task state event has already been sent. Not sending again");
        return askedToDie.get();
      }
    }

    private void addEvents(TezTaskAttemptID taskAttemptID, Collection<TezEvent> events) {
      if (events != null && !events.isEmpty()) {
        eventsToSend.addAll(events);
      }
    }
  }

  private static class HeartbeatCallback implements FutureCallback<Boolean> {

    private final ErrorReporter errorReporter;

    HeartbeatCallback(ErrorReporter errorReporter) {
      this.errorReporter = errorReporter;
    }

    @Override
    public void onSuccess(Boolean result) {
      if (result == false) {
        errorReporter.shutdownRequested();
      }
    }

    @Override
    public void onFailure(Throwable t) {
      errorReporter.reportError(t);
    }
  }

  @Override
  public synchronized boolean taskSucceeded(TezTaskAttemptID taskAttemptID) throws IOException, TezException {
    return currentCallable.taskSucceeded(taskAttemptID);
  }

  @Override
  public synchronized boolean taskFailed(TezTaskAttemptID taskAttemptID,
                                                  TaskFailureType taskFailureType,
                                                  Throwable t, String diagnostics,
                                                  EventMetaData srcMeta) throws IOException,
      TezException {
    if(!isShuttingDown()) {
      return currentCallable.taskTerminated(taskAttemptID, false, taskFailureType, t, diagnostics, srcMeta);
    }
    return false;
  }

  @Override
  public boolean taskKilled(TezTaskAttemptID taskAttemptId, Throwable t, String diagnostics,
                            EventMetaData srcMeta) throws IOException, TezException {
    if(!isShuttingDown()) {
      return currentCallable.taskTerminated(taskAttemptId, true, null, t, diagnostics, srcMeta);
    }
    return false;
  }

  @Override
  public synchronized void addEvents(TezTaskAttemptID taskAttemptID, Collection<TezEvent> events) {
    currentCallable.addEvents(taskAttemptID, events);
  }

  @Override
  public boolean canCommit(TezTaskAttemptID taskAttemptID) throws IOException {
    return umbilical.canCommit(taskAttemptID);
  }

  private static final class ResponseWrapper {
    boolean shouldDie;
    int numEvents;

    private ResponseWrapper(boolean shouldDie, int numEvents) {
      this.shouldDie = shouldDie;
      this.numEvents = numEvents;
    }
  }
}
