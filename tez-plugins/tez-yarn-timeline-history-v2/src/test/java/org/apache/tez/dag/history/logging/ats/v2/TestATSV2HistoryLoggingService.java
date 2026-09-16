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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.client.api.TimelineV2Client;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.app.rm.TaskSchedulerManager;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.events.DAGFinishedEvent;
import org.apache.tez.dag.history.events.DAGStartedEvent;
import org.apache.tez.dag.history.events.DAGSubmittedEvent;
import org.apache.tez.dag.history.logging.EntityTypes;
import org.apache.tez.dag.records.TezDAGID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class TestATSV2HistoryLoggingService {

  private static final String USER = "user";
  private static final String DAG_NAME = "dagName";
  private static final long VERIFY_TIMEOUT_MS = 5000;

  private ApplicationId applicationId;
  private ApplicationAttemptId applicationAttemptId;
  private TezDAGID dagId;
  private AppContext appContext;
  private TaskSchedulerManager taskSchedulerManager;
  private final List<ATSV2HistoryLoggingService> services = new ArrayList<>();

  @Before
  public void setup() {
    applicationId = ApplicationId.newInstance(1000L, 1);
    applicationAttemptId = ApplicationAttemptId.newInstance(applicationId, 1);
    dagId = TezDAGID.getInstance(applicationId, 1);
    taskSchedulerManager = mock(TaskSchedulerManager.class);
    appContext = mock(AppContext.class);
    when(appContext.getApplicationID()).thenReturn(applicationId);
    when(appContext.getClock()).thenReturn(SystemClock.getInstance());
    when(appContext.getTaskScheduler()).thenReturn(taskSchedulerManager);
  }

  @After
  public void teardown() {
    // closed here rather than at the end of each test so that an assertion failure cannot leave
    // the non-daemon handling thread running in the surefire fork
    for (ATSV2HistoryLoggingService service : services) {
      service.stop();
    }
    services.clear();
  }

  @Test(timeout = 10000)
  public void testTimelineServiceV2Disabled() throws Exception {
    Configuration conf = newConf();
    conf.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 1.5f);
    ATSV2HistoryLoggingService service = createService(conf, false);
    service.start();

    assertFalse(service.historyLoggingEnabled);
    assertNull(service.timelineClient);

    // must stay a no-op instead of failing the AM
    service.handle(dagStartedEvent());
  }

  @Test(timeout = 10000)
  public void testRegistersTimelineClient() throws Exception {
    ATSV2HistoryLoggingService service = createService(newConf());
    service.start();

    verify(taskSchedulerManager).registerTimelineV2Client(service.timelineClient);
  }

  @Test(timeout = 10000)
  public void testStartsWithoutTaskSchedulerManager() throws Exception {
    when(appContext.getTaskScheduler()).thenReturn(null);
    ATSV2HistoryLoggingService service = createService(newConf());

    service.start();
    service.handle(dagStartedEvent());
  }

  @Test(timeout = 10000)
  public void testPublishesEntities() throws Exception {
    ATSV2HistoryLoggingService service = createService(newConf());
    service.start();
    service.handle(dagStartedEvent());

    ArgumentCaptor<TimelineEntity> captor = ArgumentCaptor.forClass(TimelineEntity.class);
    verify(service.timelineClient, timeout(VERIFY_TIMEOUT_MS).atLeastOnce())
        .putEntitiesAsync(captor.capture());
    assertEquals(EntityTypes.TEZ_DAG_ID.name(), captor.getValue().getType());
  }

  @Test(timeout = 10000)
  public void testDagFinishedPublishedSynchronously() throws Exception {
    ATSV2HistoryLoggingService service = createService(newConf());
    service.start();
    service.handle(dagFinishedEvent());

    // a terminal event must not be left to the async dispatcher, which is cancelled on stop
    verify(service.timelineClient, timeout(VERIFY_TIMEOUT_MS).atLeastOnce())
        .putEntities(any(TimelineEntity.class));
    verify(service.timelineClient, never()).putEntitiesAsync(any(TimelineEntity.class));
  }

  @Test(timeout = 10000)
  public void testSubAppWriteSkipsNonDagEntities() throws Exception {
    ATSV2HistoryLoggingService service = createService(newConf());
    service.start();
    // DAG_SUBMITTED yields a TEZ_DAG_ID entity plus a TEZ_DAG_EXTRA_INFO one, so the filter has
    // something to reject
    service.handle(dagSubmittedEvent());

    ArgumentCaptor<TimelineEntity> published = ArgumentCaptor.forClass(TimelineEntity.class);
    verify(service.timelineClient, timeout(VERIFY_TIMEOUT_MS).atLeastOnce())
        .putEntitiesAsync(published.capture());
    assertTrue(containsType(published.getAllValues(), EntityTypes.TEZ_DAG_EXTRA_INFO));

    ArgumentCaptor<TimelineEntity> subApp = ArgumentCaptor.forClass(TimelineEntity.class);
    verify(service.timelineClient, timeout(VERIFY_TIMEOUT_MS).atLeastOnce())
        .putSubAppEntitiesAsync(subApp.capture());
    for (TimelineEntity entity : subApp.getAllValues()) {
      assertEquals(EntityTypes.TEZ_DAG_ID.name(), entity.getType());
      // configuration chunks share the DAG entity's identity but carry no events
      assertFalse(entity.getEvents().isEmpty());
    }
  }

  @Test(timeout = 10000)
  public void testSubAppWriteDisabled() throws Exception {
    Configuration conf = newConf();
    conf.setBoolean(TezConfiguration.YARN_ATS_V2_SUBAPP_WRITE, false);
    ATSV2HistoryLoggingService service = createService(conf);
    service.start();
    service.handle(dagStartedEvent());

    verify(service.timelineClient, timeout(VERIFY_TIMEOUT_MS).atLeastOnce())
        .putEntitiesAsync(any(TimelineEntity.class));
    verify(service.timelineClient, never()).putSubAppEntitiesAsync(any(TimelineEntity.class));
    verify(service.timelineClient, never()).putSubAppEntities(any(TimelineEntity.class));
  }

  @Test(timeout = 10000)
  public void testDropsEventsWhenQueueIsFull() throws Exception {
    Configuration conf = newConf();
    conf.setInt(TezConfiguration.TEZ_HISTORY_LOGGING_PROTO_QUEUE_SIZE, 2);
    ATSV2HistoryLoggingService service = createService(conf);

    // not started, so nothing drains the queue
    for (int i = 0; i < 10; i++) {
      service.handle(dagStartedEvent());
    }

    assertEquals(2, service.eventQueue.size());
    assertEquals(8, service.droppedEventCount.get());
  }

  /**
   * A full queue must not defeat the per-DAG opt-out: the filter runs before the offer, so a
   * dropped DAG_SUBMITTED cannot leave the DAG out of the skip set.
   */
  @Test(timeout = 10000)
  public void testDisabledDagStaysSkippedWhenTheQueueIsFull() throws Exception {
    Configuration conf = newConf();
    conf.setInt(TezConfiguration.TEZ_HISTORY_LOGGING_PROTO_QUEUE_SIZE, 1);
    ATSV2HistoryLoggingService service = createService(conf);

    service.handle(dagSubmittedEvent(false));
    // the opted-out DAG must not be queued, and neither must anything that follows it
    assertTrue(service.eventQueue.isEmpty());

    service.handle(dagStartedEvent());
    assertTrue(service.eventQueue.isEmpty());
    assertEquals(0, service.droppedEventCount.get());
  }

  @Test(timeout = 10000)
  public void testPublishFailureDoesNotStopTheService() throws Exception {
    Configuration conf = newConf();
    // one event per batch, so the second publish is a separate call from the failing one
    conf.setInt(TezConfiguration.YARN_ATS_MAX_EVENTS_PER_BATCH, 1);
    ATSV2HistoryLoggingService service = createService(conf);
    doThrow(new RuntimeException("collector is down")).when(service.timelineClient)
        .putEntitiesAsync(any(TimelineEntity.class));
    service.start();

    service.handle(dagStartedEvent());
    service.handle(dagStartedEvent());

    // the second event is still published, so the handling thread survived the first failure
    verify(service.timelineClient, timeout(VERIFY_TIMEOUT_MS).times(2))
        .putEntitiesAsync(any(TimelineEntity.class));
  }

  @Test(timeout = 10000)
  public void testFlushesBacklogOnStop() throws Exception {
    Configuration conf = newConf();
    conf.setLong(TezConfiguration.YARN_ATS_EVENT_FLUSH_TIMEOUT_MILLIS, 5000L);
    ATSV2HistoryLoggingService service = createService(conf);
    // no handling thread, so the backlog can only be flushed by the drain in serviceStop
    service.started = true;

    for (int i = 0; i < 5; i++) {
      service.handle(dagStartedEvent());
    }
    assertEquals(5, service.eventQueue.size());

    service.stop();

    assertTrue(service.eventQueue.isEmpty());
    verify(service.timelineClient, atLeastOnce()).putEntities(any(TimelineEntity.class));
  }

  /**
   * A client that was inited but never started owns no dispatcher threads; publishing to it or
   * stopping it dereferences a null executor inside Hadoop.
   */
  @Test(timeout = 10000)
  public void testNeverStartedServiceDoesNotTouchTheClient() throws Exception {
    ATSV2HistoryLoggingService service = createService(newConf());

    service.handle(dagStartedEvent());
    assertFalse(service.eventQueue.isEmpty());

    service.stop();

    verify(service.timelineClient, never()).putEntities(any(TimelineEntity.class));
    verify(service.timelineClient, never()).putEntitiesAsync(any(TimelineEntity.class));
    verify(service.timelineClient, never()).stop();
  }

  private Configuration newConf() {
    Configuration conf = new Configuration(false);
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    conf.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 2.0f);
    conf.setInt(TezConfiguration.YARN_ATS_MAX_EVENTS_PER_BATCH, 2);
    conf.setLong(TezConfiguration.YARN_ATS_EVENT_FLUSH_TIMEOUT_MILLIS, 5000L);
    return conf;
  }

  private ATSV2HistoryLoggingService createService(Configuration conf) throws Exception {
    return createService(conf, true);
  }

  private ATSV2HistoryLoggingService createService(Configuration conf, boolean mockClient)
      throws Exception {
    ATSV2HistoryLoggingService service = new ATSV2HistoryLoggingService();
    services.add(service);
    service.setAppContext(appContext);
    service.init(conf);
    if (mockClient) {
      service.timelineClient = mock(TimelineV2Client.class);
    }
    return service;
  }

  private static boolean containsType(List<TimelineEntity> entities, EntityTypes type) {
    for (TimelineEntity entity : entities) {
      if (type.name().equals(entity.getType())) {
        return true;
      }
    }
    return false;
  }

  private DAGHistoryEvent dagStartedEvent() {
    return new DAGHistoryEvent(dagId, new DAGStartedEvent(dagId, 1000L, USER, DAG_NAME));
  }

  private DAGHistoryEvent dagSubmittedEvent() {
    return dagSubmittedEvent(true);
  }

  private DAGHistoryEvent dagSubmittedEvent(boolean historyLoggingEnabled) {
    DAGSubmittedEvent event = new DAGSubmittedEvent(dagId, 1000L,
        DAGPlan.newBuilder().setName(DAG_NAME).build(), applicationAttemptId, null, USER,
        new Configuration(false), null, null);
    event.setHistoryLoggingEnabled(historyLoggingEnabled);
    return new DAGHistoryEvent(dagId, event);
  }

  private DAGHistoryEvent dagFinishedEvent() {
    return new DAGHistoryEvent(dagId, new DAGFinishedEvent(dagId, 1000L, 2000L,
        DAGState.SUCCEEDED, null, new TezCounters(), USER, DAG_NAME,
        new HashMap<String, Integer>(), applicationAttemptId,
        DAGPlan.newBuilder().setName(DAG_NAME).build()));
  }
}
