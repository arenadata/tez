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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;

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
import org.apache.tez.dag.history.logging.EntityTypes;
import org.apache.tez.dag.records.TezDAGID;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class TestATSV2HistoryLoggingService {

  private static final String USER = "user";
  private static final String DAG_NAME = "dagName";

  private ApplicationId applicationId;
  private ApplicationAttemptId applicationAttemptId;
  private TezDAGID dagId;
  private AppContext appContext;
  private TaskSchedulerManager taskSchedulerManager;

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

  @Test(timeout = 10000)
  public void testTimelineServiceV2Disabled() throws Exception {
    ATSV2HistoryLoggingService service = new ATSV2HistoryLoggingService();
    service.setAppContext(appContext);

    Configuration conf = newConf();
    conf.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 1.5f);
    service.init(conf);
    service.start();

    assertFalse(service.historyLoggingEnabled);
    assertNull(service.timelineClient);

    // must stay a no-op instead of failing the AM
    service.handle(dagStartedEvent());
    service.close();
  }

  @Test(timeout = 10000)
  public void testRegistersTimelineClient() throws Exception {
    ATSV2HistoryLoggingService service = createService(newConf());
    service.start();

    verify(taskSchedulerManager).registerTimelineV2Client(service.timelineClient);

    service.close();
  }

  @Test(timeout = 10000)
  public void testStartsWithoutTaskSchedulerManager() throws Exception {
    when(appContext.getTaskScheduler()).thenReturn(null);
    ATSV2HistoryLoggingService service = createService(newConf());

    service.start();
    service.handle(dagStartedEvent());
    service.close();
  }

  @Test(timeout = 10000)
  public void testPublishesEntities() throws Exception {
    ATSV2HistoryLoggingService service = createService(newConf());
    service.start();
    service.handle(dagStartedEvent());
    drain(service);

    ArgumentCaptor<TimelineEntity> captor = ArgumentCaptor.forClass(TimelineEntity.class);
    verify(service.timelineClient, atLeastOnce()).putEntitiesAsync(captor.capture());
    assertEquals(EntityTypes.TEZ_DAG_ID.name(), captor.getValue().getType());

    service.close();
  }

  @Test(timeout = 10000)
  public void testDagFinishedPublishedSynchronously() throws Exception {
    ATSV2HistoryLoggingService service = createService(newConf());
    service.start();
    service.handle(dagFinishedEvent());
    drain(service);

    // a terminal event must not be left to the async dispatcher, which is cancelled on stop
    verify(service.timelineClient, atLeastOnce()).putEntities(any(TimelineEntity.class));
    verify(service.timelineClient, never()).putEntitiesAsync(any(TimelineEntity.class));

    service.close();
  }

  @Test(timeout = 10000)
  public void testSubAppWriteForDagEntitiesOnly() throws Exception {
    ATSV2HistoryLoggingService service = createService(newConf());
    service.start();
    service.handle(dagStartedEvent());
    drain(service);

    ArgumentCaptor<TimelineEntity> captor = ArgumentCaptor.forClass(TimelineEntity.class);
    verify(service.timelineClient, atLeastOnce()).putSubAppEntitiesAsync(captor.capture());
    for (TimelineEntity entity : captor.getAllValues()) {
      assertEquals(EntityTypes.TEZ_DAG_ID.name(), entity.getType());
    }

    service.close();
  }

  @Test(timeout = 10000)
  public void testSubAppWriteDisabled() throws Exception {
    Configuration conf = newConf();
    conf.setBoolean(TezConfiguration.YARN_ATS_V2_SUBAPP_WRITE, false);
    ATSV2HistoryLoggingService service = createService(conf);
    service.start();
    service.handle(dagStartedEvent());
    drain(service);

    verify(service.timelineClient, never()).putSubAppEntitiesAsync(any(TimelineEntity.class));
    verify(service.timelineClient, never()).putSubAppEntities(any(TimelineEntity.class));

    service.close();
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

    service.close();
  }

  @Test(timeout = 10000)
  public void testPublishFailureDoesNotStopTheService() throws Exception {
    ATSV2HistoryLoggingService service = createService(newConf());
    doThrow(new RuntimeException("collector is down")).when(service.timelineClient)
        .putEntitiesAsync(any(TimelineEntity.class));
    service.start();

    service.handle(dagStartedEvent());
    drain(service);

    service.handle(dagStartedEvent());
    drain(service);

    verify(service.timelineClient, atLeastOnce()).putEntitiesAsync(any(TimelineEntity.class));
    service.close();
  }

  @Test(timeout = 10000)
  public void testFlushesBacklogOnStop() throws Exception {
    Configuration conf = newConf();
    conf.setLong(TezConfiguration.YARN_ATS_EVENT_FLUSH_TIMEOUT_MILLIS, 5000L);
    ATSV2HistoryLoggingService service = createService(conf);

    // never started: the backlog can only be flushed by the drain in serviceStop
    for (int i = 0; i < 5; i++) {
      service.handle(dagStartedEvent());
    }
    assertEquals(5, service.eventQueue.size());

    service.close();

    assertTrue(service.eventQueue.isEmpty());
    verify(service.timelineClient, atLeastOnce()).putEntities(any(TimelineEntity.class));
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
    ATSV2HistoryLoggingService service = new ATSV2HistoryLoggingService();
    service.setAppContext(appContext);
    service.init(conf);
    service.timelineClient = mock(TimelineV2Client.class);
    return service;
  }

  private static void drain(ATSV2HistoryLoggingService service) throws InterruptedException {
    while (!service.eventQueue.isEmpty()) {
      Thread.sleep(20);
    }
    // the batch is published after the poll returns, so give the handling thread a moment
    Thread.sleep(200);
  }

  private DAGHistoryEvent dagStartedEvent() {
    return new DAGHistoryEvent(dagId, new DAGStartedEvent(dagId, 1000L, USER, DAG_NAME));
  }

  private DAGHistoryEvent dagFinishedEvent() {
    return new DAGHistoryEvent(dagId, new DAGFinishedEvent(dagId, 1000L, 2000L,
        DAGState.SUCCEEDED, null, new TezCounters(), USER, DAG_NAME,
        new HashMap<String, Integer>(), applicationAttemptId,
        DAGPlan.newBuilder().setName(DAG_NAME).build()));
  }
}
