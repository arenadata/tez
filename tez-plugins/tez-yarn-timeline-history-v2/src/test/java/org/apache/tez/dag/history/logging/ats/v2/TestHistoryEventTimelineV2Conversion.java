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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.util.TimelineServiceHelper;
import org.apache.tez.common.ATSConstants;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.api.records.DAGProtos.CallerContextProto;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.app.dag.VertexState;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.events.AMLaunchedEvent;
import org.apache.tez.dag.history.events.AMStartedEvent;
import org.apache.tez.dag.history.events.AppLaunchedEvent;
import org.apache.tez.dag.history.events.ContainerLaunchedEvent;
import org.apache.tez.dag.history.events.ContainerStoppedEvent;
import org.apache.tez.dag.history.events.DAGCommitStartedEvent;
import org.apache.tez.dag.history.events.DAGFinishedEvent;
import org.apache.tez.dag.history.events.DAGInitializedEvent;
import org.apache.tez.dag.history.events.DAGKillRequestEvent;
import org.apache.tez.dag.history.events.DAGRecoveredEvent;
import org.apache.tez.dag.history.events.DAGStartedEvent;
import org.apache.tez.dag.history.events.DAGSubmittedEvent;
import org.apache.tez.dag.history.events.TaskAttemptFinishedEvent;
import org.apache.tez.dag.history.events.TaskAttemptStartedEvent;
import org.apache.tez.dag.history.events.TaskFinishedEvent;
import org.apache.tez.dag.history.events.TaskStartedEvent;
import org.apache.tez.dag.history.events.VertexCommitStartedEvent;
import org.apache.tez.dag.history.events.VertexConfigurationDoneEvent;
import org.apache.tez.dag.history.events.VertexFinishedEvent;
import org.apache.tez.dag.history.events.VertexGroupCommitFinishedEvent;
import org.apache.tez.dag.history.events.VertexGroupCommitStartedEvent;
import org.apache.tez.dag.history.events.VertexInitializedEvent;
import org.apache.tez.dag.history.events.VertexStartedEvent;
import org.apache.tez.dag.history.logging.EntityTypes;
import org.apache.tez.dag.records.TaskAttemptTerminationCause;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.TaskFailureType;
import org.junit.Before;
import org.junit.Test;

public class TestHistoryEventTimelineV2Conversion {

  private static final String USER = "user";
  private static final String CONTAINER_LOGS = "containerLogs";
  private static final int CONFIG_PUBLISH_SIZE_BYTES = 10 * 1024;

  private final Random random = new Random();

  private HistoryEventTimelineV2Conversion conversion;
  private ApplicationId applicationId;
  private ApplicationAttemptId applicationAttemptId;
  private TezDAGID tezDAGID;
  private TezVertexID tezVertexID;
  private TezTaskID tezTaskID;
  private TezTaskAttemptID tezTaskAttemptID;
  private DAGPlan dagPlan;
  private ContainerId containerId;
  private NodeId nodeId;

  @Before
  public void setup() {
    conversion = new HistoryEventTimelineV2Conversion(CONFIG_PUBLISH_SIZE_BYTES, true);
    applicationId = ApplicationId.newInstance(9999L, 1);
    applicationAttemptId = ApplicationAttemptId.newInstance(applicationId, 1);
    tezDAGID = TezDAGID.getInstance(applicationId, random.nextInt(Integer.MAX_VALUE));
    tezVertexID = TezVertexID.getInstance(tezDAGID, random.nextInt(Integer.MAX_VALUE));
    tezTaskID = TezTaskID.getInstance(tezVertexID, random.nextInt(Integer.MAX_VALUE));
    tezTaskAttemptID = TezTaskAttemptID.getInstance(tezTaskID, random.nextInt(Integer.MAX_VALUE));
    CallerContextProto.Builder callerContextProto = CallerContextProto.newBuilder();
    callerContextProto.setContext("ctxt");
    callerContextProto.setCallerId("Caller_ID");
    callerContextProto.setCallerType("Caller_Type");
    callerContextProto.setBlob("Desc_1");
    dagPlan = DAGPlan.newBuilder().setName("DAGPlanMock")
        .setCallerContext(callerContextProto).build();
    containerId = ContainerId.newInstance(applicationAttemptId, 111);
    nodeId = NodeId.newInstance("node", 13435);
  }

  /**
   * Fails when a new {@link HistoryEventType} constant is added without a conversion.
   */
  @Test(timeout = 5000)
  public void testHandlerExists() {
    for (HistoryEventType eventType : HistoryEventType.values()) {
      HistoryEvent event = newEvent(eventType);
      if (event == null || !event.isHistoryEvent()) {
        continue;
      }
      List<TimelineEntity> entities = conversion.convertToTimelineEntities(event);
      assertFalse("No entity produced for " + eventType, entities.isEmpty());
      for (TimelineEntity entity : entities) {
        assertNotNull("No type on the entity for " + eventType, entity.getType());
        assertNotNull("No id on the entity for " + eventType, entity.getId());
        // TimelineEvent.isValid() rejects a zero timestamp
        for (TimelineEvent timelineEvent : entity.getEvents()) {
          assertTrue("Invalid timeline event for " + eventType, timelineEvent.isValid());
        }
      }
    }
  }

  @Test(timeout = 5000)
  public void testUnsupportedEventTypesRejected() {
    HistoryEventType[] unsupported = {
        HistoryEventType.VERTEX_COMMIT_STARTED,
        HistoryEventType.VERTEX_GROUP_COMMIT_STARTED,
        HistoryEventType.VERTEX_GROUP_COMMIT_FINISHED,
        HistoryEventType.DAG_COMMIT_STARTED,
        HistoryEventType.DAG_KILL_REQUEST,
    };
    for (HistoryEventType eventType : unsupported) {
      try {
        conversion.convertToTimelineEntities(newEvent(eventType));
        fail("Expected an UnsupportedOperationException for " + eventType);
      } catch (UnsupportedOperationException expected) {
        // expected
      }
    }
  }

  /**
   * Timeline v2 requires every update of an entity to carry the same idPrefix; a differing prefix
   * silently writes a duplicate row instead of updating the existing one.
   */
  @Test(timeout = 5000)
  public void testIdPrefixStableAcrossEvents() {
    Map<String, Long> prefixByEntity = new HashMap<>();
    for (HistoryEventType eventType : HistoryEventType.values()) {
      HistoryEvent event = newEvent(eventType);
      if (event == null || !event.isHistoryEvent()) {
        continue;
      }
      for (TimelineEntity entity : conversion.convertToTimelineEntities(event)) {
        String key = entity.getType() + "/" + entity.getId();
        Long seen = prefixByEntity.put(key, entity.getIdPrefix());
        if (seen != null) {
          assertEquals("idPrefix differs between updates of " + key + " (" + eventType + ")",
              seen.longValue(), entity.getIdPrefix());
        }
      }
    }
    assertFalse(prefixByEntity.isEmpty());
  }

  @Test(timeout = 5000)
  public void testIdPrefixValues() {
    TimelineEntity dag = single(new DAGStartedEvent(tezDAGID, 1L, USER, dagPlan.getName()));
    assertEquals(TimelineServiceHelper.invertLong(tezDAGID.getId()), dag.getIdPrefix());

    TimelineEntity vertex =
        single(new VertexStartedEvent(tezVertexID, 1L, 2L));
    assertEquals(tezVertexID.getId(), vertex.getIdPrefix());

    TimelineEntity task = single(new TaskStartedEvent(tezTaskID, "v1", 1L, 2L));
    assertEquals(tezTaskID.getId(), task.getIdPrefix());

    TimelineEntity attempt = single(new TaskAttemptStartedEvent(tezTaskAttemptID, "v1", 1L,
        containerId, nodeId, null, null, "nodeHttpAddress"));
    assertEquals(TimelineServiceHelper.invertLong(tezTaskAttemptID.getId()),
        attempt.getIdPrefix());

    TimelineEntity attemptEntity = single(new AMStartedEvent(applicationAttemptId, 1L, USER));
    assertEquals(TimelineServiceHelper.invertLong(applicationAttemptId.getAttemptId()),
        attemptEntity.getIdPrefix());

    TimelineEntity app = first(new AppLaunchedEvent(applicationId, 1L, 2L, USER,
        new Configuration(false), null));
    assertEquals(TimelineEntity.DEFAULT_ENTITY_PREFIX, app.getIdPrefix());
  }

  /**
   * Without primary filters the ancestor chain is the only way to find an entity's siblings, and
   * v2 cannot join two hops, so tasks and attempts carry the whole chain on every event.
   */
  @Test(timeout = 5000)
  public void testAncestryOnEveryEvent() {
    TimelineEntity taskStarted = single(new TaskStartedEvent(tezTaskID, "v1", 1L, 2L));
    assertRelatedTo(taskStarted, EntityTypes.TEZ_DAG_ID, tezDAGID.toString());
    assertRelatedTo(taskStarted, EntityTypes.TEZ_VERTEX_ID, tezVertexID.toString());

    TimelineEntity taskFinished = single(new TaskFinishedEvent(tezTaskID, "v1", 1L, 2L,
        tezTaskAttemptID, TaskState.SUCCEEDED, null, new TezCounters(), 0));
    assertRelatedTo(taskFinished, EntityTypes.TEZ_DAG_ID, tezDAGID.toString());
    assertRelatedTo(taskFinished, EntityTypes.TEZ_VERTEX_ID, tezVertexID.toString());

    TimelineEntity attemptFinished = single(new TaskAttemptFinishedEvent(tezTaskAttemptID, "v1",
        1L, 2L, TaskAttemptState.SUCCEEDED, null, null, null, new TezCounters(), null, null, 0,
        null, 0, containerId, nodeId, null, null, "nodeHttpAddress"));
    assertRelatedTo(attemptFinished, EntityTypes.TEZ_DAG_ID, tezDAGID.toString());
    assertRelatedTo(attemptFinished, EntityTypes.TEZ_VERTEX_ID, tezVertexID.toString());
    assertRelatedTo(attemptFinished, EntityTypes.TEZ_TASK_ID, tezTaskID.toString());

    TimelineEntity vertexFinished = single(new VertexFinishedEvent(tezVertexID, "v1", 1, 1L, 2L,
        3L, 4L, 5L, VertexState.SUCCEEDED, null, new TezCounters(), null, null, null));
    assertRelatedTo(vertexFinished, EntityTypes.TEZ_DAG_ID, tezDAGID.toString());
  }

  @Test(timeout = 5000)
  public void testCreatedTimeOnlyOnCreatingEvent() {
    TimelineEntity started = single(new TaskStartedEvent(tezTaskID, "v1", 1000L, 2000L));
    assertEquals(Long.valueOf(2000L), started.getCreatedTime());

    TimelineEntity finished = single(new TaskFinishedEvent(tezTaskID, "v1", 1000L, 3000L,
        tezTaskAttemptID, TaskState.SUCCEEDED, null, new TezCounters(), 0));
    assertNull(finished.getCreatedTime());
  }

  @Test(timeout = 5000)
  public void testCountersToMetrics() {
    TezCounters counters = new TezCounters();
    counters.getGroup("group1").findCounter("counter1", true).setValue(7);
    // a zero counter is dropped from the ATS map but kept as a metric, where it is meaningful
    counters.getGroup("group1").findCounter("counter2", true).setValue(0);
    counters.getGroup("group2").findCounter("counter3", true).setValue(9);

    TimelineEntity entity = single(new TaskFinishedEvent(tezTaskID, "v1", 1L, 5000L,
        tezTaskAttemptID, TaskState.SUCCEEDED, null, counters, 0));

    Map<String, Number> metrics = new HashMap<>();
    for (TimelineMetric metric : entity.getMetrics()) {
      assertEquals(TimelineMetric.Type.SINGLE_VALUE, metric.getType());
      assertEquals(1, metric.getValues().size());
      assertEquals(Long.valueOf(5000L), metric.getValues().keySet().iterator().next());
      metrics.put(metric.getId(), metric.getValues().values().iterator().next());
    }
    assertEquals(7L, metrics.get("group1:counter1"));
    assertEquals(0L, metrics.get("group1:counter2"));
    assertEquals(9L, metrics.get("group2:counter3"));

    // the nested counter map stays alongside the metrics; it carries display names and aggregates
    assertNotNull(entity.getInfo().get(ATSConstants.COUNTERS));
  }

  @Test(timeout = 5000)
  public void testCountersAsMetricsDisabled() {
    TezCounters counters = new TezCounters();
    counters.getGroup("group1").findCounter("counter1", true).setValue(7);
    HistoryEventTimelineV2Conversion noMetrics =
        new HistoryEventTimelineV2Conversion(CONFIG_PUBLISH_SIZE_BYTES, false);

    List<TimelineEntity> entities = noMetrics.convertToTimelineEntities(
        new TaskFinishedEvent(tezTaskID, "v1", 1L, 5000L, tezTaskAttemptID, TaskState.SUCCEEDED,
            null, counters, 0));

    assertTrue(entities.get(0).getMetrics().isEmpty());
    assertNotNull(entities.get(0).getInfo().get(ATSConstants.COUNTERS));
  }

  @Test(timeout = 5000)
  public void testConfigurationGoesToConfigs() {
    Configuration conf = new Configuration(false);
    conf.set("key1", "value1");
    conf.set("key2", "value2");

    List<TimelineEntity> entities = conversion.convertToTimelineEntities(
        new AppLaunchedEvent(applicationId, 1L, 2L, USER, conf, null));

    assertEquals(1, entities.size());
    TimelineEntity entity = entities.get(0);
    assertEquals("value1", entity.getConfigs().get("key1"));
    assertEquals("value2", entity.getConfigs().get("key2"));
    // configs are a first-class v2 field, not a nested info map
    assertNull(entity.getInfo().get(ATSConstants.CONFIG));
  }

  @Test(timeout = 5000)
  public void testConfigChunking() {
    Configuration conf = new Configuration(false);
    int entries = 50;
    StringBuilder value = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      value.append('x');
    }
    for (int i = 0; i < entries; i++) {
      conf.set("key" + i, value.toString());
    }

    HistoryEventTimelineV2Conversion chunking = new HistoryEventTimelineV2Conversion(512, true);
    List<TimelineEntity> entities = chunking.convertToTimelineEntities(
        new AppLaunchedEvent(applicationId, 1L, 2L, USER, conf, null));

    assertTrue("Expected the configuration to be split", entities.size() > 1);
    Map<String, String> merged = new HashMap<>();
    for (TimelineEntity entity : entities) {
      // every chunk must address the same row
      assertEquals(entities.get(0).getType(), entity.getType());
      assertEquals(entities.get(0).getId(), entity.getId());
      assertEquals(entities.get(0).getIdPrefix(), entity.getIdPrefix());
      merged.putAll(entity.getConfigs());
    }
    assertEquals(entries, merged.size());
    assertEquals(value.toString(), merged.get("key0"));
  }

  /**
   * The budget is in bytes, so it has to be measured in UTF-8, not in UTF-16 code units.
   */
  @Test(timeout = 5000)
  public void testConfigChunkingCountsUtf8Bytes() {
    // 100 characters, 3 UTF-8 bytes each
    StringBuilder value = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      value.append('中');
    }
    Configuration conf = new Configuration(false);
    for (int i = 0; i < 10; i++) {
      conf.set("key" + i, value.toString());
    }

    // 10 entries at ~304 bytes each; a UTF-16 count would see ~104 and fit them all in one entity
    HistoryEventTimelineV2Conversion chunking = new HistoryEventTimelineV2Conversion(1024, true);
    List<TimelineEntity> entities = chunking.convertToTimelineEntities(
        new AppLaunchedEvent(applicationId, 1L, 2L, USER, conf, null));

    assertTrue("Expected the byte budget to split the configuration", entities.size() > 2);
    for (TimelineEntity entity : entities) {
      long bytes = 0;
      for (Map.Entry<String, String> entry : entity.getConfigs().entrySet()) {
        bytes += entry.getKey().getBytes(StandardCharsets.UTF_8).length
            + entry.getValue().getBytes(StandardCharsets.UTF_8).length;
      }
      // a single entry larger than the budget is still emitted whole, so only multi-entry
      // entities are bounded
      if (entity.getConfigs().size() > 1) {
        assertTrue("Chunk of " + bytes + " bytes exceeds the budget", bytes <= 1024);
      }
    }
  }

  @Test(timeout = 5000)
  public void testDagSubmittedSplitsExtraInfo() {
    List<TimelineEntity> entities = conversion.convertToTimelineEntities(
        new DAGSubmittedEvent(tezDAGID, 1L, dagPlan, applicationAttemptId, null, USER, null,
            CONTAINER_LOGS, null));

    Map<String, TimelineEntity> byType = byType(entities);
    TimelineEntity dag = byType.get(EntityTypes.TEZ_DAG_ID.name());
    TimelineEntity extraInfo = byType.get(EntityTypes.TEZ_DAG_EXTRA_INFO.name());
    assertNotNull(dag);
    assertNotNull(extraInfo);

    // the DAG plan is the large payload and stays off the DAG row
    assertNull(dag.getInfo().get(ATSConstants.DAG_PLAN));
    assertNotNull(extraInfo.getInfo().get(ATSConstants.DAG_PLAN));
    assertEquals(dag.getIdPrefix(), extraInfo.getIdPrefix());
    assertRelatedTo(extraInfo, EntityTypes.TEZ_DAG_ID, tezDAGID.toString());

    assertEquals(USER, dag.getInfo().get(ATSConstants.USER));
    assertEquals(dagPlan.getName(), dag.getInfo().get(ATSConstants.DAG_NAME));
    assertEquals(applicationId.toString(), dag.getInfo().get(ATSConstants.APPLICATION_ID));
    assertEquals("Caller_ID", dag.getInfo().get(ATSConstants.CALLER_CONTEXT_ID));
  }

  @Test(timeout = 5000)
  public void testDagFinishedSplitsExtraInfo() {
    TezCounters counters = new TezCounters();
    counters.getGroup("group1").findCounter("counter1", true).setValue(7);
    List<TimelineEntity> entities = conversion.convertToTimelineEntities(
        new DAGFinishedEvent(tezDAGID, 1L, 2L, DAGState.SUCCEEDED, null, counters, USER,
            dagPlan.getName(), null, applicationAttemptId, dagPlan));

    Map<String, TimelineEntity> byType = byType(entities);
    TimelineEntity dag = byType.get(EntityTypes.TEZ_DAG_ID.name());
    TimelineEntity extraInfo = byType.get(EntityTypes.TEZ_DAG_EXTRA_INFO.name());
    assertNotNull(dag);
    assertNotNull(extraInfo);

    assertEquals(DAGState.SUCCEEDED.name(), dag.getInfo().get(ATSConstants.STATUS));
    assertNull(dag.getInfo().get(ATSConstants.COUNTERS));
    assertNotNull(extraInfo.getInfo().get(ATSConstants.COUNTERS));
    assertFalse(dag.getMetrics().isEmpty());
  }

  /**
   * The v1 wire-format keys describe the ATSv1 JSON envelope and must never end up as field names.
   */
  @Test(timeout = 5000)
  public void testNoV1WireKeysLeak() {
    Set<String> forbidden = new HashSet<>();
    forbidden.add(ATSConstants.PRIMARY_FILTERS);
    forbidden.add(ATSConstants.OTHER_INFO);
    forbidden.add(ATSConstants.RELATED_ENTITIES);
    forbidden.add(ATSConstants.ENTITY);
    forbidden.add(ATSConstants.ENTITY_TYPE);

    for (HistoryEventType eventType : HistoryEventType.values()) {
      HistoryEvent event = newEvent(eventType);
      if (event == null || !event.isHistoryEvent()) {
        continue;
      }
      for (TimelineEntity entity : conversion.convertToTimelineEntities(event)) {
        for (String key : entity.getInfo().keySet()) {
          assertFalse(eventType + " leaks the v1 wire key " + key, forbidden.contains(key));
        }
        for (String key : entity.getConfigs().keySet()) {
          assertFalse(eventType + " leaks the v1 wire key " + key, forbidden.contains(key));
        }
      }
    }
  }

  private TimelineEntity single(HistoryEvent event) {
    List<TimelineEntity> entities = conversion.convertToTimelineEntities(event);
    assertEquals("Expected exactly one entity for " + event.getEventType(), 1, entities.size());
    return entities.get(0);
  }

  private TimelineEntity first(HistoryEvent event) {
    return conversion.convertToTimelineEntities(event).get(0);
  }

  private static Map<String, TimelineEntity> byType(List<TimelineEntity> entities) {
    Map<String, TimelineEntity> byType = new HashMap<>();
    for (TimelineEntity entity : entities) {
      byType.put(entity.getType(), entity);
    }
    return byType;
  }

  private static void assertRelatedTo(TimelineEntity entity, EntityTypes type, String id) {
    Set<String> related = entity.getIsRelatedToEntities().get(type.name());
    assertNotNull("No " + type + " link on " + entity.getType(), related);
    assertTrue("Expected " + id + " in the " + type + " links of " + entity.getType(),
        related.contains(id));
  }

  private HistoryEvent newEvent(HistoryEventType eventType) {
    switch (eventType) {
      case APP_LAUNCHED:
        return new AppLaunchedEvent(applicationId, 1L, 2L, USER, new Configuration(false), null);
      case AM_LAUNCHED:
        return new AMLaunchedEvent(applicationAttemptId, 1L, 2L, USER);
      case AM_STARTED:
        return new AMStartedEvent(applicationAttemptId, 1L, USER);
      case DAG_SUBMITTED:
        return new DAGSubmittedEvent(tezDAGID, 1L, dagPlan, applicationAttemptId, null, USER, null,
            CONTAINER_LOGS, null);
      case DAG_INITIALIZED:
        return new DAGInitializedEvent(tezDAGID, 1L, USER, dagPlan.getName(),
            new HashMap<String, TezVertexID>());
      case DAG_STARTED:
        return new DAGStartedEvent(tezDAGID, 1L, USER, dagPlan.getName());
      case DAG_FINISHED:
        return new DAGFinishedEvent(tezDAGID, 1L, 2L, DAGState.SUCCEEDED, null, new TezCounters(),
            USER, dagPlan.getName(), new HashMap<String, Integer>(), applicationAttemptId, dagPlan);
      case DAG_RECOVERED:
        return new DAGRecoveredEvent(applicationAttemptId, tezDAGID, dagPlan.getName(), USER, 1L,
            CONTAINER_LOGS);
      case VERTEX_INITIALIZED:
        return new VertexInitializedEvent(tezVertexID, "v1", 1L, 2L, 1, "proc", null, null, null);
      case VERTEX_STARTED:
        return new VertexStartedEvent(tezVertexID, 1L, 2L);
      case VERTEX_CONFIGURE_DONE:
        return new VertexConfigurationDoneEvent(tezVertexID, 1L, 1, null, null, null, true);
      case VERTEX_FINISHED:
        return new VertexFinishedEvent(tezVertexID, "v1", 1, 1L, 2L, 3L, 4L, 5L,
            VertexState.SUCCEEDED, null, new TezCounters(), null, new HashMap<String, Integer>(), null);
      case TASK_STARTED:
        return new TaskStartedEvent(tezTaskID, "v1", 1L, 2L);
      case TASK_FINISHED:
        return new TaskFinishedEvent(tezTaskID, "v1", 1L, 2L, tezTaskAttemptID,
            TaskState.SUCCEEDED, null, new TezCounters(), 0);
      case TASK_ATTEMPT_STARTED:
        return new TaskAttemptStartedEvent(tezTaskAttemptID, "v1", 1L, containerId, nodeId, null,
            null, "nodeHttpAddress");
      case TASK_ATTEMPT_FINISHED:
        return new TaskAttemptFinishedEvent(tezTaskAttemptID, "v1", 1L, 2L,
            TaskAttemptState.FAILED, TaskFailureType.NON_FATAL,
            TaskAttemptTerminationCause.OUTPUT_LOST, null, new TezCounters(), null, null, 0, null,
            0, containerId, nodeId, null, null, "nodeHttpAddress");
      case CONTAINER_LAUNCHED:
        return new ContainerLaunchedEvent(containerId, 1L, applicationAttemptId);
      case CONTAINER_STOPPED:
        return new ContainerStoppedEvent(containerId, 1L, -1, applicationAttemptId);
      case DAG_COMMIT_STARTED:
        return new DAGCommitStartedEvent();
      case VERTEX_COMMIT_STARTED:
        return new VertexCommitStartedEvent();
      case VERTEX_GROUP_COMMIT_STARTED:
        return new VertexGroupCommitStartedEvent();
      case VERTEX_GROUP_COMMIT_FINISHED:
        return new VertexGroupCommitFinishedEvent();
      case DAG_KILL_REQUEST:
        return new DAGKillRequestEvent();
      default:
        fail("Unhandled event type " + eventType);
        return null;
    }
  }
}
