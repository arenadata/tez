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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.util.TimelineServiceHelper;
import org.apache.tez.common.ATSConstants;
import org.apache.tez.common.counters.CounterGroup;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.api.records.DAGProtos.CallerContextProto;
import org.apache.tez.dag.app.web.AMWebController;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.events.AMLaunchedEvent;
import org.apache.tez.dag.history.events.AMStartedEvent;
import org.apache.tez.dag.history.events.AppLaunchedEvent;
import org.apache.tez.dag.history.events.ContainerLaunchedEvent;
import org.apache.tez.dag.history.events.ContainerStoppedEvent;
import org.apache.tez.dag.history.events.DAGFinishedEvent;
import org.apache.tez.dag.history.events.DAGInitializedEvent;
import org.apache.tez.dag.history.events.DAGRecoveredEvent;
import org.apache.tez.dag.history.events.DAGStartedEvent;
import org.apache.tez.dag.history.events.DAGSubmittedEvent;
import org.apache.tez.dag.history.events.TaskAttemptFinishedEvent;
import org.apache.tez.dag.history.events.TaskAttemptStartedEvent;
import org.apache.tez.dag.history.events.TaskFinishedEvent;
import org.apache.tez.dag.history.events.TaskStartedEvent;
import org.apache.tez.dag.history.events.VertexConfigurationDoneEvent;
import org.apache.tez.dag.history.events.VertexFinishedEvent;
import org.apache.tez.dag.history.events.VertexInitializedEvent;
import org.apache.tez.dag.history.events.VertexStartedEvent;
import org.apache.tez.dag.history.logging.EntityTypes;
import org.apache.tez.dag.history.utils.DAGUtils;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;

/**
 * Converts Tez history events into YARN Timeline Service v2 entities.
 *
 * <p>The v2 entity model has neither domains nor primary filters, so everything ATSv1 indexed as a
 * primary filter becomes a plain {@code info} key here, queryable through {@code infofilters}.
 * Parent links move to {@code isRelatedTo} and, unlike v1, are repeated on every event of an
 * entity: v2 cannot join two hops at read time, so tasks and attempts carry their whole ancestor
 * chain.
 *
 * <p>{@code idPrefix} orders sibling entities within a type and, per the timeline v2 contract,
 * must be identical across every update of one entity - a differing prefix writes a second row.
 * All assignments therefore go through {@link #idPrefixFor} overloads. Attempts, containers, DAGs
 * and task attempts are ordered newest first; vertices and tasks ascend, the order they are
 * rendered in.
 */
@Private
public class HistoryEventTimelineV2Conversion {

  private final int configPublishSizeBytes;
  private final boolean countersAsMetrics;

  /**
   * @param configPublishSizeBytes maximum bytes of configuration carried by one entity; a larger
   *        configuration is split across several entities sharing the same type, id and idPrefix
   * @param countersAsMetrics whether to publish counters as timeline metrics
   */
  public HistoryEventTimelineV2Conversion(int configPublishSizeBytes, boolean countersAsMetrics) {
    this.configPublishSizeBytes = configPublishSizeBytes;
    this.countersAsMetrics = countersAsMetrics;
  }

  public List<TimelineEntity> convertToTimelineEntities(HistoryEvent historyEvent) {
    validateEvent(historyEvent);
    switch (historyEvent.getEventType()) {
      case APP_LAUNCHED:
        return convertAppLaunchedEvent((AppLaunchedEvent) historyEvent);
      case AM_LAUNCHED:
        return Collections.singletonList(convertAMLaunchedEvent((AMLaunchedEvent) historyEvent));
      case AM_STARTED:
        return Collections.singletonList(convertAMStartedEvent((AMStartedEvent) historyEvent));
      case CONTAINER_LAUNCHED:
        return Collections.singletonList(
            convertContainerLaunchedEvent((ContainerLaunchedEvent) historyEvent));
      case CONTAINER_STOPPED:
        return Collections.singletonList(
            convertContainerStoppedEvent((ContainerStoppedEvent) historyEvent));
      case DAG_SUBMITTED:
        return convertDAGSubmittedEvent((DAGSubmittedEvent) historyEvent);
      case DAG_INITIALIZED:
        return Collections.singletonList(
            convertDAGInitializedEvent((DAGInitializedEvent) historyEvent));
      case DAG_STARTED:
        return Collections.singletonList(convertDAGStartedEvent((DAGStartedEvent) historyEvent));
      case DAG_FINISHED:
        return convertDAGFinishedEvent((DAGFinishedEvent) historyEvent);
      case VERTEX_INITIALIZED:
        return Collections.singletonList(
            convertVertexInitializedEvent((VertexInitializedEvent) historyEvent));
      case VERTEX_STARTED:
        return Collections.singletonList(
            convertVertexStartedEvent((VertexStartedEvent) historyEvent));
      case VERTEX_FINISHED:
        return Collections.singletonList(
            convertVertexFinishedEvent((VertexFinishedEvent) historyEvent));
      case TASK_STARTED:
        return Collections.singletonList(convertTaskStartedEvent((TaskStartedEvent) historyEvent));
      case TASK_FINISHED:
        return Collections.singletonList(
            convertTaskFinishedEvent((TaskFinishedEvent) historyEvent));
      case TASK_ATTEMPT_STARTED:
        return Collections.singletonList(
            convertTaskAttemptStartedEvent((TaskAttemptStartedEvent) historyEvent));
      case TASK_ATTEMPT_FINISHED:
        return Collections.singletonList(
            convertTaskAttemptFinishedEvent((TaskAttemptFinishedEvent) historyEvent));
      case VERTEX_CONFIGURE_DONE:
        return Collections.singletonList(
            convertVertexReconfigureDoneEvent((VertexConfigurationDoneEvent) historyEvent));
      case DAG_RECOVERED:
        return Collections.singletonList(
            convertDAGRecoveredEvent((DAGRecoveredEvent) historyEvent));
      case VERTEX_COMMIT_STARTED:
      case VERTEX_GROUP_COMMIT_STARTED:
      case VERTEX_GROUP_COMMIT_FINISHED:
      case DAG_COMMIT_STARTED:
      case DAG_KILL_REQUEST:
        throw new UnsupportedOperationException("Invalid Event, does not support history"
            + ", eventType=" + historyEvent.getEventType());
        // Do not add default, if a new event type is added, we'll get a warning for the switch.
    }
    throw new UnsupportedOperationException("Unhandled Event, eventType="
        + historyEvent.getEventType());
  }

  private static void validateEvent(HistoryEvent event) {
    if (!event.isHistoryEvent()) {
      throw new UnsupportedOperationException("Invalid Event, does not support history"
          + ", eventType=" + event.getEventType());
    }
  }

  private List<TimelineEntity> convertAppLaunchedEvent(AppLaunchedEvent event) {
    String entityId = ATSV2Constants.TEZ_ENTITY_ID_PREFIX + event.getApplicationId();
    TimelineEntity entity = newEntity(EntityTypes.TEZ_APPLICATION, entityId,
        TimelineEntity.DEFAULT_ENTITY_PREFIX);
    entity.setCreatedTime(event.getLaunchTime());

    entity.addInfo(ATSConstants.APPLICATION_ID, event.getApplicationId().toString());
    entity.addInfo(ATSConstants.USER, event.getUser());
    if (event.getVersion() != null) {
      entity.addInfo(ATSConstants.TEZ_VERSION,
          DAGUtils.convertTezVersionToATSMap(event.getVersion()));
    }
    entity.addInfo(ATSConstants.DAG_AM_WEB_SERVICE_VERSION, AMWebController.VERSION);

    return withConfiguration(entity, event.getConf());
  }

  private TimelineEntity convertAMLaunchedEvent(AMLaunchedEvent event) {
    TimelineEntity entity = newApplicationAttemptEntity(event.getApplicationAttemptId());
    entity.setCreatedTime(event.getLaunchTime());
    entity.addEvent(newEvent(HistoryEventType.AM_LAUNCHED, event.getLaunchTime()));

    entity.addInfo(ATSConstants.APP_SUBMIT_TIME, event.getAppSubmitTime());
    entity.addInfo(ATSConstants.APPLICATION_ID,
        event.getApplicationAttemptId().getApplicationId().toString());
    entity.addInfo(ATSConstants.APPLICATION_ATTEMPT_ID,
        event.getApplicationAttemptId().toString());
    entity.addInfo(ATSConstants.USER, event.getUser());

    return entity;
  }

  private TimelineEntity convertAMStartedEvent(AMStartedEvent event) {
    TimelineEntity entity = newApplicationAttemptEntity(event.getApplicationAttemptId());
    entity.addEvent(newEvent(HistoryEventType.AM_STARTED, event.getStartTime()));

    entity.addInfo(ATSConstants.APPLICATION_ID,
        event.getApplicationAttemptId().getApplicationId().toString());
    entity.addInfo(ATSConstants.USER, event.getUser());

    return entity;
  }

  private TimelineEntity convertContainerLaunchedEvent(ContainerLaunchedEvent event) {
    TimelineEntity entity = newContainerEntity(event.getContainerId(),
        event.getApplicationAttemptId());
    entity.setCreatedTime(event.getLaunchTime());
    entity.addEvent(newEvent(HistoryEventType.CONTAINER_LAUNCHED, event.getLaunchTime()));

    entity.addInfo(ATSConstants.APPLICATION_ID,
        event.getApplicationAttemptId().getApplicationId().toString());
    entity.addInfo(ATSConstants.CONTAINER_ID, event.getContainerId().toString());

    return entity;
  }

  private TimelineEntity convertContainerStoppedEvent(ContainerStoppedEvent event) {
    // a container may be stopped by a different attempt than the one that launched it
    TimelineEntity entity = newContainerEntity(event.getContainerId(),
        event.getApplicationAttemptId());
    entity.addEvent(newEvent(HistoryEventType.CONTAINER_STOPPED, event.getStoppedTime()));

    entity.addInfo(ATSConstants.APPLICATION_ID,
        event.getApplicationAttemptId().getApplicationId().toString());
    entity.addInfo(ATSConstants.EXIT_STATUS, event.getExitStatus());
    entity.addInfo(ATSConstants.FINISH_TIME, event.getStoppedTime());

    return entity;
  }

  private TimelineEntity convertDAGRecoveredEvent(DAGRecoveredEvent event) {
    TimelineEntity entity = newDagEntity(event.getDagID(),
        event.getApplicationAttemptId().getApplicationId().toString());

    TimelineEvent recoverEvt = newEvent(HistoryEventType.DAG_RECOVERED, event.getRecoveredTime());
    recoverEvt.addInfo(ATSConstants.APPLICATION_ATTEMPT_ID,
        event.getApplicationAttemptId().toString());
    if (event.getRecoveredDagState() != null) {
      recoverEvt.addInfo(ATSConstants.DAG_STATE, event.getRecoveredDagState().name());
    }
    if (event.getRecoveryFailureReason() != null) {
      recoverEvt.addInfo(ATSConstants.RECOVERY_FAILURE_REASON, event.getRecoveryFailureReason());
    }
    entity.addEvent(recoverEvt);

    entity.addInfo(ATSConstants.USER, event.getUser());
    entity.addInfo(ATSConstants.DAG_NAME, event.getDagName());
    entity.addInfo(ATSConstants.IN_PROGRESS_LOGS_URL + "_"
        + event.getApplicationAttemptId().getAttemptId(), event.getContainerLogs());

    return entity;
  }

  private List<TimelineEntity> convertDAGSubmittedEvent(DAGSubmittedEvent event) {
    TimelineEntity entity = newDagEntity(event.getDAGID(), event.getApplicationId().toString());
    entity.addIsRelatedToEntity(EntityTypes.TEZ_APPLICATION_ATTEMPT.name(),
        ATSV2Constants.TEZ_ENTITY_ID_PREFIX + event.getApplicationAttemptId());
    entity.setCreatedTime(event.getSubmitTime());
    entity.addEvent(newEvent(HistoryEventType.DAG_SUBMITTED, event.getSubmitTime()));

    entity.addInfo(ATSConstants.USER, event.getUser());
    entity.addInfo(ATSConstants.DAG_NAME, event.getDAGName());
    entity.addInfo(ATSConstants.APPLICATION_ATTEMPT_ID,
        event.getApplicationAttemptId().toString());
    entity.addInfo(ATSConstants.DAG_AM_WEB_SERVICE_VERSION, AMWebController.VERSION);
    entity.addInfo(ATSConstants.IN_PROGRESS_LOGS_URL + "_"
        + event.getApplicationAttemptId().getAttemptId(), event.getContainerLogs());
    if (event.getDAGPlan().hasCallerContext()
        && event.getDAGPlan().getCallerContext().hasCallerId()) {
      CallerContextProto callerContext = event.getDAGPlan().getCallerContext();
      entity.addInfo(ATSConstants.CALLER_CONTEXT_ID, callerContext.getCallerId());
      entity.addInfo(ATSConstants.CALLER_CONTEXT, callerContext.getContext());
      if (callerContext.hasCallerType()) {
        entity.addInfo(ATSConstants.CALLER_CONTEXT_TYPE, callerContext.getCallerType());
      }
    }
    if (event.getQueueName() != null) {
      entity.addInfo(ATSConstants.DAG_QUEUE_NAME, event.getQueueName());
    }

    List<TimelineEntity> entities = withConfiguration(entity, event.getConf());
    entities.add(convertDAGSubmittedToDAGExtraInfoEntity(event));
    return entities;
  }

  private TimelineEntity convertDAGSubmittedToDAGExtraInfoEntity(DAGSubmittedEvent event) {
    TimelineEntity entity = newDagExtraInfoEntity(event.getDAGID());
    entity.setCreatedTime(event.getSubmitTime());
    entity.addEvent(newEvent(HistoryEventType.DAG_SUBMITTED, event.getSubmitTime()));

    try {
      entity.addInfo(ATSConstants.DAG_PLAN, DAGUtils.convertDAGPlanToATSMap(event.getDAGPlan()));
    } catch (IOException e) {
      throw new TezUncheckedException(e);
    }
    return entity;
  }

  private TimelineEntity convertDAGInitializedEvent(DAGInitializedEvent event) {
    TimelineEntity entity = newDagEntity(event.getDAGID(), event.getApplicationId().toString());
    entity.addEvent(newEvent(HistoryEventType.DAG_INITIALIZED, event.getInitTime()));

    entity.addInfo(ATSConstants.USER, event.getUser());
    entity.addInfo(ATSConstants.DAG_NAME, event.getDagName());
    entity.addInfo(ATSConstants.INIT_TIME, event.getInitTime());

    if (event.getVertexNameIDMap() != null) {
      Map<String, String> nameIdStrMap = new TreeMap<>();
      for (Entry<String, TezVertexID> entry : event.getVertexNameIDMap().entrySet()) {
        nameIdStrMap.put(entry.getKey(), entry.getValue().toString());
      }
      entity.addInfo(ATSConstants.VERTEX_NAME_ID_MAPPING, nameIdStrMap);
    }

    return entity;
  }

  private TimelineEntity convertDAGStartedEvent(DAGStartedEvent event) {
    TimelineEntity entity = newDagEntity(event.getDAGID(), event.getApplicationId().toString());
    entity.addEvent(newEvent(HistoryEventType.DAG_STARTED, event.getStartTime()));

    entity.addInfo(ATSConstants.USER, event.getUser());
    entity.addInfo(ATSConstants.DAG_NAME, event.getDagName());
    entity.addInfo(ATSConstants.START_TIME, event.getStartTime());
    entity.addInfo(ATSConstants.STATUS, event.getDagState().toString());

    return entity;
  }

  private List<TimelineEntity> convertDAGFinishedEvent(DAGFinishedEvent event) {
    TimelineEntity entity = newDagEntity(event.getDAGID(), event.getApplicationId().toString());
    entity.addEvent(newEvent(HistoryEventType.DAG_FINISHED, event.getFinishTime()));

    entity.addInfo(ATSConstants.USER, event.getUser());
    entity.addInfo(ATSConstants.DAG_NAME, event.getDagName());
    entity.addInfo(ATSConstants.START_TIME, event.getStartTime());
    entity.addInfo(ATSConstants.FINISH_TIME, event.getFinishTime());
    entity.addInfo(ATSConstants.TIME_TAKEN, event.getFinishTime() - event.getStartTime());
    entity.addInfo(ATSConstants.STATUS, event.getState().name());
    entity.addInfo(ATSConstants.DIAGNOSTICS, event.getDiagnostics());
    entity.addInfo(ATSConstants.COMPLETION_APPLICATION_ATTEMPT_ID,
        event.getApplicationAttemptId().toString());
    if (event.getDAGPlan().hasCallerContext()
        && event.getDAGPlan().getCallerContext().hasCallerId()) {
      entity.addInfo(ATSConstants.CALLER_CONTEXT_ID,
          event.getDAGPlan().getCallerContext().getCallerId());
    }
    addTaskStats(entity, event.getDagTaskStats());
    addCounters(entity, event.getTezCounters(), event.getFinishTime());

    TimelineEntity extraInfo = newDagExtraInfoEntity(event.getDAGID());
    extraInfo.addEvent(newEvent(HistoryEventType.DAG_FINISHED, event.getFinishTime()));
    extraInfo.addInfo(ATSConstants.COUNTERS,
        DAGUtils.convertCountersToATSMap(event.getTezCounters()));

    List<TimelineEntity> entities = new ArrayList<>(2);
    entities.add(entity);
    entities.add(extraInfo);
    return entities;
  }

  private TimelineEntity convertVertexInitializedEvent(VertexInitializedEvent event) {
    TimelineEntity entity = newVertexEntity(event.getVertexID(),
        event.getApplicationId().toString());
    entity.setCreatedTime(event.getInitedTime());
    entity.addEvent(newEvent(HistoryEventType.VERTEX_INITIALIZED, event.getInitedTime()));

    entity.addInfo(ATSConstants.VERTEX_NAME, event.getVertexName());
    entity.addInfo(ATSConstants.INIT_REQUESTED_TIME, event.getInitRequestedTime());
    entity.addInfo(ATSConstants.INIT_TIME, event.getInitedTime());
    entity.addInfo(ATSConstants.NUM_TASKS, event.getNumTasks());
    entity.addInfo(ATSConstants.PROCESSOR_CLASS_NAME, event.getProcessorName());
    if (event.getServicePluginInfo() != null) {
      entity.addInfo(ATSConstants.SERVICE_PLUGIN,
          DAGUtils.convertServicePluginToATSMap(event.getServicePluginInfo()));
    }

    return entity;
  }

  private TimelineEntity convertVertexStartedEvent(VertexStartedEvent event) {
    TimelineEntity entity = newVertexEntity(event.getVertexID(),
        event.getApplicationId().toString());
    entity.addEvent(newEvent(HistoryEventType.VERTEX_STARTED, event.getStartTime()));

    entity.addInfo(ATSConstants.START_REQUESTED_TIME, event.getStartRequestedTime());
    entity.addInfo(ATSConstants.START_TIME, event.getStartTime());
    entity.addInfo(ATSConstants.STATUS, event.getVertexState().toString());

    return entity;
  }

  private TimelineEntity convertVertexReconfigureDoneEvent(VertexConfigurationDoneEvent event) {
    TimelineEntity entity = newVertexEntity(event.getVertexID(),
        event.getApplicationId().toString());

    TimelineEvent updateEvt =
        newEvent(HistoryEventType.VERTEX_CONFIGURE_DONE, event.getReconfigureDoneTime());
    Map<String, Object> eventInfo = new HashMap<>();
    if (event.getSourceEdgeProperties() != null && !event.getSourceEdgeProperties().isEmpty()) {
      Map<String, Object> updatedEdgeManagers = new HashMap<>();
      for (Entry<String, EdgeProperty> entry : event.getSourceEdgeProperties().entrySet()) {
        updatedEdgeManagers.put(entry.getKey(), DAGUtils.convertEdgeProperty(entry.getValue()));
      }
      eventInfo.put(ATSConstants.UPDATED_EDGE_MANAGERS, updatedEdgeManagers);
    }
    eventInfo.put(ATSConstants.NUM_TASKS, event.getNumTasks());
    updateEvt.setInfo(eventInfo);
    entity.addEvent(updateEvt);

    entity.addInfo(ATSConstants.NUM_TASKS, event.getNumTasks());

    return entity;
  }

  private TimelineEntity convertVertexFinishedEvent(VertexFinishedEvent event) {
    TimelineEntity entity = newVertexEntity(event.getVertexID(),
        event.getApplicationId().toString());
    entity.addEvent(newEvent(HistoryEventType.VERTEX_FINISHED, event.getFinishTime()));

    entity.addInfo(ATSConstants.VERTEX_NAME, event.getVertexName());
    entity.addInfo(ATSConstants.FINISH_TIME, event.getFinishTime());
    entity.addInfo(ATSConstants.TIME_TAKEN, event.getFinishTime() - event.getStartTime());
    entity.addInfo(ATSConstants.STATUS, event.getState().name());
    entity.addInfo(ATSConstants.DIAGNOSTICS, event.getDiagnostics());
    entity.addInfo(ATSConstants.COUNTERS,
        DAGUtils.convertCountersToATSMap(event.getTezCounters()));
    entity.addInfo(ATSConstants.STATS,
        DAGUtils.convertVertexStatsToATSMap(event.getVertexStats()));
    if (event.getServicePluginInfo() != null) {
      entity.addInfo(ATSConstants.SERVICE_PLUGIN,
          DAGUtils.convertServicePluginToATSMap(event.getServicePluginInfo()));
    }
    addTaskStats(entity, event.getVertexTaskStats());
    addCounters(entity, event.getTezCounters(), event.getFinishTime());

    return entity;
  }

  private TimelineEntity convertTaskStartedEvent(TaskStartedEvent event) {
    TimelineEntity entity = newTaskEntity(event.getTaskID(), event.getApplicationId().toString());
    entity.setCreatedTime(event.getStartTime());
    entity.addEvent(newEvent(HistoryEventType.TASK_STARTED, event.getStartTime()));

    entity.addInfo(ATSConstants.START_TIME, event.getStartTime());
    entity.addInfo(ATSConstants.SCHEDULED_TIME, event.getScheduledTime());
    entity.addInfo(ATSConstants.STATUS, event.getState().name());

    return entity;
  }

  private TimelineEntity convertTaskFinishedEvent(TaskFinishedEvent event) {
    TimelineEntity entity = newTaskEntity(event.getTaskID(), event.getApplicationId().toString());
    entity.addEvent(newEvent(HistoryEventType.TASK_FINISHED, event.getFinishTime()));

    entity.addInfo(ATSConstants.FINISH_TIME, event.getFinishTime());
    entity.addInfo(ATSConstants.TIME_TAKEN, event.getFinishTime() - event.getStartTime());
    entity.addInfo(ATSConstants.STATUS, event.getState().name());
    entity.addInfo(ATSConstants.NUM_FAILED_TASKS_ATTEMPTS, event.getNumFailedAttempts());
    if (event.getSuccessfulAttemptID() != null) {
      entity.addInfo(ATSConstants.SUCCESSFUL_ATTEMPT_ID,
          event.getSuccessfulAttemptID().toString());
    }
    entity.addInfo(ATSConstants.DIAGNOSTICS, event.getDiagnostics());
    entity.addInfo(ATSConstants.COUNTERS,
        DAGUtils.convertCountersToATSMap(event.getTezCounters()));
    addCounters(entity, event.getTezCounters(), event.getFinishTime());

    return entity;
  }

  private TimelineEntity convertTaskAttemptStartedEvent(TaskAttemptStartedEvent event) {
    TimelineEntity entity = newTaskAttemptEntity(event.getTaskAttemptID(),
        event.getApplicationId().toString());
    entity.setCreatedTime(event.getStartTime());
    entity.addEvent(newEvent(HistoryEventType.TASK_ATTEMPT_STARTED, event.getStartTime()));

    entity.addInfo(ATSConstants.START_TIME, event.getStartTime());
    if (event.getInProgressLogsUrl() != null) {
      entity.addInfo(ATSConstants.IN_PROGRESS_LOGS_URL, event.getInProgressLogsUrl());
    }
    if (event.getCompletedLogsUrl() != null) {
      entity.addInfo(ATSConstants.COMPLETED_LOGS_URL, event.getCompletedLogsUrl());
    }
    entity.addInfo(ATSConstants.NODE_ID, event.getNodeId().toString());
    entity.addInfo(ATSConstants.NODE_HTTP_ADDRESS, event.getNodeHttpAddress());
    entity.addInfo(ATSConstants.CONTAINER_ID, event.getContainerId().toString());
    entity.addInfo(ATSConstants.STATUS, TaskAttemptState.RUNNING.name());

    return entity;
  }

  private TimelineEntity convertTaskAttemptFinishedEvent(TaskAttemptFinishedEvent event) {
    TimelineEntity entity = newTaskAttemptEntity(event.getTaskAttemptID(),
        event.getApplicationId().toString());
    entity.addEvent(newEvent(HistoryEventType.TASK_ATTEMPT_FINISHED, event.getFinishTime()));

    if (event.getTaskFailureType() != null) {
      entity.addInfo(ATSConstants.TASK_FAILURE_TYPE, event.getTaskFailureType().name());
    }
    entity.addInfo(ATSConstants.CREATION_TIME, event.getCreationTime());
    entity.addInfo(ATSConstants.ALLOCATION_TIME, event.getAllocationTime());
    entity.addInfo(ATSConstants.START_TIME, event.getStartTime());
    entity.addInfo(ATSConstants.FINISH_TIME, event.getFinishTime());
    if (event.getCreationCausalTA() != null) {
      entity.addInfo(ATSConstants.CREATION_CAUSAL_ATTEMPT,
          event.getCreationCausalTA().toString());
    }
    entity.addInfo(ATSConstants.TIME_TAKEN, event.getFinishTime() - event.getStartTime());
    entity.addInfo(ATSConstants.STATUS, event.getState().name());
    if (event.getTaskAttemptError() != null) {
      entity.addInfo(ATSConstants.TASK_ATTEMPT_ERROR_ENUM, event.getTaskAttemptError().name());
    }
    entity.addInfo(ATSConstants.DIAGNOSTICS, event.getDiagnostics());
    entity.addInfo(ATSConstants.COUNTERS, DAGUtils.convertCountersToATSMap(event.getCounters()));
    if (event.getDataEvents() != null && !event.getDataEvents().isEmpty()) {
      entity.addInfo(ATSConstants.LAST_DATA_EVENTS,
          DAGUtils.convertDataEventDependecyInfoToATS(event.getDataEvents()));
    }
    if (event.getNodeId() != null) {
      entity.addInfo(ATSConstants.NODE_ID, event.getNodeId().toString());
    }
    if (event.getContainerId() != null) {
      entity.addInfo(ATSConstants.CONTAINER_ID, event.getContainerId().toString());
    }
    if (event.getInProgressLogsUrl() != null) {
      entity.addInfo(ATSConstants.IN_PROGRESS_LOGS_URL, event.getInProgressLogsUrl());
    }
    if (event.getCompletedLogsUrl() != null) {
      entity.addInfo(ATSConstants.COMPLETED_LOGS_URL, event.getCompletedLogsUrl());
    }
    if (event.getNodeHttpAddress() != null) {
      entity.addInfo(ATSConstants.NODE_HTTP_ADDRESS, event.getNodeHttpAddress());
    }
    addCounters(entity, event.getCounters(), event.getFinishTime());

    return entity;
  }

  private static TimelineEntity newEntity(EntityTypes type, String id, long idPrefix) {
    TimelineEntity entity = new TimelineEntity();
    entity.setType(type.name());
    entity.setId(id);
    entity.setIdPrefix(idPrefix);
    return entity;
  }

  private static TimelineEvent newEvent(HistoryEventType type, long timestamp) {
    TimelineEvent event = new TimelineEvent();
    event.setId(type.name());
    event.setTimestamp(timestamp);
    return event;
  }

  private static TimelineEntity newApplicationAttemptEntity(
      ApplicationAttemptId appAttemptId) {
    TimelineEntity entity = newEntity(EntityTypes.TEZ_APPLICATION_ATTEMPT,
        ATSV2Constants.TEZ_ENTITY_ID_PREFIX + appAttemptId,
        TimelineServiceHelper.invertLong(appAttemptId.getAttemptId()));
    entity.addIsRelatedToEntity(EntityTypes.TEZ_APPLICATION.name(),
        ATSV2Constants.TEZ_ENTITY_ID_PREFIX + appAttemptId.getApplicationId());
    return entity;
  }

  private static TimelineEntity newContainerEntity(
      ContainerId containerId,
      ApplicationAttemptId appAttemptId) {
    TimelineEntity entity = newEntity(EntityTypes.TEZ_CONTAINER_ID,
        ATSV2Constants.TEZ_ENTITY_ID_PREFIX + containerId,
        TimelineServiceHelper.invertLong(containerId.getContainerId()));
    entity.addIsRelatedToEntity(EntityTypes.TEZ_APPLICATION_ATTEMPT.name(),
        ATSV2Constants.TEZ_ENTITY_ID_PREFIX + appAttemptId);
    return entity;
  }

  private static TimelineEntity newDagEntity(TezDAGID dagId, String applicationId) {
    TimelineEntity entity = newEntity(EntityTypes.TEZ_DAG_ID, dagId.toString(), idPrefixFor(dagId));
    entity.addIsRelatedToEntity(EntityTypes.TEZ_APPLICATION.name(),
        ATSV2Constants.TEZ_ENTITY_ID_PREFIX + applicationId);
    entity.addInfo(ATSConstants.APPLICATION_ID, applicationId);
    return entity;
  }

  private static TimelineEntity newDagExtraInfoEntity(TezDAGID dagId) {
    TimelineEntity entity =
        newEntity(EntityTypes.TEZ_DAG_EXTRA_INFO, dagId.toString(), idPrefixFor(dagId));
    entity.addIsRelatedToEntity(EntityTypes.TEZ_DAG_ID.name(), dagId.toString());
    return entity;
  }

  private static TimelineEntity newVertexEntity(TezVertexID vertexId, String applicationId) {
    TimelineEntity entity =
        newEntity(EntityTypes.TEZ_VERTEX_ID, vertexId.toString(), vertexId.getId());
    entity.addIsRelatedToEntity(EntityTypes.TEZ_DAG_ID.name(), vertexId.getDAGID().toString());
    entity.addInfo(ATSConstants.APPLICATION_ID, applicationId);
    return entity;
  }

  private static TimelineEntity newTaskEntity(TezTaskID taskId, String applicationId) {
    TimelineEntity entity = newEntity(EntityTypes.TEZ_TASK_ID, taskId.toString(), taskId.getId());
    entity.addIsRelatedToEntity(EntityTypes.TEZ_DAG_ID.name(), taskId.getDAGID().toString());
    entity.addIsRelatedToEntity(EntityTypes.TEZ_VERTEX_ID.name(),
        taskId.getVertexID().toString());
    entity.addInfo(ATSConstants.APPLICATION_ID, applicationId);
    return entity;
  }

  private static TimelineEntity newTaskAttemptEntity(
      TezTaskAttemptID attemptId, String applicationId) {
    TimelineEntity entity = newEntity(EntityTypes.TEZ_TASK_ATTEMPT_ID, attemptId.toString(),
        TimelineServiceHelper.invertLong(attemptId.getId()));
    entity.addIsRelatedToEntity(EntityTypes.TEZ_DAG_ID.name(), attemptId.getDAGID().toString());
    entity.addIsRelatedToEntity(EntityTypes.TEZ_VERTEX_ID.name(),
        attemptId.getVertexID().toString());
    entity.addIsRelatedToEntity(EntityTypes.TEZ_TASK_ID.name(), attemptId.getTaskID().toString());
    entity.addInfo(ATSConstants.APPLICATION_ID, applicationId);
    return entity;
  }

  private static long idPrefixFor(TezDAGID dagId) {
    return TimelineServiceHelper.invertLong(dagId.getId());
  }

  private static void addTaskStats(TimelineEntity entity, Map<String, Integer> taskStats) {
    if (taskStats == null) {
      return;
    }
    for (Entry<String, Integer> entry : taskStats.entrySet()) {
      entity.addInfo(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Publishes counters as timeline metrics, which is what makes {@code metricfilters},
   * {@code metricstoretrieve} and flow-level aggregation usable. Unlike
   * {@link DAGUtils#convertCountersToATSMap}, zero-valued counters are kept: a zero is meaningful
   * once metrics are aggregated.
   */
  private void addCounters(TimelineEntity entity, TezCounters counters, long timestamp) {
    if (!countersAsMetrics || counters == null) {
      return;
    }
    for (CounterGroup group : counters) {
      for (TezCounter counter : group) {
        TimelineMetric metric = new TimelineMetric(TimelineMetric.Type.SINGLE_VALUE);
        metric.setId(group.getName() + ATSV2Constants.COUNTER_METRIC_SEPARATOR + counter.getName());
        metric.addValue(timestamp, counter.getValue());
        entity.addMetric(metric);
      }
    }
  }

  /**
   * Splits a configuration across the given entity and, when it does not fit, further entities
   * sharing its type, id and idPrefix. The server merges same-identity entities column-wise, so
   * the chunks reassemble into one row. There is no server-side entity size limit, which is why
   * the budget has to be applied here.
   */
  private List<TimelineEntity> withConfiguration(TimelineEntity entity, Configuration conf) {
    List<TimelineEntity> entities = new ArrayList<>();
    entities.add(entity);
    if (conf == null) {
      return entities;
    }

    TimelineEntity current = entity;
    int currentSize = 0;
    for (Entry<String, String> entry : DAGUtils.convertConfigurationToATSMap(conf).entrySet()) {
      int entrySize = entry.getKey().length()
          + (entry.getValue() == null ? 0 : entry.getValue().length());
      if (currentSize > 0 && currentSize + entrySize > configPublishSizeBytes) {
        current = newEntity(EntityTypes.valueOf(entity.getType()), entity.getId(),
            entity.getIdPrefix());
        entities.add(current);
        currentSize = 0;
      }
      current.addConfig(entry.getKey(), entry.getValue());
      currentSize += entrySize;
    }
    return entities;
  }
}
