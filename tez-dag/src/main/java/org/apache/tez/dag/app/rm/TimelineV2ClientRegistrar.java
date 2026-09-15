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

package org.apache.tez.dag.app.rm;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.client.api.TimelineV2Client;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * Implemented by task schedulers that own an AMRMClient, through which the timeline v2 collector
 * address is delivered. A timeline v2 client only learns where to write once the registered
 * AMRMClient receives an allocate response carrying collector info, so a history logging service
 * writing to ATSv2 has to hand its client to the scheduler.
 *
 * <p>This is kept out of the {@code TaskScheduler} plugin API on purpose: it is opt-in, so a
 * custom {@code tez.am.yarn.scheduler.class} can support ATSv2 without the API committing to it.
 */
@Private
public interface TimelineV2ClientRegistrar {

  /**
   * Registers a timeline v2 client with the underlying AMRMClient. May be called before or after
   * the scheduler is started; registration is a plain assignment and the heartbeat thread picks
   * the client up on the next allocate response.
   *
   * @param timelineClient the client to register
   * @throws YarnException if the AMRMClient rejects the registration
   */
  void registerTimelineV2Client(TimelineV2Client timelineClient) throws YarnException;
}
