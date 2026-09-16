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

package org.apache.tez.dag.history.logging.ats;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.dag.api.TezConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates the ATS v1.x {@link TimelineClient} used by the timeline history plugins.
 */
@Private
public final class TimelineClientFactory {

  private static final Logger LOG = LoggerFactory.getLogger(TimelineClientFactory.class);

  private TimelineClientFactory() {
  }

  /**
   * Creates and initializes a Timeline v1.x client, or returns null when the cluster is not
   * running a v1.x timeline service. A null return lets the caller degrade to a no-op instead of
   * failing: {@link TimelineClient#init(Configuration)} throws when the configured version is not
   * 1.x, which would otherwise take the whole AM down.
   *
   * @param conf the configuration to initialize the client with
   * @param requestingServiceClassName class name of the history logging service asking for the
   *        client; a warning is only logged when that service is the configured one
   * @return an initialized but not started client, or null
   */
  public static TimelineClient createTimelineClientIfV1Enabled(Configuration conf,
      String requestingServiceClassName) {
    if (YarnConfiguration.timelineServiceV1Enabled(conf)) {
      TimelineClient timelineClient = TimelineClient.createTimelineClient();
      timelineClient.init(conf);
      return timelineClient;
    }
    if (requestingServiceClassName.equals(
        conf.get(TezConfiguration.TEZ_HISTORY_LOGGING_SERVICE_CLASS, ""))) {
      if (YarnConfiguration.timelineServiceEnabled(conf)) {
        String versions = conf.get(YarnConfiguration.TIMELINE_SERVICE_VERSIONS);
        LOG.warn("{} is disabled because it requires Timeline Service v1.x, but the configured"
            + " version is {}", requestingServiceClassName,
            versions != null ? versions : YarnConfiguration.getTimelineServiceVersion(conf));
      } else {
        LOG.warn("{} is disabled due to Timeline Service being disabled, {} set to false",
            requestingServiceClassName, YarnConfiguration.TIMELINE_SERVICE_ENABLED);
      }
    }
    return null;
  }
}
