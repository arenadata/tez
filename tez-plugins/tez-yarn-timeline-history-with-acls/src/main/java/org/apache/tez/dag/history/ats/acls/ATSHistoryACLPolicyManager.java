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

package org.apache.tez.dag.history.ats.acls;

import java.io.IOException;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.exceptions.YarnException;

public class ATSHistoryACLPolicyManager extends ATSHistoryACLPolicyManagerBase {

  private static final String ATS_HISTORY_LOGGING_SERVICE_CLASS_NAME =
      "org.apache.tez.dag.history.logging.ats.ATSHistoryLoggingService";

  @Override
  protected String getHistoryLoggingServiceClassName() {
    return ATS_HISTORY_LOGGING_SERVICE_CLASS_NAME;
  }

  @Override
  protected void putDomain(ApplicationId applicationId, TimelineDomain timelineDomain)
      throws IOException, YarnException {
    timelineClient.putDomain(timelineDomain);
  }
}
