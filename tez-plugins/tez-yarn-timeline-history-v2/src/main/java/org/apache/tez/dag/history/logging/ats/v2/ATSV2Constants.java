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

import org.apache.hadoop.classification.InterfaceAudience.Private;

/**
 * Constants specific to the timeline v2 entity model. Field names shared with ATSv1 stay in
 * {@link org.apache.tez.common.ATSConstants}.
 */
@Private
public final class ATSV2Constants {

  /**
   * Separates the counter group from the counter name in a metric id. Matches what MapReduce
   * publishes. Counter group names are class names and never contain a colon; counter names are
   * not escaped.
   */
  public static final String COUNTER_METRIC_SEPARATOR = ":";

  /** Prefix for entity ids derived from a YARN id rather than a Tez id. */
  public static final String TEZ_ENTITY_ID_PREFIX = "tez_";

  private ATSV2Constants() {
  }
}
