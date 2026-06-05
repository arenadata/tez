/*global more*/
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

import Ember from 'ember';
import TableController from '../table';
import ColumnDefinition from '../../utils/column-definition';

var MoreObject = more.Object;

function toArray(records) {
  var result = [];

  if(!records) {
    return result;
  }

  if(records.forEach) {
    records.forEach(function (record) {
      if(record) {
        result.push(record);
      }
    });
  }
  else {
    result.push(records);
  }

  return result;
}

function getMergedIntervalsDuration(intervals) {
  var duration = 0,
      current;

  intervals = intervals.sort(function (left, right) {
    return left[0] - right[0];
  });

  intervals.forEach(function (interval) {
    if(!current || interval[0] > current[1]) {
      if(current) {
        duration += current[1] - current[0];
      }
      current = interval.slice();
    }
    else {
      current[1] = Math.max(current[1], interval[1]);
    }
  });

  if(current) {
    duration += current[1] - current[0];
  }

  return duration;
}

export default TableController.extend({

  columns: ColumnDefinition.make([{
    id: 'perfLogName',
    headerTitle: 'Raw Perf Log Name',
    contentPath: 'perfLogName',
  }, {
    id: 'perfLogValue',
    headerTitle: 'Value',
    contentPath: 'perfLogValue',
    cellDefinition: {
      type: 'duration'
    }
  }]),

  timelinePerf: Ember.computed(
    "model.perf",
    "model.status",
    "model.loadTime",
    "model.dag.[]",
    "model.dag.@each.status",
    "model.dag.@each.startTime",
    "model.dag.@each.endTime",
    "model.dag.@each.loadTime",
    function () {
      var perf = this.get("model.perf"),
          dags = toArray(this.get("model.dag")),
          intervals = [],
          hasRunningDAG = false,
          now = Date.now();

      if(perf) {
        return perf;
      }

      dags.forEach(function (dag) {
        var dagStartTime = dag.get("startTime"),
            dagEndTime = dag.get("endTime"),
            dagStatus = dag.get("status");

        if(dagStatus === "RUNNING") {
          hasRunningDAG = true;
          dagEndTime = dagEndTime || now;
        }

        if(dagStartTime && dagEndTime > dagStartTime) {
          intervals.push([dagStartTime, dagEndTime]);
        }
      });

      if(!intervals.length) {
        return null;
      }

      if(this.get("model.status") !== "RUNNING" && !hasRunningDAG) {
        return null;
      }

      return {
        TezRunDag: getMergedIntervalsDuration(intervals)
      };
    }
  ),

  hasTimeline: Ember.computed("timelinePerf", function () {
    return !!this.get("timelinePerf");
  }),

  rows: Ember.computed("timelinePerf", function () {
    var perf = this.get("timelinePerf"),
        rows = [];

    if(perf) {
      MoreObject.forEach(perf, function (key, value) {
        rows.push(Ember.Object.create({
          perfLogName: key,
          perfLogValue: value
        }));
      });
    }

    return Ember.A(rows);
  })

});
