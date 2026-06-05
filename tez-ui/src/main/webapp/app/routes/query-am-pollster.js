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

import SingleAmPollsterRoute from './single-am-pollster';

function isAMCapable(record) {
  return record && record.get("needs.am") && record.get("appID");
}

function isIncomplete(record) {
  return record && record.get("isComplete") === false;
}

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

export default SingleAmPollsterRoute.extend({

  getAssociatedDAGs: function (record) {
    return toArray(record && record.get("dag"));
  },

  getAMRecords: function (record) {
    if(isAMCapable(record)) {
      return [record];
    }

    return this.getAssociatedDAGs(record).filter(function (dag) {
      return isAMCapable(dag) && dag.get("isComplete") === false;
    });
  },

  areAllDAGsComplete: function (dags) {
    dags = dags || [];

    return !!dags.length && dags.every(function (dag) {
      return dag.get("isComplete");
    });
  },

  onRecordPoll: function (record) {
    var that = this,
        query = {},
        amRecords,
        countersToPoll = this.get("countersToPoll");

    if(countersToPoll !== null) {
      query.counters = countersToPoll;
    }

    amRecords = this.getAMRecords(record);
    if(amRecords.length) {
      return Ember.RSVP.all(amRecords.map(function (amRecord) {
        return that.get("loader").loadNeed(amRecord, "am", {reload: true}, query);
      })).then(function (records) {
        return records[0] || record;
      });
    }

    if(isIncomplete(record) && record.get("needs.dag")) {
      this.scheduleReload();
    }

    return Ember.RSVP.resolve(record);
  },

  onPollSuccess: function (records) {
    var record = this.get("polledRecords.0"),
        dags = this.getAssociatedDAGs(record);

    if(!isAMCapable(record) && isIncomplete(record) && this.areAllDAGsComplete(dags)) {
      this.scheduleReload();
    }

    return records;
  },

  reloadDAGRecords: function (dags) {
    var loader = this.get("loader"),
        reloads = [];

    dags.forEach(function (dag) {
      var entityID = dag && dag.get("entityID");

      if(entityID && entityID.indexOf("dag_") === 0) {
        reloads.push(loader.queryRecord("dag", entityID, {reload: true}).then(function (reloadedDag) {
          return reloadedDag;
        }, function () {
          return dag;
        }));
      }
      else {
        reloads.push(Ember.RSVP.resolve(dag));
      }
    });

    return Ember.RSVP.all(reloads);
  },

  checkAppStatus: function (record, error) {
    var that = this;

    return this.get("loader").queryRecord("appRm", record.get("appID"), {reload: true}).then(function (appRm) {
      if(appRm.get('isComplete')) {
        that.scheduleReload();
      }
      else {
        that.send("error", error);
      }
    }, function (error) {
      that.send("error", error);
      that.scheduleReload();
    });
  },

  onPollFailure: function (error) {
    var that = this,
        record = this.get("polledRecords.0"),
        dags;

    if(isAMCapable(record)) {
      return this.checkAppStatus(record, error);
    }

    dags = this.getAssociatedDAGs(record);
    if(!dags.length) {
      if(isIncomplete(record) && record.get("needs.dag")) {
        this.scheduleReload();
      }
      return;
    }

    return this.reloadDAGRecords(dags).then(function (reloadedDags) {
      if(isIncomplete(record) && that.areAllDAGsComplete(reloadedDags)) {
        that.scheduleReload();
      }
      else {
        return reloadedDags;
      }
    }, function () {
      return;
    });
  },

});
