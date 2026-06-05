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
import { moduleFor, test } from 'ember-qunit';

moduleFor('route:query-am-pollster', 'Unit | Route | query am pollster', {
  // Specify the other units that are required for this test.
  // needs: ['controller:foo']
});

test('Basic creation test', function(assert) {
  let route = this.subject();

  assert.ok(route);
  assert.ok(route.onRecordPoll);
  assert.ok(route.onPollSuccess);
  assert.ok(route.getAssociatedDAGs);
  assert.ok(route.getAMRecords);
  assert.ok(route.reloadDAGRecords);
  assert.ok(route.checkAppStatus);
  assert.ok(route.onPollFailure);
});

test('getAMRecords returns all running associated DAG records', function(assert) {
  let route = this.subject(),
      runningDag1 = Ember.Object.create({
        needs: {
          am: {}
        },
        appID: "application_1_1",
        isComplete: false
      }),
      completedDag = Ember.Object.create({
        needs: {
          am: {}
        },
        appID: "application_1_2",
        isComplete: true
      }),
      runningDag2 = Ember.Object.create({
        needs: {
          am: {}
        },
        appID: "application_1_3",
        isComplete: false
      }),
      record = Ember.Object.create({
        dag: Ember.A([runningDag1, completedDag, runningDag2])
      }),
      amRecords = route.getAMRecords(record);

  assert.equal(amRecords.length, 2);
  assert.equal(amRecords[0], runningDag1);
  assert.equal(amRecords[1], runningDag2);
});

test('onRecordPoll loads AM data for all running associated DAGs', function(assert) {
  assert.expect(9);

  let runningDag1 = Ember.Object.create({
        needs: {
          am: {}
        },
        appID: "application_1_1",
        isComplete: false
      }),
      runningDag2 = Ember.Object.create({
        needs: {
          am: {}
        },
        appID: "application_1_2",
        isComplete: false
      }),
      record = Ember.Object.create({
        dag: Ember.A([runningDag1, runningDag2])
      }),
      route = this.subject({
        countersToPoll: "counterGroup/counter",
        loader: {
          loadNeed: function (pollRecord, needName, options, query) {
            assert.ok(pollRecord === runningDag1 || pollRecord === runningDag2);
            assert.equal(needName, "am");
            assert.ok(options.reload);
            assert.equal(query.counters, "counterGroup/counter");

            return Ember.RSVP.resolve(pollRecord);
          }
        }
      });

  return route.onRecordPoll(record).then(function (pollRecord) {
    assert.equal(pollRecord, runningDag1);
  });
});

test('onRecordPoll schedules reload for incomplete records without known DAGs', function(assert) {
  assert.expect(2);

  let record = Ember.Object.create({
        needs: {
          dag: {}
        },
        isComplete: false
      }),
      route = this.subject({
        loader: {
          loadNeed: function () {
            assert.ok(false, "loadNeed must not be called without an AM target");
          }
        },
        scheduleReload: function () {
          assert.ok(true);
        }
      });

  return route.onRecordPoll(record).then(function (pollRecord) {
    assert.equal(pollRecord, record);
  });
});

test('onPollSuccess schedules reload when all known DAGs are complete', function(assert) {
  assert.expect(1);

  let completedDag1 = Ember.Object.create({
        isComplete: true
      }),
      completedDag2 = Ember.Object.create({
        isComplete: true
      }),
      record = Ember.Object.create({
        dag: Ember.A([completedDag1, completedDag2]),
        isComplete: false
      }),
      route = this.subject({
        polledRecords: Ember.A([record]),
        scheduleReload: function () {
          assert.ok(true);
        }
      });

  route.onPollSuccess(Ember.A([completedDag1]));
});

test('onPollFailure ignores AM failures while an associated ATS DAG is still running', function(assert) {
  assert.expect(5);

  let error = new Error("am failed"),
      dag = Ember.Object.create({
        entityID: "dag_1_2_3",
        needs: {
          am: {}
        },
        appID: "application_1_2",
        isComplete: false
      }),
      runningDag = Ember.Object.create({
        isComplete: false
      }),
      record = Ember.Object.create({
        dag: Ember.A([dag]),
        isComplete: false
      }),
      route = this.subject({
        polledRecords: Ember.A([record]),
        loader: {
          queryRecord: function (type, id, options) {
            assert.equal(type, "dag");
            assert.equal(id, "dag_1_2_3");
            assert.ok(options.reload);
            return Ember.RSVP.resolve(runningDag);
          }
        },
        scheduleReload: function () {
          assert.ok(false, "running ATS DAG must not trigger a full query reload");
        },
        send: function () {
          assert.ok(false, "AM failure must not be shown while ATS DAG is running");
        }
      });

  return route.onPollFailure(error).then(function (dags) {
    assert.equal(dags[0], runningDag);
    assert.ok(true);
  });
});

test('onPollFailure checks the full DAG set when a DAG reload fails', function(assert) {
  assert.expect(8);

  let error = new Error("am failed"),
      requestedIDs = [],
      completedDag = Ember.Object.create({
        entityID: "dag_1_2_1",
        isComplete: true
      }),
      dagToReload = Ember.Object.create({
        entityID: "dag_1_2_1",
        needs: {
          am: {}
        },
        appID: "application_1_2",
        isComplete: false
      }),
      runningDag = Ember.Object.create({
        entityID: "dag_1_2_2",
        needs: {
          am: {}
        },
        appID: "application_1_2",
        isComplete: false
      }),
      record = Ember.Object.create({
        dag: Ember.A([dagToReload, runningDag]),
        isComplete: false
      }),
      route = this.subject({
        polledRecords: Ember.A([record]),
        loader: {
          queryRecord: function (type, id, options) {
            requestedIDs.push(id);
            assert.equal(type, "dag");
            assert.ok(options.reload);

            if(id === "dag_1_2_1") {
              return Ember.RSVP.resolve(completedDag);
            }
            return Ember.RSVP.reject(new Error("dag reload failed"));
          }
        },
        scheduleReload: function () {
          assert.ok(false, "partial DAG reload must not trigger a full query reload");
        },
        send: function () {
          assert.ok(false, "AM failure must not be shown while a known DAG may still be running");
        }
      });

  return route.onPollFailure(error).then(function (dags) {
    assert.deepEqual(requestedIDs, ["dag_1_2_1", "dag_1_2_2"]);
    assert.equal(dags.length, 2);
    assert.equal(dags[0], completedDag);
    assert.equal(dags[1], runningDag);
  });
});

test('onPollFailure schedules reload when all associated ATS DAGs are complete', function(assert) {
  assert.expect(5);

  let error = new Error("am failed"),
      dag = Ember.Object.create({
        entityID: "dag_1_2_3",
        needs: {
          am: {}
        },
        appID: "application_1_2",
        isComplete: false
      }),
      completedDag = Ember.Object.create({
        isComplete: true
      }),
      record = Ember.Object.create({
        dag: Ember.A([dag]),
        isComplete: false
      }),
      route = this.subject({
        polledRecords: Ember.A([record]),
        loader: {
          queryRecord: function (type, id, options) {
            assert.equal(type, "dag");
            assert.equal(id, "dag_1_2_3");
            assert.ok(options.reload);
            return Ember.RSVP.resolve(completedDag);
          }
        },
        scheduleReload: function () {
          assert.ok(true);
        },
        send: function () {
          assert.ok(false, "error must not be sent when DAG completion can be verified");
        }
      });

  return route.onPollFailure(error).then(function () {
    assert.ok(true);
  });
});

test('onPollFailure preserves app status fallback for directly polled records', function(assert) {
  assert.expect(5);

  let error = new Error("am failed"),
      record = Ember.Object.create({
        needs: {
          am: {}
        },
        appID: "application_1_2"
      }),
      completedApp = Ember.Object.create({
        isComplete: true
      }),
      route = this.subject({
        polledRecords: Ember.A([record]),
        loader: {
          queryRecord: function (type, id, options) {
            assert.equal(type, "appRm");
            assert.equal(id, "application_1_2");
            assert.ok(options.reload);
            return Ember.RSVP.resolve(completedApp);
          }
        },
        scheduleReload: function () {
          assert.ok(true);
        },
        send: function () {
          assert.ok(false, "error must not be sent when app completion can be verified");
        }
      });

  return route.onPollFailure(error).then(function () {
    assert.ok(true);
  });
});
