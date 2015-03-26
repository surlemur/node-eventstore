var expect = require('expect.js'),
  Base = require('../lib/base'),
  async = require('async'),
  _ = require('lodash');

var Mongo = require('../lib/databases/mongodb');

function createEvent() {
  return {
    aggregateId: 'id2',
    streamRevision: 0,
    id: '112',
    commitId: 'zyx987',
    commitStamp: new Date(Date.now() + 1),
    commitSequence: 0,
    payload: {
      event:'bla'
    }
  };
}

describe('mongodb store implementation', function () {

  var store;

  before(function (done) {
    store = new Mongo();
    store.connect(done);
  });

  beforeEach(function (done) {
    store.clear(done);
  });

  after(function (done) {
    store.clear(function() {
      store.disconnect(done);
    });
  });

  describe('calling addEvents', function () {

    describe('with missing commitId', function () {

      it('it should callback with an error', function(done) {

        var event1 = createEvent();

        var event2 = createEvent();
        event2.commitId = null;

        store.addEvents([event1, event2], function(err) {
          expect(err).to.be.ok();
          expect(err.message).to.match(/commitId/);
          done();
        });

      });

    });

    describe('with multiple events in the array with mismatching commitId', function () {

      it('it should callback with an error', function(done) {

        var event1 = createEvent();
        event1.commitId = 'zyx987';

        var event2 = createEvent();
        event2.commitId = 'yxw876';

        store.addEvents([event1, event2], function(err) {
          expect(err).to.be.ok();
          expect(err.message).to.match(/commitId/);
          done();
        });

      });

    });

  });
});

  