var expect = require('expect.js'),
  Base = require('../lib/base'),
  async = require('async'),
  _ = require('lodash');

var types = ['inmemory', 'mongodb', 'tingodb', 'redis'/*, 'azuretable'*/];

types.forEach(function (type) {

  describe('"' + type + '" store implementation', function () {

    var Store = require('../lib/databases/' + type);
    var store;

    describe('creating an instance', function () {
      
      before(function () {
        store = new Store();
      });
      
      it('it should return correct object', function () {
        expect(store).to.be.a(Base);
        expect(store.connect).to.be.a('function');
        expect(store.disconnect).to.be.a('function');
        expect(store.getNewId).to.be.a('function');
        expect(store.getEvents).to.be.a('function');
        expect(store.getEventsByRevision).to.be.a('function');
        expect(store.getSnapshot).to.be.a('function');
        expect(store.addSnapshot).to.be.a('function');
        expect(store.addEvents).to.be.a('function');
        expect(store.getUndispatchedEvents).to.be.a('function');
        expect(store.setEventToDispatched).to.be.a('function');
        expect(store.clear).to.be.a('function');
      });
      
      describe('calling connect', function () {
        
        afterEach(function (done) {
          store.disconnect(done);
        });
        
        it('it should callback successfully', function (done) {

          store.connect(function (err) {
            expect(err).not.to.be.ok();
            done();
          });

        });

        it('it should emit connect', function (done) {

          store.once('connect', done);
          store.connect();

        });
        
      });

      describe('having connected', function () {

        describe('calling disconnect', function () {

          beforeEach(function (done) {
            store.connect(done);
          });

          it('it should callback successfully', function (done) {

            store.disconnect(function (err) {
              expect(err).not.to.be.ok();
              done();
            });

          });

          it('it should emit disconnect', function (done) {

            store.once('disconnect', done);
            store.disconnect();

          });

        });

        describe('using the store', function () {

          before(function (done) {
            store.connect(done);
          });
          
          beforeEach(function (done) {
            store.clear(done);
          });

          after(function (done) {
            store.clear(done);
          });
  
          describe('calling getNewId', function () {
  
            it('it should callback with a new Id as string', function (done) {
  
              store.getNewId(function (err, id) {
                expect(err).not.to.be.ok();
                expect(id).to.be.a('string');
                done();
              });
  
            });
  
          });
  
          describe('calling addEvents', function () {
            
            describe('with one event in the array', function () {

              it('it should save the event', function(done) {

                var event = {
                  aggregateId: 'id1',
                  id: '111',
                  streamRevision: 0,
                  commitId: '111',
                  commitStamp: new Date(),
                  commitSequence: 0,
                  payload: {
                    event:'bla'
                  }
                };

                store.addEvents([event], function(err) {
                  expect(err).not.to.be.ok();

                  store.getEvents({}, 0, -1, function(err, evts) {
                    expect(err).not.to.be.ok();
                    expect(evts).to.be.an('array');
                    expect(evts).to.have.length(1);
                    expect(evts[0].commitStamp.getTime()).to.eql(event.commitStamp.getTime());
                    expect(evts[0].aggregateId).to.eql(event.aggregateId);
                    expect(evts[0].commitId).to.eql(event.commitId);
                    expect(evts[0].payload.event).to.eql(event.payload.event);

                    done();
                  });
                });

              });
              
            });

            describe('with multiple events in the array', function () {

              it('it should save the event', function(done) {

                var event1 = {
                  aggregateId: 'id2',
                  streamRevision: 0,
                  id: '112',
                  commitId: '987',
                  commitStamp: new Date(Date.now() + 1),
                  commitSequence: 0,
                  payload: {
                    event:'bla'
                  }
                };

                var event2 = {
                  aggregateId: 'id2',
                  streamRevision: 0,
                  id:'113',
                  commitId: '987',
                  commitStamp: new Date(Date.now() + 1),
                  commitSequence: 1,
                  payload: {
                    event:'bla2'
                  }
                };

                store.addEvents([event1, event2], function(err) {
                  expect(err).not.to.be.ok();

                  store.getEvents({}, 0, -1, function(err, evts) {
                    expect(err).not.to.be.ok();
                    expect(evts).to.be.an('array');
                    expect(evts).to.have.length(2);
                    expect(evts[0].commitStamp.getTime()).to.eql(event1.commitStamp.getTime());
                    expect(evts[0].aggregateId).to.eql(event1.aggregateId);
                    expect(evts[0].commitId).to.eql(event1.commitId);
                    expect(evts[0].payload.event).to.eql(event1.payload.event);
                    expect(evts[1].commitStamp.getTime()).to.eql(event2.commitStamp.getTime());
                    expect(evts[1].aggregateId).to.eql(event2.aggregateId);
                    expect(evts[1].commitId).to.eql(event2.commitId);
                    expect(evts[1].payload.event).to.eql(event2.payload.event);

                    done();
                  });
                });

              });

            });

            describe('without aggregateId', function () {
              
              it('it should callback with an error', function (done) {

                var event = {
                  //aggregateId: 'id1',
                  streamRevision: 0,
                  commitId: '114',
                  commitStamp: new Date(),
                  commitSequence: 0,
                  payload: {
                    event:'bla'
                  }
                };

                store.addEvents([event], function(err) {
                  expect(err).to.be.ok();
                  done();
                });
                
              });
              
            });

            describe('only with aggregateId', function () {

              it('it should save the event', function (done) {

                var event = {
                  aggregateId: 'idhaha',
                  streamRevision: 0,
                  id: '115',
                  commitId: '115',
                  commitStamp: new Date(),
                  commitSequence: 0,
                  payload: {
                    event:'blaffff'
                  }
                };

                store.addEvents([event], function(err) {
                  expect(err).not.to.be.ok();
                  
                  store.getEvents({}, 0, -1, function(err, evts) {
                    expect(err).not.to.be.ok();
                    expect(evts).to.be.an('array');
                    expect(evts).to.have.length(1);
                    expect(evts[0].commitStamp.getTime()).to.eql(event.commitStamp.getTime());
                    expect(evts[0].aggregateId).to.eql(event.aggregateId);
                    expect(evts[0].commitId).to.eql(event.commitId);
                    expect(evts[0].payload.event).to.eql(event.payload.event);

                    done();
                  });
                });

              });

            });

            describe('with aggregateId and aggregate', function () {

              it('it should save the event correctly', function (done) {

                var event = {
                  aggregateId: 'aggId',
                  aggregate: 'myAgg',
                  streamRevision: 0,
                  id:'116',
                  commitId: '116',
                  commitStamp: new Date(),
                  commitSequence: 0,
                  payload: {
                    event:'blaffff'
                  }
                };

                store.addEvents([event], function(err) {
                  expect(err).not.to.be.ok();

                  store.getEvents({}, 0, -1, function(err, evts) {
                    expect(err).not.to.be.ok();
                    expect(evts).to.be.an('array');
                    expect(evts).to.have.length(1);
                    expect(evts[0].commitStamp.getTime()).to.eql(event.commitStamp.getTime());
                    expect(evts[0].aggregateId).to.eql(event.aggregateId);
                    expect(evts[0].aggregate).to.eql(event.aggregate);
                    expect(evts[0].commitId).to.eql(event.commitId);
                    expect(evts[0].payload.event).to.eql(event.payload.event);

                    done();
                  });
                });

              });

            });

            describe('with aggregateId and aggregate and context', function () {

              it('it should save the event correctly', function (done) {

                var event = {
                  aggregateId: 'aggId',
                  aggregate: 'myAgg',
                  context: 'myContext',
                  streamRevision: 0,
                  id:'117',
                  commitId: '117',
                  commitStamp: new Date(),
                  commitSequence: 0,
                  payload: {
                    event:'blaffff'
                  }
                };

                store.addEvents([event], function(err) {
                  expect(err).not.to.be.ok();

                  store.getEvents({}, 0, -1, function(err, evts) {
                    expect(err).not.to.be.ok();
                    expect(evts).to.be.an('array');
                    expect(evts).to.have.length(1);
                    expect(evts[0].commitStamp.getTime()).to.eql(event.commitStamp.getTime());
                    expect(evts[0].aggregateId).to.eql(event.aggregateId);
                    expect(evts[0].aggregate).to.eql(event.aggregate);
                    expect(evts[0].context).to.eql(event.context);
                    expect(evts[0].commitId).to.eql(event.commitId);
                    expect(evts[0].payload.event).to.eql(event.payload.event);

                    done();
                  });
                });

              });

            });

            describe('with aggregateId and context', function () {

              it('it should save the event correctly', function (done) {

                var event = {
                  aggregateId: 'aggId',
                  context: 'myContext',
                  streamRevision: 0,
                  id:'118',
                  commitStamp: new Date(),
                  commitSequence: 0,
                  commitId: '118',
                  payload: {
                    event:'blaffff'
                  }
                };

                store.addEvents([event], function(err) {
                  expect(err).not.to.be.ok();

                  store.getEvents({}, 0, -1, function(err, evts) {
                    expect(err).not.to.be.ok();
                    expect(evts).to.be.an('array');
                    expect(evts).to.have.length(1);
                    expect(evts[0].commitStamp.getTime()).to.eql(event.commitStamp.getTime());
                    expect(evts[0].aggregateId).to.eql(event.aggregateId);
                    expect(evts[0].context).to.eql(event.context);
                    expect(evts[0].commitId).to.eql(event.commitId);
                    expect(evts[0].payload.event).to.eql(event.payload.event);

                    done();
                  });
                });

              });

            });
            
          });
          
          describe('having some events in the eventstore', function () {
            
            var stream1 = [{
              aggregateId: 'id',
              streamRevision: 0,
              id: '119',
              commitId: '1119',
              commitStamp: new Date(Date.now() + 10),
              commitSequence: 0,
              payload: {
                event:'bla'
              }
            }, {
              aggregateId: 'id',
              streamRevision: 1,
              id: '120',
              commitId: '1119',
              commitStamp: new Date(Date.now() + 10),
              commitSequence: 1,
              payload: {
                event:'bla2'
              }
            }];

            var stream2 = [{
              aggregateId: 'idWithAgg',
              aggregate: 'myAgg',
              streamRevision: 0,
              id: '121',
              commitId: '1121',
              commitStamp: new Date(Date.now() + 30),
              commitSequence: 0,
              payload: {
                event:'bla'
              }
            }, {
              aggregateId: 'idWithAgg',
              aggregate: 'myAgg',
              streamRevision: 1,
              id: '122',
              commitId: '1121',
              commitStamp: new Date(Date.now() + 30),
              commitSequence: 1,
              payload: {
                event: 'bla2'
              }
            }];
            
            var stream3 = [{
              aggregateId: 'id', // id already existing...
              aggregate: 'myAgg',
              streamRevision: 0,
              id: '123',
              commitId: '1123',
              commitStamp: new Date(Date.now() + 50),
              commitSequence: 0,
              payload: {
                event:'bla2'
              }
            }];

            var stream4 = [{
              aggregateId: 'idWithCont',
              context: 'myCont',
              streamRevision: 0,
              id: '124',
              commitId: '1124',
              commitStamp: new Date(Date.now() + 60),
              commitSequence: 0,
              payload: {
                event:'bla'
              }
            }, {
              aggregateId: 'idWithCont',
              context: 'myCont',
              streamRevision: 1,
              id: '125',
              commitId: '1124',
              commitStamp: new Date(Date.now() + 60),
              commitSequence: 1,
              payload: {
                event: 'bla2'
              }
            }];

            var stream5 = [{
              aggregateId: 'id', // id already existing...
              context: 'myCont',
              streamRevision: 0,
              id: '126',
              commitId: '1126',
              commitStamp: new Date(Date.now() + 80),
              commitSequence: 0,
              payload: {
                event:'bla2'
              }
            }];

            var stream6 = [{
              aggregateId: 'idWithAggrAndCont',
              aggregate: 'myAggrrr',
              context: 'myConttttt',
              streamRevision: 0,
              id: '127',
              commitId: '1127',
              commitStamp: new Date(Date.now() + 90),
              commitSequence: 0,
              payload: {
                event:'bla'
              }
            }, {
              aggregateId: 'idWithAggrAndCont',
              aggregate: 'myAggrrr',
              context: 'myConttttt',
              streamRevision: 1,
              id: '128',
              commitId: '1127',
              commitStamp: new Date(Date.now() + 90),
              commitSequence: 1,
              payload: {
                event: 'bla2'
              }
            }];

            var stream7 = [{
              aggregateId: 'idWithAggrAndCont2',
              aggregate: 'myAggrrr2',
              context: 'myConttttt',
              streamRevision: 0,
              id: '129',
              commitId: '1129',
              commitStamp: new Date(Date.now() + 110),
              commitSequence: 0,
              payload: {
                event:'bla'
              }
            }, {
              aggregateId: 'idWithAggrAndCont2',
              aggregate: 'myAggrrr2',
              context: 'myConttttt',
              streamRevision: 1,
              id: '130',
              commitId: '1129',
              commitStamp: new Date(Date.now() + 110),
              commitSequence: 1,
              payload: {
                event: 'bla2'
              }
            }];

            var stream8 = [{
              aggregateId: 'idWithAggrAndCont2',
              aggregate: 'myAggrrr',
              context: 'myConttttt',
              streamRevision: 0,
              id: '131',
              commitId: '1131',
              commitStamp: new Date(Date.now() + 130),
              commitSequence: 0,
              payload: {
                event:'bla'
              }
            }];

            var stream9 = [{
              aggregateId: 'idWithAggrAndCont',
              aggregate: 'myAggrrr2',
              context: 'myConttttt',
              streamRevision: 0,
              id: '132',
              commitId: '1132',
              commitStamp: new Date(Date.now() + 140),
              commitSequence: 0,
              payload: {
                event: 'bla2'
              }
            }];

            var stream10 = [{
              aggregateId: 'id', // id already existing...
              aggregate: 'wowAgg',
              context: 'wowCont',
              streamRevision: 0,
              id: '133',
              commitId: '1133',
              commitStamp: new Date(Date.now() + 150),
              commitSequence: 0,
              payload: {
                event:'bla2'
              }
            }];
            
            var allEvents = [].concat(stream1).concat(stream2).concat(stream3)
                              .concat(stream4).concat(stream5).concat(stream6)
                              .concat(stream7).concat(stream8).concat(stream9).concat(stream10);
            
            beforeEach(function (done) {
              async.series([
                function (callback) {
                  store.addEvents(stream1, callback);
                },
                function (callback) {
                  store.addEvents(stream2, callback);
                },
                function (callback) {
                  store.addEvents(stream3, callback);
                },
                function (callback) {
                  store.addEvents(stream4, callback);
                },
                function (callback) {
                  store.addEvents(stream5, callback);
                },
                function (callback) {
                  store.addEvents(stream6, callback);
                },
                function (callback) {
                  store.addEvents(stream7, callback);
                },
                function (callback) {
                  store.addEvents(stream8, callback);
                },
                function (callback) {
                  store.addEvents(stream9, callback);
                },
                function (callback) {
                  store.addEvents(stream10, callback);
                }
              ], done);
            });
            
            describe('calling getEvents', function () {
              
              describe('to get all events', function () {
                
                it('it should return the correct values', function (done) {
                  
                  store.getEvents({}, 0, -1, function (err, evts) {
                    expect(err).not.to.be.ok();
                    expect(evts.length).to.eql(allEvents.length);
                    
                    var lastCommitStamp = 0;
                    var lastCommitId = 0;
                    var lastId = 0;
                    _.each(evts, function (evt) {
                      expect(evt.id).to.be.greaterThan(lastId);
                      expect(evt.commitId >= lastCommitId).to.eql(true);
                      expect(evt.commitStamp.getTime() >= lastCommitStamp).to.eql(true);
                      lastId = evt.id;
                      lastCommitId = evt.commitId;
                      lastCommitStamp = evt.commitStamp.getTime();
                    });
                    
                    done();
                  });
                  
                });

                describe('with a skip value', function () {

                  it('it should return the correct values', function (done) {

                    var expectedEvts = allEvents.slice(3);
                    
                    store.getEvents({}, 3, -1, function (err, evts) {
                      expect(err).not.to.be.ok();
                      expect(evts.length).to.eql(expectedEvts.length);

                      var lastCommitStamp = 0;
                      var lastCommitId = 0;
                      var lastId = 0;
                      _.each(evts, function (evt) {
                        expect(evt.id).to.be.greaterThan(lastId);
                        expect(evt.commitId >= lastCommitId).to.eql(true);
                        expect(evt.commitStamp.getTime() >= lastCommitStamp).to.eql(true);
                        lastId = evt.id;
                        lastCommitId = evt.commitId;
                        lastCommitStamp = evt.commitStamp.getTime();
                      });

                      done();
                    });

                  });

                });

                describe('with a limit value', function () {

                  it('it should return the correct values', function (done) {

                    var expectedEvts = allEvents.slice(0, 5);

                    store.getEvents({}, 0, 5, function (err, evts) {
                      expect(err).not.to.be.ok();
                      expect(evts.length).to.eql(expectedEvts.length);

                      var lastCommitStamp = 0;
                      var lastCommitId = 0;
                      var lastId = 0;
                      _.each(evts, function (evt) {
                        expect(evt.id).to.be.greaterThan(lastId);
                        expect(evt.commitId >= lastCommitId).to.eql(true);
                        expect(evt.commitStamp.getTime() >= lastCommitStamp).to.eql(true);
                        lastId = evt.id;
                        lastCommitId = evt.commitId;
                        lastCommitStamp = evt.commitStamp.getTime();
                      });

                      done();
                    });

                  });

                });

                describe('with a skip and a limit value', function () {

                  it('it should return the correct values', function (done) {

                    var expectedEvts = allEvents.slice(3, 5);

                    store.getEvents({}, 3, 2, function (err, evts) {
                      expect(err).not.to.be.ok();
                      expect(evts.length).to.eql(expectedEvts.length);

                      var lastCommitStamp = 0;
                      var lastCommitId = 0;
                      var lastId = 0;
                      _.each(evts, function (evt) {
                        expect(evt.id).to.be.greaterThan(lastId);
                        expect(evt.commitId >= lastCommitId).to.eql(true);
                        expect(evt.commitStamp.getTime() >= lastCommitStamp).to.eql(true);
                        lastId = evt.id;
                        lastCommitId = evt.commitId;
                        lastCommitStamp = evt.commitStamp.getTime();
                      });

                      done();
                    });

                  });

                });
                
              });

              describe('with an aggregateId being used only in one context and aggregate', function () {

                it('it should return the correct events', function (done) {

                  store.getEvents({ aggregateId: 'idWithAgg' }, 0, -1, function (err, evts) {
                    expect(err).not.to.be.ok();
                    expect(evts.length).to.eql(2);
                    expect(evts[0].id).to.eql(stream2[0].id);
                    expect(evts[0].aggregateId).to.eql(stream2[0].aggregateId);
                    expect(evts[0].commitStamp.getTime()).to.eql(stream2[0].commitStamp.getTime());
                    expect(evts[0].commitSequence).to.eql(stream2[0].commitSequence);
                    expect(evts[0].streamRevision).to.eql(stream2[0].streamRevision);
                    expect(evts[1].id).to.eql(stream2[1].id);
                    expect(evts[1].aggregateId).to.eql(stream2[1].aggregateId);
                    expect(evts[1].commitStamp.getTime()).to.eql(stream2[1].commitStamp.getTime());
                    expect(evts[1].commitSequence).to.eql(stream2[1].commitSequence);
                    expect(evts[1].streamRevision).to.eql(stream2[1].streamRevision);

                    done();
                  });

                });

                describe('and limit it with skip and limit', function () {

                  it('it should return the correct events', function (done) {

                    store.getEvents({ aggregateId: 'idWithAgg' }, 1, 2, function (err, evts) {
                      expect(err).not.to.be.ok();
                      expect(evts.length).to.eql(1);
                      expect(evts[0].aggregateId).to.eql(stream2[1].aggregateId);
                      expect(evts[0].commitStamp.getTime()).to.eql(stream2[1].commitStamp.getTime());
                      expect(evts[0].streamRevision).to.eql(stream2[1].streamRevision);

                      done();
                    });

                  });

                });

              });

              describe('with an aggregateId being used in an other context or aggregate', function () {

                it('it should return the correct events', function (done) {

                  store.getEvents({ aggregateId: 'id' }, 0, -1, function (err, evts) {
                    expect(err).not.to.be.ok();
                    expect(evts.length).to.eql(5);
                    expect(evts[0].aggregateId).to.eql(stream1[0].aggregateId);
                    expect(evts[0].commitStamp.getTime()).to.eql(stream1[0].commitStamp.getTime());
                    expect(evts[0].streamRevision).to.eql(stream1[0].streamRevision);
                    expect(evts[1].aggregateId).to.eql(stream1[1].aggregateId);
                    expect(evts[1].commitStamp.getTime()).to.eql(stream1[1].commitStamp.getTime());
                    expect(evts[1].streamRevision).to.eql(stream1[1].streamRevision);
                    expect(evts[2].aggregateId).to.eql(stream3[0].aggregateId);
                    expect(evts[2].commitStamp.getTime()).to.eql(stream3[0].commitStamp.getTime());
                    expect(evts[2].streamRevision).to.eql(stream3[0].streamRevision);
                    expect(evts[3].aggregateId).to.eql(stream5[0].aggregateId);
                    expect(evts[3].commitStamp.getTime()).to.eql(stream5[0].commitStamp.getTime());
                    expect(evts[3].streamRevision).to.eql(stream5[0].streamRevision);
                    expect(evts[4].aggregateId).to.eql(stream10[0].aggregateId);
                    expect(evts[4].commitStamp.getTime()).to.eql(stream10[0].commitStamp.getTime());
                    expect(evts[4].streamRevision).to.eql(stream10[0].streamRevision);

                    done();
                  });

                });

                describe('and limit it with revMin and revMax', function () {

                  it('it should return the correct events', function (done) {

                    store.getEvents({ aggregateId: 'id' }, 1, 2, function (err, evts) {
                      expect(err).not.to.be.ok();
                      expect(evts.length).to.eql(2);
                      expect(evts[0].aggregateId).to.eql(stream1[1].aggregateId);
                      expect(evts[0].commitStamp.getTime()).to.eql(stream1[1].commitStamp.getTime());
                      expect(evts[0].streamRevision).to.eql(stream1[1].streamRevision);

                      done();
                    });

                  });

                });

              });

              describe('without an aggregateId but with an aggregate', function () {

                it('it should return the correct events', function (done) {

                  store.getEvents({ aggregate: 'myAggrrr2' }, 0, -1, function (err, evts) {
                    expect(err).not.to.be.ok();
                    expect(evts.length).to.eql(3);
                    expect(evts[0].aggregateId).to.eql(stream7[0].aggregateId);
                    expect(evts[0].commitStamp.getTime()).to.eql(stream7[0].commitStamp.getTime());
                    expect(evts[0].streamRevision).to.eql(stream7[0].streamRevision);
                    expect(evts[1].aggregateId).to.eql(stream7[1].aggregateId);
                    expect(evts[1].commitStamp.getTime()).to.eql(stream7[1].commitStamp.getTime());
                    expect(evts[1].streamRevision).to.eql(stream7[1].streamRevision);
                    expect(evts[2].aggregateId).to.eql(stream9[0].aggregateId);
                    expect(evts[2].commitStamp.getTime()).to.eql(stream9[0].commitStamp.getTime());
                    expect(evts[2].streamRevision).to.eql(stream9[0].streamRevision);

                    done();
                  });

                });

                describe('and limit it with skip and limit', function () {

                  it('it should return the correct events', function (done) {

                    store.getEvents({ aggregate: 'myAggrrr2' }, 1, 2, function (err, evts) {
                      expect(err).not.to.be.ok();
                      expect(evts.length).to.eql(2);
                      expect(evts[0].aggregateId).to.eql(stream7[1].aggregateId);
                      expect(evts[0].commitStamp.getTime()).to.eql(stream7[1].commitStamp.getTime());
                      expect(evts[0].streamRevision).to.eql(stream7[1].streamRevision);
                      expect(evts[1].aggregateId).to.eql(stream9[0].aggregateId);
                      expect(evts[1].commitStamp.getTime()).to.eql(stream9[0].commitStamp.getTime());
                      expect(evts[1].streamRevision).to.eql(stream9[0].streamRevision);

                      done();
                    });

                  });

                });

              });

              describe('with an aggregateId and with an aggregate', function () {

                it('it should return the correct events', function (done) {

                  store.getEvents({ aggregate: 'myAggrrr2', aggregateId: 'idWithAggrAndCont' }, 0, -1, function (err, evts) {
                    expect(err).not.to.be.ok();
                    expect(evts.length).to.eql(1);
                    expect(evts[0].aggregateId).to.eql(stream9[0].aggregateId);
                    expect(evts[0].commitStamp.getTime()).to.eql(stream9[0].commitStamp.getTime());
                    expect(evts[0].streamRevision).to.eql(stream9[0].streamRevision);

                    done();
                  });

                });

                describe('and limit it with skip and limit', function () {

                  it('it should return the correct events', function (done) {

                    store.getEvents({ aggregate: 'myAggrrr2', aggregateId: 'idWithAggrAndCont' }, 1, 2, function (err, evts) {
                      expect(err).not.to.be.ok();
                      expect(evts.length).to.eql(0);

                      done();
                    });

                  });

                });

              });

              describe('with an aggregateId and without an aggregate but with a context', function () {

                it('it should return the correct events', function (done) {

                  store.getEvents({ aggregateId: 'idWithAggrAndCont', context: 'myConttttt' }, 0, -1, function (err, evts) {
                    expect(err).not.to.be.ok();
                    expect(evts.length).to.eql(3);
                    expect(evts[0].aggregateId).to.eql(stream6[0].aggregateId);
                    expect(evts[0].commitStamp.getTime()).to.eql(stream6[0].commitStamp.getTime());
                    expect(evts[0].streamRevision).to.eql(stream6[0].streamRevision);
                    expect(evts[1].aggregateId).to.eql(stream6[1].aggregateId);
                    expect(evts[1].commitStamp.getTime()).to.eql(stream6[1].commitStamp.getTime());
                    expect(evts[1].streamRevision).to.eql(stream6[1].streamRevision);
                    expect(evts[2].aggregateId).to.eql(stream9[0].aggregateId);
                    expect(evts[2].commitStamp.getTime()).to.eql(stream9[0].commitStamp.getTime());
                    expect(evts[2].streamRevision).to.eql(stream9[0].streamRevision);

                    done();
                  });

                });

                describe('and limit it with skip and limit', function () {

                  it('it should return the correct events', function (done) {

                    store.getEvents({ aggregateId: 'idWithAggrAndCont', context: 'myConttttt' }, 1, 2, function (err, evts) {
                      expect(err).not.to.be.ok();
                      expect(evts.length).to.eql(2);
                      expect(evts[0].aggregateId).to.eql(stream6[1].aggregateId);
                      expect(evts[0].commitStamp.getTime()).to.eql(stream6[1].commitStamp.getTime());
                      expect(evts[0].streamRevision).to.eql(stream6[1].streamRevision);
                      expect(evts[1].aggregateId).to.eql(stream9[0].aggregateId);
                      expect(evts[1].commitStamp.getTime()).to.eql(stream9[0].commitStamp.getTime());
                      expect(evts[1].streamRevision).to.eql(stream9[0].streamRevision);

                      done();
                    });

                  });

                });

              });
              
              describe('with an aggregateId and with an aggregate and with a context', function () {

                it('it should return the correct events', function (done) {

                  store.getEvents({ aggregateId: 'id', aggregate: 'wowAgg', context: 'wowCont' }, 0, -1, function (err, evts) {
                    expect(err).not.to.be.ok();
                    expect(evts.length).to.eql(1);
                    expect(evts[0].aggregateId).to.eql(stream10[0].aggregateId);
                    expect(evts[0].commitStamp.getTime()).to.eql(stream10[0].commitStamp.getTime());
                    expect(evts[0].streamRevision).to.eql(stream10[0].streamRevision);

                    done();
                  });

                });

                describe('and limit it with skip and limit', function () {

                  it('it should return the correct events', function (done) {

                    store.getEvents({ aggregateId: 'id', aggregate: 'wowAgg', context: 'wowCont' }, 1, 2, function (err, evts) {
                      expect(err).not.to.be.ok();
                      expect(evts.length).to.eql(0);

                      done();
                    });

                  });

                });

              });

              describe('without an aggregateId and without an aggregate but with a context', function () {

                it('it should return the correct events', function (done) {

                  store.getEvents({ context: 'myCont' }, 0, -1, function (err, evts) {
                    expect(err).not.to.be.ok();
                    expect(evts.length).to.eql(3);
                    expect(evts[0].aggregateId).to.eql(stream4[0].aggregateId);
                    expect(evts[0].commitStamp.getTime()).to.eql(stream4[0].commitStamp.getTime());
                    expect(evts[0].streamRevision).to.eql(stream4[0].streamRevision);
                    expect(evts[1].aggregateId).to.eql(stream4[1].aggregateId);
                    expect(evts[1].commitStamp.getTime()).to.eql(stream4[1].commitStamp.getTime());
                    expect(evts[1].streamRevision).to.eql(stream4[1].streamRevision);
                    expect(evts[2].aggregateId).to.eql(stream5[0].aggregateId);
                    expect(evts[2].commitStamp.getTime()).to.eql(stream5[0].commitStamp.getTime());
                    expect(evts[2].streamRevision).to.eql(stream5[0].streamRevision);

                    done();
                  });

                });

                describe('and limit it with skip and limit', function () {

                  it('it should return the correct events', function (done) {

                    store.getEvents({ context: 'myCont' }, 1, 2, function (err, evts) {
                      expect(err).not.to.be.ok();
                      expect(evts.length).to.eql(2);
                      expect(evts[0].aggregateId).to.eql(stream4[1].aggregateId);
                      expect(evts[0].commitStamp.getTime()).to.eql(stream4[1].commitStamp.getTime());
                      expect(evts[0].streamRevision).to.eql(stream4[1].streamRevision);
                      expect(evts[1].aggregateId).to.eql(stream5[0].aggregateId);
                      expect(evts[1].commitStamp.getTime()).to.eql(stream5[0].commitStamp.getTime());
                      expect(evts[1].streamRevision).to.eql(stream5[0].streamRevision);

                      done();
                    });

                  });

                });

              });

              describe('without an aggregateId but with an aggregate and with a context', function () {

                it('it should return the correct events', function (done) {

                  store.getEvents({ context: 'myConttttt', aggregate: 'myAggrrr' }, 0, -1, function (err, evts) {
                    expect(err).not.to.be.ok();
                    expect(evts.length).to.eql(3);
                    expect(evts[0].aggregateId).to.eql(stream6[0].aggregateId);
                    expect(evts[0].commitStamp.getTime()).to.eql(stream6[0].commitStamp.getTime());
                    expect(evts[0].streamRevision).to.eql(stream6[0].streamRevision);
                    expect(evts[1].aggregateId).to.eql(stream6[1].aggregateId);
                    expect(evts[1].commitStamp.getTime()).to.eql(stream6[1].commitStamp.getTime());
                    expect(evts[1].streamRevision).to.eql(stream6[1].streamRevision);
                    expect(evts[2].aggregateId).to.eql(stream8[0].aggregateId);
                    expect(evts[2].commitStamp.getTime()).to.eql(stream8[0].commitStamp.getTime());
                    expect(evts[2].streamRevision).to.eql(stream8[0].streamRevision);

                    done();
                  });

                });

                describe('and limit it with skip and limit', function () {

                  it('it should return the correct events', function (done) {

                    store.getEvents({ context: 'myConttttt', aggregate: 'myAggrrr' }, 1, 2, function (err, evts) {
                      expect(err).not.to.be.ok();
                      expect(evts.length).to.eql(2);
                      expect(evts[0].aggregateId).to.eql(stream6[1].aggregateId);
                      expect(evts[0].commitStamp.getTime()).to.eql(stream6[1].commitStamp.getTime());
                      expect(evts[0].streamRevision).to.eql(stream6[1].streamRevision);
                      expect(evts[1].aggregateId).to.eql(stream8[0].aggregateId);
                      expect(evts[1].commitStamp.getTime()).to.eql(stream8[0].commitStamp.getTime());
                      expect(evts[1].streamRevision).to.eql(stream8[0].streamRevision);

                      done();
                    });

                  });

                });

              });

            });
            
            describe('calling getEventsByRevision', function () {
              
              describe('with an aggregateId being used only in one context and aggregate', function () {
                
                it('it should return the correct events', function (done) {
                  
                  store.getEventsByRevision({ aggregateId: 'idWithAgg' }, 0, -1, function (err, evts) {
                    expect(err).not.to.be.ok();
                    expect(evts.length).to.eql(2);
                    expect(evts[0].aggregateId).to.eql(stream2[0].aggregateId);
                    expect(evts[0].commitStamp.getTime()).to.eql(stream2[0].commitStamp.getTime());
                    expect(evts[0].streamRevision).to.eql(stream2[0].streamRevision);
                    expect(evts[1].aggregateId).to.eql(stream2[1].aggregateId);
                    expect(evts[1].commitStamp.getTime()).to.eql(stream2[1].commitStamp.getTime());
                    expect(evts[1].streamRevision).to.eql(stream2[1].streamRevision);
                    
                    done();
                  });
                  
                });

                describe('and limit it with revMin and revMax', function () {

                  it('it should return the correct events', function (done) {

                    store.getEventsByRevision({ aggregateId: 'idWithAgg' }, 1, 2, function (err, evts) {
                      expect(err).not.to.be.ok();
                      expect(evts.length).to.eql(1);
                      expect(evts[0].aggregateId).to.eql(stream2[1].aggregateId);
                      expect(evts[0].commitStamp.getTime()).to.eql(stream2[1].commitStamp.getTime());
                      expect(evts[0].streamRevision).to.eql(stream2[1].streamRevision);

                      done();
                    });

                  });

                });
                
              });

              describe('with an aggregateId being used in an other context or aggregate', function () {

                it('it should return the correct events', function (done) {

                  store.getEventsByRevision({ aggregateId: 'id' }, 0, -1, function (err, evts) {
                    expect(err).not.to.be.ok();
                    expect(evts.length).to.eql(5);
                    expect(evts[0].aggregateId).to.eql(stream1[0].aggregateId);
                    expect(evts[0].commitStamp.getTime()).to.eql(stream1[0].commitStamp.getTime());
                    expect(evts[0].streamRevision).to.eql(stream1[0].streamRevision);
                    expect(evts[1].aggregateId).to.eql(stream1[1].aggregateId);
                    expect(evts[1].commitStamp.getTime()).to.eql(stream1[1].commitStamp.getTime());
                    expect(evts[1].streamRevision).to.eql(stream1[1].streamRevision);
                    expect(evts[2].aggregateId).to.eql(stream3[0].aggregateId);
                    expect(evts[2].commitStamp.getTime()).to.eql(stream3[0].commitStamp.getTime());
                    expect(evts[2].streamRevision).to.eql(stream3[0].streamRevision);
                    expect(evts[3].aggregateId).to.eql(stream5[0].aggregateId);
                    expect(evts[3].commitStamp.getTime()).to.eql(stream5[0].commitStamp.getTime());
                    expect(evts[3].streamRevision).to.eql(stream5[0].streamRevision);
                    expect(evts[4].aggregateId).to.eql(stream10[0].aggregateId);
                    expect(evts[4].commitStamp.getTime()).to.eql(stream10[0].commitStamp.getTime());
                    expect(evts[4].streamRevision).to.eql(stream10[0].streamRevision);

                    done();
                  });

                });

                describe('and limit it with revMin and revMax', function () {

                  it('it should return the correct events', function (done) {

                    store.getEventsByRevision({ aggregateId: 'id' }, 1, 2, function (err, evts) {
                      expect(err).not.to.be.ok();
                      expect(evts.length).to.eql(1);
                      expect(evts[0].aggregateId).to.eql(stream1[1].aggregateId);
                      expect(evts[0].commitStamp.getTime()).to.eql(stream1[1].commitStamp.getTime());
                      expect(evts[0].streamRevision).to.eql(stream1[1].streamRevision);

                      done();
                    });

                  });

                });

              });

              describe('with an aggregateId and with an aggregate', function () {

                it('it should return the correct events', function (done) {

                  store.getEventsByRevision({ aggregate: 'myAggrrr2', aggregateId: 'idWithAggrAndCont' }, 0, -1, function (err, evts) {
                    expect(err).not.to.be.ok();
                    expect(evts.length).to.eql(1);
                    expect(evts[0].aggregateId).to.eql(stream9[0].aggregateId);
                    expect(evts[0].commitStamp.getTime()).to.eql(stream9[0].commitStamp.getTime());
                    expect(evts[0].streamRevision).to.eql(stream9[0].streamRevision);

                    done();
                  });

                });

                describe('and limit it with skip and limit', function () {

                  it('it should return the correct events', function (done) {

                    store.getEventsByRevision({ aggregate: 'myAggrrr2', aggregateId: 'idWithAggrAndCont' }, 1, 2, function (err, evts) {
                      expect(err).not.to.be.ok();
                      expect(evts.length).to.eql(0);

                      done();
                    });

                  });

                });

                describe('and an other combination of limit and skip', function () {

                  it('it should return the correct events', function (done) {

                    store.getEventsByRevision({ aggregate: 'myAggrrr2', aggregateId: 'idWithAggrAndCont' }, 0, 2, function (err, evts) {
                      expect(err).not.to.be.ok();
                      expect(evts.length).to.eql(1);

                      done();
                    });

                  });

                });

              });

              describe('with an aggregateId and with an aggregate and with a context', function () {

                it('it should return the correct events', function (done) {

                  store.getEventsByRevision({ aggregateId: 'id', aggregate: 'wowAgg', context: 'wowCont' }, 0, -1, function (err, evts) {
                    expect(err).not.to.be.ok();
                    expect(evts.length).to.eql(1);
                    expect(evts[0].aggregateId).to.eql(stream10[0].aggregateId);
                    expect(evts[0].commitStamp.getTime()).to.eql(stream10[0].commitStamp.getTime());
                    expect(evts[0].streamRevision).to.eql(stream10[0].streamRevision);

                    done();
                  });

                });

                describe('and limit it with skip and limit', function () {

                  it('it should return the correct events', function (done) {

                    store.getEventsByRevision({ aggregateId: 'id', aggregate: 'wowAgg', context: 'wowCont' }, 1, 2, function (err, evts) {
                      expect(err).not.to.be.ok();
                      expect(evts.length).to.eql(0);

                      done();
                    });

                  });

                });

                describe('and an other combination of limit and skip', function () {

                  it('it should return the correct events', function (done) {

                    store.getEventsByRevision({ aggregateId: 'id', aggregate: 'wowAgg', context: 'wowCont' }, 0, 2, function (err, evts) {
                      expect(err).not.to.be.ok();
                      expect(evts.length).to.eql(1);

                      done();
                    });

                  });

                });

              });

              describe('with an aggregateId and without an aggregate but with a context', function () {

                it('it should return the correct events', function (done) {

                  store.getEventsByRevision({ aggregateId: 'idWithAggrAndCont', context: 'myConttttt' }, 0, -1, function (err, evts) {
                    expect(err).not.to.be.ok();
                    expect(evts.length).to.eql(3);
                    expect(evts[0].aggregateId).to.eql(stream6[0].aggregateId);
                    expect(evts[0].commitStamp.getTime()).to.eql(stream6[0].commitStamp.getTime());
                    expect(evts[0].streamRevision).to.eql(stream6[0].streamRevision);
                    expect(evts[1].aggregateId).to.eql(stream6[1].aggregateId);
                    expect(evts[1].commitStamp.getTime()).to.eql(stream6[1].commitStamp.getTime());
                    expect(evts[1].streamRevision).to.eql(stream6[1].streamRevision);
                    expect(evts[2].aggregateId).to.eql(stream9[0].aggregateId);
                    expect(evts[2].commitStamp.getTime()).to.eql(stream9[0].commitStamp.getTime());
                    expect(evts[2].streamRevision).to.eql(stream9[0].streamRevision);

                    done();
                  });

                });

                describe('and limit it with skip and limit', function () {

                  it('it should return the correct events', function (done) {

                    store.getEventsByRevision({ aggregateId: 'idWithAggrAndCont', context: 'myConttttt' }, 1, 2, function (err, evts) {
                      expect(err).not.to.be.ok();
                      expect(evts.length).to.eql(1);
                      expect(evts[0].aggregateId).to.eql(stream6[1].aggregateId);
                      expect(evts[0].commitStamp.getTime()).to.eql(stream6[1].commitStamp.getTime());
                      expect(evts[0].streamRevision).to.eql(stream6[1].streamRevision);

                      done();
                    });

                  });

                });

              });
              
            });
            
          });

          describe('adding some events', function () {

            var stream = [{
              aggregateId: 'id',
              streamRevision: 0,
              id: '119',
              commitId: '11119',
              commitStamp: new Date(Date.now() + 10),
              commitSequence: 0,
              payload: {
                event:'bla'
              }
            }, {
              aggregateId: 'id',
              streamRevision: 1,
              id: '120',
              commitId: '11119',
              commitStamp: new Date(Date.now() + 10),
              commitSequence: 1,
              payload: {
                event:'bla2'
              }
            }];
            
            beforeEach(function (done) {
              store.addEvents(stream, done);
            });

            describe('and requesting all undispatched events', function () {
              
              it('it should return the correct events', function (done) {
                
                store.getUndispatchedEvents(function (err, evts) {
                  expect(err).not.to.be.ok();
                  expect(evts.length).to.eql(2);
                  expect(evts[0].id).to.eql(stream[0].id);
                  expect(evts[0].commitId).to.eql(stream[0].commitId);
                  expect(evts[0].commitStamp.getTime()).to.eql(stream[0].commitStamp.getTime());
                  expect(evts[1].id).to.eql(stream[1].id);
                  expect(evts[1].commitId).to.eql(stream[1].commitId);
                  expect(evts[1].commitStamp.getTime()).to.eql(stream[1].commitStamp.getTime());
                  
                  done();
                });
                
              });
              
            });
            
            describe('calling setEventToDispatched', function () {
              
              beforeEach(function (done) {
                store.getUndispatchedEvents(function (err, evts) {
                  expect(evts.length).to.eql(2);
                  done();
                });
              });
              
              it('it should work correctly', function (done) {

                store.setEventToDispatched('119', function (err) {
                  expect(err).not.to.be.ok();

                  store.getUndispatchedEvents(function (err, evts) {
                    expect(err).not.to.be.ok();
                    expect(evts.length).to.eql(1);
                    expect(evts[0].commitId).to.eql(stream[1].commitId);
                    expect(evts[0].commitStamp.getTime()).to.eql(stream[1].commitStamp.getTime());
                    
                    done();
                  });
                });
                
              });
              
            });

          });
          
          describe('calling addSnapshot', function () {
            
            var snap = {
              id: '12345',
              aggregateId: '920193847',
              aggregate: 'myCoolAggregate',
              context: 'myEvenCoolerContext',
              commitStamp: new Date(Date.now() + 400),
              revision: 3,
              version: 1,
              data: {
                mySnappi: 'data'
              }
            };
            
            it('it should save the snapshot', function (done) {
              
              store.addSnapshot(snap, function (err) {
                expect(err).not.to.be.ok();
                
                store.getSnapshot({ aggregateId: snap.aggregateId }, -1 , function (err, shot) {
                  expect(err).not.to.be.ok();
                  expect(shot.id).to.eql(snap.id);
                  expect(shot.aggregateId).to.eql(snap.aggregateId);
                  expect(shot.aggregate).to.eql(snap.aggregate);
                  expect(shot.context).to.eql(snap.context);
                  expect(shot.commitStamp.getTime()).to.eql(snap.commitStamp.getTime());
                  expect(shot.revision).to.eql(snap.revision);
                  expect(shot.version).to.eql(snap.version);
                  expect(shot.data.mySnappi).to.eql(snap.data.mySnappi);
                  
                  done();
                });
              });
              
            });

            describe('having some snapshots in the eventstore calling getSnapshot', function () {
              
              var snap1 = {
                id: '12345',
                aggregateId: '920193847',
                commitStamp: new Date(Date.now() + 405),
                revision: 3,
                version: 1,
                data: {
                  mySnappi: 'data'
                }
              };

              var snap2 = {
                id: '123456',
                aggregateId: '920193847',
                commitStamp: new Date(Date.now() + 410),
                revision: 8,
                version: 1,
                data: {
                  mySnappi: 'data2'
                }
              };

              var snap3 = {
                id: '1234567',
                aggregateId: '142351',
                aggregate: 'myCoolAggregate',
                context: 'conntttt',
                commitStamp: new Date(Date.now() + 420),
                revision: 5,
                version: 1,
                data: {
                  mySnappi: 'data2'
                }
              };

              var snap4 = {
                id: '12345678',
                aggregateId: '920193847',
                aggregate: 'myCoolAggregate',
                commitStamp: new Date(Date.now() + 430),
                revision: 9,
                version: 1,
                data: {
                  mySnappi: 'data6'
                }
              };

              var snap5 = {
                id: '123456789',
                aggregateId: '938179341',
                aggregate: 'myCoolAggregate',
                context: 'myCoolContext',
                commitStamp: new Date(Date.now() + 440),
                revision: 2,
                version: 1,
                data: {
                  mySnappi: 'dataXY'
                }
              };

              var snap6 = {
                id: '12345678910',
                aggregateId: '920193847',
                aggregate: 'myCoolAggregate2',
                context: 'myCoolContext',
                commitStamp: new Date(Date.now() + 450),
                revision: 12,
                version: 1,
                data: {
                  mySnappi: 'dataaaaa'
                }
              };

              var snap7 = {
                id: '123456789104',
                aggregateId: '920193847313131313',
                aggregate: 'myCoolAggregate2',
                context: 'myCoolContext',
                commitStamp: new Date(Date.now() + 555),
                revision: 16,
                version: 2,
                data: {
                  mySnappi: 'dataaaaa2'
                }
              };

              var snap8 = {
                id: '123456789102',
                aggregateId: '920193847313131313',
                aggregate: 'myCoolAggregate2',
                context: 'myCoolContext',
                commitStamp: new Date(Date.now() + 575),
                revision: 16,
                version: 3,
                data: {
                  mySnappi: 'dataaaaa3'
                }
              };
              
              beforeEach(function (done) {
                async.series([
                  function (callback) {
                    store.addSnapshot(snap1, callback);
                  },
                  function (callback) {
                    store.addSnapshot(snap2, callback);
                  },
                  function (callback) {
                    store.addSnapshot(snap3, callback);
                  },
                  function (callback) {
                    store.addSnapshot(snap4, callback);
                  },
                  function (callback) {
                    store.addSnapshot(snap5, callback);
                  },
                  function (callback) {
                    store.addSnapshot(snap6, callback);
                  },
                  function (callback) {
                    store.addSnapshot(snap7, callback);
                  },
                  function (callback) {
                    store.addSnapshot(snap8, callback);
                  }
                ], done);
              });

              describe('with an aggregateId being used only in one context and aggregate', function () {
                
                it('it should return the correct snapshot', function (done) {
                  
                  store.getSnapshot({ aggregateId: '142351' }, -1, function (err, shot) {
                    expect(err).not.to.be.ok();
                    expect(shot.id).to.eql(snap3.id);
                    expect(shot.aggregateId).to.eql(snap3.aggregateId);
                    expect(shot.aggregate).to.eql(snap3.aggregate);
                    expect(shot.context).to.eql(snap3.context);
                    expect(shot.commitStamp.getTime()).to.eql(snap3.commitStamp.getTime());
                    expect(shot.revision).to.eql(snap3.revision);
                    expect(shot.version).to.eql(snap3.version);
                    expect(shot.data.mySnappi).to.eql(snap3.data.mySnappi);
                    
                    done();
                  });
                  
                });

                describe('and limit it with revMax', function () {

                  it('it should return the correct snapshot', function (done) {

                    store.getSnapshot({ aggregateId: '142351' }, 1, function (err, shot) {
                      expect(err).not.to.be.ok();
                      expect(shot).not.to.be.ok();

                      done();
                    });

                  });

                });

                describe('and an other revMax', function () {

                  it('it should return the correct snapshot', function (done) {

                    store.getSnapshot({ aggregateId: '142351' }, 5, function (err, shot) {
                      expect(err).not.to.be.ok();
                      expect(shot.id).to.eql(snap3.id);
                      expect(shot.aggregateId).to.eql(snap3.aggregateId);
                      expect(shot.aggregate).to.eql(snap3.aggregate);
                      expect(shot.context).to.eql(snap3.context);
                      expect(shot.commitStamp.getTime()).to.eql(snap3.commitStamp.getTime());
                      expect(shot.revision).to.eql(snap3.revision);
                      expect(shot.version).to.eql(snap3.version);
                      expect(shot.data.mySnappi).to.eql(snap3.data.mySnappi);

                      done();
                    });

                  });

                });
                
              });

              describe('with an aggregateId being used in an other context or aggregate', function () {

                it('it should return the correct snapshot', function (done) {

                  store.getSnapshot({ aggregateId: '920193847' }, -1, function (err, shot) {
                    expect(err).not.to.be.ok();
                    expect(shot.id).to.eql(snap6.id);
                    expect(shot.aggregateId).to.eql(snap6.aggregateId);
                    expect(shot.aggregate).to.eql(snap6.aggregate);
                    expect(shot.context).to.eql(snap6.context);
                    expect(shot.commitStamp.getTime()).to.eql(snap6.commitStamp.getTime());
                    expect(shot.revision).to.eql(snap6.revision);
                    expect(shot.version).to.eql(snap6.version);
                    expect(shot.data.mySnappi).to.eql(snap6.data.mySnappi);

                    done();
                  });

                });

                describe('and limit it with revMax', function () {

                  it('it should return the correct snapshot', function (done) {

                    store.getSnapshot({ aggregateId: '920193847' }, 1, function (err, shot) {
                      expect(err).not.to.be.ok();
                      expect(shot).not.to.be.ok();

                      done();
                    });

                  });

                });

                describe('and an other revMax', function () {

                  it('it should return the correct snapshot', function (done) {

                    store.getSnapshot({ aggregateId: '920193847' }, 5, function (err, shot) {
                      expect(err).not.to.be.ok();
                      expect(shot.id).to.eql(snap1.id);
                      expect(shot.aggregateId).to.eql(snap1.aggregateId);
                      expect(shot.aggregate).to.eql(snap1.aggregate);
                      expect(shot.context).to.eql(snap1.context);
                      expect(shot.commitStamp.getTime()).to.eql(snap1.commitStamp.getTime());
                      expect(shot.revision).to.eql(snap1.revision);
                      expect(shot.version).to.eql(snap1.version);
                      expect(shot.data.mySnappi).to.eql(snap1.data.mySnappi);

                      done();
                    });

                  });

                });

              });

              describe('with an aggregateId and with an aggregate', function () {

                it('it should return the correct snapshot', function (done) {

                  store.getSnapshot({ aggregateId: '920193847', aggregate: 'myCoolAggregate' }, -1, function (err, shot) {
                    expect(err).not.to.be.ok();
                    expect(shot.id).to.eql(snap4.id);
                    expect(shot.aggregateId).to.eql(snap4.aggregateId);
                    expect(shot.aggregate).to.eql(snap4.aggregate);
                    expect(shot.context).to.eql(snap4.context);
                    expect(shot.commitStamp.getTime()).to.eql(snap4.commitStamp.getTime());
                    expect(shot.revision).to.eql(snap4.revision);
                    expect(shot.version).to.eql(snap4.version);
                    expect(shot.data.mySnappi).to.eql(snap4.data.mySnappi);

                    done();
                  });

                });

                describe('and limit it with revMax', function () {

                  it('it should return the correct snapshot', function (done) {

                    store.getSnapshot({ aggregateId: '920193847', aggregate: 'myCoolAggregate' }, 1, function (err, shot) {
                      expect(err).not.to.be.ok();
                      expect(shot).not.to.be.ok();

                      done();
                    });

                  });

                });

                describe('and an other revMax', function () {

                  it('it should return the correct snapshot', function (done) {

                    store.getSnapshot({ aggregateId: '920193847', aggregate: 'myCoolAggregate' }, 9, function (err, shot) {
                      expect(err).not.to.be.ok();
                      expect(shot.id).to.eql(snap4.id);
                      expect(shot.aggregateId).to.eql(snap4.aggregateId);
                      expect(shot.aggregate).to.eql(snap4.aggregate);
                      expect(shot.context).to.eql(snap4.context);
                      expect(shot.commitStamp.getTime()).to.eql(snap4.commitStamp.getTime());
                      expect(shot.revision).to.eql(snap4.revision);
                      expect(shot.version).to.eql(snap4.version);
                      expect(shot.data.mySnappi).to.eql(snap4.data.mySnappi);

                      done();
                    });

                  });

                });

              });

              describe('with an aggregateId and with an aggregate and with a context', function () {

                it('it should return the correct snapshot', function (done) {

                  store.getSnapshot({ aggregateId: '938179341', aggregate: 'myCoolAggregate', context: 'myCoolContext' }, -1, function (err, shot) {
                    expect(err).not.to.be.ok();
                    expect(shot.id).to.eql(snap5.id);
                    expect(shot.aggregateId).to.eql(snap5.aggregateId);
                    expect(shot.aggregate).to.eql(snap5.aggregate);
                    expect(shot.context).to.eql(snap5.context);
                    expect(shot.commitStamp.getTime()).to.eql(snap5.commitStamp.getTime());
                    expect(shot.revision).to.eql(snap5.revision);
                    expect(shot.version).to.eql(snap5.version);
                    expect(shot.data.mySnappi).to.eql(snap5.data.mySnappi);

                    done();
                  });

                });

                describe('and limit it with revMax', function () {

                  it('it should return the correct snapshot', function (done) {

                    store.getSnapshot({ aggregateId: '938179341', aggregate: 'myCoolAggregate', context: 'myCoolContext' }, 1, function (err, shot) {
                      expect(err).not.to.be.ok();
                      expect(shot).not.to.be.ok();

                      done();
                    });

                  });

                });

                describe('and an other revMax', function () {

                  it('it should return the correct snapshot', function (done) {

                    store.getSnapshot({ aggregateId: '938179341', aggregate: 'myCoolAggregate', context: 'myCoolContext' }, 2, function (err, shot) {
                      expect(err).not.to.be.ok();
                      expect(shot.id).to.eql(snap5.id);
                      expect(shot.aggregateId).to.eql(snap5.aggregateId);
                      expect(shot.aggregate).to.eql(snap5.aggregate);
                      expect(shot.context).to.eql(snap5.context);
                      expect(shot.commitStamp.getTime()).to.eql(snap5.commitStamp.getTime());
                      expect(shot.revision).to.eql(snap5.revision);
                      expect(shot.version).to.eql(snap5.version);
                      expect(shot.data.mySnappi).to.eql(snap5.data.mySnappi);

                      done();
                    });

                  });

                });

              });

              describe('with an aggregateId and without an aggregate but with a context', function () {

                it('it should return the correct snapshot', function (done) {

                  store.getSnapshot({ aggregateId: '142351', context: 'conntttt' }, -1, function (err, shot) {
                    expect(err).not.to.be.ok();
                    expect(shot.id).to.eql(snap3.id);
                    expect(shot.aggregateId).to.eql(snap3.aggregateId);
                    expect(shot.aggregate).to.eql(snap3.aggregate);
                    expect(shot.context).to.eql(snap3.context);
                    expect(shot.commitStamp.getTime()).to.eql(snap3.commitStamp.getTime());
                    expect(shot.revision).to.eql(snap3.revision);
                    expect(shot.version).to.eql(snap3.version);
                    expect(shot.data.mySnappi).to.eql(snap3.data.mySnappi);

                    done();
                  });

                });

                describe('and limit it with revMax', function () {

                  it('it should return the correct snapshot', function (done) {

                    store.getSnapshot({ aggregateId: '142351', context: 'conntttt' }, 1, function (err, shot) {
                      expect(err).not.to.be.ok();
                      expect(shot).not.to.be.ok();

                      done();
                    });

                  });

                });

                describe('and an other revMax', function () {

                  it('it should return the correct snapshot', function (done) {

                    store.getSnapshot({ aggregateId: '142351', context: 'conntttt' }, 5, function (err, shot) {
                      expect(err).not.to.be.ok();
                      expect(shot.id).to.eql(snap3.id);
                      expect(shot.aggregateId).to.eql(snap3.aggregateId);
                      expect(shot.aggregate).to.eql(snap3.aggregate);
                      expect(shot.context).to.eql(snap3.context);
                      expect(shot.commitStamp.getTime()).to.eql(snap3.commitStamp.getTime());
                      expect(shot.revision).to.eql(snap3.revision);
                      expect(shot.version).to.eql(snap3.version);
                      expect(shot.data.mySnappi).to.eql(snap3.data.mySnappi);

                      done();
                    });

                  });

                });

              });
              
              describe('with a revision that already exists but with a newer version', function () {

                it('it should return the correct snapshot', function (done) {

                  store.getSnapshot({ aggregateId: '920193847313131313', aggregate: 'myCoolAggregate2', context: 'myCoolContext' }, -1, function (err, shot) {
                    expect(err).not.to.be.ok();
                    expect(shot.id).to.eql(snap8.id);
                    expect(shot.aggregateId).to.eql(snap8.aggregateId);
                    expect(shot.aggregate).to.eql(snap8.aggregate);
                    expect(shot.context).to.eql(snap8.context);
                    expect(shot.commitStamp.getTime()).to.eql(snap8.commitStamp.getTime());
                    expect(shot.revision).to.eql(snap8.revision);
                    expect(shot.version).to.eql(snap8.version);
                    expect(shot.data.mySnappi).to.eql(snap8.data.mySnappi);

                    done();
                  });

                });
                
              });
              
            });
            
          });
          
        });
        
      });
      
    });
    
  });

});
  