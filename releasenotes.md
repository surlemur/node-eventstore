#### [v1.3.0](https://github.com/adrai/node-eventstore/compare/v1.2.0...v1.3.0)
- mongodb: added possibility to repair failed transaction (insert of multiple events) from outside

#### [v1.2.0](https://github.com/adrai/node-eventstore/compare/v1.1.7...v1.2.0)
- performance improvements in inmemory and mongodb store [#31](https://github.com/adrai/node-eventstore/pull/31) thanks to [surlemur](https://github.com/surlemur)
- IMPORTANT for mongodb: removed data compatability for events older v1.0.0

#### [v1.1.7](https://github.com/adrai/node-eventstore/compare/v1.1.6...v1.1.7)
- performance improvements in inmemory store

#### [v1.1.6](https://github.com/adrai/node-eventstore/compare/v1.1.5...v1.1.6)
- fix inmemory store

#### [v1.1.5](https://github.com/adrai/node-eventstore/compare/v1.1.4...v1.1.5)
- fix usage with own db implementation [#29](https://github.com/adrai/node-eventstore/pull/29)

#### [v1.1.4](https://github.com/adrai/node-eventstore/compare/v1.1.2...v1.1.4)
- fix usage with own db implementation [#27](https://github.com/adrai/node-eventstore/issues/27)

#### [v1.1.2](https://github.com/adrai/node-eventstore/compare/v1.1.1...v1.1.2)
- azure-table: fix issue in getEvents [#23](https://github.com/adrai/node-eventstore/pull/23) thanks to [rvin100](https://github.com/rvin100)

#### [v1.1.1](https://github.com/adrai/node-eventstore/compare/v1.1.0...v1.1.1)
- azure-table storage optimization [#22](https://github.com/adrai/node-eventstore/pull/22) thanks to [sbiaudet](https://github.com/sbiaudet) and [rvin100](https://github.com/rvin100)

#### [v1.1.0](https://github.com/adrai/node-eventstore/compare/v1.0.5...v1.1.0)
- added azure-table support [#21](https://github.com/adrai/node-eventstore/pull/21) thanks to [sbiaudet](https://github.com/sbiaudet)

#### v1.0.5
- mongodb get all events fix [#20](https://github.com/adrai/node-eventstore/pull/20) thanks to [nikolaylukyanchuk](https://github.com/nikolaylukyanchuk)

#### v1.0.4
- mongodb get all events fix [#20](https://github.com/adrai/node-eventstore/pull/20) thanks to [nikolaylukyanchuk](https://github.com/nikolaylukyanchuk)

#### v1.0.3
- little fix for redis

#### v1.0.2
- optimized indexes

#### v1.0.1
- optimized getSnapshot when using versioning of same revision

#### v1.0.0
- refactored whole module
- added possibility to define aggregateId, aggregate and context
- added a lot of tests
- stabilized everything
- optimized performance
- mongodb legacy data should be usable to (so you can update from eventstore.mongodb to eventstore) without migrating data
- IMPORTANT: changed API!!!

#### v0.7.0
- make using of eventDispatcher configurable
- map getUndispatchedEvents and setEventToDispatched to eventstore

#### v0.6.2
- optimized storage initialization

#### v0.6.1
- forking of event dispatching is configurable now

#### v0.6.0
- removed couchDb implementation
- rewritten tests in mocha and expect.js
- updated to node.js 0.6.15

#### v0.5.0
- simplified API for storage usage
- if possible fork dispatching to own childprocess
- optimized lastRevision handling

#### v0.3.0
- eventstreams
- snapshoting
- get all events with paging for replay
- console.logger
- db implementations for mongoDb, couchDb, redis
