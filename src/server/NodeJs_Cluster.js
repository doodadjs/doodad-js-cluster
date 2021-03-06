//! BEGIN_MODULE()

//! REPLACE_BY("// Copyright 2015-2018 Claude Petit, licensed under Apache License version 2.0\n", true)
	// doodad-js - Object-oriented programming framework
	// File: NodeJs_Cluster.js - Cluster tools extension for NodeJs
	// Project home: https://github.com/doodadjs/
	// Author: Claude Petit, Quebec city
	// Contact: doodadjs [at] gmail.com
	// Note: I'm still in alpha-beta stage, so expect to find some bugs or incomplete parts !
	// License: Apache V2
	//
	//	Copyright 2015-2018 Claude Petit
	//
	//	Licensed under the Apache License, Version 2.0 (the "License");
	//	you may not use this file except in compliance with the License.
	//	You may obtain a copy of the License at
	//
	//		http://www.apache.org/licenses/LICENSE-2.0
	//
	//	Unless required by applicable law or agreed to in writing, software
	//	distributed under the License is distributed on an "AS IS" BASIS,
	//	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	//	See the License for the specific language governing permissions and
	//	limitations under the License.
//! END_REPLACE()

//! IF_SET("mjs")
	//! INJECT("import {default as nodeCluster} from 'cluster';");
//! ELSE()
	"use strict";

	const nodeCluster = require('cluster');
//! END_IF()


const nodeClusterIsMaster = nodeCluster.isMaster,
	nodeClusterIsWorker = nodeCluster.isWorker,
	nodeClusterWorker = nodeCluster.worker,
	nodeClusterWorkers = nodeCluster.workers;


exports.add = function add(modules) {
	modules = (modules || {});
	modules['Doodad.NodeJs.Cluster'] = {
		version: /*! REPLACE_BY(TO_SOURCE(VERSION(MANIFEST("name")))) */ null /*! END_REPLACE()*/,
		create: function create(root, /*optional*/_options, _shared) {
			const doodad = root.Doodad,
				types = doodad.Types,
				tools = doodad.Tools,
				//files = tools.Files,
				namespaces = doodad.Namespaces,
				mixIns = doodad.MixIns,
				//interfaces = doodad.Interfaces,
				//extenders = doodad.Extenders,
				io = doodad.IO,
				ioInterfaces = io.Interfaces,
				//ioMixIns = io.MixIns,
				server = doodad.Server,
				//serverInterfaces = server.Interfaces,
				ipc = server.Ipc,
				ipcInterfaces = ipc.Interfaces,
				ipcMixIns = ipc.MixIns,
				nodejs = doodad.NodeJs,
				//nodejsIO = nodejs.IO,
				//nodejsServer = nodejs.Server,
				cluster = nodejs.Cluster;


			cluster.ADD('ClusterMessageTypes', types.freezeObject(tools.nullObject({
				Request: 0,
				Response: 1,
				Notify: 2,
				Console: 3,
				Ping: 4,
				Pong: 5,
			})));

			cluster.REGISTER(ipc.Request.$extend(
				{
					$TYPE_NAME: 'ClusterMessengerRequest',
					$TYPE_UUID: '' /*! INJECT('+' + TO_SOURCE(UUID('ClusterMessengerRequest')), true) */,

					msg: doodad.PUBLIC(doodad.READ_ONLY(  null  )),

					__ended: doodad.PROTECTED(false),

					create: doodad.OVERRIDE(function create(msg, server, /*optional*/session) {
						if (root.DD_ASSERT) {
							root.DD_ASSERT(types.isObject(msg), "Invalid message.");
						};
						this._super(server, session);
						types.setAttribute(this, 'msg', msg);
					}),

					end: doodad.OVERRIDE(function end(/*optional*/result) {
						const Promise = types.getPromise();
						return Promise.try(function tryEnd() {
							if (!this.__ended) {
								this.__ended = true;
								if (this.msg.type === cluster.ClusterMessageTypes.Request) {
									return Promise.resolve(result)
										.then(function(result) {
											return this.server.sendAsync({
												id: this.msg.id,
												type: cluster.ClusterMessageTypes.Response,
												result: doodad.PackedValue.$pack(result),
											}, {noResponse: true, worker: this.msg.worker});
										}, null, this);
								};
							};
							return undefined;
						}, this)
							.catch(function(err) {
								return this.server.sendAsync({
									id: this.msg.id,
									type: cluster.ClusterMessageTypes.Response,
									result: doodad.PackedValue.$pack(err),
								}, {noResponse: true, worker: this.msg.worker});
							}, this)
							.then(function thenThrow(dummy) {
								// NOTE: 'end' must always throws 'EndOfRequest' when not rejected.
								throw new server.EndOfRequest();
							}, this);
					}),

					respondWithError: doodad.OVERRIDE(function respondWithError(ex) {
						if (this.__ended) {
							throw new server.EndOfRequest();
						};

						if (ex.critical) {
							throw ex;
						} else if (!ex.bubble) {
							ex.trapped = true;

							this.onError(ex);

							return this.end(ex);
						};

						return undefined;
					}),
				}));


			cluster.REGISTER(ipc.Error.$inherit({
				$TYPE_NAME: 'QueueLimitReached',
				$TYPE_UUID: /*! REPLACE_BY(TO_SOURCE(UUID('QueueLimitReached')), true) */ null /*! END_REPLACE() */,

				[types.ConstructorSymbol](/*optional*/message, /*optional*/params) {
					return [message || "Message queue limit reached.", params];
				},
			}));


			cluster.REGISTER(doodad.Object.$extend(
				ipcInterfaces.IServer,
				ipcMixIns.IClient,
				ioInterfaces.IConsole,
				mixIns.NodeEvents,
				{
					$TYPE_NAME: 'ClusterMessenger',
					$TYPE_UUID: '' /*! INJECT('+' + TO_SOURCE(UUID('ClusterMessenger')), true) */,

					defaultTTL: doodad.PUBLIC(  1000 * 60 * 60 * 2  ),  // Time To Live (milliseconds)

					__pending: doodad.PROTECTED(  null  ),
					__purgeMinTTL: doodad.PROTECTED(  null  ),
					__purgeTimeoutID: doodad.PROTECTED(  null  ),

					create: doodad.OVERRIDE(function(/*optional*/service) {
						if (!types.isNothing(service)) {
							if (types.isString(service)) {
								service = namespaces.get(service);
								root.DD_ASSERT && root.DD_ASSERT(types._implements(service, ipcMixIns.Service), "Unknown service.");
							};

							if (types.isType(service)) {
								service = new service();
								service = service.getInterface(ipcMixIns.Service);
							};

							root.DD_ASSERT && root.DD_ASSERT(types._implements(service, ipcMixIns.Service), "Invalid service.");
						};

						this._super();

						types.setAttribute(this, 'service', service);

						this.__pending = tools.nullObject();
					}),

					connect: doodad.OVERRIDE(function connect(/*optional*/options) {
						if (nodeClusterIsMaster) {
							this.onNodeMessage.attach(nodeCluster);
						} else {
							this.onNodeMessage.attach(process);
						};
					}),

					createId: doodad.PROTECTED(function createId() {
						let ok = false,
							id;
						for (let i = 0; i < 10; i++) {
							id = tools.generateUUID();
							if (!types.has(this.__pending, id)) {
								ok = true;
								break;
							};
						};
						if (!ok) {
							throw new types.Error("Failed to generate an unique ID.");
						};
						return id;
					}),

					createPacket: doodad.PROTECTED(function createPacket(innerMsg, msg, options) {
						return {
							innerMsg: !!innerMsg,
							msg: tools.nullObject(msg),
							options: tools.nullObject(options),
							worker: null, // worker ID
							request: null,
							time: null,
							proceedTime: null,
						};
					}),

					purgePending: doodad.PROTECTED(function purgePending() {
						const ids = types.keys(this.__pending);
						const toCancelIds = [];
						let minTTL = this.__purgeMinTTL || this.defaultTTL;
						for (let i = 0; i < ids.length; i++) {
							const id = ids[i],
								packet = this.__pending[id],
								time = process.hrtime(packet.time),
								diff = (time[0] + (time[1] / 1e9)) * 1e3;
							if (diff >= (packet.msg.ttl || this.defaultTTL)) {
								if (!packet.innerMsg && (packet.options.retryDelay > 0)) {
									delete this.__pending[id];
									const type = packet.msg.type;
									if ((type === cluster.ClusterMessageTypes.Request) || (type === cluster.ClusterMessageTypes.Ping)) {
										delete packet.msg.id;
									};
									tools.callAsync(this.send, packet.options.retryDelay, this, [
										packet.msg,
										tools.extend({}, packet.options, {worker: packet.worker})
									]);
									if (packet.msg.ttl < minTTL) {
										minTTL = packet.msg.ttl;
									};
								} else {
									toCancelIds.push(id);
								};
							} else if (packet.msg.ttl - diff < minTTL) {
								minTTL = packet.msg.ttl - diff;
							};
						};
						if (toCancelIds.length > 0) {
							this.cancel(toCancelIds, new types.TimeoutError("TTL expired."));
						};
						if (minTTL !== this.__purgeMinTTL) {
							this.__purgeMinTTL = minTTL;
							if (this.__purgeTimeoutID) {
								this.__purgeTimeoutID.cancel();
								this.__purgeTimeoutID = null;
							};
						};
						if ((minTTL > 0) && !this.__purgeTimeoutID) {
							this.__purgeTimeoutID = tools.callAsync(function() {
								this.__purgeTimeoutID = null;
								this.__purgeMinTTL = null;
								this.purgePending();
							}, minTTL, this, null, true);
						};
					}),

					cancel: doodad.PUBLIC(function cancel(ids, /*optional*/reason) {
						if (ids) {
							if (!types.isArray(ids)) {
								ids = [ids];
							};
							tools.callAsync(function() {
								const createCallbackHandler = function _createCallbackHandler(callback, reason, worker) {
									return function callCallback(err, dummy) {
										if (callback) {
											callback(err || reason, null, worker);
										};
									};
								};
								for (let i = 0; i < ids.length; i++) {
									if (types.has(ids, i)) {
										const id = ids[i];
										if (types.has(this.__pending, id)) {
											const packet = this.__pending[id];
											delete this.__pending[id];
											const callback = packet.options.callback;
											const cancelable = packet.request && packet.request.isCancelable();
											if (callback || cancelable) {
												if (types.isNothing(reason)) {
													reason = new types.CanceledError();
												};
												const worker = (nodeClusterIsMaster ? nodeClusterWorkers[packet.worker] : nodeClusterWorker);
												if (cancelable) {
													packet.request.cancel(reason)
														.nodeify(createCallbackHandler(callback, reason, worker))
														.catch(tools.catchAndExit);
												} else if (callback) {
													callback(reason, null, worker);
												};
											};
										};
									};
								};
							}, -1, this);
						};
					}),

					send: doodad.PUBLIC(function send(msg, /*optional*/options) {
						options = tools.nullObject(options);
						const callback = types.getDefault(options, 'callback', null),
							worker = types.getDefault(options, 'worker', null),
							noResponse = types.getDefault(options, 'noResponse', false),
							ttl = types.getDefault(options, 'ttl', this.defaultTTL),
							retryDelay = types.getDefault(options, 'retryDelay', 0),
							type = types.get(msg, 'type'),
							id = types.get(msg, 'id');
						if ((type === cluster.ClusterMessageTypes.Request) || (type === cluster.ClusterMessageTypes.Ping)) {
							if (id) {
								throw new types.ValueError("Invalid message ID.");
							};
						} else if (type === cluster.ClusterMessageTypes.Response) {
							if (!id || !(id in this.__pending)) {
								return [];
							};
							delete this.__pending[id];
						} else if (type === cluster.ClusterMessageTypes.Pong) {
							if (!id) {
								return [];
							};
						};
						let emitters,
							workers;
						if (nodeClusterIsMaster) {
							if (!id && types.isNothing(worker)) {
								workers = types.values(nodeClusterWorkers);
								emitters = workers;
							} else if (types.isInteger(worker)) {
								workers = [nodeClusterWorkers[worker]];
								emitters = workers;
							} else if (!types.isArray(worker)) {
								// TODO: "worker instanceof ???" if possible
								root.DD_ASSERT && root.DD_ASSERT(types.isObject(worker), "Invalid worker.");
								workers = [worker];
								emitters = workers;
							};
						} else {
							emitters = [process];
							workers = [nodeClusterWorker];
						};
						const ids = [];
						for (let i = 0; i < emitters.length; i++) {
							const emitter = emitters[i],
								worker = workers[i];
							if (emitter && worker) {
								const packet = this.createPacket(false, msg, options);
								if (!packet.msg.id) {
									packet.msg.id = this.createId();
								};
								packet.msg.ttl = ttl;
								packet.worker = worker.id;
								const proceedCallback = function(packet) {
									return doodad.Callback(this, function(result) {
										packet.proceedTime = process.hrtime();
										if (noResponse && packet.options.callback) {
											const worker = (nodeClusterIsMaster ? nodeClusterWorkers[packet.worker] : nodeClusterWorker);
											if (worker) {
												packet.options.callback(null, result, worker);
											};
										};
									});
								};
								const result = emitter.send(packet.msg, null, proceedCallback(packet));
								if (result) {
									if (!noResponse) {
										packet.time = process.hrtime();
										this.__pending[packet.msg.id] = packet;
										ids.push(packet.msg.id);
										this.purgePending();
									};
								} else {
									if (retryDelay > 0) {
										if ((type === cluster.ClusterMessageTypes.Request) || (type === cluster.ClusterMessageTypes.Ping)) {
											delete msg.id;
										};
										tools.callAsync(this.send, retryDelay, this, [
											msg,
											tools.extend({}, options, {worker: packet.worker})
										]);
									} else {
										if (callback) {
											const worker = (nodeClusterIsMaster ? nodeClusterWorkers[packet.worker] : nodeClusterWorker);
											if (worker) {
												tools.callAsync(function() {
													callback(new cluster.QueueLimitReached(), null, worker);
												}, -1, this);
											};
										} else {
											throw new cluster.QueueLimitReached();
										};
									};
								};
							} else {
								throw new ipc.InvalidRequest("Missing destination.");
							};
						};
						return ids;
					}),

					sendAsync: doodad.PUBLIC(doodad.ASYNC(function sendAsync(msg, /*optional*/options) {
						const Promise = types.getPromise();
						const type = types.get(msg, 'type');
						//const worker = types.get(options, 'worker');
						if (type === cluster.ClusterMessageTypes.Notify) {
							this.send(msg, options);
						} else {
							return Promise.create(function sendAsyncPromise(resolve, reject) {
								const timeout = types.get(options, 'timeout');
								const rejectOnError = types.get(options, 'rejectOnError', true);
								const state = {
									result: {},
									hasError: false,
									ids: null,
									timeoutId: null,
								};
								if (!types.isNothing(timeout)) {
									state.timeoutId = tools.callAsync(function() {
										const reason = new types.TimeoutError();
										this.cancel(state.ids, reason);
										reject(reason);
									}, timeout, this, null, true);
								};
								state.ids = this.send(msg, tools.extend({}, options, {callback: function sendCallback(err, res, worker) {
									if (state.timeoutId) {
										state.timeoutId.cancel();
										state.timeoutId = null;
									};
									state.count--;
									// NOTE: "worker" can be undefined when the worker has crashed !
									if (worker) {
										state.result[worker.id] = err || res;
									} else {
										state.hasError = true;
									};
									if (err || types.isError(res)) {
										state.hasError = true;
									};
									if (state.count <= 0) {
										state.count = Infinity; // Prevents the following to be executed twice
										if (rejectOnError && state.hasError) {
											// TODO: Should we box into an "SomethingError" object (where "Something" is replaced by a more appropriated word) ?
											reject(state.result);
										} else {
											resolve(state.result);
										};
									};
								}}));
								state.count = state.ids.length;
							}, this);
						};
						return undefined;
					})),

					callMethod: doodad.OVERRIDE(function callMethod(method, /*optional*/args, /*optional*/options) {
						const noResponse = types.get(options, 'noResponse');
						return this.sendAsync({
							type: (noResponse ? cluster.ClusterMessageTypes.Notify : cluster.ClusterMessageTypes.Request),
							method: method,
							params: doodad.PackedValue.$pack(args),
						}, options);
					}),

					ping: doodad.PUBLIC(function ping(/*optional*/options) {
						if (nodeClusterIsMaster) {
							return this.sendAsync({
								type: cluster.ClusterMessageTypes.Ping,
							}, options);
						};
						return undefined;
					}),

					disconnect: doodad.OVERRIDE(function disconnect() {
						this.onNodeMessage.clear();
					}),

					onNodeMessage: doodad.NODE_EVENT('message', function onNodeMessage(context, /*optional*/worker, msg, handle) {
						// <PRB> Since Node.Js 6.0, a new argument ("worker") has been PREPENDED.
						if (arguments.length <= 3) {
							handle = msg;
							msg = worker;
							worker = undefined;
							//console.log(tools.toSource(msg, {depth: 15}));
						} else {
							//console.log(tools.toSource(msg, {depth: 15}));
							msg.worker = worker;
						};
						if (nodeClusterIsMaster && !worker) {
							throw new types.NotSupported("That Node.js version is not supported. Please upgrade to >= 6.x.");
						};
						if (types.isObject(msg)) {
							const service = this.service;
							const id = types.get(msg, 'id');
							const type = types.get(msg, 'type');
							if (service && ((type === cluster.ClusterMessageTypes.Request) || (type === cluster.ClusterMessageTypes.Notify))) {
								if (id && !types.has(this.__pending, id)) {
									const method = types.get(msg, 'method');
									if (method) {
										const params = doodad.PackedValue.$unpack(types.get(msg, 'params')),
											rpcRequest = new cluster.ClusterMessengerRequest(msg, this /*, session*/);

										const promise = service.execute(rpcRequest, method, params)
											.nodeify(function endRequestPromise(err, result) {
												return rpcRequest.end(err || result)
													.nodeify(function throwErr(err2, dummy) {
														if (err || err2) {
															throw err || err2;
														};
													});
											})
											.catch(rpcRequest.catchError, rpcRequest)
											.nodeify(function cleanupRequestPromise(err, dummy) {
												types.DESTROY(rpcRequest);
												if (err) {
													throw err;
												};
											})
											.catch(tools.catchAndExit);

										if (msg.type === cluster.ClusterMessageTypes.Request) {
											types.getDefault(msg, 'ttl', this.defaultTTL);

											const packet = this.createPacket(true, msg, null);

											packet.request = rpcRequest;
											packet.time = process.hrtime();
											packet.promise = promise;

											this.__pending[id] = packet;

											this.purgePending();
										};

									} else {
										this.send({
											id: id,
											type: cluster.ClusterMessageTypes.Response,
											result: doodad.PackedValue.$pack(new ipc.InvalidRequest()),
										}, {noResponse: true});
									};
								};
							} else if (type === cluster.ClusterMessageTypes.Response) {
								if (id && types.has(this.__pending, id)) {
									const packet = this.__pending[id];
									delete this.__pending[id];
									const callback = packet.options.callback;
									if (callback) {
										const worker = (nodeClusterIsMaster ? nodeClusterWorkers[packet.worker] : nodeClusterWorker);
										if (worker) {
											const result = doodad.PackedValue.$unpack(msg.result);
											callback(null, result, worker);
										};
									};
								};
							} else if (type === cluster.ClusterMessageTypes.Ping) {
								if (nodeClusterIsWorker) {
									this.send({
										id: id,
										type: cluster.ClusterMessageTypes.Pong,
									}, {noResponse: true});
								};
							} else if (type === cluster.ClusterMessageTypes.Pong) {
								if (nodeClusterIsMaster && id && types.has(this.__pending, id)) {
									const packet = this.__pending[id];
									delete this.__pending[id];
									const callback = packet.options.callback;
									if (callback) {
										const worker = nodeClusterWorkers[packet.worker];
										if (worker) {
											const time = process.hrtime(packet.proceedTime);
											callback(null, (time[0] + (time[1] / 1e9)) * 1e3, worker);
										};
									};
								};
							} else if (type === cluster.ClusterMessageTypes.Console) {
								const message = types.get(msg, 'message');
								if (nodeClusterIsMaster && message) {
									const messageType = types.get(msg, 'messageType', 'log');
									if (['log', 'info', 'warn', 'error'].indexOf(messageType) >= 0) {
										const fn = global.console[messageType];
										fn.call(global.console, message);
									};
								};
							};
						};
					}),


					// Console hook
					log: doodad.OVERRIDE(ioInterfaces.IConsole, function log(raw, /*optional*/options) {
						if (raw && nodeClusterIsWorker) {
							this[doodad.HostSymbol].send({
								type: cluster.ClusterMessageTypes.Console,
								message: raw,
								messageType: 'log',
							}, {noResponse: true});
						};
					}),

					info: doodad.OVERRIDE(ioInterfaces.IConsole, function info(raw, /*optional*/options) {
						if (raw && nodeClusterIsWorker) {
							this[doodad.HostSymbol].send({
								type: cluster.ClusterMessageTypes.Console,
								message: raw,
								messageType: 'info',
							}, {noResponse: true});
						};
					}),

					warn: doodad.OVERRIDE(ioInterfaces.IConsole, function warn(raw, /*optional*/options) {
						if (raw && nodeClusterIsWorker) {
							this[doodad.HostSymbol].send({
								type: cluster.ClusterMessageTypes.Console,
								message: raw,
								messageType: 'warn',
							}, {noResponse: true});
						};
					}),

					error: doodad.OVERRIDE(ioInterfaces.IConsole, function error(raw, /*optional*/options) {
						if (raw && nodeClusterIsWorker) {
							this[doodad.HostSymbol].send({
								type: cluster.ClusterMessageTypes.Console,
								message: raw,
								messageType: 'error',
							}, {noResponse: true});
						};
					}),
				}));


			//return function init(/*optional*/options) {
			//};
		},
	};
	return modules;
};

//! END_MODULE()
