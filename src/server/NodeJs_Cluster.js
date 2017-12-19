//! BEGIN_MODULE()

//! REPLACE_BY("// Copyright 2015-2017 Claude Petit, licensed under Apache License version 2.0\n", true)
// doodad-js - Object-oriented programming framework
// File: NodeJs_Cluster.js - Cluster tools extension for NodeJs
// Project home: https://github.com/doodadjs/
// Author: Claude Petit, Quebec city
// Contact: doodadjs [at] gmail.com
// Note: I'm still in alpha-beta stage, so expect to find some bugs or incomplete parts !
// License: Apache V2
//
//	Copyright 2015-2017 Claude Petit
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
	const nodeCluster = require('cluster');
//! END_IF()


const nodeClusterIsMaster = nodeCluster.isMaster,
	nodeClusterIsWorker = nodeCluster.isWorker,
	nodeClusterWorker = nodeCluster.worker,
	nodeClusterWorkers = nodeCluster.workers;


exports.add = function add(DD_MODULES) {
	DD_MODULES = (DD_MODULES || {});
	DD_MODULES['Doodad.NodeJs.Cluster'] = {
		version: /*! REPLACE_BY(TO_SOURCE(VERSION(MANIFEST("name")))) */ null /*! END_REPLACE()*/,
		create: function create(root, /*optional*/_options, _shared) {
			"use strict";

			const doodad = root.Doodad,
				types = doodad.Types,
				tools = doodad.Tools,
				files = tools.Files,
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
					
				create: doodad.OVERRIDE(function(msg, server, /*optional*/session) {
					if (root.DD_ASSERT) {
						root.DD_ASSERT(types.isObject(msg), "Invalid message.");
					};
					this._super(server, session);
					_shared.setAttribute(this, 'msg', msg);
				}),
					
				end: doodad.OVERRIDE(function end(/*optional*/result) {
					if (!this.__ended) {
						this.__ended = true;
						if (this.msg.type === cluster.ClusterMessageTypes.Request) {
							try {
								const resultFn = (types.isCancelable(result) ? 'start' : (types.isPromise(result) ? 'then' : null));
								if (resultFn) {
									return result[resultFn](
										function _resolveCb(result) {
											if (types.isError(result)) {
												result = new types.TypeError("Resolved result returned to '~0~.prototype.end' must not be an error object.", [types.getTypeName(this)]);
											};
											return this.server.sendAsync({
												id: this.msg.id,
												type: cluster.ClusterMessageTypes.Response,
												result: doodad.PackedValue.$pack(result),
											}, {noResponse: true, worker: this.msg.worker});
										}, 
										function _rejectCb(err) {
											if (!types.isError(err)) {
												err = new types.TypeError("Rejected result returned '~0~.prototype.end' must be an error object.", [types.getTypeName(this)]);
											};
											return this.server.sendAsync({
												id: this.msg.id,
												type: cluster.ClusterMessageTypes.Response,
												result: doodad.PackedValue.$pack(err),
											}, {noResponse: true, worker: this.msg.worker});
										},
										this
									);
								} else {
									return this.server.sendAsync({
										id: this.msg.id,
										type: cluster.ClusterMessageTypes.Response,
										result: doodad.PackedValue.$pack(result),
									}, {noResponse: true, worker: this.msg.worker});
								};
							} catch(ex) {
								return this.server.sendAsync({
									id: this.msg.id,
									type: cluster.ClusterMessageTypes.Response,
									result: doodad.PackedValue.$pack(ex),
								}, {noResponse: true, worker: this.msg.worker});
							};
						};
					};
						
					throw new server.EndOfRequest();
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
				}),
			}));
				

			cluster.REGISTER(types.createErrorType('QueueLimitReached', ipc.Error, function _super(/*optional*/message, /*optional*/params) {
				return [message || "Message queue limit reached.", params];
			}, null, null, null, /*! REPLACE_BY(TO_SOURCE(UUID('QueueLimitReached')), true) */ null /*! END_REPLACE() */));
				
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

					_shared.setAttribute(this, 'service', service);
						
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
					
				purgePending: doodad.PROTECTED(function purgePending() {
					const ids = types.keys(this.__pending);
					let minTTL = this.__purgeMinTTL || this.defaultTTL;
					for (let i = 0; i < ids.length; i++) {
						const id = ids[i],
							req = this.__pending[id],
							time = process.hrtime(req.time),
							diff = (time[0] + (time[1] / 1e9)) * 1e3;
						if (diff >= req.options.ttl) {
							delete this.__pending[id];
							const type = types.get(req.msg, 'type');
							if (req.options.retryDelay > 0) {
								if ((type === cluster.ClusterMessageTypes.Request) || (type === cluster.ClusterMessageTypes.Ping)) {
									delete req.msg.id;
								};
								tools.callAsync(this.send, req.options.retryDelay, this, [
									req.msg,
									tools.extend({}, req.options, {worker: req.worker})
								]);
								if (req.options.ttl < minTTL) {
									minTTL = req.options.ttl;
								};
							} else {
								const callback = req.options.callback;
								const cancelable = req.cancelable;
								if (callback || cancelable) {
									try {
										let reason = new types.TimeoutError("TTL expired.");
										tools.callAsync(function() {
											if (cancelable) {
												try {
													cancelable.cancel(reason);
												} catch(o) {
													if (callback) {
														reason = o;
													} else {
														throw o;
													};
												};
											};
											if (callback) {
												const worker = (nodeClusterIsMaster ? nodeClusterWorkers[req.worker] : nodeClusterWorker);
												if (worker) {
													callback(reason, null, worker);
												};
											};
										}, -1, this);
									} catch (ex) {
										if (ex.bubble) {
											throw ex;
										};
									};
								};
							};
						} else if (req.options.ttl - diff < minTTL) {
							minTTL = req.options.ttl - diff;
						};
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
						for (let i = 0; i < ids.length; i++) {
							if (types.has(ids, i)) {
								const id = ids[i];
								if (types.has(this.__pending, id)) {
									const req = this.__pending[id]
									delete this.__pending[id];
									const callback = req.options.callback;
									const cancelable = req.cancelable;
									if (callback || cancelable) {
										if (types.isNothing(reason)) {
											reason = new types.CanceledError();
										};
										try {
											tools.callAsync(function() {
												if (cancelable) {
													try {
														cancelable.cancel(reason);
													} catch(o) {
														if (callback) {
															reason = o;
														} else {
															throw o;
														};
													};
												};
												if (callback) {
													const worker = (nodeClusterIsMaster ? nodeClusterWorkers[req.worker] : nodeClusterWorker);
													if (worker) {
														callback(reason, null, worker);
													};
												};
											}, -1, this);
										} catch (ex) {
											if (ex.bubble) {
												throw ex;
											};
										};
									};
								};
							};
						};
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
							throw new types.Error("Invalid message ID.");
						};
					} else if ((type === cluster.ClusterMessageTypes.Response) || (type === cluster.ClusterMessageTypes.Pong)) {
						if (!id || !(id in this.__pending)) {
							return [];
						};
						delete this.__pending[id];
					};
					let emitters,
						workers;
					if (nodeClusterIsMaster) {
						if (!id && types.isNothing(worker)) {
							workers = emitters = types.values(nodeClusterWorkers);
						} else if (types.isInteger(worker)) {
							workers = emitters = [nodeClusterWorkers[worker]];
						} else if (!types.isArray(worker)) {
							// TODO: "worker instanceof ???" if possible
							root.DD_ASSERT && root.DD_ASSERT(types.isObject(worker), "Invalid worker.");
							workers = emitters = [worker];
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
							let reqMsg = msg;
							if (emitters.length > 1) {
								reqMsg = tools.extend({}, msg);
							};
							let reqId = id;
							if (!reqId) {
								reqMsg.id = reqId = this.createId();
							};
							const req = tools.nullObject({
								msg: reqMsg,
								worker: worker.id,
								options: options,
							});
							const proceedCallback = function(req) {
								return doodad.Callback(this, function(result) {
									req.proceedTime = process.hrtime();
									if (noResponse && req.options.callback) {
										const worker = (nodeClusterIsMaster ? nodeClusterWorkers[req.worker] : nodeClusterWorker);
										if (worker) {
											req.options.callback(null, result, worker);
										};
									};
								});
							};
							req.msg.ttl = ttl;
							const result = emitter.send(req.msg, null, proceedCallback(req));
							if (result) {
								if (!noResponse) {
									req.time = process.hrtime();
									this.__pending[reqId] = req;
									ids.push(reqId);
									this.purgePending();
								};
							} else {
								if (retryDelay > 0) {
									if ((type === cluster.ClusterMessageTypes.Request) || (type === cluster.ClusterMessageTypes.Ping)) {
										delete req.msg.id;
									};
									tools.callAsync(this.send, retryDelay, this, [
										req.msg,
										tools.extend({}, req.options, {worker: req.worker})
									]);
								} else {
									if (callback) {
										const worker = (nodeClusterIsMaster ? nodeClusterWorkers[req.worker] : nodeClusterWorker);
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
							throw new ipc.Error("Invalid request.");
						};
					};
					return ids;
				}),
					
				sendAsync: doodad.PUBLIC(doodad.ASYNC(function sendAsync(msg, /*optional*/options) {
					const Promise = types.getPromise();
					return Promise.create(function sendAsyncPromise(resolve, reject) {
							const type = types.get(msg, 'type');
							if (type === cluster.ClusterMessageTypes.Notify) {
								this.send(msg, options);
								resolve();
							} else {
								const timeout = types.get(options, 'timeout');
								const worker = types.get(options, 'worker');
								const rejectOnError = types.get(options, 'rejectOnError', true);
								const result = {};
								let ids = null;
								let asyncId = null;
								let count = 1;
								let resolved = false;
								if (!types.isNothing(timeout)) {
									asyncId = tools.callAsync(function() {
										const reason = new types.TimeoutError();
										this.cancel(ids, reason);
										reject(reason);
										resolved = true;
									}, timeout, this, null, true);
								};
								if (types.isNothing(worker)) {
									count = types.keys(nodeClusterWorkers).length;
								} else if (types.isArray(worker)) {
									count = worker.length;
								};
								const callback = function(err, res, worker) {
									if (asyncId) {
										asyncId.cancel();
										asyncId = null;
									};
									if (!resolved) {
										if (rejectOnError && (err || types.isError(res))) {
											reject(err || res);
											resolved = true;
										} else {
											result[worker.id] = err || res;
											count--;
											if (count <= 0) {
												resolve(result);
												resolved = true;
											};
										};
									};
								};
								ids = this.send(msg, tools.extend({}, options, {callback: callback}));
							};
						}, this);
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
//console.log(tools.toSource(msg, 15));
					} else {
//console.log(tools.toSource(msg, 15));
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
												.nodeify(function(err2, dummy) {
													if (err || err2) {
														throw err || err2;
													};
												});
										})
										.catch(rpcRequest.catchError)
										.nodeify(function cleanupRequestPromise(err, dummy) {
											types.DESTROY(rpcRequest);
											if (err) {
												throw err;
											};
										})
										.catch(tools.catchAndExit);
									if (msg.type === cluster.ClusterMessageTypes.Request) {
										const isCancelable = types.isCancelable(promise);
//console.log(isCancelable);
										this.__pending[id] = {
											msg: msg,
											options: {
												ttl: msg.ttl || this.defaultTTL, // TODO: Limit TTL
											},
											time: process.hrtime(),
											promise: (isCancelable ? promise.start() : promise), // NOTE: 'wait' will start the task
											cancelable: (isCancelable ? promise : null),
										};

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
								const req = this.__pending[id];
								delete this.__pending[id];
								const callback = types.get(req.options, 'callback');
								if (callback) {
									const worker = (nodeClusterIsMaster ? nodeClusterWorkers[req.worker] : nodeClusterWorker);
									if (worker) {
										const result = doodad.PackedValue.$unpack(msg.result);
										callback(null, result, worker);
									};
								};
							};
						} else if (type === cluster.ClusterMessageTypes.Ping) {
							if (nodeClusterIsWorker) {
								this.__pending[id] = {
									msg: msg,
									options: {
										ttl: msg.ttl || this.defaultTTL, // TODO: Limit TTL
									},
									time: process.hrtime(),
								};;
								this.send({
									id: id,
									type: cluster.ClusterMessageTypes.Pong,
								}, {noResponse: true});
							};
						} else if (type === cluster.ClusterMessageTypes.Pong) {
							if (nodeClusterIsMaster && id && types.has(this.__pending, id)) {
								const req = this.__pending[id];
								delete this.__pending[id];
								const callback = types.get(req.options, 'callback');
								if (callback) {
									const worker = nodeClusterWorkers[req.worker];
									if (worker) {
										const time = process.hrtime(req.proceedTime);
										callback(null, (time[0] + (time[1] / 1e9)) * 1e3, worker);
									};
								};
							};
						} else if (type === cluster.ClusterMessageTypes.Console) {
							const message = types.get(msg, 'message');
							if (nodeClusterIsMaster && message) {
								const messageType = types.get(msg, 'messageType', 'log');
								if (['log', 'info', 'warn', 'error', 'exception'].indexOf(messageType) >= 0) {
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
	return DD_MODULES;
};

//! END_MODULE()