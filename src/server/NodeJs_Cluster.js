//! BEGIN_MODULE()

//! REPLACE_BY("// Copyright 2016 Claude Petit, licensed under Apache License version 2.0\n", true)
// doodad-js - Object-oriented programming framework
// File: NodeJs_Cluster.js - Cluster tools extension for NodeJs
// Project home: https://github.com/doodadjs/
// Author: Claude Petit, Quebec city
// Contact: doodadjs [at] gmail.com
// Note: I'm still in alpha-beta stage, so expect to find some bugs or incomplete parts !
// License: Apache V2
//
//	Copyright 2016 Claude Petit
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

module.exports = {
	add: function add(DD_MODULES) {
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
					nodejsCluster = nodejs.Cluster,
					
					nodeCluster = require('cluster');

				
				types.complete(_shared.Natives, {
					globalSetImmediate: global.setImmediate,
					processNextTick: process.nextTick,
				});
				
				
				nodejsCluster.ADD('ClusterMessageTypes', types.freezeObject(types.nullObject({
					Request: 0,
					Response: 1,
					Notify: 2,
					Console: 3,
					Ping: 4,
					Pong: 5,
				})));
				
				nodejsCluster.REGISTER(ipc.Request.$extend(
				{
					$TYPE_NAME: 'ClusterMessengerRequest',
					$TYPE_UUID: '' /*! INJECT('+' + TO_SOURCE(UUID('ClusterMessengerRequest')), true) */,
					
					msg: doodad.PUBLIC(doodad.READ_ONLY(  null  )),
					
					__ended: doodad.PROTECTED(false),
					
					create: doodad.OVERRIDE(function(msg, server, method, /*optional*/args, /*optional*/session) {
						if (root.DD_ASSERT) {
							root.DD_ASSERT(types.isObject(msg), "Invalid message.");
						};
						this._super(server, method, args, session);
						_shared.setAttribute(this, 'msg', msg);
					}),
					
					end: doodad.OVERRIDE(function end(/*optional*/result) {
						if (!this.__ended) {
							this.__ended = true;
							if (this.msg.type === nodejsCluster.ClusterMessageTypes.Request) {
								this.server.send({
									id: this.msg.id,
									type: nodejsCluster.ClusterMessageTypes.Response,
									result: doodad.PackedValue.$pack(result),
								}, {noResponse: true, worker: this.msg.worker});
							};
						};
						
						throw new server.EndOfRequest();
					}),

					respondWithError: doodad.OVERRIDE(function respondWithError(ex) {
						if (this.__ended) {
							throw new server.EndOfRequest();
						};
						this.onError(new doodad.ErrorEvent(ex));
						this.end(ex);
					}),
				}));
				

				nodejsCluster.REGISTER(types.createErrorType('QueueLimitReached', ipc.Error, function(/*optional*/message, /*optional*/params) {
					this._super.call(this._this, message || "Message queue limit reached.", params);
				}));
				
				nodejsCluster.REGISTER(doodad.Object.$extend(
									ipcInterfaces.IServer,
									ipcMixIns.IClient,
									ioInterfaces.IConsole,
									mixIns.NodeEvents,
				{
					$TYPE_NAME: 'ClusterMessenger',
					$TYPE_UUID: '' /*! INJECT('+' + TO_SOURCE(UUID('ClusterMessenger')), true) */,

					defaultTTL: doodad.PUBLIC(  1000 * 60 * 60 * 2  ),  // Time To Live (milliseconds)
					
					__pending: doodad.PROTECTED(  null  ),
					
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
						
						this.__pending = {};
					}),
					
					connect: doodad.OVERRIDE(function connect(/*optional*/options) {
						if (nodeCluster.isMaster) {
							this.onNodeMessage.attach(nodeCluster);
						} else {
							this.onNodeMessage.attach(process);
						};
					}),
						
					createId: doodad.PROTECTED(function createId() {
						let ok = false,
							id;
						for (var i = 0; i < 100; i++) {
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
						for (var i = 0; i < ids.length; i++) {
							const id = ids[i],
								msg = this.__pending[id],
								time = process.hrtime(msg.time),
								diff = (time[0] + (time[1] / 1e9)) * 1e3;
							if (diff >= msg.ttl) {
								delete this.__pending[id];
								if (msg.callback) {
									try {
										(function(msg) {
											_shared.Natives.processNextTick(doodad.Callback(this, function() {
												msg.callback(new types.TimeoutError("TTL expired."), null, (nodeCluster.isMaster ? nodeCluster.workers[msg.worker] : process));
											}));
										})(msg);
									} catch (ex) {
										if (ex instanceof types.ScriptInterruptedError) {
											throw ex;
										};
										break;
									};
								};
							};
						};
					}),

					send: doodad.PUBLIC(function send(msg, /*optional*/options) {
						const callback = types.get(options, 'callback');
						const noResponse = types.get(options, 'noResponse'),
							ttl = types.get(options, 'ttl', this.defaultTTL);
						if (noResponse) {
							if (types.has(this.__pending, msg.id)) {
								delete this.__pending[msg.id];
							};
						};
						const msgId = msg.id;
						let emitters,
							workers;
						if (nodeCluster.isMaster) {
							let worker = types.get(options, 'worker');
							if (types.isNothing(worker)) {
								workers = emitters = types.values(nodeCluster.workers);
							} else if (types.isInteger(worker)) {
								workers = emitters = [nodeCluster.workers[worker]];
							} else if (!types.isArray(worker)) {
								// TODO: "worker instanceof ???" if possible
								root.DD_ASSERT && root.DD_ASSERT(types.isObject(worker), "Invalid worker.");
								workers = emitters = [worker];
							};
						} else {
							emitters = [process];
							workers = [nodeCluster.worker];
						};
						for (let i = 0; i < emitters.length; i++) {
							const emitter = emitters[i],
								worker = workers[i];
							if (!msgId) {
								msg.id = this.createId();
							};
							const reqMsg = types.extend({}, msg);
							reqMsg.worker = worker.id;
							const proceedCallback = doodad.Callback(this, function(result) {
								reqMsg.proceedTime = process.hrtime();
								noResponse && callback && callback(null, result, (nodeCluster.isMaster ? nodeCluster.workers[reqMsg.worker] : process));
							});
							const result = emitter.send(msg, null, proceedCallback);
							// <PRB> Node v4 always returns "undefined". It has been fixed on Node v5.
							if ((result === undefined) || result) {
								if (!noResponse && callback) {
									this.purgePending();
									reqMsg.callback = callback;
									reqMsg.time = process.hrtime();
									reqMsg.ttl = ttl;
									this.__pending[reqMsg.id] = reqMsg;
								};
							} else {
								if (callback) {
									_shared.Natives.processNextTick(doodad.Callback(this, function() {
										callback(new nodejsCluster.QueueLimitReached(), null, (nodeCluster.isMaster ? nodeCluster.workers[reqMsg.worker] : process));
									}));
								} else {
									throw new nodejsCluster.QueueLimitReached();
								};
							};
						};
					}),
					
					callMethod: doodad.OVERRIDE(function callMethod(method, /*optional*/args, /*optional*/options) {
						const noResponse = types.get(options, 'noResponse');
						const msg = {
								type: (noResponse ? nodejsCluster.ClusterMessageTypes.Notify : nodejsCluster.ClusterMessageTypes.Request),
								method: method,
								params: doodad.PackedValue.$pack(args),
							};
						this.send(msg, options);
					}),
					
					ping: doodad.PUBLIC(function ping(/*optional*/options) {
						if (nodeCluster.isMaster) {
							this.send({
								type: nodejsCluster.ClusterMessageTypes.Ping,
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
						};
						if (types.isObject(msg)) {
							const service = this.service;
							if (service && ((msg.type === nodejsCluster.ClusterMessageTypes.Request) || (msg.type === nodejsCluster.ClusterMessageTypes.Notify))) {
								if (msg.method && !types.has(this.__pending, msg.id)) {
									const params = doodad.PackedValue.$unpack(msg.params),
										rpcRequest = new nodejsCluster.ClusterMessengerRequest(msg, this, msg.method, params/*, session*/);
									service.execute(rpcRequest)
										.then(function endRequestPromise(result) {
											rpcRequest.end(result);
										})
										.catch(rpcRequest.catchError)
										.finally(function cleanupRequestPromise() {
											if (!rpcRequest.isDestroyed()) {
												rpcRequest.destroy();
											};
										});
									//if (msg.type === nodejsCluster.ClusterMessageTypes.Request) {
										//this.__pending[msg.id] = msg;
									//};
								} else {
									this.send({
										id: msg.id,
										type: nodejsCluster.ClusterMessageTypes.Response,
										result: doodad.PackedValue.$pack(new ipc.InvalidRequest()),
									});
								};
							} else if (msg.type === nodejsCluster.ClusterMessageTypes.Response) {
								if (msg.id && types.has(this.__pending, msg.id)) {
									const reqMsg = this.__pending[msg.id];
									if (reqMsg.callback) {
										delete this.__pending[reqMsg.id];
										const result = doodad.PackedValue.$unpack(msg.result);
										reqMsg.callback(null, result, (nodeCluster.isMaster ? nodeCluster.workers[reqMsg.worker] : process));
									};
								};
							} else if (msg.type === nodejsCluster.ClusterMessageTypes.Ping) {
								if (nodeCluster.isWorker) {
									this.send({
										id: msg.id,
										type: nodejsCluster.ClusterMessageTypes.Pong,
									}, {noResponse: true});
								};
							} else if (msg.type === nodejsCluster.ClusterMessageTypes.Pong) {
								if (nodeCluster.isMaster && msg.id && types.has(this.__pending, msg.id)) {
									const reqMsg = this.__pending[msg.id];
									if (reqMsg.callback) {
										delete this.__pending[reqMsg.id];
										const time = process.hrtime(reqMsg.proceedTime);
										reqMsg.callback(null, (time[0] + (time[1] / 1e9)) * 1e3, nodeCluster.workers[reqMsg.worker]);
									};
								};
							} else if (msg.type === nodejsCluster.ClusterMessageTypes.Console) {
								if (nodeCluster.isMaster && msg.message) {
									const messageType = types.get(msg, 'messageType');
									if (['log', 'info', 'warn', 'error', 'exception'].indexOf(messageType) >= 0) {
										const fn = global.console[messageType];
										fn.call(global.console, msg.message);
									};
								};
							};
						};
					}),
					
					
					
					// Console hook
					log: doodad.OVERRIDE(ioInterfaces.IConsole, function log(raw, /*optional*/options) {
						if (nodeCluster.isWorker) {
							this[doodad.HostSymbol].send({
								type: nodejsCluster.ClusterMessageTypes.Console,
								message: raw,
								messageType: 'log',
							}, {noResponse: true});
						};
					}),
					info: doodad.OVERRIDE(ioInterfaces.IConsole, function info(raw, /*optional*/options) {
						if (nodeCluster.isWorker) {
							this[doodad.HostSymbol].send({
								type: nodejsCluster.ClusterMessageTypes.Console,
								message: raw,
								messageType: 'info',
							}, {noResponse: true});
						};
					}),
					warn: doodad.OVERRIDE(ioInterfaces.IConsole, function warn(raw, /*optional*/options) {
						if (nodeCluster.isWorker) {
							this[doodad.HostSymbol].send({
								type: nodejsCluster.ClusterMessageTypes.Console,
								message: raw,
								messageType: 'warn',
							}, {noResponse: true});
						};
					}),
					error: doodad.OVERRIDE(ioInterfaces.IConsole, function error(raw, /*optional*/options) {
						if (nodeCluster.isWorker) {
							this[doodad.HostSymbol].send({
								type: nodejsCluster.ClusterMessageTypes.Console,
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
	},
};
//! END_MODULE()