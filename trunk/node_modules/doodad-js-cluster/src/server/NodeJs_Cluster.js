//! REPLACE_BY("// Copyright 2016 Claude Petit, licensed under Apache License version 2.0\n")
// dOOdad - Object-oriented programming framework
// File: NodeJs_Cluster.js - Cluster tools extension for NodeJs
// Project home: https://sourceforge.net/projects/doodad-js/
// Trunk: svn checkout svn://svn.code.sf.net/p/doodad-js/code/trunk doodad-js-code
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

(function() {
	var global = this;

	var exports = {};
	if (typeof process === 'object') {
		module.exports = exports;
	};
	
	exports.add = function add(DD_MODULES) {
		DD_MODULES = (DD_MODULES || {});
		DD_MODULES['Doodad.NodeJs.Cluster'] = {
			type: null,
			version: '1.2d',
			namespaces: null,
			dependencies: [
				{
					name: 'Doodad.IO',
					version: '0.2',
				}, 
				{
					name: 'Doodad.Server',
					version: '0.2',
				}, 
				{
					name: 'Doodad.Server.Ipc',
					version: '0.2',
				},
			],

			create: function create(root, /*optional*/_options) {
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

				
				const __Natives__ = {
					globalSetImmediate: global.setImmediate,
					processNextTick: process.nextTick,
				};
				
				
				nodejsCluster.ClusterMessageTypes = {
					Request: 0,
					Response: 1,
					Notify: 2,
					Console: 3,
					Ping: 4,
					Pong: 5,
				};
				
				nodejsCluster.REGISTER(ipc.Request.$extend(
				{
					$TYPE_NAME: 'ClusterMessengerRequest',
					
					msg: doodad.PUBLIC(doodad.READ_ONLY(  null  )),
					
					create: doodad.OVERRIDE(function(msg, server, method, /*optional*/args, /*optional*/session) {
						if (root.DD_ASSERT) {
							root.DD_ASSERT(types.isObject(msg), "Invalid message.");
						};
						this._super(server, method, args, session);
						this.setAttribute('msg', msg);
					}),
					
					end: doodad.OVERRIDE(function end(/*optional*/result) {
						if (this.msg.type === nodejsCluster.ClusterMessageTypes.Request) {
							this.server.send({
								id: this.msg.id,
								type: nodejsCluster.ClusterMessageTypes.Response,
								result: doodad.PackedValue.$pack(result),
							}, {noResponse: true, worker: this.msg.worker});
						};
						
						throw new server.EndOfRequest();
					}),

					respondWithError: doodad.OVERRIDE(function respondWithError(ex) {
						this.onError(new doodad.ErrorEvent(ex));
						this.end(ex);
					}),
				}));
				

				nodejsCluster.QueueLimitReached = types.createErrorType('QueueLimitReached', ipc.Error, function() {
					ipc.Error.call(this, "Message queue limit reached.");
				});
				
				nodejsCluster.REGISTER(doodad.Object.$extend(
									ipcInterfaces.IServer,
									ipcMixIns.IClient,
									ioInterfaces.IConsole,
									mixIns.NodeEvents,
				{
					$TYPE_NAME: 'ClusterMessenger',

					defaultTTL: doodad.PUBLIC(  1000 * 60 * 60 * 2  ),  // Time To Live (milliseconds)
					
					__pending: doodad.PROTECTED(  null  ),
					
					create: doodad.OVERRIDE(function(service) {
						if (types.isString(service)) {
							service = namespaces.getNamespace(service);
						};
							
						root.DD_ASSERT && root.DD_ASSERT(types._implements(service, ipcMixIns.Service), "Unknown service.");

						if (types.isType(service)) {
							service = new service();
							service = service.getInterface(ipcMixIns.Service);
						};
							
						root.DD_ASSERT && root.DD_ASSERT(types._implements(service, ipcMixIns.Service), "Invalid service.");

						this._super();

						this.setAttribute('service', service);
						
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
							if (!types.hasKey(this.__pending, id)) {
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
											__Natives__.processNextTick(new doodad.Callback(this, function() {
												msg.callback(new types.TimeoutError("TTL expired."), null, (nodeCluster.isMaster ? nodeCluster.workers[msg.worker] : process));
											}));
										})(msg);
									} catch (ex) {
										if (ex instanceof types.ScriptAbortedError) {
											throw ex;
										};
										break;
									};
								};
							};
						};
					}),

					send: doodad.PUBLIC(function send(msg, /*optional*/options) {
						const callback = types.get(options, 'callback'),
							noResponse = types.get(options, 'noResponse'),
							ttl = types.get(options, 'ttl', this.defaultTTL);
						if (noResponse) {
							if (types.hasKey(this.__pending, msg.id)) {
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
							const proceedCallback = new doodad.Callback(this, function(result) {
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
									__Natives__.processNextTick(new doodad.Callback(this, function() {
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

					onNodeMessage: doodad.NODE_EVENT('message', function onNodeMessage(context, msg) {
						if (types.isObject(msg)) {
							if ((msg.type === nodejsCluster.ClusterMessageTypes.Request) || (msg.type === nodejsCluster.ClusterMessageTypes.Notify)) {
								if (msg.method && !types.hasKey(this.__pending, msg.id)) {
									const service = this.service,
										params = doodad.PackedValue.$unpack(msg.params),
										rpcRequest = new nodejsCluster.ClusterMessengerRequest(msg, this, msg.method, params/*, session*/);
									__Natives__.globalSetImmediate(new ipc.RequestCallback(rpcRequest, this, function setImmediateHandler() {
										service.execute(rpcRequest);
									}));
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
								if (msg.id && types.hasKey(this.__pending, msg.id)) {
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
								if (nodeCluster.isMaster && msg.id && types.hasKey(this.__pending, msg.id)) {
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
										const fn = console[messageType];
										fn.call(console, msg.message);
									};
								};
							};
						};
					}),
					
					
					
					// Console hook
					log: doodad.OVERRIDE(ioInterfaces.IConsole, function log(raw, /*optional*/options) {
						if (nodeCluster.isWorker) {
							this.__host.send({
								type: nodejsCluster.ClusterMessageTypes.Console,
								message: raw,
								messageType: 'log',
							}, {noResponse: true});
						};
					}),
					info: doodad.OVERRIDE(ioInterfaces.IConsole, function info(raw, /*optional*/options) {
						if (nodeCluster.isWorker) {
							this.__host.send({
								type: nodejsCluster.ClusterMessageTypes.Console,
								message: raw,
								messageType: 'info',
							}, {noResponse: true});
						};
					}),
					warn: doodad.OVERRIDE(ioInterfaces.IConsole, function warn(raw, /*optional*/options) {
						if (nodeCluster.isWorker) {
							this.__host.send({
								type: nodejsCluster.ClusterMessageTypes.Console,
								message: raw,
								messageType: 'warn',
							}, {noResponse: true});
						};
					}),
					error: doodad.OVERRIDE(ioInterfaces.IConsole, function error(raw, /*optional*/options) {
						if (nodeCluster.isWorker) {
							this.__host.send({
								type: nodejsCluster.ClusterMessageTypes.Console,
								message: raw,
								messageType: 'error',
							}, {noResponse: true});
						};
					}),
					exception: doodad.OVERRIDE(ioInterfaces.IConsole, function exception(raw, /*optional*/options) {
						if (nodeCluster.isWorker) {
							this.__host.send({
								type: nodejsCluster.ClusterMessageTypes.Console,
								message: raw,
								messageType: 'exception',
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
	
	if (typeof process !== 'object') {
		// <PRB> export/import are not yet supported in browsers
		global.DD_MODULES = exports.add(global.DD_MODULES);
	};
}).call((typeof global !== 'undefined') ? global : ((typeof window !== 'undefined') ? window : this));