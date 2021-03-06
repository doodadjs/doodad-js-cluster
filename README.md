Cluster manager (alpha).

[![NPM Version][npm-image]][npm-url]
 
<<<< PLEASE UPGRADE TO THE LATEST VERSION AS OFTEN AS POSSIBLE >>>>

## Installation

```bash
$ npm install @doodad-js/cluster
```

## Features

  -  IPC master <==> worker.
  -  Console to master.
  -  Ping workers.

## Example (IPC) :

# master.js
```js
    module.exports = {
        run: function run(root) {
            const nodeJsCluster = require('cluster');
        
            const doodad = root.Doodad,
                server = doodad.Server,
                ipc = server.Ipc,
                cluster = doodad.NodeJs.Cluster;

            const MyService = doodad.Object.$extend(
                    ipc.MixIns.Service,
            {
                $TYPE_NAME: 'MyService',
    
                hello: ipc.CALLABLE(function hello(request) {
                    request.end("Hello world !");
                }),
            });

            const messenger = new cluster.ClusterMessenger(MyService);
            messenger.connect();
        
            nodeJsCluster.fork();
            nodeJsCluster.fork();
        },
    };
```
    
# worker.js
```js
    module.exports = {
        run: function run(root) {
            const nodeJsCluster = require('cluster');
            
            const doodad = root.Doodad,
                cluster = doodad.NodeJs.Cluster;

            const messenger = new cluster.ClusterMessenger();
            messenger.connect();

            function proceed() {
                setTimeout(function() {
                    messenger.callMethod('hello', [], {callback: function(err, result) {
                        console.log('<W:' + String(nodeJsCluster.worker.id) + '> ' + result);
                        proceed();
                    }});
                }, 500);
            };
            
            proceed();
        },
    };
```

# index.js
```js
    const nodeJsCluster = require('cluster');

	const modules = {};
	require('@doodad-js/io').add(modules);
	require('@doodad-js/server').add(modules);
	require('@doodad-js/ipc').add(modules);
	require('@doodad-js/cluster').add(modules);
	
    require('@doodad-js/core').createRoot(modules)
		.then(root => {
			if (nodeJsCluster.isMaster) {
				require('./master.js').run(root);
			} else {
				require('./worker.js').run(root);
			};
		})
		.catch(err => {
			console.error(err);
		});
```

## Example (Console) :

# master.js
```js
	module.exports = {
		run: function run(root) {
			const nodeJsCluster = require('cluster');
			
			const cluster = root.Doodad.NodeJs.Cluster;

			const messenger = new cluster.ClusterMessenger();
			messenger.connect();
			
			nodeJsCluster.fork();
			nodeJsCluster.fork();
		},
	};
```
    
# worker.js
```js
	module.exports = {
		run: function run(root) {
			const nodeJsCluster = require('cluster');
			
			const doodad = root.Doodad,
				cluster = doodad.NodeJs.Cluster,
				ioInterfaces = doodad.IO.Interfaces;

			const messenger = new cluster.ClusterMessenger();
			messenger.connect();

			const con = messenger.getInterface(ioInterfaces.IConsole);

			function proceed() {
				setTimeout(function() {
					con.log('<W:' + nodeJsCluster.worker.id + '> Hello world !');
					
					proceed();
				}, 500);
			};
			
			proceed();
		},
	};
```

# index.js
```js
    const nodeJsCluster = require('cluster');

    const modules = {};
	require('@doodad-js/io').add(modules);
	require('@doodad-js/server').add(modules);
	require('@doodad-js/ipc').add(modules);
	require('@doodad-js/cluster').add(modules);
	
    require('@doodad-js/core').createRoot(modules)
		.then(root => {
			if (nodeJsCluster.isMaster) {
				require('./master.js').run(root);
			} else {
				require('./worker.js').run(root);
			};
		})
		.catch(err => {
			console.error(err);
		});
```

## Other available packages

  - **@doodad-js/core**: Object-oriented programming framework (release)
  - **@doodad-js/cluster**: Cluster manager (alpha)
  - **@doodad-js/dates**: Dates formatting (beta)
  - **@doodad-js/http**: Http server (alpha)
  - **@doodad-js/http_jsonrpc**: JSON-RPC over http server (alpha)
  - **@doodad-js/io**: I/O module (alpha)
  - **@doodad-js/ipc**: IPC/RPC server (alpha)
  - **@doodad-js/json**: JSON parser (alpha)
  - **@doodad-js/loader**: Scripts loader (beta)
  - **@doodad-js/locale**: Locales (beta)
  - **@doodad-js/make**: Make tools for doodad (alpha)
  - **@doodad-js/mime**: Mime types (beta)
  - **@doodad-js/minifiers**: Javascript minifier used by doodad (alpha)
  - **@doodad-js/safeeval**: SafeEval (beta)
  - **@doodad-js/server**: Servers base module (alpha)
  - **@doodad-js/templates**: HTML page templates (alpha)
  - **@doodad-js/terminal**: Terminal (alpha)
  - **@doodad-js/test**: Test application
  - **@doodad-js/unicode**: Unicode Tools (beta)
  - **@doodad-js/widgets**: Widgets base module (alpha)
  - **@doodad-js/xml**: XML Parser (beta)
  
## License

  [Apache-2.0][license-url]

[npm-image]: https://img.shields.io/npm/v/@doodad-js/cluster.svg
[npm-url]: https://npmjs.org/package/@doodad-js/cluster
[license-url]: http://opensource.org/licenses/Apache-2.0