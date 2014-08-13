#!/usr/local/bin/node

var argv= require('optimist').argv, 
    _= require('underscore'),
    mw= require('./server/middleware'),
    express= require('express'),
    swim= require('express-swim');

var cap= _.defaults(argv.cap || {}, { n: 3, w: 2, r: 2 }), 
    node= _.defaults(argv,{ host: '127.0.0.1',
                            port: 3000,
                            path: './data',
                             cap: cap }),
    app = express();

node.string= [node.host,node.port].join(':');

var swimApp= swim(node.string,_.extend(argv.swim || {},{ base: '/gossip' }));

app.use(mw.log);
app.use('/gossip',swimApp);

node.gossip= swimApp.swim;

require('./server/ring')(app,node,_.extend(argv.ring || {},{ replicas: cap.n }));
require('./server/buckets')(app,node);
require('./server/data')(app,node);
//require('./server/crdt')(app,node);

app.listen(node.port,node.host);
console.log('listening '+node.string);

if (argv.join) node.gossip.join(argv.join);

