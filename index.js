var argv= require('optimist').argv, 
    _= require('underscore'),
    mw= require('./server/middleware'),
    express= require('express');

var node= _.defaults(argv,{ host: '127.0.0.1', port: 3000, path: './data', cap: { n: 3, w: 2, r: 2 } }),
    bucket= require('./server/bucket'), 
    app = express();

node.string= [node.host,node.port].join(':');

app.use(mw.log);

require('./server/swim')(app,node,argv);
bucket(app,node);
require('./server/data')(app,node);

app.listen(node.port,node.host);
console.log('listening '+node.string);
