var _= require('underscore'),
    mw= require('./middleware'),
    express= require('express'),
    swim= require('express-swim');

module.exports= function (argv)
{
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

    require('./ring')(app,node,_.extend(argv.ring || {},{ replicas: cap.n }));
    require('./buckets')(app,node);
    require('./data')(app,node,argv);
    //require('./crdt')(app,node);

    app.node= node;

    return app;
};
