#!/usr/local/bin/node

var argv= require('optimist').argv,
    app= require('..').server(argv),
    node= app.node;

app.listen(node.port,node.host);
console.log('listening '+node.string);

if (argv.join) node.gossip.join(argv.join);
