var mw= require('./middleware'),
    async= require('async'),
    request= require('request'),
    _= require('underscore'),
    HashRing = require('hashring');

module.exports= function (app,node)
{

    var nodes= function ()
        {
           return _.pluck(node.ring.continuum().servers,'string');
        };
    
    app.get('/dull/node', function (req, res)
    {
        res.send(nodes());
    });

    app.post('/dull/node', mw.text, function (req,res)
    {
        var all= _.union(nodes(),[req.text]);

        async.forEach(all,
        function (node,done)
        {
           if (node==req.text)
               request.post
               ({
                  headers: {'Content-Type': 'application/json'},
                  url:     'http://'+node+'/dull/coord/nodes',
                  body:    JSON.stringify(all)
               },done);
           else
               request.post
               ({
                  headers: {'Content-Type': 'text/plain'},
                  url:     'http://'+node+'/dull/coord/node',
                  body:    req.text
               },done);
        },
        function (err)
        {
           if (err)
             res.send(500,err);
           else
             res.end()
        });
    });

    app.delete('/dull/node/:node', function (req,res)
    {
        async.forEach(nodes(),
        function (node,done)
        {
           request.del('http://'+node+'/dull/coord/node/'+req.params.node,done);
        },
        function (err)
        {
           if (err)
             res.send(500,err);
           else
             res.end()
        });
    });

    app.post('/dull/coord/node', mw.text, function (req,res)
    {
            console.log('adding node',req.text);
            node.ring.add(req.text);
            res.end();    
    });

    app.post('/dull/coord/nodes', mw.json, function (req,res)
    {
            console.log('setting nodes',req.json);
            node.ring= new HashRing(req.json);
            res.end();    
    });

    app.delete('/dull/coord/node/:node', function (req,res)
    {
            console.log('deleting node',req.params.node);
            node.ring.remove(req.params.node);
            res.end();
    });

};
