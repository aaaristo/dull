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
        },
        rering= function (nodes)
        {
           node.ring= new HashRing(nodes);
        };

    rering(node.string);
    
    app.get('/dull/nodes', function (req, res)
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
                  url:     'http://'+node+'/nodes',
                  body:    JSON.stringify(all)
               },done);
           else
               request.post
               ({
                  headers: {'Content-Type': 'text/plain'},
                  url:     'http://'+node+'/node',
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
           request.del('http://'+node+'/node/'+req.params.node,done);
        },
        function (err)
        {
           if (err)
             res.send(500,err);
           else
             res.end()
        });
    });

    app.post('/node', mw.text, function (req,res)
    {
            console.log('adding node',req.text);
            node.ring.add(req.text);
            res.end();    
    });

    app.delete('/node/:node', function (req,res)
    {
            console.log('deleting node',req.params.node);
            node.ring.remove(req.params.node);
            res.end();
    });

    app.post('/nodes', mw.json, function (req,res)
    {
            console.log('setting nodes',req.json);
            rering(req.json);
            res.end();    
    });

};
