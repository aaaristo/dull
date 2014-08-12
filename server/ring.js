var HashRing= require('hashring'),
    _= require('underscore');

module.exports= function (app,node)
{
    node.ring= new HashRing([node.string]);

    node.gossip.on('join',function (server)
    {
        node.ring.add(server.string);
        console.log(server.string,'joined');
    }); 

    node.gossip.on('leave',function (server)
    {
        node.ring.remove(server.string);
        console.log(server.string,'leaved');
    }); 

    node.gossip.on('fail',function (server)
    {
        node.ring.remove(server.string);
        console.log(server.string,'failed');
    }); 

    node.ring.nodes= function ()
    {
        return _.pluck(node.ring.continuum().servers,'string');
    };
};
