var HashRing= require('hashring'),
    _= require('underscore');

module.exports= function (app,node)
{
    node.ring= new HashRing([node.string]);

    node.gossip.on('join',function (server)
    {
        node.ring.add(server.string);
    }); 

    node.gossip.on('leave',function (server)
    {
        node.ring.remove(server.string);
    }); 

    node.ring.nodes= function ()
    {
        return _.pluck(node.ring.continuum().servers,'string');
    };
};
