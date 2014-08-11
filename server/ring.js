var HashRing= require('hashring');

module.exports= function (app,node)
{
    node.ring= new HashRing([node.string]);

    node.swim.on('join',function (server)
    {
        node.ring.add(server.string);
    }); 

    node.swim.on('leave',function (server)
    {
        node.ring.remove(server.string);
    }); 

    node.ring.nodes= function ()
    {
        return _.pluck(node.continuum().servers,'string');
    };
};
