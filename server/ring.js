var HashRing= require('hashring'),
    _= require('underscore');

module.exports= function (app,node,opts)
{
    opts= _.defaults(opts,{ vnode_count: 40, max_cache_size: 5000 });

    node.ring= new HashRing([node.string],'sha1',
                            {
                                'vnode count': opts.vnode_count,
                                'compatibility': 'hash_ring',
                                'replicas': opts.replicas,
                                'max cache size': opts.max_cache_size
                            });

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
