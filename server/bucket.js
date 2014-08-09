var mw= require('./middleware'),
    ut= require('./util'),
    async= require('async'),
    multilevel= require('multilevel-http'),
    _= require('underscore');

module.exports= function (app,node)
{
    var nodes= function ()
        {
           return _.pluck(node.ring.continuum().servers,'string');
        },
        init= function (db)
        {
           node.buckets= { db: db, open: {} };

           db.readStream().on('data',function (data)
           {
               var bucket= { name: data.key, opts: ut.json(data.value) };

               node.buckets.open[bucket.name]= multilevel.server(node.path+'/'+bucket.name,{ base: '/mnt/'+bucket.name },app).db;
           });
        };

    var s= multilevel.server(node.path+'/buckets',{ base: '/buckets' },app);

    init(s.db);

    node.swim.on('bucket_put',function (bucket)
    {
        node.buckets.db.put(bucket.name, JSON.stringify(bucket.opts), function (err)
        {
             if (err) return console.log(err);

             if (!_.filter(app._router.stack,
                 function (r) { return r.route&&r.route.path=='/mnt/'+bucket.name+'/data'; })[0])
               node.buckets.open[bucket.name]= multilevel
                    .server(node.path+'/'+bucket.name,{ base: '/mnt/'+bucket.name },app).db;
        });
    });

    node.swim.on('bucket_delete',function (bucket)
    {
        // node.buckets.db del
        ut.rmf(app._router.stack,function (r)
        {
             return (r.route&&r.route.path.indexOf('/mnt/'+bucket+'/')==0);
        });

        node.buckets.open[bucket].close(console.log); 
    });

    app.put('/dull/bucket/:bucket', mw.json, function (req,res)
    {
        node.swim.send('bucket_put',{ name: req.params.bucket, opts: req.json });
        res.end();
    });

    app.delete('/dull/bucket/:bucket', function (req,res)
    {
        node.swim.send('bucket_delete',req.params.bucket);
        res.end();
    });

};
