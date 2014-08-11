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
        filter= function (bucket)
        {
             return function (r)
             {
                return (''+r.regexp=='/^\\/mnt\\/'+bucket+'\\/?(?=\\/|$)/i');
             }
        },
        mount= function (bucket)
        {
           var bucketApp= multilevel.server(node.path+'/'+bucket);
           node.buckets.open[bucket]= bucketApp.db;
           app.use('/mnt/'+bucket,bucketApp);
        },
        umount= function (bucket)
        {
           ut.rmf(app._router.stack,filter(bucket));
           node.buckets.open[bucket].close(console.log); 
        },
        init= function (db)
        {
           node.buckets= { db: db, open: {} };

           db.readStream().on('data',function (data)
           {
               var bucket= { name: data.key, opts: ut.json(data.value) };
               mount(bucket.name);
           });
        },
        close= function ()
        {
           console.log('closing bucket(s) leveldb...');

           _.values(node.buckets.open).forEach(function (db)
           {
              db.close(console.log);
           });

           bucketsApp.db.close(console.log);
           process.exit(1);
        };

    var bucketsApp= multilevel.server(node.path+'/buckets');
    app.use('/buckets',bucketsApp);

    init(bucketsApp.db);

    node.swim.on('bucket_put',function (bucket)
    {
        node.buckets.db.put(bucket.name, JSON.stringify(bucket.opts), function (err)
        {
             if (err) return console.log(err);

             if (!_.filter(app._router.stack,filter(bucket.name))[0])
               mount(bucket.name);
        });
    });

    node.swim.on('bucket_delete',function (bucket)
    {
        node.buckets.db.del(bucket, function (err)
        {
            if (err) return console.log(err);
            umount(bucket);    
        });
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

/*
    process.on('exit', close);
    process.on('SIGINT', process.exit);
    process.on('SIGTERM', process.exit);
    process.on('SIGHUP', process.exit);
*/
};
