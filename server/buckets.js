var mw= require('./middleware'),
    ut= require('./util'),
    async= require('async'),
    multilevel= require('multilevel-http-temp'),
    JSONStream= require('JSONStream'),
    _= require('underscore'),
    map=  require('map-stream'),
    rimraf=  require('rimraf');

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

           node.buckets.get= function (req, res, next)
           {
              var bucket= { name: req.params.bucket, opts: { cap: {} } };

              db.get(bucket.name,function (err,data)
              {
                 if (err)
                   next(err); 
                 else
                 {
                   try
                   {
                      _.extend(bucket.opts,JSON.parse(data));
                   }
                   catch (ex)
                   {
                      console.log(ex);
                   }
                     
                   req.bucket= bucket;
                   next();
                 }
              });
           };

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

    // when a node joins start gossiping about buckets
    node.gossip.on('join',function (server)
    {
         bucketsApp.db.readStream().on('data',function (data)
         {
              var bucket= { name: data.key, opts: ut.json(data.value) };
              node.gossip.send('bucket_put',bucket);
         });
    });

    node.gossip.on('bucket_put',function (bucket)
    {
        node.buckets.db.put(bucket.name, JSON.stringify(bucket.opts), function (err)
        {
             if (err) return console.log(err);

             if (!_.filter(app._router.stack,filter(bucket.name))[0])
               mount(bucket.name);
        });
    });

    node.gossip.on('bucket_delete',function (bucket)
    {
        node.buckets.db.del(bucket, function (err)
        {
            if (err) return console.log(err);
            umount(bucket);    
            rimraf.sync(node.path+'/'+bucket); 
        });
    });

    app.get('/bucket',function (req, res)
    {
         bucketsApp.db
                   .readStream()
                   .pipe(map(function (data,cb)
                   {
                      cb(null,{ name: data.key, opts: ut.json(data.value) });
                   }))
                   .pipe(JSONStream.stringify());
    });

    app.put('/bucket/:bucket', mw.json, function (req,res)
    {
        node.gossip.send('bucket_put',{ name: req.params.bucket, opts: req.json });
        res.end();
    });

    app.delete('/bucket/:bucket', function (req,res)
    {
        node.gossip.send('bucket_delete',req.params.bucket);
        res.end();
    });

/*
    process.on('exit', close);
    process.on('SIGINT', process.exit);
    process.on('SIGTERM', process.exit);
    process.on('SIGHUP', process.exit);
*/
};
