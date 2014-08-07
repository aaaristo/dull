var mw= require('./middleware'),
    async= require('async'),
    request= require('request'),
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
               var bucket= { name: data.key, opts: JSON.parse(data.value) };

               node.buckets.open[bucket.name]= multilevel.server(node.path+'/'+bucket.name,{ base: '/mnt/'+bucket.name },app).db;
           });
        };

    var s= multilevel.server(node.path+'/buckets',{ base: '/buckets' },app);

    init(s.db);

    app.put('/dull/bucket/:bucket', mw.text, function (req,res)
    {
        async.forEach(nodes(),
        function (node,done)
        {
               multilevel.client('http://'+node+'/buckets/')
               .put(req.params.bucket,req.text,done);
        },
        function (err)
        {
           if (err)
             res.send(500,err);
           else
             res.end()
        });
    });

    app.delete('/dull/bucket/:bucket', function (req,res)
    {
        async.forEach(nodes(),
        function (node,done)
        {
             multilevel.client('http://'+node+'/buckets/')
             .del(req.params.bucket,done);
        },
        function (err)
        {
           if (err)
             res.send(500,err);
           else
             res.end()
        });
    });

};

module.exports.mount= function (app,node)
{
    return function (req,res,next)
    {
       // /buckets/data/:bucket
       if (req.originalUrl.indexOf('/buckets/data/')==0)
       {
            var bucket= req.originalUrl.substring('/buckets/data/'.length).split('?')[0];
            console.log(req.method,bucket);

            if (_.contains(['POST','PUT'],req.method))
            {
              if (!_.filter(app._router.stack,function (r) { return r.route&&r.route.path=='/mnt/'+bucket+'/data'; })[0])
                node.buckets.open[bucket]= multilevel.server(node.path+'/'+bucket,{ base: '/mnt/'+bucket },app).db;
            }
            else
            if (req.method=='DELETE')
            {
              var torm= [],
                  rmRoute= function (r)
                  {
                     app._router.stack.splice(app._router.stack.indexOf(r),1);
                  };

              app._router.stack.forEach(function (r)
              {
                 if (r.route&&r.route.path.indexOf('/mnt/'+bucket+'/')==0)
                   torm.push(r); 
              });

              torm.forEach(rmRoute); 

              node.buckets.open[bucket].close(console.log); 
            }
       }

       next();
    };
};
