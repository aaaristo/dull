var mw= require('./middleware'),
    ut= require('./util'),
    _= require('underscore'),
    async= require('async'),
    uuid= require('node-uuid').v4,
    JSONStream= require('JSONStream'),
    map=  require('map-stream');

module.exports= function (app,node,argv)
{
    var sibOpts= { timeout: argv.node_timeout || 1000 },
        sib= require('./sibling')(sibOpts),
        coord= require('./coordinator')(node,sibOpts),
        vclock= require('pvclock')(argv.vclock),
        pickVclock= function (res)
        {
           return res.meta.vclock;
        },
        unique= function ()
        {
           var last, metas= [];

           return map(function (meta, cb)
           {
                if (last==undefined||last.key==meta.key)
                {
                   metas.push(meta);
                   last= meta;
                   cb();
                }   
                else
                {
                   var converged= vclock.converge(metas,'vclock'),
                       first= converged[0];

                   metas= [];
                   metas.push(meta);
                   last= meta;

                   if (converged.length>1) // conflicts
                     cb(null, first.key);
                   else
                   if (first.thumbstone) // deleted
                     cb();
                   else
                     cb(null, first.key); // resolved
                }
                
           });
        },
        readRepair= function (bucket,key,res,siblings)
        {

            // merge all vclocks
            res.meta.vclock= vclock.merge(_.union([res.meta.vclock],
                                          _.collect(siblings,function (s)
                                          {
                                             return s.meta ? s.meta.vclock : {};
                                          })));

            var meta= JSON.stringify(res.meta);

            // repair/remove siblings
            async.forEach(siblings,
            function (sibling,done)
            {
                var batch= sib.batch(bucket)
                              .put(key,res.meta.siblingId,meta,res.content);

                if (sibling.meta)
                  batch.del(key,sibling.meta.siblingId);

                batch.perform(sibling.node,function (err)
                {
                   if (err)
                     console.log('read repair','cannot repair',
                                 sibling.node,bucket,key,err);
                   else
                     console.log('read repair','repaired',
                                 sibling.node,bucket,key);

                   done();
                });
            },
            function ()
            {
               console.log('read repair','end'); 
            });
        };

    app.put('/bucket/:bucket/data/:key',
    node.buckets.get,
    mw.binary,
    mw.client,
    function (req,res,next)
    {
        var siblingId= uuid(),
            vc= vclock.increment(req.client.vclock,
                                 req.client.id),
            meta= JSON.stringify
                  ({ 
                            key: req.params.key,
                      siblingId: siblingId,
                         vclock: vc,
                           hash: ut.hash(req.binary),
                        headers: _.defaults(_.pick(req.headers,['content-type',                                                                'content-length']),
                                 { 
                                    'content-type': 'application/json',
                                    'content-length': req.binary.length
                                 })
                  });

        coord.batch(req.bucket,req.params.key)
             .put(siblingId,meta,
                  req.headers['content-type']!='application/json' 
                       ? req.binary.toString('base64') 
                       : req.binary.toString('utf8'))
             .perform(req.params.w,
              function () // w nodes
              {
                 res.setHeader('x-dull-vclock',JSON.stringify(vc));
                 res.end();
              },
              function (err,errors) // n nodes
              {
                 if (err)
                 {
                   next(err);
                   console.log('put error',err,errors);
                 }
              });
    });

    app.get('/bucket/:bucket/data/:key',
    node.buckets.get,
    function (req, res, next)
    {
        var buildResponse= function (converged)
            {
               var first= converged[0];

               if (converged.length==0)
                 res.status(404).send('Key not found');
               else
               if (converged.length==1)
               {
                   if (first.meta.thumbstone) // has been deleted
                   {
                       res.status(404);
                       res.setHeader('x-dull-vclock', JSON.stringify(first.meta.vclock)); // if you want to recreate it better to use the vclock
                       res.setHeader('x-dull-thumbstone', 'true');
                       res.send('Key not found');
                   }
                   else
                   {
                       _.keys(first.meta.headers).forEach(function (name)
                       {
                           res.setHeader(name,first.meta.headers[name]);
                       });

                       res.setHeader('x-dull-vclock', JSON.stringify(first.meta.vclock));

                       if (first.meta.headers&&first.meta.headers['content-type']!='application/json')
                         res.end(new Buffer(first.content,'base64'));
                       else
                         res.end(first.content);
                   }
               }
               else
               { 
                   var parts= [], vclocks= [];

                   converged.forEach(function (response)
                   {
                      vclocks.push(response.meta.vclock);

                      parts.push
                      ({
                          headers: response.meta.thumbstone ?
                                    { 'x-dull-vclock': JSON.stringify(response.meta.vclock),
                                      'x-dull-thumbstone': 'true' } :
                                            _.extend(response.meta.headers,
                                                { 'x-dull-vclock': JSON.stringify(response.meta.vclock) },
                                                response.meta.headers['content-type']!='application/json' ? 
                                                { 'Content-Transfer-Encoding': 'base64' } : undefined),
                          body: response.content
                      });
                   });

                   res.status(300);
                   res.setHeader('x-dull-vclock', JSON.stringify(vclock.merge(vclocks)));
                   ut.multipart(res,parts);
               }
            };

        coord.list(req.params.r,
                   req.bucket,
                   req.params.key,
        function (responses) // r nodes responded
        {
           var found= _.filter(responses,function (r) { return !r.notfound; });

           buildResponse(vclock.converge(found,pickVclock));
        },
        function (err, responses) // n nodes responded
        {
           if (err)
           {
               next(err);
               console.log('get error',err,responses);
           }
           else
           {
              var needRepair= [],
                  found= _.filter(responses,
                           function (r) { return !r.notfound; }),
                  converged= vclock.converge(found,pickVclock,
                             function (res)
                             {
                                 needRepair.push({ node: res.node,
                                                   meta: res.meta });
                             });

              if (converged.length==1) // we have a value
              {
                 var notfound= _.filter(responses,
                                 function (r) { return !!r.notfound; });

                 notfound.forEach(function (res)
                 { 
                    needRepair.push({ node: res.node });
                 });

                 if (needRepair.length>0)
                   readRepair(req.params.bucket,
                              req.params.key,
                              converged[0],
                              needRepair);
              }
           }

        });
    });

    app.delete('/bucket/:bucket/data/:key',
    node.buckets.get,
    mw.client,
    function (req,res)
    {
        var siblingId= uuid(),
            vc= vclock.increment(req.client.vclock,
                                 req.client.id),
            meta= JSON.stringify
                  ({
                            key: req.params.key,
                      siblingId: siblingId,
                         vclock: vc,
                     thumbstone: true
                  });
            
        coord.batch(req.bucket,req.params.key)
             .put(siblingId,meta,'#')
             .perform(req.params.w,
              function () // w nodes
              {
                 res.setHeader('x-dull-vclock',JSON.stringify(vc));
                 res.end();
              },
              function (err,errors) // all nodes
              {
                 if (err)
                 {
                   next(err);
                   console.log('put error',err,errors);
                 }
              });
    });

    app.get('/bucket/:bucket/keys', 
    node.buckets.get,
    function (req, res, next)
    {
        res.type('json');

        coord.keys(req.bucket)
             .on('error',function (err)
             {
                next(err);
             })
             .pipe(unique())
             .pipe(JSONStream.stringify())
             .pipe(res);
    });
};
