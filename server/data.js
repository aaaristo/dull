var mw= require('./middleware'),
    ut= require('./util'),
    _= require('underscore'),
    async= require('async'),
    uuid= require('node-uuid').v4,
    multilevel= require('multilevel-http-temp'),
    merge= require('mergesort-stream'),
    JSONStream= require('JSONStream'),
    map=  require('map-stream');

const KS= '::',                   // key separator
      LC= '\xff';                 // last character

module.exports= function (app,node,argv)
{
    var vclock= require('pvclock')(argv.vclock),
        vclockDesc= function (a,b) { return vclock.desc(a.meta.vclock,b.meta.vclock); },
        compareKeys= function (key1, key2)
        {
              if (key1 > key2) return 1;
              else if (key1 < key2) return -1;
              return 0;
        },
        unique= function ()
        {
           var last;

           return map(function (data, cb)
           {
                last!==data ? cb(null, data) : cb();
                last= data;
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

            // repair/remove siblings
            async.forEach(siblings,
            function (sibling,done)
            {
                var ops= [{ // key
                              key: ['K',key,'_'].join(KS),
                            value: key,
                             type: 'put'
                          },
                          { // value meta 
                              key: ['V',key,res.meta.siblingId,'M'].join(KS),
                            value: JSON.stringify(res.meta),
                             type: 'put'
                          },
                          { // value content
                              key: ['V',key,res.meta.siblingId,'C'].join(KS),
                            value: res.content,
                             type: 'put'
                          }];

                if (sibling.meta)
                  ops.push.apply(ops,
                    [{ // sibling meta
                          key: ['V',key,sibling.meta.siblingId,'M'].join(KS),
                         type: 'del'
                    },
                    { // sibling content
                          key: ['V',key,sibling.meta.siblingId,'C'].join(KS),
                         type: 'del'
                    }]);

                multilevel.client('http://'+sibling.node+'/mnt/'+bucket+'/').batch(ops,
                function (err,res)
                {
                   if (err)
                     console.log('read repair','cannot repair',node.string,bucket,key,err);
                   else
                     console.log('read repair','repaired',node.string,bucket,key);

                   done();
                });
            },
            function ()
            {
               console.log('read repair','end'); 
            });
        };

    app.put('/dull/bucket/:bucket/data/:key', node.buckets.get, mw.binary, function (req,res)
    {
        var n= req.bucket.opts.cap.n || node.cap.n,
            w= req.query.w || node.cap.w,
            nodes= node.ring.range(req.params.key,n),
            errors= [],
            siblingId= uuid(),
            client= {
                       id: req.headers['x-dull-clientid'] ? req.headers['x-dull-clientid'] : uuid(),
                       vclock: req.headers['x-dull-vclock'] ? JSON.parse(req.headers['x-dull-vclock']) : {}
                    },
            success= _.after(w,_.once(function ()
                     {
                         res.end();
                     }));

        if (n < w)
          res.status(500).send('the cluster has n='+n+' you cannot specify a greater w. ('+w+')');
        else
        if (nodes.length < w)
          res.status(500).send('we have only '+nodes.length+' nodes active, and you specified w='+w);
        else
        async.forEach(nodes,
        function (node,done)
        {
            multilevel.client('http://'+node+'/mnt/'+req.params.bucket+'/')
            .batch([{ // key
                          key: ['K',req.params.key,'_'].join(KS),
                        value: req.params.key,
                         type: 'put'
                    },
                    { // value meta 
                          key: ['V',req.params.key,siblingId,'M'].join(KS),
                        value: JSON.stringify
                               ({ 
                                  siblingId: siblingId,
                                     vclock: vclock.increment(client.vclock,client.id),
                                       hash: ut.hash(req.binary),
                                    headers: _.defaults(_.pick(req.headers,['content-type','content-length']),
                                             { 
                                                'content-type': 'application/json',
                                                'content-length': req.binary.length
                                             })
                               }),
                         type: 'put'
                    },
                    { // value content
                          key: ['V',req.params.key,siblingId,'C'].join(KS),
                        value: req.headers['content-type']!='application/json' ? req.binary.toString('base64') : req.binary.toString('utf8'),
                         type: 'put' 
                    }],
            function (err,res)
            {
               if (err)
                 errors.push({ node: node, err: err, statusCode: res.statusCode });
               else
                 success();

               done();
            });
        },
        function ()
        {
           if (errors.length > (n-w) || errors.length == nodes.length)
             res.send(500,errors);
           else
             console.log('put success'); 
        });
    });

    app.get('/dull/bucket/:bucket/data/:key', node.buckets.get, function (req, res)
    {
        var n= req.bucket.opts.cap.n || node.cap.n,
            r= req.query.r || node.cap.r,
            nodes= node.ring.range(req.params.key,n),
            errors= [],
            responses= [],
            success= _.after(r,_.once(function () // after r replicas respond, return to the client
                     {
                           var found= _.filter(responses,function (r) { return !r.notfound; });
                           found.sort(vclockDesc);

                           var first= found.shift(), repaired = first ? [first] : [];

                           found.forEach(function (response, index)
                           {
                              if (vclock.compare(first.meta.vclock, response.meta.vclock) == vclock.DIFFERENT)
                                repaired.push(response);
                           });

                           
                           if (repaired.length==0)
                             res.status(404).send('Key not found');
                           else
                           if (repaired.length==1)
                           {
                               _.keys(first.meta.headers).forEach(function (name)
                               {
                                   res.setHeader(name,first.meta.headers[name]);
                               });

                               res.setHeader('x-dull-vclock', JSON.stringify(first.meta.vclock));

                               if (first.meta.headers['content-type']=='application/json')
                                 res.end(first.content);
                               else
                                 res.end(new Buffer(first.content,'base64'));
                           }
                           else
                           { 
                               var parts= [], vclocks= [];

                               repaired.forEach(function (response)
                               {
                                  vclocks.push(response.meta.vclock);

                                  parts.push
                                  ({
                                      headers: _.extend(response.meta.headers,
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
                     }));

        if (n < r)
          res.status(500).send('the cluster has n='+n+' you cannot specify a greater r. ('+r+')');
        else
        if (nodes.length < r)
          res.status(500).send('we have only '+nodes.length+' nodes active, and you specified r='+r);
        else
        async.forEach(nodes,
        function (node,done)
        {
            var parts= [];
                console.log(node);

            multilevel.client('http://'+node+'/mnt/'+req.params.bucket+'/')
            .valueStream({ start: ['V',req.params.key,''].join(KS),
                             end: ['V',req.params.key,LC].join(KS) })
            .on('error', function (err)
            {
                errors.push({ node: node, err: err });
            })
            .on('data', function (data)
            {
                parts.push(data); 
            })
            .on('end', function ()
            {
                if (parts.length<2)
                    responses.push({ node: node, notfound: true });
                else
                    for (var n=0;n<parts.length;n+=2)
                    {
                        var meta= JSON.parse(parts[n+1]);
                        responses.push({ node: node, meta: meta, content: parts[n] });
                    }

                success();
                done();
            });
        },
        function ()
        {
           if (errors.length > (n-r) || errors.length == nodes.length)
             res.send(500,errors);
           else
           if (responses.length<r)
             res.status(206).send('Only '+responses.length+' replicas responded with a value, you specified r='+r);
           else
           {
               // we might have more responses so lets re-resolve vclocks
               var found= _.filter(responses,function (r) { return !r.notfound; });
               found.sort(vclockDesc); 

               var first= found.shift(), repaired = first ? [first] : [], needRepair= [];

               found.forEach(function (response, index)
               {
                  var cmp= vclock.compare(first.meta.vclock, response.meta.vclock); 

                  if (cmp==vclock.DIFFERENT)
                    repaired.push(response);
                  else                  
                  if (cmp==1)
                    needRepair.push({ node: response.node, meta: response.meta });
               });

               if (repaired.length==1) // we have a value
               {
                 var notfound= _.filter(responses,function (r) { return !!r.notfound; });

                 notfound.forEach(function (res)
                 { 
                    needRepair.push({ node: res.node });
                 });

                 if (needRepair.length>0)
                   readRepair(req.params.bucket,req.params.key,first,needRepair);
               }
           }
        });
    });

    app.delete('/dull/bucket/:bucket/data/:key', node.buckets.get, function (req,res)
    {
        var n= req.bucket.opts.cap.n || node.cap.n,
            w= req.query.w || node.cap.w,
            nodes= node.ring.nodes(), // we may have keys on any server if the hashring changed
            errors= [],
            success= _.after(w,_.once(function ()
                     {
                         res.end();
                     }));

        if (n < w)
          res.status(500).send('the cluster has n='+n+' you cannot specify a greater w. ('+w+')');
        else
        if (nodes.length < w)
          res.status(500).send('we have only '+nodes.length+' nodes active, and you specified w='+w);
        else
        async.forEach(nodes,
        function (node,done)
        {
            multilevel.client('http://'+node+'/mnt/'+req.params.bucket+'/')
            .batch([{ // key
                          key: ['K',req.params.key,'_'].join(KS),
                         type: 'del'
                    },
                    { // value meta 
                          key: ['V',req.params.key,'M'].join(KS),
                         type: 'del'
                    },
                    { // value content
                          key: ['V',req.params.key,'C'].join(KS),
                         type: 'del' 
                    }],
            function (err,res)
            {
               if (err)
                 errors.push({ node: node, err: err, statusCode: res.statusCode });
               else
                 success();

               done();
            });
        },
        function ()
        {
           if (errors.length > (n-w) || errors.length == nodes.length)
             res.send(500,errors);
           else
             console.log('del success'); 
        });

    });

    app.get('/dull/bucket/:bucket/keys', function (req, res, next)
    {
        var streams= [];

        async.forEach(node.ring.nodes(),
        function (node,done)
        {
            var stream= multilevel.client('http://'+node+'/mnt/'+req.params.bucket+'/')
                                  .valueStream({ start: ['K',''].join(KS),
                                                   end: ['K',LC].join(KS) });

            stream.node= node;
            streams.push(stream);

            done();
        },
        function ()
        {
           var merged= merge(compareKeys,streams);

           streams.forEach(function (stream)
           {
                stream.on('error', function (err)
                {
                    console.log('keys',req.params.bucket,stream.node,err);
                    stream.emit('end'); // if a node is failing we may get the keys from other nodes anyway, so lets try to go on.
                                        // check if fails >= n ? the failed nodes may be from different partitions..
                });
           });

           res.type('json');

           merged
             .on('error',function (err)
             {
                next(err);
             })
             .pipe(unique())
             .pipe(JSONStream.stringify())
             .pipe(res);
        });
    });
};
