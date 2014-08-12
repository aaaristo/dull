var mw= require('./middleware'),
    ut= require('./util'),
    _= require('underscore'),
    async= require('async'),
    multilevel= require('multilevel-http'),
    merge = require('mergesort-stream'),
    JSONStream = require('JSONStream'),
    map   =  require('map-stream'),
    vectorclock   = require('vectorclock');

const KS= '::';

module.exports= function (app,node)
{
    var compareKeys= function (key1, key2)
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
        readRepair= function (bucket,key,res,nodes)
        {
            async.forEach(nodes,
            function (node,done)
            {
                multilevel.client('http://'+node.string+'/mnt/'+bucket+'/')
                .batch([{ // key
                              key: ['K',key,'_'].join(KS),
                            value: key,
                             type: 'put'
                        },
                        { // value meta 
                              key: ['V',key,'M'].join(KS),
                            value: JSON.stringify(_.extend(res.meta,{ vclock: vectorclock.merge(node.vclock,res.meta.vclock) })),
                             type: 'put'
                        },
                        { // value content
                              key: ['V',key,'C'].join(KS),
                            value: res.content,
                             type: 'put'
                        }],
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

    app.put('/dull/bucket/:bucket/data/:key', mw.binary, function (req,res)
    {
        var w= req.query.w || node.cap.w,
            nodes= node.ring.range(req.params.key,node.cap.n),
            errors= [],
            currentNode= node.string,
            success= _.after(w,_.once(function ()
                     {
                         res.end();
                     }));

        if (node.cap.n < w)
          res.status(500).send('the cluster has n='+node.cap.n+' you cannot specify a greater w. ('+w+')');
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
                          key: ['V',req.params.key,'M'].join(KS),
                        value: JSON.stringify
                               ({ 
                                 vclock: vectorclock.increment(req.headers['x-dull-vclock'] ? JSON.parse(req.headers['x-dull-vclock']) : {},currentNode),
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
                          key: ['V',req.params.key,'C'].join(KS),
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
           if (errors.length > (node.cap.n-w) || errors.length == nodes.length)
             res.send(500,errors);
           else
             console.log('put success'); 
        });
    });

    app.get('/dull/bucket/:bucket/data/:key', function (req, res)
    {
        var r= req.query.r || node.cap.r,
            nodes= node.ring.range(req.params.key,node.cap.n),
            errors= [],
            responses= [],
            success= _.after(r,_.once(function () // after r replicas respond, return to the client
                     {
                           var found= _.filter(responses,function (r) { return !r.notfound; });
                           found.sort(vectorclock.descSort); 

                           var first= found.shift(), repaired = first ? [first] : [];

                           found.forEach(function (response, index)
                           {
                              // if they are concurrent with that item, then there is a conflict
                              // that we cannot resolve, so we need to return the item.
                              if (vectorclock.isConcurrent(first, response)
                                  && !vectorclock.isIdentical(response, first))
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
                               var parts= [];

                               repaired.forEach(function (response)
                               {
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
                               ut.multipart(res,parts);
                           }
                     }));

        if (node.cap.n < r)
          res.status(500).send('the cluster has n='+node.cap.n+' you cannot specify a greater r. ('+r+')');
        else
        if (nodes.length < r)
          res.status(500).send('we have only '+nodes.length+' nodes active, and you specified r='+r);
        else
        async.forEach(nodes,
        function (node,done)
        {
            var parts= [];

            multilevel.client('http://'+node+'/mnt/'+req.params.bucket+'/')
            .valueStream({ start: ['V',req.params.key,'C'].join(KS),
                             end: ['V',req.params.key,'M'].join(KS) })
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
                if (parts.length!=2)
                    responses.push({ node: node, notfound: true });
                else
                {
                    var meta= JSON.parse(parts[1]);
                    responses.push({ node: node, clock: meta.vclock, meta: meta, content: parts[0] });
                }

                success();
                done();
            });
        },
        function ()
        {
           if (errors.length > (node.cap.n-r) || errors.length == nodes.length)
             res.send(500,errors);
           else
           if (responses.length<r)
             res.status(206).send('Only '+responses.length+' replicas responded with a value, you specified r='+r);
           else
           {
               // we might have more responses so lets re-resolve vclocks
               var found= _.filter(responses,function (r) { return !r.notfound; });
               found.sort(vectorclock.descSort); 

               var first= found.shift(), repaired = first ? [first] : [], needRepair= [];

               found.forEach(function (response, index)
               {
                  var cmp= vectorclock.compare(first, response); 

                  // if they are concurrent with that item, then there is a conflict
                  // that we cannot resolve, so we need to return the item.
                  if (cmp==0 && !vectorclock.isIdentical(response, first))
                    repaired.push(response);
                  else                  
                  if (cmp==1)
                    needRepair.push({ string: response.node, vclock: response.meta.vclock });
               });

               if (repaired.length==1) // we have a value
               {
                 var notfound= _.filter(responses,function (r) { return !!r.notfound; });

                 notfound.forEach(function (res)
                 { 
                    needRepair.push({ string: res.node, vclock: {} });
                 });

                 if (needRepair.length>0)
                   readRepair(req.params.bucket,req.params.key,first,needRepair);
               }
           }
        });
    });

    app.delete('/dull/bucket/:bucket/data/:key', function (req,res)
    {
        var w= req.query.w || node.cap.w,
            nodes= node.ring.range(req.params.key,node.cap.n),
            errors= [],
            success= _.after(w,_.once(function ()
                     {
                         res.end();
                     }));

        if (node.cap.n < w)
          res.status(500).send('the cluster has n='+node.cap.n+' you cannot specify a greater w. ('+w+')');
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
           if (errors.length > (node.cap.n-w) || errors.length == nodes.length)
             res.send(500,errors);
           else
             console.log('del success'); 
        });

    });

    app.get('/dull/bucket/:bucket/approximateSize/:from..:to', function (req, res, next)
    {
        var size= 0;

        async.forEach(node.ring.nodes(),
        function (node,done)
        {
            multilevel.client('http://'+node+'/mnt/'+req.params.bucket+'/')
            .approximateSize(req.params.from,req.params.to,function (err,size)
            {
                if (!err) size+= size; 
                done(err); 
            });
        },
        function (err)
        {
           if (err)
             next(err);
           else
             res.send(size);
        });
    });

    app.get('/dull/bucket/:bucket/keys', function (req, res, next)
    {
        var streams= [];

        async.forEach(node.ring.nodes(),
        function (node,done)
        {
            streams.push(multilevel.client('http://'+node+'/mnt/'+req.params.bucket+'/')
                                   .valueStream({ start: 'K'+KS, end: ['K','\xff'].join(KS) }));
            done();
        },
        function ()
        {
           var merged= merge(compareKeys,streams);

           streams.forEach(function (stream)
           {
                stream.on('error', function (err)
                {
                    merged.emit('error', err);
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
