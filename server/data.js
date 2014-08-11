var mw= require('./middleware'),
    _= require('underscore'),
    async= require('async'),
    multilevel= require('multilevel-http'),
    merge = require('mergesort-stream'),
    JSONStream = require('JSONStream'),
    map   =  require('map-stream'),
    bytewise = require('bytewise/hex'),
    encodeKey = bytewise.encode.bind(bytewise),
    crypto = require('crypto');

module.exports= function (app,node)
{
    var resolveErrors= function (errors, res, wr, put)
        {
              var uerr= _.groupBy(errors,function (err)
                        {
                            if (err.statusCode == 404 || err.err.indexOf('NotFoundError') > -1)
                              return 'notfound';
                            else
                              return 'error';
                        }),
                  len= _.pluck(_.values(uerr),'length'),
                  max= _.max(len),
                  cnt= _.countBy(len,_.identity);

               if (max < wr || cnt[max]>1)
                 res.send(500,errors);
               else
                 _.keys(uerr).some(function (type)
                 {
                     var errs= uerr[type];

                     if (errs.length==max)
                     {
                       if (type=='notfound')
                         res.status(404).send(put ? 'bucket not found' : 'key not found');
                       else
                         res.status(500).send(errors);

                       return true; 
                     } 
                 });
        },
        compareKeys= function (value1, value2)
        {
              var key1 = encodeKey(value1),
                  key2 = encodeKey(value2);
              if (key1 > key2) return 1;
              else if (key1 < key2) return -1;
              return 0;
        },
        unique= function ()
        {
           var last;

           return map(function (data, cb)
           {
                last!==data ? 
                  cb(null, data) : 
                  cb();
                last= data;
           });
        };

    app.put('/dull/bucket/:bucket/data/:key', mw.text, function (req,res)
    {
        var w= req.query.w || node.cap.w,
            nodes= node.ring.range(req.params.key,node.cap.n),
            errors= [],
            success= _.after(w,function ()
                     {
                         res.end();
                     });

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
            .put(req.params.key,req.text,function (err,res)
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
             resolveErrors(errors,res,w,true);
           else
             console.log('put success'); 
        });
    });

    app.get('/dull/bucket/:bucket/data/:key', function (req, res)
    {
        var r= req.query.r || node.cap.r,
            nodes= node.ring.range(req.params.key,node.cap.n),
            errors= [],
            values= [];

        if (node.cap.n < r)
          res.status(500).send('the cluster has n='+node.cap.n+' you cannot specify a greater r. ('+r+')');
        else
        if (nodes.length < r)
          res.status(500).send('we have only '+nodes.length+' nodes active, and you specified r='+r);
        else
        async.forEach(nodes,
        function (node,done)
        {
            multilevel.client('http://'+node+'/mnt/'+req.params.bucket+'/')
            .get(req.params.key,function (err,value,resp)
            {
               if (err)
                 errors.push({ node: node, err: err, statusCode: resp.statusCode });
               else
                 values.push(value);

               done();
            });
        },
        function ()
        {
           if (errors.length > (node.cap.n-r) || errors.length == nodes.length)
               resolveErrors(errors,res,r);
           else
           {
               var uval= _.groupBy(values,function (val)
                         {
                            return crypto.createHash('md5').update(val).digest('hex');
                         }),
                   len= _.pluck(_.values(uval),'length'),
                   max= _.max(len),
                   cnt= _.countBy(len,_.identity);


               
               if (max < r)
                 res.status(500).send('We have only '+max+' replicas that agree on a value for that key, you specified r='+r);
               else
               if (cnt[max]>1)
                 res.status(500).send('Doh, we have diverging replicas for that key');
               else
                 _.keys(uval).some(function (hash)
                 {
                     var vals= uval[hash];

                     if (vals.length==max)
                     {
                       res.send(vals[0]);
                       return true; 
                     } 
                 });
           }
        });
    });

    app.delete('/dull/bucket/:bucket/data/:key', function (req,res)
    {
        var w= req.query.w || node.cap.w,
            nodes= node.ring.range(req.params.key,node.cap.n),
            errors= [],
            success= _.after(w,function ()
                     {
                         res.end();
                     });

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
            .del(req.params.key,function (err,value,resp)
            {
               if (err)
                 errors.push({ node: node, err: err, statusCode: resp.statusCode });
               else
                 success();

               done();
            });
        },
        function ()
        {
           if (errors.length > (node.cap.n-w) || errors.length == nodes.length)
             resolveErrors(errors,res,w);
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
                                   .keyStream());
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
             .pipe(unique())
             .pipe(JSONStream.stringify())
             .pipe(res);
        });
    });
};
