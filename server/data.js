var mw= require('./middleware'),
    _= require('underscore'),
    async= require('async'),
    multilevel= require('multilevel-http'),
    crypto = require('crypto');

module.exports= function (app,node)
{
    var resolveErrors= function (errors, res, wr)
        {
              var uerr= _.groupBy(errors,function (err)
                        {
                            if (err.err.indexOf('NotFoundError') > -1)
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
                         res.send(404,'key not found');
                       else
                         res.send(500,errors);

                       return true; 
                     } 
                 });
        };

    app.put('/dull/data/:key', mw.log, mw.text, function (req,res)
    {
        var w= req.query.w || node.cap.w,
            nodes= node.ring.range(req.params.key,node.cap.n),
            errors= [],
            success= _.after(w,function ()
                     {
                         res.end();
                     });

        if (node.cap.n < w)
          res.send(500,'the cluster has n='+node.cap.n+' you cannot specify a greater w. ('+w+')');
        else
        if (nodes.length < w)
          res.send(500,'we have only '+nodes.length+' nodes active, and you specified w='+w);

        async.forEach(nodes,
        function (node,done)
        {

            multilevel.client('http://'+node+'/')
            .put(req.params.key,req.text,function (err,value)
            {
               if (err)
                 errors.push({ node: node, err: err });
               else
                 success();

               done();
            });

        },
        function ()
        {
           if (errors.length > (node.cap.n-w))
             res.send(500,errors);
           else
             console.log('put success'); 
        });
    });

    app.get('/dull/data/:key', mw.log, function (req, res)
    {
        var r= req.query.r || node.cap.r,
            nodes= node.ring.range(req.params.key,node.cap.n),
            errors= [],
            values= [];

        if (node.cap.n < r)
          res.send(500,'the cluster has n='+node.cap.n+' you cannot specify a greater r. ('+r+')');
        else
        if (nodes.length < r)
          res.send(500,'we have only '+nodes.length+' nodes active, and you specified r='+r);

        async.forEach(nodes,
        function (node,done)
        {
            multilevel.client('http://'+node+'/')
            .get(req.params.key,function (err,value)
            {
               if (err)
                 errors.push({ node: node, err: err });
               else
                 values.push(value);

               done();
            });
        },
        function ()
        {
           if (errors.length > (node.cap.n-r))
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
                 res.send(500,'We have only '+max+' replicas that agree on a value for that key, you specified r='+r);
               else
               if (cnt[max]>1)
                 res.send(500,'Doh, we have diverging replicas for that key');
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

    app.delete('/dull/data/:key', mw.log, function (req,res)
    {
        var w= req.query.w || node.cap.w,
            nodes= node.ring.range(req.params.key,node.cap.n),
            errors= [],
            success= _.after(w,function ()
                     {
                         res.end();
                     });

        if (node.cap.n < w)
          res.send(500,'the cluster has n='+node.cap.n+' you cannot specify a greater w. ('+w+')');
        else
        if (nodes.length < w)
          res.send(500,'we have only '+nodes.length+' nodes active, and you specified w='+w);

        async.forEach(nodes,
        function (node,done)
        {
            multilevel.client('http://'+node+'/')
            .del(req.params.key,function (err,value)
            {
               if (err)
                 errors.push({ node: node, err: err });
               else
                 success();

               done();
            });
        },
        function ()
        {
           if (errors.length > (node.cap.n-w))
             resolveErrors(errors,res,w);
           else
             console.log('del success'); 
        });

    });

};
