var request= require('request'), 
    lrequest= require('./request')(),
    ut= require('./util'),
    JSONStream= require('JSONStream'),
    _= require('underscore'),
    swim= require('express-swim');

module.exports= function (clientProcessId,seed,opts)
{
     opts= opts || {};

     var TIMEOUT= opts.timeout || 1000,
         gossip= swim(clientProcessId,{ base: '/gossip', client: true }).swim,
         node= function ()
         {
            return gossip.rnodes()[0] || seed;
         };

     // gossip.join(seed);

     return function (id)
     {
         var client= {},
             headers= function (meta,value)
             {
                meta= meta||{};

                return _.extend({ 'x-dull-clientid': id },
                                value ? { 'content-type': value.constructor.name=='Buffer' ? 'application/octet-stream' : 'application/json' } : undefined,
                                meta.vclock ?
                                { 'x-dull-vclock': meta.vclock }
                                : undefined);
             },
             buildMeta= function (res)
             {
                return _.extend({ vclock: res.headers['x-dull-vclock'] },
                       res.headers['x-dull-thumbstone'] ? { thumbstone: true }
                                                        : undefined);
             },
             siblings= function (parts)
             {
                return _.collect(parts,function (p)
                {
                    var json= p.headers['content-type']=='application/json';
                    return { meta: buildMeta(p), 
                          content: json ? JSON.parse(p.body) : p.body };
                });
             };

         client.saveBucket= function (name,opts,cb)
         {
             request.put
             ({ 
                 url: 'http://'+node()+'/bucket/'+name,
                body: JSON.stringify(opts),
             timeout: TIMEOUT 
             },
             function (err, res, body)
             {
                if (err)
                  cb(err);
                else
                if (res.statusCode!=200)
                  cb(body);
                else
                  cb();
             });
         };

         client.dropBucket= function (name,cb)
         {
             request.del
             ({ 
                  url: 'http://'+node()+'/bucket/'+name,
              timeout: TIMEOUT
             },
             function (err, res, body)
             {
                if (err)
                  cb(err);
                else
                if (res.statusCode!=200)
                  cb(body);
                else
                  cb();
             });
         };

         client.put= function (bucket,key,value,meta,cb)
         {
             request.put
             ({ 
                 url: 'http://'+node()+'/bucket/'
                      +bucket+'/data/'+key,
             headers: headers(meta,value),
                body: value.constructor.name=='Buffer' ? value : JSON.stringify(value),
             timeout: TIMEOUT 
             },
             function (err, res, body)
             {
                if (err)
                  cb(err);
                else
                if (res.statusCode!=200)
                  cb(body);
                else
                  cb(null,buildMeta(res));
             });
         };

         client.get= function (bucket,key,cb,resolve)
         {
            request.get
            ({ 
               url: 'http://'+node()+'/bucket/'
                    +bucket+'/data/'+key,
               encoding: null,
               timeout: TIMEOUT
             },
            function (err,res,body)
            {
               if (err) return cb(err);

               if (res.statusCode==200)
                 cb(null,
                    res.headers['content-type']=='application/json' ?
                    JSON.parse(body.toString('utf8')) : body,
                    buildMeta(res));
               else
               if (res.statusCode==404)
                 cb({ notfound: true },undefined,buildMeta(res));
               else
               if (res.statusCode==300)
               {
                 var resolved= resolve ? resolve(siblings(ut.multipart(res,body)))
                                       : undefined;

                 if (resolved!==undefined)
                 {
                   if (resolved===null)
                     cb({ notfound: true },null,buildMeta(res));
                   else
                     cb(null,resolved,buildMeta(res));
                 }
                 else
                   cb(new Error('Cannot resolve siblings'));
               }
               else
                 cb(body.toString('utf8'));
            });
         };

         client.del= function (bucket,key,meta,cb)
         {
             request.del
             ({ 
                  url: 'http://'+node()+'/bucket/'
                                +bucket+'/data/'+key,
              headers: headers(meta),
              timeout: TIMEOUT
             },
             function (err, res, body)
             {
                if (err)
                  cb(err);
                else
                if (res.statusCode!=200)
                  cb(body);
                else
                  cb(null,buildMeta(res));
             });
         };

         client.createKeyStream= function (bucket,opts)
         {
             return lrequest.stream
             ({
                uri: 'http://'+node()+'/bucket/'
                     +bucket+'/keys',
                qs: opts || {},
                timeout: TIMEOUT
             })
             .pipe(JSONStream.parse());
         };

         return client;
     };
};
