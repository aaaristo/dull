var request= require('../node_modules/multilevel-http-temp/lib/request')(),
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
                return { vclock: res.headers['x-dull-vclock'] };
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
                  cb(body,buildMeta(res));
                else
                  cb();
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
                 cb({ notfound: true });
               else
               if (res.statusCode==303)
               {
                 var resolved= resolve ? resolve(ut.multipart.parse(res))
                                       : undefined;

                 if (resolved!==undefined)
                   cb(null,resolved);
                 else
                   cb(new Error('Cannot resolve siblings'));
               }
               else
                 cb(body);
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
                  cb(body,buildMeta(res));
                else
                  cb();
             });
         };

         client.createKeyStream= function (bucket,opts)
         {
             return request.stream
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