var request= require('request'),
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

                return _.extend({ 'x-dull-clientid': id,
                                  'content-type': value.constructor.name=='Buffer' ? 'application/octet-stream' : 'application/json' },
                                meta.vclock ?
                                { 'x-dull-vclock': JSON.stringify(meta.vclock) }
                                : undefined);
             };

         client.saveBucket= function (name,opts,cb)
         {
             request.put
             ({ 
                 url: 'http://'+node()+'/dull/bucket/'+name,
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
                  url: 'http://'+node()+'/dull/bucket/'+name,
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
                 url: 'http://'+node()+'/dull/bucket/'
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
                  cb();
             });
         };

         client.get= function (bucket,key,cb,resolve)
         {
            request.get
            ({ 
               url: 'http://'+node()+'/dull/bucket/'
                    +bucket+'/data/'+key,
               timeout: TIMEOUT
             },
            function (err,res,body)
            {
               if (err) return cb(err);

               if (res.statusCode==200)
                 cb(null,res.headers['content-type']=='application/json' ?
                         JSON.parse(body) : body);
               else
               if (res.statusCode==404)
                 cb({ notfound: true },null);
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
                  url: 'http://'+node()+'/dull/bucket/'
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
                  cb();
             });
         };

         client.createKeyStream= function (bucket,opts)
         {
             return request.stream
             ({
                uri: 'http://'+node()+'/dull/bucket/'
                     +bucket+'/keys',
                qs: opts || {},
                timeout: TIMEOUT
             })
             .pipe(JSONStream.parse);
         };

         return client;
     };
};
