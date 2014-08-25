var multilevel= require('multilevel-http-temp'),
    JSONStream= require('JSONStream');

const KS= '::',                   // key separator
      LC= '\xff';                 // last character

module.exports= function (opts)
{
    var sib= {};

    sib.batch= function (bucket)
    {
        var batch= { ops: [], bucket: bucket };

        batch.put= function (key,id,meta,value)
        {
           batch.ops.push.apply(batch.ops,
             [{ // key
                  key: ['K',key,id].join(KS),
                value: meta,
                 type: 'put'
              },
              { // value meta 
                  key: ['V',key,id,'M'].join(KS),
                value: meta,
                 type: 'put'
              },
              { // value content
                  key: ['V',key,id,'C'].join(KS),
                value: value,
                 type: 'put'
              }]);

           return batch;
        };

        batch.del= function (key,id)
        {
           batch.ops.push.apply(batch.ops,
              [{ // key
                      key: ['K',key,id].join(KS),
                     type: 'del'
               },
               { // sibling meta
                      key: ['V',key,id,'M'].join(KS),
                     type: 'del'
               },
               { // sibling content
                      key: ['V',key,id,'C'].join(KS),
                     type: 'del'
               }]);

           return batch;
        };

        batch.perform= function (node,cb)
        {
            multilevel.client('http://'+node+'/mnt/'+batch.bucket+'/',
                              { timeout: opts.timeout })
                      .batch(batch.ops,cb);
        };

        return batch;
    };

    sib.list= function (bucket,key,node,cb)
    {
        var parts= [];

        multilevel.client('http://'+node+'/mnt/'+bucket+'/',{ timeout: opts.timeout })
        .valueStream({ start: ['V',key,''].join(KS),
                         end: ['V',key,LC].join(KS) })
        .on('error', cb)
        .on('data', function (data)
        {
            parts.push(data); 
        })
        .on('end', function ()
        {
            if (parts.length<2)
              cb(null,{ notfound: true });
            else
            {
              var siblings= [];

              for (var n=0;n<parts.length;n+=2)
              {
                 var meta= JSON.parse(parts[n+1]);
                 siblings.push({ meta: meta, content: parts[n] });
              }
            
              cb(null,siblings);
            }
        });
    };

    sib.keys= function (node,bucket)
    {
        return multilevel.client('http://'+node+'/mnt/'+bucket+'/',
                                 { timeout: opts.timeout })
                         .valueStream({ start: ['K',''].join(KS),
                                               end: ['K',LC].join(KS) })
                         .pipe(JSONStream.parse());
    };

    return sib;

};
