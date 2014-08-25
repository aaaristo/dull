var _= require('underscore'),
    merge= require('mergesort-stream'),
    stream= require('stream'),
    async= require('async');

const LC= '\xff',  // last character
      compareKeys= function (meta1, meta2)
      {
          if (meta1.key > meta2.key) return 1;
          else if (meta1.key < meta2.key) return -1;
          return 0;
      },
      atLeast= function (n,nodes,iterator,least,all)
      {
          var errors= [],
              done= _.after(n,_.once(least));

          async.forEach(nodes,
          function (node,finish)
          {
             iterator(node,function (err)
             {
                errors.push({ node: node, err: err });
                done();
             });
          },
          function ()
          {
             all(errors);
          });
      };

module.exports= function (node,opts)
{
    var sib= require('./sibling')(opts),
        coord= {};

    coord.batch= function (bucket,key)
    {
       var batch= sib.batch(bucket.name);

       batch.put= _.wrap(batch.put,
       function (put,siblingId,meta,value)
       {
          put(key,siblingId,meta,value);
          return batch;
       });
       
       batch.del= _.wrap(batch.del,
       function (del,siblingId)
       {
          del(key,siblingId);
          return batch;
       });

       batch.perform= _.wrap(batch.perform,
       function (perform,rw,leastNodes,allNodes)
       {
          var n= bucket.opts.cap.n || node.cap.n,
              w= rw || bucket.opts.cap.w || node.cap.w,
              nodes= node.ring.range(key,n);

          if (n < w)
            allNodes(new Error('the cluster has n='+n+' you cannot specify a greater w. ('+w+')'));
          else
          if (nodes.length < w)
            allNodes(new Error('we have only '+nodes.length+' nodes active, and you specified w='+w));
          else
            atLeast(w,nodes,perform,leastNodes,function (errors)
            {
               if (errors.length > (n-w)
                 || errors.length == nodes.length)
                 allNodes(new Error('Too many errors ('+errors.length+')'),errors);
               else
                 allNodes();
            });
       });

       return batch;
    };

    coord.list= function (rw,bucket,key,leastNodes,allNodes)
    {
       var n= bucket.opts.cap.n || node.cap.n,
           r= rw || bucket.opts.cap.r || node.cap.r,
           nodes= node.ring.range(key,n),
           responses= [];

        if (n < r)
          allNodes(new Error('the cluster has n='+n+' you cannot specify a greater r. ('+r+')'));
        else
        if (nodes.length < r)
          allNodes(new Error('we have only '+nodes.length+' nodes active, and you specified r='+r));
        else
          atLeast(r,nodes,
          function (node,done)
          {
             sib.list(bucket.name,key,node,function (err,partial)
             {
                 if (responses)
                   responses.push.apply(responses,
                   _.collect(partial,function (r)
                   {
                      return {
                                  node: node,
                                  meta: r.meta,
                               content: r.content
                             };
                   }));

                 done(err); 
             });
          },
          function ()
          {
               leastNodes(responses);
          },
          function (errors)
          {
               if (errors.length > (n-r) || errors.length == nodes.length)
                 allNodes(new Errors('Too many errors ('+errors.length+')'),errors);
               else
                 allNodes(null,responses);
          });
    };

    coord.keys= function (bucket,cb)
    {
        var streams= [];

        node.ring.nodes().forEach(function (node,done)
        {
            var stream= sib.keys(node,bucket.name);

            stream.node= node;

            stream.on('error', function (err)
            {
                console.log('keys',req.params.bucket,stream.node,err);
                stream.emit('end'); // if a node is failing we may get the keys from other nodes anyway, so lets try to go on.
                                    // check if fails >= n ? the failed nodes may be from different partitions..
            });

            streams.push(stream);
        });

        var endstream = new stream.Stream()
        endstream.readable = true;
        streams.push(endstream);

        var merged= merge(compareKeys,streams);

        endstream.emit('data',{ key: LC });
        endstream.emit('end');

        return merged;
    };

    return coord;
};
