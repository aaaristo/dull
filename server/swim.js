var _= require('underscore'),
    async= require('async'),
    request= require('request');

const T1= 3000, PING_TIMEOUT= 1000, k= 2;

if (PING_TIMEOUT*3>T1) throw('quote: "which is chosen smaller than the protocol period...'+
                             'Note that the protocol period has to be at least three times'+
                             ' the round-trip estimate"');

module.exports= function (app,node)
{
    var rnodes= function (n)
        {
           var nodes= {},
               others= _.without(_.pluck(node.ring.continuum().servers,'string'),node.string);

           if (others.length<=n)
             return others;
           else
           while (_.keys(nodes).length < n)
           {
              var rnode= others[_.random(0,others.length-1)];
              nodes[rnode]= true;
           }

           return _.keys(nodes);
        },
        ping= function (node,seq,cb)
        {
               request({ timeout: PING_TIMEOUT, uri: 'http://'+node+'/swim/ping/'+seq },
               function (err, res, body)
               {
                   if (err)
                     cb(err);
                   else
                   if (res.statusCode!=200)
                     cb({ code: 200, message: body });
                   else
                     cb(null,body);
               });
        },
        pingReq= function (node,target,seq,cb)
        {
               request.post({ timeout: PING_TIMEOUT, uri: 'http://'+node+'/swim/ping-req/'+target+'/'+seq },
               function (err, res, body)
               {
                   if (err)
                     cb(err);
                   else
                   if (res.statusCode!=200)
                     cb({ code: 200, message: body });
                   else
                     cb(null,body);
               });
        },
        failed= function (node,errors)
        {
            console.log('swim','failed',node,errors);
        };

    var periodSeq= 0,
        periodInterval= setInterval(function ()
        {
           periodSeq++;

           var Mj= rnodes(1)[0];

           console.log('swim','period',periodSeq,Mj);

           if (!Mj) return; // disabled we need more nodes on the cluster

           ping(Mj,periodSeq,function (err, ack)
           {
              if (err)
              {
                 var Mr= rnodes(k), errors= [];

                 async.forEach(Mr,
                 function (node,done)
                 {
                    pingReq(node,Mj,periodSeq,function (err, ack)
                    {
                       if (err)
                         errors.push({ node: node, err: err });
                 
                       done(ack);
                    });
                 },
                 function (ack)
                 {
                    if (!ack)
                      failed(Mj,errors);
                 }); 
              }
              else
                console.log('swim','ping','OK',Mj,ack);
           });
        },T1);

    app.get('/swim/ping/:seq', function (req, res)
    {
        res.send(req.params.seq);
    });

    app.get('/swim/ping-req/:target/:seq', function (req, res)
    {
        ping(req.params.target,req.params.seq,function (err, ack)
        {
            if (err)
              res.status(204).end();
            else
              res.send(ack);
        });
    });
};
