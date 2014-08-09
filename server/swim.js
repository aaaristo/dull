var _= require('underscore'),
    mw= require('./middleware'),
    ut= require('./util'),
    async= require('async'),
    request= require('request'),
    HashRing = require('hashring');

// @see http://www.cs.cornell.edu/~asdas/research/dsn02-swim.pdf
        http://www.cs.ucsb.edu/~ravenben/classes/papers/aodv-wmcsa99.pdf

const T1= 3000,               // period length
      PING_TIMEOUT= 1000,     // timeout of a ping request
      FAILING_TIMEOUT= 9000,  // timeout of a suspected state before failing a node
      k= 2,                   // number of random nodes to select for a ping-req
      lambda= 2,              // parameter to tune maximum piggybacking of messages (keep it "small")
      MAX_MESSAGES= 10;       // max piggybacked messages per request

if (PING_TIMEOUT*3>T1) throw('quote: "which is chosen smaller than the protocol period...'+
                             'Note that the protocol period has to be at least three times'+
                             ' the round-trip estimate"');

module.exports= function (app,node)
{
    // todo:
    // adapt ping_timeout avg(response time)

    node.ring= new HashRing();

    var periodSeq= 0,
        incSeq= 0,
        messageSeq= 0,
        membershipUpdates= [],
        servers= (function(servers){ servers[node.string]= { string: node.string, inc: 0 }; return servers; })({}),
        ring= {
           add: function (server)
           {
              node.ring.add(server.string);
              delete servers[server.string];
              servers[server.string]= server;
           },
           remove: function (string)
           {
              node.ring.remove(string);
              delete servers[string];
           },
           fail: function (string)
           {
              node.ring.remove(string);
              servers[string].failed= true;
           },
           alive: function (string)
           {
              if (servers[string].failed);
              {
                node.ring.add(string);
                servers[string].suspected= servers[string].failed= false;
              }
           },
           inc: function (string)
           {
              return (servers[string]||{}).inc;
           },
           find: function (string)
           {
              return servers[string];
           },
           nodes: function ()
           {
              return _.pluck(_.filter(_.values(servers),function (s) { return !s.failed; }),'string');
           }
        },
        piggyback= function (seq,target)
        {
           var max= Math.round(Math.log(ring.nodes().length)*lambda),
               messages= _.filter(membershipUpdates,function (upd)
                        { 
                            return upd.message.source!=target
                                && upd.counter<max;
                        });

           messages= _.sortBy(messages,'counter').slice(0,MAX_MESSAGES);

           messages.forEach(function (upd)
           {
               ++upd.counter;

               if (upd.counter>=max)
                 upd.rmTimeout= setTimeout(function ()
                 {
                    ut.rm(membershipUpdates,upd);
                 },FAILING_TIMEOUT);
           });

           messages= _.pluck(messages,'message');

           return { seq: seq, sender: node.string, messages: messages };
        },
        processMessages= function (ack)
        {
           if (!ack) return;

           if (ack.messages)
           ack.messages.forEach(function (message)
           {
              if (message.source==node.string) return; // ignore my messages

              if (_.filter(membershipUpdates,
                    function (upd) { return upd.message.source==message.source
                                          &&upd.message.id==message.id }).length)
                return; // ignore known messages

              console.log('swim','receive',message);

              receive[message.type](message.subject,message.inc);

              membershipUpdates.unshift({ message: message, counter: 0 });
           });
        },
        rnodes= function (n,suspect)
        {
           var nodes= {},
               others= _.without(ring.nodes(),node.string,suspect);

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
        json= function (x)
        {
            try
            {
               return JSON.parse(x); 
            }
            catch (ex)
            {
               return {};
            }
        },
        ping= function (node,seq,cb)
        {
               request.post({ timeout: PING_TIMEOUT,
                                  uri: 'http://'+node+'/swim/ping/'+seq,
                                 body: JSON.stringify(piggyback(seq,node)) },
               function (err, res, body)
               {
                   if (err)
                     cb(err);
                   else
                   if (res.statusCode!=200)
                     cb({ code: res.statusCode, message: processMessages(json(body)) });
                   else
                     cb(null,processMessages(json(body)));
               });
        },
        pingReq= function (node,target,seq,cb)
        {
               request.post({ timeout: PING_TIMEOUT,
                                  uri: 'http://'+node+'/swim/ping-req/'+target+'/'+seq,
                                 body: JSON.stringify(piggyback(seq,node)) },
               function (err, res, body)
               {
                   if (err)
                     cb(err);
                   else
                   if (res.statusCode!=200)
                     cb({ code: 200, message: res.statusCode < 300 ? processMessages(json(body)) : body });
                   else
                     cb(null,processMessages(json(body)));
               });
        },
        sendMessage= function (type,subject)
        {
               var upd, server= ring.find(subject);

               if (subject!=node.string&&!server) return;

               membershipUpdates.unshift(upd={ message: { source: node.string, id: messageSeq++,
                                                        type: type, subject: subject,
                                                         inc: subject==node.string ? 
                                                             incSeq++ : server.inc },
                                           counter: 0 });

               console.log('swim','send',upd.message);
        },
        receive= {
            join: function (subject,inc)
            {
                ring.add({ string: subject, inc: inc });
            },
            leave: function (subject,inc)
            {
                if (inc>=ring.inc(subject));
                  ring.remove(subject);
            },
            alive: function (subject,inc)
            {
                var server= ring.find(subject);

                if (server&&inc>server.inc)
                {
                  server.inc= inc;
                  server.suspected= clearTimeout(server.suspected);
                }
                else
                  ring.alive(subject);
            },
            fail: function (subject,inc) // (confirm)
            {
                if (node.string==subject)
                  sendMessage('alive',node.string);
                else
                  ring.fail(subject);
            },
            suspect: function (subject,inc)
            {
                if (node.string==subject)
                    sendMessage('alive',node.string);
                else
                {
                    var server= ring.find(subject);

                    if (!server||server.failed) return;

                    if (server.suspected)
                    {
                       if (inc>server.inc)
                       {
                           clearTimeout(server.suspected);
                           server.suspected= setTimeout(function ()
                           {
                               ring.fail(subject);
                               sendMessage('fail',subject);  
                           },FAILING_TIMEOUT);
                       }
                    }
                    else  
                    if (inc>=server.inc)
                      server.suspected= setTimeout(function ()
                      {
                          ring.fail(subject);
                          sendMessage('fail',subject);  
                      },FAILING_TIMEOUT);
                }
            } 
        },
        pingStack= [],
        periodInterval= setInterval(function ()
        {
           periodSeq++;

           var Mj= pingStack.pop();

           if (!Mj)
             Mj= (pingStack= _.shuffle(ring.nodes())).pop();

           console.log('swim','period',periodSeq,Mj);

           if (!Mj) return; // disabled we need more nodes on the cluster

           ping(Mj,periodSeq,function (err, ack)
           {
              if (err)
              {
                 var Mr= rnodes(k,Mj), errors= [];

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
                    {
                      receive.suspect(Mj,ring.inc(Mj));
                      sendMessage('suspect',Mj);
                    }
                 }); 
              }
              else
                 console.log('swim','ping','OK',Mj,ack);
           });
        },T1);

    app.post('/swim/ping/:seq', mw.json, function (req, res)
    {
        processMessages(req.json);

        res.send(piggyback(req.params.seq,req.json.sender));
    });

    app.post('/swim/ping-req/:target/:seq', mw.json, function (req, res)
    {
        processMessages(req.json);

        ping(req.params.target,req.params.seq,function (err, ack)
        {
            if (err)
              res.status(504).send(piggyback(req.params.seq,req.json.sender));
            else
            {
              processMessages(ack);
              res.send(piggyback(ack.seq,req.json.sender));
            }
        });
    });

    app.post('/dull/join/:target', function (req, res)
    {
        ring.add({ string: req.params.target, inc: 0 });
        sendMessage('join',node.string);
        res.end();
    });

    app.delete('/dull/leave', function (req, res)
    {
        sendMessage('leave',node.string);
        res.end();
    });

    app.get('/dull/nodes', function (req, res)
    {
        res.send(ring.nodes());
    });

};
