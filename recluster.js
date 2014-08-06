var request= require('request'),
    async= require('async');

var nodes= ['127.0.0.1:3001', '127.0.0.1:3002', '127.0.0.1:3003'];

setTimeout(function ()
{
    async.forEachSeries(nodes,
    function (node, done)
    {
       request.post({ url: 'http://127.0.0.1:3000/dull/node', body: node },done);
    },
    function (err)
    {
       if (err)
         console.log('doh',err);
       else
         console.log('OK!');
    });
},3000);
