var request= require('request'),
    async= require('async');

var nodes= ['127.0.0.1:3001', '127.0.0.1:3002', '127.0.0.1:3003'];

(function timer()
{
    setTimeout(function ()
    {
        var first= nodes[0];

        async.forEachSeries(nodes.splice(1),
        function (node, done)
        {
           request.post({ url: 'http://'+first+'/dull/node', body: node },done);
        },
        function (err)
        {
           if (err)
           {
             console.log('doh',err);
             timer();
           }
           else
             console.log('OK!');
        });
    },3000);
})();
