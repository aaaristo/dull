var http= require('http');

// @FIXME: find some multipart/mixed library
exports.multipart= function (res,body)
{
   body= body.toString('utf8');

   var boundary= res.headers['content-type'].split('"')[1];
   boundary= boundary.substring(0,boundary.length);

   var parts= body.split('\n--'+boundary+'\n');

   parts.shift();
   parts.pop();

   var ret= [];

   parts.forEach(function (p)
   {
       var pos= p.indexOf('\n\n'),
           head= p.substring(0,pos).split('\n'),
           headers= {},
           body= p.substring(pos+2);

       head.forEach(function (h)
       {
            var nv= h.split(': ');
            headers[nv[0]]= nv[1];
       });

       ret.push({ headers: headers, body: body }); 
   });

   return ret;
};
