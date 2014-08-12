var _= require('underscore'),
    crypto = require('crypto');

exports.rmf= function (arr,fn)
{
    var torm= _.filter(arr,fn),
        rm= function (e) { arr.splice(arr.indexOf(e),1); };
    
    torm.forEach(rm);
};

exports.rm= function (arr,e)
{
    var pos= arr.indexOf(e);

    if (pos > -1)
      arr.splice(pos,1);
};

exports.json= function (x)
{
    try
    {
       return JSON.parse(x); 
    }
    catch (ex)
    {
       return {};
    }
};

exports.hash= function (value)
{
   return crypto.createHash('sha1').update(value).digest('hex');
};

const BOUNDARY= '\n--'+exports.hash('dull-boundary')+'\n';

exports.multipart= function (res,parts)
{
   res.setHeader('Content-Type','Multipart/mixed; boundary="'+BOUNDARY+'";');
   res.write(BOUNDARY);
   
   parts.forEach(function (part)
   {
       _.keys(part.headers).forEach(function (name)
       {
            res.write(name+': '+part.headers[name]+'\n');
       });

       res.write('\n'+part.body);

       res.write(BOUNDARY);
   });

   res.end();
};
