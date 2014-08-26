exports.text= function (req,res,next)
{
    req.text = '';
    req.setEncoding('utf8');
    req.on('data', function(chunk){ req.text += chunk });
    req.on('end', next);
};

exports.json= function (req,res,next)
{
    req.json = '';
    req.setEncoding('utf8');
    req.on('data', function(chunk){ req.json += chunk });
    req.on('end', function ()
    { 
       try
       {
          req.json= JSON.parse(req.json);
          next(); 
       }
       catch (ex)
       {
          next(ex);
       }
    });
};

exports.binary= function (req,res,next)
{
    var chunks= [];
    req.on('data', function(chunk){ chunks.push(chunk); });
    req.on('end', function ()
    { 
       try
       {
          req.binary= Buffer.concat(chunks);
          next(); 
       }
       catch (ex)
       {
          next(ex);
       }
    });
};

exports.client= function (req, res, next)
{
    req.client= {
                    id: req.headers['x-dull-clientid'] ? 
                        req.headers['x-dull-clientid'] : uuid(),
                vclock: req.headers['x-dull-vclock'] ? 
                        JSON.parse(req.headers['x-dull-vclock']) : {}
                };

    next();
};

exports.log= function (req, res, next)
{
   if (req.originalUrl.indexOf('/gossip')!=0)
     console.log(req.method,req.protocol + '://' + req.get('host') + req.originalUrl);

   next();
};
