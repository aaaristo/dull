

module.exports= function (app,node)
{
    app.post('/dull/crdt/counter/:bucket/:key', mw.json, function (req, res)
    {
       var val= req.json; 

       if (typeof val!='number')
         res.status(400).send('you should send a number');
       else
       {}; 
    });
};
