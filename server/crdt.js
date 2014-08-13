
module.exports= function (app,node)
{
   require('./crdt/counter')(app,node);
};
