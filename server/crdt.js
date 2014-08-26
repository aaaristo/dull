
module.exports= function (app,node,argv)
{
   require('./crdt/counter')(app,node,argv);
};
