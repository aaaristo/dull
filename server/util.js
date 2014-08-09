var _= require('underscore');

exports.rmf= function (arr,fn)
{
    var torm= _.filter(arr,fn),
        rm= function (e) { arr.splice(arr.indexOf(e),1); };
    
    torm.forEach(_rm);
};

exports.rm= function (arr,e)
{
    var pos= arr.indexOf(e);

    if (pos > -1)
      arr.splice(pos,1);
};
