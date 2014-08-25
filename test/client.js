var client= require('..').client('test-process','127.0.0.1:3000');

var should= require('chai').should(),
    assert= require('chai').assert;

describe('client',function ()
{
     before(function (done)
     {
         var adm= client('mocha');

         adm.dropBucket('notes',
         function (err)
         {
             if (err) return done(err); 

             adm.saveBucket('notes',{},done);
         });
     });

     it('can put a kv',function (done)
     {
         client('Alice').put('notes','date','Wednesday',null,function (err)
         {
             done(err);
         });
     });

     it('can get the kv',function (done)
     {
         client('Alice').get('notes','date',function (err,value,meta)
         {
             if (err) return done(err);

             'Wednesday'.should.equal(value);
             done();
         });
     });

     it('can get keys',function (done)
     {
         client('Alice').createKeyStream('notes')
                        .on('error',done)
                        .on('data',function (data)
                        {
                            if (data!='date')
                              done('Wrong key');
                        })
                        .on('end',done);
     });

     it('can delete the kv',function (done)
     {
         client('Alice').del('notes','date',null,function (err)
         {
             done(err);
         });
     });

     it('can save buffers',function (done)
     {
         var c= client('Duffy'), buff= new Buffer('andrea','utf8');

         c.put('notes','name',buff,null,function (err)
         {
             if (err) return done(err);

             c.get('notes','name',function (err, value)
             {
                 if (err) return done(err);

                 buff.should.eql(value);
                 done();
             });
         });
     });  

     it('can save json',function (done)
     {
         var c= client('Jeff'), obj= { name: 'Andrea' };

         c.put('notes','obj',obj,null,function (err)
         {
             if (err) return done(err);

             c.get('notes','obj',function (err, value)
             {
                 if (err) return done(err);

                 obj.should.eql(value);
                 done();
             });
         });
     });  
});