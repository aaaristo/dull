var _= require('underscore'),
    client= require('..').client('test-process','127.0.0.1:3000');

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
         client('Alice').put('notes','basic','Wednesday',null,function (err)
         {
             done(err);
         });
     });

     it('can get the kv',function (done)
     {
         client('Alice').get('notes','basic',function (err,value,meta)
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
                            if (data!='basic')
                              done('Wrong key');
                        })
                        .on('end',done);
     });

     it('can delete the kv',function (done)
     {
         client('Alice').del('notes','basic',null,function (err)
         {
             if (err) return done(err);

             var del= false;

             client('Alice').get('notes','basic',function (err,value,meta)
             {
                 if (err)
                 {
                    if (err.notfound)
                    {
                      del.should.equal(true);

                      client('Alice').del('notes','basic',meta,
                      function (err,meta)
                      {
                         if (err) return done(err);

                         client('Alice').get('notes','basic',
                         function (err,value,meta)
                         {
                             if (err.notfound) return done();

                             done(err || 'found');  
                         });
                      });
                    }
                    else 
                      done(err);
                 }
                 else
                    done(new Error('found'));
             },
             function (siblings)
             {
                 // resolve to deleted
                 siblings.length.should.equal(2);
                 _.pluck(siblings,'content').should.contain('Wednesday');
                 _.collect(siblings,function (s) { return s.meta.thumbstone; })
                  .should.contain(true);
                 del= true;
                 return null;
             });
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

     it('can resolve siblings',function (done)
     {
         // http://basho.com/why-vector-clocks-are-hard/

         var alice= client('Alice'),
             ben= client('Ben'),
             cathy= client('Cathy'),
             dave= client('Dave');

         alice.put('notes','date','Wednesday',null,
         function (err,meta)
         {
             if (err) return done(err);

             var conflict= _.after(2,function ()
                 {
                     dave.get('notes','date',
                     function (err,value,meta)
                     {
                         if (err) return done(err);

                         dave.put('notes','date',value,meta,
                         function (err,meta)
                         {
                             if (err) return done(err);

                             cathy.get('notes','date',
                             function (err,value,meta)
                             {
                                 if (err) return done(err);

                                 'Thursday'.should.equal(value);
                                 done();
                             },
                             should.not.exist);
                         });
                     },
                     function (siblings) // resolve
                     {
                         siblings.length.should.equal(2);
                         _.pluck(siblings,'content').should.contain('Tuesday');
                         _.pluck(siblings,'content').should.contain('Thursday');
                         return 'Thursday';
                     });
                 });

             ben.put('notes','date','Tuesday',meta,
             function (err,meta)
             {
                 if (err) return done(err);

                 dave.put('notes','date','Tuesday',meta,
                 function (err,meta)
                 {
                     if (err) return done(err);

                     conflict();
                 });
             });

             cathy.put('notes','date','Thursday',meta,
             function (err,meta)
             {
                 conflict();
             });
         });
     });
});
