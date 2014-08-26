var _= require('underscore'),
    sib= require('../server/sibling')({ timeout: 1000 }),
    node= '127.0.0.1:3000';

var should= require('chai').should(),
    assert= require('chai').assert;

describe('server',function ()
{
     it('read repair is working',function (done)
     {
         sib.list('notes','date',node,
         function (err,responses)
         {
            if (err) return done(err);

            responses.length.should.equal(1);
            responses[0].content.should.equal('"Thursday"');
            
            sib.list('notes','basic',node,
            function (err,responses)
            {
                if (err) return done(err);

                responses.length.should.equal(1);
                responses[0].meta.thumbstone.should.equal(true);
                
                done();
            });
         });
     });
});
