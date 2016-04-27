# neo4js-objects
Schema Based API for Neo4j in Nodejs

Friends,

This is putting together an idea to easy dealing of Neo4j Graph DB. I used Philipp Küng's node-neo4j.

Roadmap:
1. Transactions
2. Pipe way of doing sequencial operations.

Please send feature requests to <a href='mailto:techpanga@gmail.com'>techpanga</a>


Sample Code:

var Schema=require('neo4js-objects');

var schema=new Schema();

schema.node("UserSession").props({"uid":{},"token":{}});

schema.node("Pref").props({});

schema.node("Media").props({"mtype":{}, "subtype":{},"url":{},"html":{},"content":{},"keywords":{}});

schema.node("User").props({"identifier":{unique:true},"name":{},"emailId":{},"mobile":{},auth:{unique:true},app:{},channel:{},tempKey:{},secode:{}})

    .$out([
    
    {{name:"personUser",type:"Person",label:"personUser"},
    
    {name:"organizationUser",type:"Organization",label:"organizationUser"},
    
    {name:"devices",type:"Device[]",label:"userdevice"}
    
    ]);

schema.node("Person").props({"mobile":{},"name":{},"age":{},"emailId":{},"birthMonth":{},"birthYear":{},"title":{},"profession":{},"interests":{}})

    .$out([
    {name:"user",type:"User",label:"personUser"}

    , {name:"addr",type:"Address"}, {name:"media",type:"Media"}


]);

var neo4j = require('node-neo4j');

var db = new neo4j('http://localhost:7474');

var sys = schema.db(db);

sys.User({_id:108798}).get(function(data){

    //console.log(data);

},function(err){

    //console.log(err);
});
