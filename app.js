/**
 * New node file
 */
var neo4j = require('node-neo4j');
var util = require('util');
var extend = util._extend;
var inherits = function (ctor, superCtor) {
  ctor.super_ = superCtor;
  ctor.prototype = Object.create(superCtor.prototype, {
    constructor: {
      value: ctor,
      enumerable: false
    }
  });
};
//var json=function(d){
//  return JSON.stringify(d, null,10);
//};
String.prototype.padLeft = function(padString, length) {return (new Array(length+1).join(padString)+this).slice(-length);};
String.prototype.padRight = function(padString, length) {return (this+new Array(length+1).join(padString)).slice(0,length);};
String.prototype.rightOf=function(sub){var pos = this.indexOf(sub);return (pos>=0)?this.substring(sub.length+pos):this;};
String.prototype.leftOf=function(sub){var pos = this.indexOf(sub);return (pos>=0)?this.substring(0,pos):this;};
String.prototype.replaceAll=function(s,r){return this.replace(new RegExp(s,'g'),r);};
String.prototype.endsWith = function (suffix) {return (this.substr(this.length - suffix.length) === suffix);};
String.prototype.startsWith = function(prefix) {return (this.substr(0, prefix.length) === prefix);};
String.prototype.trim = function() {return this.replace(/^\s+|\s+$/g, "");};
String.prototype.ltrim = function() {return this.replace(/^\s+/, "");};
String.prototype.rtrim = function() {return this.replace(/\s+$/, "");};
String.prototype.isNumber = function(){return /^\d+$/.test(this);};
String.prototype.camelCase=function(){var s1 = this.replace(new RegExp(' ','g'),'');return s1.substring(0,1).toLowerCase() + s1.substring(1);};
var neoSchema={};
function NeoSchema(){
  this.nodes = {};
  this.validations={};
}
function NeoSchemaNode(schema,label){
  this.$label = label;
  this.$schema = schema;
}
NeoSchema.prototype.node=function(label){
  return new NeoSchemaNode(this,label);
};
NeoSchemaNode.prototype.props=function(arr){
  this.$schema.nodes[this.$label]=this.$schema.nodes[this.$label]||{};
  for(var k in arr){
    this.$schema.nodes[this.$label][k]=arr[k];
  }
  return this;
};
NeoSchemaNode.prototype.$out=function(arr){
  this.$schema.nodes[this.$label].$out=this.$schema.nodes[this.$label].$out||{};
  var _this=this;
  arr.forEach(function(v,k,arr){
    _this.$schema.nodes[_this.$label].$out[arr[k].name]=arr[k];
  });
//	for(var k in arr){
//
//	}
  return this;
};
NeoSchemaNode.prototype.$in=function(arr){
  this.$schema.nodes[this.$label].$in=this.$schema.nodes[this.$label].$in||{};
//	for(var k in arr){
//		//this.$schema.nodes[this.$label].$in[arr[k].name]=arr[k];
//	}
  var _this=this;
  arr.forEach(function(v,k,arr){
    _this.$schema.nodes[_this.$label].$in[arr[k].name]=arr[k];
  });
  return this;
};
NeoSchemaNode.prototype.$validations=function(arr){
  this.$schema.nodes[this.$label].$v=this.$schema.nodes[this.$label].$v||{};
//	for(var k in arr){
//		//this.$schema.validations[this.$label].$v[k]=arr[k];
//	}
  var _this=this;
  arr.forEach(function(v,k,arr){
    _this.$schema.validations[_this.$label].$v[k]=arr[k];
  });
  return this;
};

//schema.node("User").props([]).outgoing([]).incoming([]).validations({});

var $q = require("q");
function NeoDB(schema,db2) {
  this.db = db2;
  //var db = db2;
  this.schema = schema;
  //neoSchema = schema;
  var _db = this;


  function NeoNode(label, n) {
    this.label = label;
    this.core = n;
    this.validationRules = _db.schema.validations[label];
    this._schema=_db.schema.nodes[label];
    this.$schema = _db.schema;
  }
  this.Q=function(){
    return $q.fcall(function(){});
  };
  NeoNode.prototype.attrs=function(){
    console.log(this);
    return this._schema;
  };
  NeoNode.prototype.get = function (sc, fc) {
    var _this = this;
    var deferred = $q.defer();
    console.log("["+_this.label+"]Get...Reading Node");
    _db.db.readNode(this.core._id, function (err, node) {
      if (err) {
        console.log("["+_this.label+"]Get...Reading Node. Error: " + err);
        if(typeof fc === 'function'){
          fc(err);
        }
        deferred.reject(err);
      } else {
        console.log("["+_this.label+"]Get...Reading Node Successful: " + JSON.stringify(node,null,10));
        _this.core = node;
        if(typeof sc === 'function'){
          sc(_this);
        }
        deferred.resolve(_this);
      }
    });
    return deferred.promise;
  };

  NeoNode.prototype.put = function (o, sc, fc) {
    var _this = this;
    var obj={};
    var isUnique=false;
    for(var k in this._schema){
      var attr = this._schema[k];
      if(attr.unique){
        obj[k]=o[k];
        isUnique=true;
      }
    }
    var deferred = $q.defer();
    console.log("["+_this.label+"]Put...isUnique: " + isUnique);
    if(isUnique){
      console.log("["+_this.label+"]Put...finding one with: " + JSON.stringify(obj,null,10));
      this.findOneBy(obj, function (rs) {
        console.log("["+_this.label+"]Put...finding one, updating...");
        _this.update(o,function(rs2){
          console.log("["+_this.label+"]Put...updated.");
          if(typeof sc === 'function'){
            sc(_this);
          }
          deferred.resolve(_this);
        }, function (err) {
          console.log("["+_this.label+"]Put...update Error."+JSON.stringify(err,null,10));
          if(typeof fc === 'function'){
            fc(err);
          }
          deferred.reject(err);
        });
      },function(err){
        console.log("["+_this.label+"]Put...did not found one, Creating...");
        _this.create(o,function(rs2){
          console.log("["+_this.label+"]Put...Created.");
          if(typeof sc === 'function'){
            sc(_this);
          }
          deferred.resolve(_this);
        }, function (err) {
          console.log("["+_this.label+"]Put...create Error."+JSON.stringify(err,null,10));
          if(typeof fc === 'function'){
            fc(err);
          }
          deferred.reject(err);
        },true);
      });
    }else{
      console.log("["+_this.label+"]Put... Creating...");
      _this.create(o,function(rs2){
        console.log("["+_this.label+"]Put...Created.");
        if(typeof sc === 'function'){
          sc(_this);
        }
        deferred.resolve(_this);
      }, function (err) {
        console.log("["+_this.label+"]Put...create Error."+JSON.stringify(err,null,10));
        if(typeof fc === 'function'){
          fc(err);
        }
        deferred.reject(err);
      });
    }
    return deferred.promise;
  };

  NeoNode.prototype.update = function (o, sc, fc) {
    var _this = this;
    o.updatedDate=global.now();
    var messages=this.validate4Update(o);
    if(messages.length>0){
      if(typeof fc === 'function'){
        fc(messages);return;
      }
    }
    var deferred = $q.defer();
    console.log("["+_this.label+"]Update...: ");
    _db.db.updateNode(_this.core._id, o, function (err, node) {

      if (err) {
        console.log("["+_this.label+"]Update... Error."+JSON.stringify(err,null,10));
        if(typeof fc === 'function'){
          fc(err);
        }
        deferred.reject(err);
      } else {
        console.log("["+_this.label+"]Update... Successful.");
        _db.db.readNode(_this.core._id, function (err, node) {
          if (err) {
            if(typeof fc === 'function'){
              fc(err);
            }
            deferred.reject(err);
          } else {
            _this.core = node;
            if(typeof sc === 'function'){
              sc(_this);
            }
            deferred.resolve(_this);
          }
        });
      }
    });
    return deferred.promise;
  };
  NeoNode.prototype['delete'] = function (sc, fc) {
    var deferred = $q.defer();
    var _this=this;
    console.log("["+_this.label+"]Delete..."+this.core._id);
    _db.db.deleteNode(this.core._id, function (err, node) {
      if (err || !node) {
        console.log("["+_this.label+"]Delete... Error."+JSON.stringify(err||node,null,10));
        if(typeof fc === 'function'){
          fc(err);
        }
        deferred.reject(err);
      } else {
        console.log("["+_this.label+"]Delete... Successful.");
        delete _this.core;
        if(typeof sc === 'function'){
          sc(_this);
        }
        deferred.resolve(_this);
      }
    });
    return deferred.promise;
  };
  var valueByDataType=function(v,n){
    return n;
  };
  NeoNode.prototype.validate = function(n,locale){
    var messages = [];
    var loc = locale||n.locale||"default";
    if(typeof this.validationRules !== 'undefined'){
      for(var k in this.validationRules.$v){
        var fld = this.validationRules.$v[k];
        var fldval = n[k];
        for(var r=0;r<fld.length;r++){
          var rule=fld[r];
          var rulecode = rule.type;
          if(rulecode==='regex'){
            var re = new RegExp(rule.regex,"ig");
            if(!re.test(valueByDataType(rule.datatype,fldval))){
              messages.push(rule.message[loc]);
              break;
            }
          }else if(rulecode==='required'){
            if(fldval===''){
              messages.push(rule.message[loc]);
              break;
            }
          }else if(rulecode==='range'){
            var fromDate = valueByDataType(rule.datatype,rule.from);
            var currentDate = valueByDataType(rule.datatype,fldval);
            var toDate = valueByDataType(rule.datatype,rule.to);
            if(fromDate === null || fromDate === '' || isNaN(fromDate))
              fromDate = '-999999999999';
            if(toDate === null || toDate === '' || isNaN(toDate))
              toDate = '999999999999';
            isless = true;
            if(typeof rule.from !== 'undefined' && typeof rule.to !== 'undefined' && rule.from !== '' && rule.to !== '' && fldval !=='' && !(fromDate <= currentDate) && !(currentDate <= toDate)){
              messages.push(rule.message[loc]);
              break;
            }
          }else if(rulecode==='notrange'){
            if(fldval!='' && (valueByDataType(rule.datatype,rule.from) <= valueByDataType(rule.datatype,fldval) && valueByDataType(rule.datatype,fldval) <= valueByDataType(rule.datatype,rule.to))){
              messages.push(rule.message[loc]);
              break;
            }
          }


        }
      }
    }
    return messages;
  };
  NeoNode.prototype.validate4Update = function(n,locale){
    var messages = [];
    var loc = locale||n.locale||"default";
    for(var k in this.validationRules){
      var fld = this.validationRules[k];
      var fldval = n[k];
      if(typeof fldval == 'undefined'){
        continue;
      }
      for(var r=0;r<fld.length;r++){
        var rule=fld[r];
        var rulecode = rule.type;
        if(rulecode==='regex'){
          var re = new RegExp(rule.regex,"ig");
          if(!re.test(valueByDataType(rule.datatype,fldval))){
            messages.push(rule.message[loc]);
            break;
          }
        }else if(rulecode==='required'){
          if(fldval==''){
            messages.push(rule.message[loc]);
            break;
          }
        }else if(rulecode==='range'){
          var fromDate = valueByDataType(rule.datatype,rule.from);
          var currentDate = valueByDataType(rule.datatype,fldval);
          var toDate = valueByDataType(rule.datatype,rule.to);
          if(fromDate === null || fromDate === '' || isNaN(fromDate))
            fromDate = '-999999999999';
          if(toDate === null || toDate === '' || isNaN(toDate))
            toDate = '999999999999';
          isless = true;
          if(typeof rule.from !== 'undefined' && typeof rule.to !== 'undefined' && rule.from !== '' && rule.to !== '' && fldval !=='' && !(fromDate <= currentDate) && !(currentDate <= toDate)){
            messages.push(rule.message[loc]);
            break;
          }
        }else if(rulecode==='notrange'){
          if(fldval!='' && (valueByDataType(rule.datatype,rule.from) <= valueByDataType(rule.datatype,fldval) && valueByDataType(rule.datatype,fldval) <= valueByDataType(rule.datatype,rule.to))){
            messages.push(rule.message[loc]);
            break;
          }
        }


      }
    }
    return messages;
  };
  NeoNode.prototype.list = function (pi, sc, fc) {
    pi.f = pi.f || 0;
    pi.s = pi.s || 9;
    var qry = "";
    var _this=this;
    if(typeof pi.qry != 'undefined' && pi.qry != ''){
      qry=pi.qry + " SKIP " + pi.f + " LIMIT " + pi.s;
    }else {
      var where = '';
      if (typeof pi.q != 'undefined' && pi.q != '') {
        where = " WHERE " + pi.q + " ";
      }
      qry = "MATCH (n:" + this.label + ") " + where + " RETURN n ORDER BY n.updatedDate SKIP " + pi.f + " LIMIT " + pi.s;
    }
    console.log("["+_this.label+"]List... Qry: " + qry);
    var deferred = $q.defer();
    _db.db.cypherQuery(qry, function (err, result) {
      if (err) {
        console.log("["+_this.label+"]List... Error."+JSON.stringify(err,null,10));
        if(typeof fc === 'function'){
          fc(err);
        }
        deferred.reject(err);
      } else {
        console.log("["+_this.label+"]List... Successful. - Count " + result.data.length);
        _this.lst = {
          columns: result.columns,
          data: result.data,
          pi: {f: pi.f, s: pi.s, nomore: (pi.s > result.data.length)}
        };
        if(typeof sc === 'function'){
          sc(_this);
        }
        deferred.resolve(_this);
      }
    });
    return deferred.promise;
  };

  NeoNode.prototype.findOneBy = function (kv, sc, fc) {
    var qry = "";
    var qArr = [];
    var _this = this;

    for(var k in kv){
      var v = kv[k];
      if(typeof v == 'undefined'){
        qArr.push(" n."+k+" is null ");
      }else if(typeof v == 'number'){
        qArr.push(" n."+k+" = "+v+" ");
      }else{
        qArr.push(" n."+k+" = '"+v+"' ");
      }
    }
    var where = " WHERE "+  qArr.join(" and ") + " ";
    qry = "MATCH (n:" + this.label + ") " + where + " RETURN n LIMIT 1";
    console.log("["+_this.label+"]FindOneBy... Qry: "+qry);
    var deferred = $q.defer();
    _db.db.cypherQuery(qry, function (err, result) {
      if (err || result.data.length==0) {
        console.log("["+_this.label+"]FindOneBy... Error."+JSON.stringify(err||result.data.length,null,10));
        if(typeof fc === 'function'){
          fc(err);
        }
        deferred.reject(err);
      } else {
        console.log("["+_this.label+"]FindOneBy..." + result.data[0]._id);
        _this.core = result.data[0];
        if(typeof sc === 'function'){
          sc(_this);
        }
        deferred.resolve(_this);
      }
    });
    return deferred.promise;
  };
  NeoNode.prototype.findBy = function (kv,pi, sc, fc) {
    pi = pi||{};
    pi.f = pi.f || 0;
    pi.s = pi.s || 9;
    var _this = this;

    var qry = "";
    var qArr = [];
    for(var k in kv){
      var v = kv[k];
      if(typeof v == 'undefined'){
        qArr.push(" n."+k+" is null ");
      }else if(typeof v == 'number'){
        qArr.push(" n."+k+" = "+v+" ");
      }else{
        qArr.push(" n."+k+" = '"+v+"' ");
      }

    }
    if(typeof pi.qry != 'undefined' && pi.qry != ''){
      qry= qArr.join(" and ") + " and " + pi.qry + " SKIP " + pi.f + " LIMIT " + pi.s;
    }else {
      var where = '';
      if (typeof pi.q != 'undefined' && pi.q != '') {
        where = " WHERE " +  qArr.join(" and ") + " and " + pi.q + ".*'  ";
      }else{
        where = " WHERE "+  qArr.join(" and ") + " ";
      }
      qry = "MATCH (n:" + this.label + ") " + where + " RETURN n ORDER BY n.updatedDate SKIP " + pi.f + " LIMIT " + pi.s;
    }
    console.log("["+_this.label+"]FindBy... Qry: "+qry);
    var deferred = $q.defer();
    _db.db.cypherQuery(qry, function (err, result) {
      if (err) {
        console.log("["+_this.label+"]FindBy... Error."+JSON.stringify(err,null,10));
        if(typeof fc === 'function'){
          fc(err);
        }
        deferred.reject(err);
      } else {
        console.log("["+_this.label+"]FindBy...Count: " + result.data.length);
        _this.lst = {
          columns: result.columns,
          data: result.data,
          pi: {f: pi.f, s: pi.s, nomore: (pi.s > result.data.length)}
        };
        if(typeof sc === 'function'){
          sc(_this);
        }
        deferred.resolve(_this);
      }
    });
    return deferred.promise;
  };
  NeoNode.prototype.create = function (o, sc, fc,force) {
    o.createdDate=global.now();
    var messages=this.validate(o);
    var deferred = $q.defer();
    if(messages.length>0){
      if(typeof fc === 'function'){
        fc(messages);return;
      }
    }
    var _this = this;
    var deferred = $q.defer();
    if(force) {
      console.log("["+_this.label+"]Create... : ");
      _db.db.insertNode(o, _this.label, function (err, node) {
        if (err) {
          console.log("["+_this.label+"]Create... Error : "+JSON.stringify(err,null,10));
          if (typeof fc === 'function') {
            fc(err);
          }
          deferred.reject(err);
        } else {
          console.log("["+_this.label+"]Create... Successful : " + node._id);
          _this.core = node;
          if (typeof sc === 'function') {
            sc(_this);
          }
          deferred.resolve(_this);
        }
      });
    }else{
      var obj={};
      var isUnique=false;
      console.log("["+_this.label+"]Create... Unique Fields : ");
      for(var k in this._schema){
        var attr = this._schema[k];
        //console.log(attr);
        if(attr.unique){
          obj[k]=o[k];
          isUnique=true;
        }
      }
      console.log("["+_this.label+"]Create... Unique Fields : " + isUnique + "..." + json(obj));
      if(isUnique){
        this.findOneBy(obj, function (rs) {
          var err = "EXISTS";
          console.log("["+_this.label+"]Create... Already Exists : ");
          if(typeof fc === 'function'){
            fc(err,rs.core);
          }
          deferred.reject(err,rs.core);
        },function(err){
          _db.db.insertNode(o, _this.label, function (err, node) {
            if (err) {
              console.log("["+_this.label+"]Create... Error : "+JSON.stringify(err,null,10));
              if (typeof fc === 'function') {
                fc(err);
              }
              deferred.reject(err);
            } else {
              console.log("["+_this.label+"]Create... Successful : " + node._id);
              _this.core = node;
              if (typeof sc === 'function') {
                sc(_this);
              }
              deferred.resolve(_this);
            }
          });
        });
      }else{
        console.log("["+_this.label+"]Create...: ");
        _db.db.insertNode(o, _this.label, function (err, node) {
          if (err) {
            console.log("["+_this.label+"]Create... Error : "+JSON.stringify(err,null,10));
            if (typeof fc === 'function') {
              fc(err);
            }
            deferred.reject(err);
          } else {
            console.log("["+_this.label+"]Create... Successful : " + node._id);
            _this.core = node;
            if (typeof sc === 'function') {
              sc(_this);
            }
            deferred.resolve(_this);
          }
        });
      }
    }
    return deferred.promise;

  }


  NeoNode.prototype.$out = function (label,sc, fc) {
    var _this = this;
    var _this = this;
    pi = pi || {};
    pi.f = pi.f || 0;
    pi.s = pi.s || 9;
    var where = '';
    if(typeof pi.qry != 'undefined'){
      where = " and " + pi.qry;
    }
    console.log("MATCH n1-[n:" + label + "]->n2 WHERE id(n1) = " + this.core._id + where + " RETURN n2,n ORDER BY n2.updatedDate SKIP " + pi.f + " LIMIT " + pi.s);
    var deferred = $q.defer();
    _db.db.cypherQuery("MATCH n1-[n:" + label + "]->n2 WHERE id(n1) = " + this.core._id + where + " RETURN n2,n ORDER BY n2.updatedDate SKIP " + pi.f + " LIMIT " + pi.s, function (err, result) {
      if (err) {
        if(typeof fc === 'function'){
          fc(err);
        }
        deferred.reject(err);
      } else {
        _this.lst = {
          columns: result.columns,
          data: result.data,
          pi: {f: pi.f, s: pi.s, nomore: (pi.s > result.data.length)}
        };
        if(typeof sc === 'function'){
          sc(_this);
        }
        deferred.resolve(_this);
      }
    });
    return deferred.promise;
  }
  NeoNode.prototype.$in = function (label,sc, fc) {
    var _this = this;
    var _this = this;
    pi = pi || {};
    pi.f = pi.f || 0;
    pi.s = pi.s || 9;
    var where = '';
    if(typeof pi.qry != 'undefined'){
      where = " and " + pi.qry;
    }
    console.log("MATCH n1<-[n:" + label + "]-n2 WHERE id(n1) = " + this.core._id + where + " RETURN n2,n ORDER BY n2.updatedDate SKIP " + pi.f + " LIMIT " + pi.s);
    var deferred = $q.defer();
    _db.db.cypherQuery("MATCH n1<-[n:" + label + "]-n2 WHERE id(n1) = " + this.core._id + where + " RETURN n2,n ORDER BY n2.updatedDate SKIP " + pi.f + " LIMIT " + pi.s, function (err, result) {
      if (err) {
        if(typeof fc === 'function'){
          fc(err);
        }
        deferred.reject(err);
      } else {
        _this.lst = {
          columns: result.columns,
          data: result.data,
          pi: {f: pi.f, s: pi.s, nomore: (pi.s > result.data.length)}
        };
        if(typeof sc === 'function'){
          sc(_this);
        }
        deferred.resolve(_this);
      }
    }); return deferred.promise;
  }
  NeoNode.prototype.any = function (label,sc, fc) {
    var _this = this;
    var _this = this;
    pi = pi || {};
    pi.f = pi.f || 0;
    pi.s = pi.s || 9;
    var where = '';
    if(typeof pi.qry != 'undefined'){
      where = " and " + pi.qry;
    }
    console.log("MATCH n1-[n:" + label + "]-n2 WHERE id(n1) = " + this.core._id + where + " RETURN n2,n ORDER BY n2.updatedDate SKIP " + pi.f + " LIMIT " + pi.s);
    var deferred = $q.defer();
    _db.db.cypherQuery("MATCH n1-[n:" + label + "]-n2 WHERE id(n1) = " + this.core._id + where + " RETURN n2,n ORDER BY n2.updatedDate SKIP " + pi.f + " LIMIT " + pi.s, function (err, result) {
      if (err) {
        if(typeof fc === 'function'){
          fc(err);
        }
        deferred.reject(err);
      } else {
        _this.lst = {
          columns: result.columns,
          data: result.data,
          pi: {f: pi.f, s: pi.s, nomore: (pi.s > result.data.length)}
        };
        if(typeof sc === 'function'){
          sc(_this);
        }
        deferred.resolve(_this);
      }
    }); return deferred.promise;
  }
  NeoNode.prototype.relWith = function (n,sc, fc) {
    var _this = this;
    var pi={};
    pi = pi || {};
    pi.f = pi.f || 0;
    pi.s = pi.s || 9;
    var where = '';
    if(typeof pi.qry != 'undefined'){
      where = " and " + pi.qry;
    }
    console.log("MATCH n1-[n]-n2 WHERE id(n1) = " + this.core._id + " and id(n2) = "+n._id+" " + where + " RETURN n,type(n) ORDER BY n2.updatedDate SKIP " + pi.f + " LIMIT " + pi.s);
    var deferred = $q.defer();_db.db.cypherQuery("MATCH n1-[n]-n2 WHERE id(n1) = " + this.core._id + " and id(n2) = "+n._id+" " + where + " RETURN n,type(n) ORDER BY n2.updatedDate SKIP " + pi.f + " LIMIT " + pi.s, function (err, result) {
      if (err) {
        if(typeof fc === 'function'){
          fc(err);
        }
        deferred.reject(err);
      } else {
        _this.lst = {
          columns: result.columns,
          data: result.data,
          pi: {f: pi.f, s: pi.s, nomore: (pi.s > result.data.length)}
        };
        if(typeof sc === 'function'){
          sc(_this);
        }
        deferred.resolve(_this);
      }
    }); return deferred.promise;
  }

  function NeoEdge(sch,n1, label, dir) {
    this.direction = dir;
    this.label = label;
    this.$core = n1;
    this._schema = sch;
    this.core=n1.core;
  }

  NeoEdge.prototype.list = function (pi,sc, fc) {
    pi = pi || {};
    pi.f = pi.f || 0;
    pi.s = pi.s || 9;
    var _this = this;
    var where = '';
    if(typeof pi.q != 'undefined'){
      where = " and " + pi.q;
    }
    var deferred = $q.defer();
    if (this.direction === 'out') {
      console.log("MATCH n1-[n:" + this.label + "]->n2 WHERE id(n1) = " + this.core._id + where + " RETURN n2,n ORDER BY n2.updatedDate SKIP " + pi.f + " LIMIT " + pi.s);
      _db.db.cypherQuery("MATCH n1-[n:" + this.label + "]->n2 WHERE id(n1) = " + this.core._id + where + " RETURN n2,n ORDER BY n2.updatedDate SKIP " + pi.f + " LIMIT " + pi.s, function (err, result) {
        if (err) {
          if(typeof fc === 'function'){
            fc(err);
          }
          deferred.reject(err);
        } else {
          _this.lst = {
            columns: result.columns,
            data: result.data,
            pi: {f: pi.f, s: pi.s, nomore: (pi.s > result.data.length)}
          };
          if(typeof sc === 'function'){
            sc(_this);
          }
          deferred.resolve(_this);
        }
      });
    } else if (this.direction === 'in') {
      console.log("MATCH n1<-[n:" + this.label + "]-n2 WHERE id(n1) = " + this.core._id + where + " RETURN n2,n ORDER BY n2.updatedDate SKIP " + pi.f + " LIMIT " + pi.s);
      _db.db.cypherQuery("MATCH n1<-[n:" + this.label + "]-n2 WHERE id(n1) = " + this.core._id +  where + " RETURN n2,n ORDER BY n2.updatedDate SKIP " + pi.f + " LIMIT " + pi.s, function (err, result) {
        if (err) {
          if(typeof fc === 'function'){
            fc(err);
          }
          deferred.reject(err);
        } else {
          _this.lst = {
            columns: result.columns,
            data: result.data,
            pi: {f: pi.f, s: pi.s, nomore: (pi.s > result.data.length)}
          };
          if(typeof sc === 'function'){
            sc(_this);
          }
          deferred.resolve(_this);
        }
      });
    } else {
      console.log("MATCH n1-[n:" + this.label + "]-n2 WHERE id(n1) = " + this.core._id + where + " RETURN n2,n ORDER BY n2.updatedDate SKIP " + pi.f + " LIMIT " + pi.s);
      _db.db.cypherQuery("MATCH n1-[n:" + this.label + "]-n2 WHERE id(n1) = " + this.core._id +  where + " RETURN n2,n ORDER BY n2.updatedDate SKIP " + pi.f + " LIMIT " + pi.s, function (err, result) {
        if (err) {
          if(typeof fc === 'function'){
            fc(err);
          }
          deferred.reject(err);
        } else {
          _this.lst = {
            columns: result.columns,
            data: result.data,
            pi: {f: pi.f, s: pi.s, nomore: (pi.s > result.data.length)}
          };
          if(typeof sc === 'function'){
            sc(_this);
          }
          deferred.resolve(_this);
        }
      });
    }
    return deferred.promise;
  }

  NeoEdge.prototype.listEx = function (pi,sc, fc) {
    pi = pi || {};
    pi.f = pi.f || 0;
    pi.s = pi.s || 9;
    var _this = this;
    var where = '';
    var qry = "";

    var deferred = $q.defer();
    if (this.direction === 'out') {
      if(typeof pi.q != 'undefined'){
        where = " and " + pi.q;
        qry = "MATCH n1-[n:" + this.label + "]->n2 WHERE id(n1) = " + this.core._id + where + " RETURN n2,n ORDER BY n2.updatedDate SKIP " + pi.f + " LIMIT " + pi.s;
      }
      if(typeof pi.qry != 'undefined'){
        qry = "MATCH n1-[n:" + this.label + "]->n2 WHERE id(n1) = " + this.core._id + " and " + pi.qry + " SKIP " + pi.f + " LIMIT " + pi.s;
      }
    } else if (this.direction === 'in') {
      if(typeof pi.q != 'undefined'){
        where = " and " + pi.q;
        qry = "MATCH n1<-[n:" + this.label + "]-n2 WHERE id(n1) = " + this.core._id + where + " RETURN n2,n ORDER BY n2.updatedDate SKIP " + pi.f + " LIMIT " + pi.s;
      }
      if(typeof pi.qry != 'undefined'){
        qry = "MATCH n1<-[n:" + this.label + "]-n2 WHERE id(n1) = " + this.core._id + " and " + pi.qry + " SKIP " + pi.f + " LIMIT " + pi.s;
      }
    } else {
      if(typeof pi.q != 'undefined'){
        where = " and " + pi.q;
        qry = "MATCH n1-[n:" + this.label + "]-n2 WHERE id(n1) = " + this.core._id + where + " RETURN n2,n ORDER BY n2.updatedDate SKIP " + pi.f + " LIMIT " + pi.s;
      }
      if(typeof pi.qry != 'undefined'){
        qry = "MATCH n1-[n:" + this.label + "]-n2 WHERE id(n1) = " + this.core._id + " and " + pi.qry + " SKIP " + pi.f + " LIMIT " + pi.s;
      }
    }
    console.log(qry);
    _db.db.cypherQuery(qry, function (err, result) {
      if (err) {
        if(typeof fc === 'function'){
          fc(err);
        }
        deferred.reject(err);
      } else {
        _this.lst = {
          columns: result.columns,
          data: result.data,
          pi: {f: pi.f, s: pi.s, nomore: (pi.s > result.data.length)}
        };
        if(typeof sc === 'function'){
          sc(_this);
        }
        deferred.resolve(_this);
      }
    });
    return deferred.promise;
  }

  NeoEdge.prototype.findOneBy = function (nCriteria,rCriteria,sc, fc) {
    var _this = this;
    var deferred = $q.defer();
    console.log("["+_this.label+"]["+_this.direction+"]findOneBy... : ");
    this.findBy(nCriteria,rCriteria,{f:0,s:1}, function (rs) {
      if(_this.lst.data.length==0){
        var err="NOT_EXISTS";
        console.log("["+_this.label+"]["+_this.direction+"]findOneBy... NOT EXISTS: ");
        if(typeof fc === 'function'){
          fc(err);
        }
        deferred.reject(err);
      }else{
        _this.core2 = _this.lst.data[0][0];
        _this.rel = _this.lst.data[0][1];
        console.log("["+_this.label+"]["+_this.direction+"]findOneBy... : " + _this.core._id);
        delete _this.lst ;
        if(typeof sc === 'function'){
          sc(_this);
        }
        deferred.resolve(_this);
      }
    },fc);return deferred.promise;
  }

  NeoEdge.prototype.findBy = function (nCriteria,rCriteria,pi,sc, fc) {
    pi = pi || {};
    pi.f = pi.f || 0;
    pi.s = pi.s || 9;
    var _this = this;
    var where = '';

    var qArr1 = [];
    for(var k in nCriteria){
      var v = nCriteria[k];
      if(v=='_id'){
        qArr1.push(" id(n2) = "+v+" ");
      }else if(typeof v == 'number'){
        qArr1.push(" n2."+k+" = "+v+" ");
      }else{
        qArr1.push(" n2."+k+" = '"+v+"' ");
      }
    }
    for(var k in rCriteria){
      var v = rCriteria[k];
      if(typeof v == 'undefined'){
        qArr1.push(" n."+k+" is null ");
      }else if(typeof v == 'number'){
        qArr1.push(" n."+k+" = "+v+" ");
      }else{
        qArr1.push(" n."+k+" = '"+v+"' ");
      }
    }
    var where = " and " + qArr1.join(" and ") + " ";

    var deferred = $q.defer();
    console.log("["+_this.label+"]["+_this.direction+"]findBy... : ");
    if (this.direction === 'out') {
      console.log("["+_this.label+"]["+_this.direction+"]findBy... Qry: MATCH n1-[n:" + this.label + "]->n2 WHERE id(n1) = " + this.core._id + where + " RETURN n2,n ORDER BY n2.updatedDate SKIP " + pi.f + " LIMIT " + pi.s);
      _db.db.cypherQuery("MATCH n1-[n:" + this.label + "]->n2 WHERE id(n1) = " + this.core._id + where + " RETURN n2,n ORDER BY n2.updatedDate SKIP " + pi.f + " LIMIT " + pi.s, function (err, result) {
        if (err) {
          console.log("["+_this.label+"]["+_this.direction+"]findBy... : Error. " + json(err));
          if(typeof fc === 'function'){
            fc(err);
          }
          deferred.reject(err);
        } else {
          console.log("["+_this.label+"]["+_this.direction+"]findBy... : Count. " + result.data.length);
          _this.lst = {
            columns: result.columns,
            data: result.data,
            pi: {f: pi.f, s: pi.s, nomore: (pi.s > result.data.length)}
          };
          if(typeof sc === 'function'){
            sc(_this);
          }
          deferred.resolve(_this);
        }
      });
    } else if (this.direction === 'in') {
      console.log("["+_this.label+"]["+_this.direction+"]findBy... Qry: MATCH n1<-[n:" + this.label + "]-n2 WHERE id(n1) = " + this.core._id + where + " RETURN n2,n ORDER BY n2.updatedDate SKIP " + pi.f + " LIMIT " + pi.s);
      _db.db.cypherQuery("MATCH n1<-[n:" + this.label + "]-n2 WHERE id(n1) = " + this.core._id +  where + " RETURN n2,n ORDER BY n2.updatedDate SKIP " + pi.f + " LIMIT " + pi.s, function (err, result) {
        if (err) {
          console.log("["+_this.label+"]["+_this.direction+"]findBy... : Error. " + json(err));
          if(typeof fc === 'function'){
            fc(err);
          }
          deferred.reject(err);
        } else {
          console.log("["+_this.label+"]["+_this.direction+"]findBy... : Count. " + result.data.length);
          _this.lst = {
            columns: result.columns,
            data: result.data,
            pi: {f: pi.f, s: pi.s, nomore: (pi.s > result.data.length)}
          };
          if(typeof sc === 'function'){
            sc(_this);
          }
          deferred.resolve(_this);
        }
      });
    } else {
      console.log("["+_this.label+"]["+_this.direction+"]findBy... Qry: MATCH n1-[n:" + this.label + "]-n2 WHERE id(n1) = " + this.core._id + where + " RETURN n2,n ORDER BY n2.updatedDate SKIP " + pi.f + " LIMIT " + pi.s);
      _db.db.cypherQuery("MATCH n1-[n:" + this.label + "]-n2 WHERE id(n1) = " + this.core._id +  where + " RETURN n2,n ORDER BY n2.updatedDate SKIP " + pi.f + " LIMIT " + pi.s, function (err, result) {
        if (err) {
          console.log("["+_this.label+"]["+_this.direction+"]findBy... : Error. " + json(err));
          if(typeof fc === 'function'){
            fc(err);
          }
          deferred.reject(err);
        } else {
          console.log("["+_this.label+"]["+_this.direction+"]findBy... : Count. " + result.data.length);
          _this.lst = {
            columns: result.columns,
            data: result.data,
            pi: {f: pi.f, s: pi.s, nomore: (pi.s > result.data.length)}
          };
          if(typeof sc === 'function'){
            sc(_this);
          }
          deferred.resolve(_this);
        }
      });
    }
    return deferred.promise;
  }

  NeoEdge.prototype.get = function (sc, fc) {
    var deferred = $q.defer();
    var _this = this;
    this.list({f:0,s:1}, function (res) {
      if(res.lst.data.length==0){
        console.log("["+_this.label+"]["+_this.direction+"]Get... Error: " + json("NO_RECORDS"));
        if(typeof fc === 'function'){
          fc({messages:["NO_RECORDS"]});
        }
        deferred.reject({messages:["NO_RECORDS"]});
      }else{
        console.log("["+_this.label+"]["+_this.direction+"]Get... Successful: " + json(res.lst.data));
        _this.core2=res.lst.data[0][0];
        _this.rel=res.lst.data[0][1];
        delete _this.lst;
        if(typeof sc === 'function'){
          sc(_this);
        }
        deferred.resolve({core2:_this.core2,rel:_this.rel});
      }
    },function(err){
      console.log("["+_this.label+"]["+_this.direction+"]Get... Error: " + json(err));
      if(typeof fc === 'function'){
        fc(err);
      }
      deferred.reject(err);
    });
    return deferred.promise;
  }

  NeoEdge.prototype.addOrUpdate = function (n, rel, sc, fc) {
    var deferred = $q.defer();var _this=this;
    console.log("Direction..."+this.direction);
    if(this.direction=='out'){
      console.log("OUT...." + this.core._id + " ==> " + n._id);
      _db.db.insertRelationship(this.core._id, n._id, _this.label, rel, function (err, relationship) {
        if (err) {
          if(typeof fc === 'function'){
            fc(err);
          }
          deferred.reject(err);
        } else {
          _this.rel = relationship;
          _this.core = n;
          if(typeof sc === 'function'){
            sc(_this);
          }
          deferred.resolve(_this);
        }
      });
    }else{
      console.log("IN...." + this.core._id + " <== " + n._id);
      _db.db.insertRelationship(n._id, this.core._id,  _this.label, rel, function (err, relationship) {
        if (err) {
          if(typeof fc === 'function'){
            fc(err);
          }
          deferred.reject(err);
        } else {
          _this.rel = relationship;
          _this.core = n;
          if(typeof sc === 'function'){
            sc(_this);
          }
          deferred.resolve(_this);
        }
      });
    }
    _db.db.updateRelationship(o._id||_this.rel._id,o,function (err, relationship) {
      if (err) {
        if(typeof fc === 'function'){
          fc(err);
        }
        deferred.reject(err);
      } else {
        _this.rel = relationship;
        if(typeof sc === 'function'){
          sc(_this);
        }
        deferred.resolve(_this);
      }
    })
    return deferred.promise;
  }
  NeoEdge.prototype.addIfNotExists = function (n, rel, sc, fc) {

  }
  NeoEdge.prototype.addNew = function (n, rel, sc, fc) {
    var typ = this._schema.type.replace("[]","");
    var nam = this._schema.name;
    var deferred = $q.defer();var _this=this;
    _db[typ]().create(n,function(rs){
      _this.$core[nam]().add(rs.core, rel, function (rs2) {
        if(typeof sc === 'function'){
          sc(_this);
        }
        deferred.resolve(_this);
      }, function (err) {
        if(typeof fc === 'function'){
          fc(err);
        }
        deferred.reject(err);
      });
    },function(err,fcore){
      if(err=="EXISTS"){
        _this.$core[nam]().add(fcore, rel, function (rs2) {
          if(typeof sc === 'function'){
            sc(_this);
          }
          deferred.resolve(_this);
        }, function (err) {
          if(typeof fc === 'function'){
            fc(err);
          }
          deferred.reject(err);
        });
      }else{
        if(typeof fc === 'function'){
          fc(err);
        }
        deferred.reject(err);
      }

    });
    return deferred.promise;
  };

  NeoEdge.prototype.add = function (n, rel, sc, fc) {
    var deferred = $q.defer();var _this=this;
    console.log("["+_this.label+"]["+_this.direction+"]Add... ");
    if(this.direction=='out'){
      console.log("["+_this.label+"]["+_this.direction+"]Add... " + this.core._id + " ==> " + n._id);
      _db.db.insertRelationship(this.core._id, n._id, _this.label, rel, function (err, relationship) {
        if (err) {
          console.log("["+_this.label+"]["+_this.direction+"]Add... Error: " + json(err));
          if(typeof fc === 'function'){
            fc(err);
          }
          deferred.reject(err);
        } else {
          _this.rel = relationship;
          _this.core2 = n;
          console.log("["+_this.label+"]["+_this.direction+"]Add... : " + json(_this.rel) + "..." + json(_this.core));
          if(typeof sc === 'function'){
            sc(_this);
          }
          deferred.resolve(_this);
        }
      });
    }else{
      console.log("["+_this.label+"]["+_this.direction+"]Add... " + this.core._id + " <== " + n._id);
      _db.db.insertRelationship(n._id, this.core._id,  _this.label, rel, function (err, relationship) {
        if (err) {
          console.log("["+_this.label+"]["+_this.direction+"]Add... Error: " + json(err));
          if(typeof fc === 'function'){
            fc(err);
          }
          deferred.reject(err);
        } else {
          _this.rel = relationship;
          _this.core2 = n;
          console.log("["+_this.label+"]["+_this.direction+"]Add... : " + json(_this.rel) + "..." + json(_this.core));
          if(typeof sc === 'function'){
            sc(_this);
          }
          deferred.resolve(_this);
        }
      });
    }
    return deferred.promise;
  }
  NeoEdge.prototype.update = function(o,sc,fc){
    var deferred = $q.defer();var _this=this;
    _db.db.updateRelationship(o._id||_this.rel._id,o,function (err, relationship) {
      if (err) {
        console.log("["+_this.label+"]["+_this.direction+"]Update... Error: " + json(err));
        if(typeof fc === 'function'){
          fc(err);
        }
        deferred.reject(err);
      } else {
        _this.rel = relationship;
        console.log("["+_this.label+"]["+_this.direction+"]Update... : " + json(_this.rel));
        if(typeof sc === 'function'){
          sc(_this);
        }
        deferred.resolve(_this);
      }
    })
    return deferred.promise;
  }
  NeoEdge.prototype.getRel = function(o,sc,fc){
    var deferred = $q.defer();var _this=this;
    _db.db.readRelationship(o._id,function (err, relationship) {
      if (err) {
        if(typeof fc === 'function'){
          fc(err);
        }
        deferred.reject(err);
      } else {
        _this.rel = relationship;
        if(typeof sc === 'function'){
          sc(_this);
        }
        deferred.resolve(_this);
      }
    })
    return deferred.promise;
  }
  NeoEdge.prototype.remove = function (sc, fc) {
    var deferred = $q.defer();var _this=this;

    _db.db.deleteRelationship(this.rel._id, function (err, rl) {
      if (err || !rl) {
        console.log("["+_this.label+"]["+_this.direction+"]Delete... : Error: " + json(err|| !rl));
        if(typeof fc === 'function'){
          fc(err);
        }
        deferred.reject(err);
      } else {
        console.log("["+_this.label+"]["+_this.direction+"]Delete... : Successful." );
        delete _this.rel;
        if(typeof sc === 'function'){
          sc(_this);
        }
        deferred.resolve(_this);
      }
    });

    return deferred.promise;
  };
  //To be Verified
  NeoEdge.prototype.removeRel = function (n, rid, sc, fc) {
    var deferred = $q.defer();var _this=this;
    this.listRelsWith(n, function (rels) {
      for(var i=0;i<rels.length;i++){
        if(rid==rels[i]._id || rid==-1){
          _db.db.deleteRelationship(rels[i]._id, function (err, rl) {
            if (err) {
              fc(err);
              return;
            }
            if (rl == true) {
              sc(true);
            }
          });
        }
      }
    }, function (err) {
      fc(err);
    });
    return deferred.promise;
  }
  NeoEdge.prototype.listRels = function (sc, fc) {
    var deferred = $q.defer();var _this=this;
    _db.db.readRelationshipsOfNode(this.core._id, {direction: this.direction, types: [this.label]}, function (err, rels) {
      if (err) {
        fc(err);
      } else {
        sc(rels);
      }
    });
    return deferred.promise;
  }





  NeoEdge.prototype.listRelsWith = function (n2, sc, fc, pi) {

    var pi={};
    pi = pi || {};
    pi.f = pi.f || 0;
    pi.s = pi.s || 9;
    var deferred = $q.defer();var _this=this;
    if (this.direction == 'out') {
      console.log("MATCH n1-[n:" + this.label + "]->n2 WHERE id(n1) = " + this.core._id + " and id(n2) = " + n2._id + " RETURN n ORDER BY n.updatedDate SKIP " + pi.f + " LIMIT " + pi.s);
      _db.db.cypherQuery("MATCH n1-[n:" + this.label + "]->n2 WHERE id(n1) = " + this.core._id + " and id(n2) = " + n2._id + " RETURN n ORDER BY n.updatedDate SKIP " + pi.f + " LIMIT " + pi.s, function (err, result) {
        if (err) {
          if(typeof fc === 'function'){
            fc(err);
          }
          deferred.reject(err);
        } else {
          _this.lst = {
            columns: result.columns,
            data: result.data,
            pi: {f: pi.f, s: pi.s, nomore: (pi.s > result.data.length)}
          };
          if(typeof sc === 'function'){
            sc(_this);
          }
          deferred.resolve(_this);
        }
      });
    } else if (this.direction == 'in') {
      console.log("MATCH n1<-[n:" + this.label + "]-n2 WHERE id(n1) = " + this.core._id + " and id(n2) = " + n2._id + " RETURN n ORDER BY n.updatedDate SKIP " + pi.f + " LIMIT " + pi.s);
      _db.db.cypherQuery("MATCH n1<-[n:" + this.label + "]-n2 WHERE id(n1) = " + this.core._id + " and id(n2) = " + n2._id + " RETURN n ORDER BY n.updatedDate SKIP " + pi.f + " LIMIT " + pi.s, function (err, result) {
        if (err) {
          if(typeof fc === 'function'){
            fc(err);
          }
          deferred.reject(err);
        } else {
          _this.lst = {
            columns: result.columns,
            data: result.data,
            pi: {f: pi.f, s: pi.s, nomore: (pi.s > result.data.length)}
          };
          if(typeof sc === 'function'){
            sc(_this);
          }
          deferred.resolve(_this);
        }
      });
    } else {
      console.log("MATCH n1-[n:" + this.label + "]-n2 WHERE id(n1) = " + this.core._id + " and id(n2) = " + n2._id + " RETURN n ORDER BY n.updatedDate SKIP " + pi.f + " LIMIT " + pi.s);

      _db.db.cypherQuery("MATCH n1-[n:" + this.label + "]-n2 WHERE id(n1) = " + this.core._id + " and id(n2) = " + n2._id + " RETURN n ORDER BY n.updatedDate SKIP " + pi.f + " LIMIT " + pi.s, function (err, result) {
        if (err) {
          if(typeof fc === 'function'){
            fc(err);
          }
          deferred.reject(err);
        } else {
          _this.lst = {
            columns: result.columns,
            data: result.data,
            pi: {f: pi.f, s: pi.s, nomore: (pi.s > result.data.length)}
          };
          if(typeof sc === 'function'){
            sc(_this);
          }
          deferred.resolve(_this);
        }
      });
    }
    return deferred.promise;
  }

  for (var nn in this.schema.nodes) {
    //util.inherits(" + nn + ",NeoNode);
    eval("function " + nn + "(){NeoNode.call(this,'"+nn+"');if(arguments.length>=1){this.core=arguments[0];};};"+nn+".prototype=new NeoNode('"+nn+"');"+nn+".prototype.constructor="+nn+";");
    var label = nn;
    var $node = this.schema.nodes[label];
    var genNode=function($node) {
      for (var k in $node) {
        var s = $node[k];
        s = extend({type: 'string', index: false}, s);
        if (k == '$in') {
          for (var kin in $node.$in) {
            var rel = $node.$in[kin];
            var ss = rel.type;
            var lbl = rel.label||rel.name;
            var name = rel.name||rel.label;
            if (ss.endsWith("[]")) {
              var sss = ss.leftOf("[]");
              eval(nn + ".prototype." + name + "=function(){var _this=this;return new NeoEdge(_this._schema.$in['" + name + "'],_this,'"+lbl+"','in');};");
            } else {
              var sss = ss;
              eval(nn + ".prototype." + name + "=function(){var _this=this;return new NeoEdge(_this._schema.$in['" + name + "'],_this,'"+lbl+"','in');};");
            }
            //var _like=rel.extends;
            //eval("util.inherits("+nn+","+_like+");");
          }
        }else if (k == '$out') {
          for (var kin in $node.$out) {
            var rel = $node.$out[kin];
            var ss = rel.type;
            var lbl = rel.label||rel.name;
            var name = rel.name||rel.label;
            if (ss.endsWith("[]")) {
              var sss = ss.leftOf("[]");
              eval(nn + ".prototype." + name + "=function(){var _this=this;return new NeoEdge(_this._schema.$out['" + name + "'],_this,'"+lbl+"','out');};");
            } else {
              var sss = ss;
              eval(nn + ".prototype." + name + "=function(){var _this=this;return new NeoEdge(_this._schema.$out['" + name + "'],_this,'"+lbl+"','out');};");
            }
            //var _like=rel.extends;
            //eval("util.inherits("+nn+","+_like+");");
          }
        } else
        {
          eval(nn + ".prototype." + k + ";");
        }
      }
      eval("NeoDB.prototype[nn]=function(){return (arguments.length==0)?(new " + nn + "()):(new " + nn + "(arguments[0]));};");
    };
    genNode($node);

  }
}
NeoSchema.prototype.db=function(db){
  return new NeoDB(this,db);
}

module.exports = NeoSchema;