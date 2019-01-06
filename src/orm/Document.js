/* nor-nopg -- NoPg.Document implementation */
"use strict";

var util = require("util");
var NoPgORM = require("./ORM.js");

var meta = require('./meta.js')({
	"table": "documents",
	"datakey": '$content',
	"keys":['$id', '$type', '$content', '$types_id', '$created', '$modified', '$documents']
});

/** The constructor */
function NoPgDocument(opts) {
	var self = this;
	opts = opts || {};
	//debug.log("NoPg.Document(opts = ", opts, ")");
	NoPgORM.call(this);
	meta(self).set_meta_keys(opts).resolve();
}

util.inherits(NoPgDocument, NoPgORM);

/* Universal typing information */
NoPgDocument.prototype.nopg = function() {
	return {
		'orm_type': 'Document'
	};
};

NoPgDocument.meta = meta;

module.exports = NoPgDocument;

/** Get internal database object */
NoPgDocument.prototype.valueOf = function() {
	var self = this;
	return meta(self).unresolve();
};

/** Update changes to current instance */
NoPgDocument.prototype.update = function(data) {
	var self = this;
	//debug.log("NoPg.Document.prototype.update(data = ", data, ")");
	// FIXME: If values are removed from the database, local copy properties are NOT removed currently!
	meta(self).set_meta_keys(data).resolve();
	return self;
};

/** Get plain data presentation */
NoPgDocument.prototype.toData = function() {
	var self = this;
	return meta(self).toData();
};

/** Get JSON data presentation */
NoPgDocument.prototype.toJSON = function() {
	var self = this;
	return self.toData();
};

/* EOF */
