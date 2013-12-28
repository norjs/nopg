/* nor-nopg */

var debug = require('nor-debug');
var Q = require('q');
var pg = require('nor-pg');
var extend = require('nor-extend').setup({useFunctionPromises:true});
var NoPgObject = require('./Object.js');
var NoPgType = require('./Type.js');
var NoPgAttachment = require('./Attachment.js');
var NoPgLib = require('./Lib.js');

/** */
function assert(valid, text) {
	if(!valid) {
		throw new TypeError(text);
	}
}

/** Assert that the `doc` is NoPgObject */
function assert_type(doc, type, text) {
	assert(doc instanceof type, text || "Not correct type: " + type);
}


/** The constructor */
function NoPG(db) {
	var self = this;
	self._db = db;
	self._values = [];
}

module.exports = NoPG;

// Object constructors
NoPG.Object = NoPgObject;

/** Start */
NoPG.start = function(pgconfig) {
	return extend.promise( [NoPG], pg.start(pgconfig).then(function(db) {
		return new NoPG(db);
	}));
};

/** Fetch next value from queue */
NoPG.prototype.fetch = function() {
	return this._values.shift();
};

/** Get object handler */
function get_object(rows) {
	var doc = rows.shift();
	if(!doc) { throw new TypeError("failed to parse object"); }
	var obj = {};
	Object.keys(doc).forEach(function(key) {
		obj['$'+key] = doc[key];
	});
	return new NoPgObject(obj);
}

/** Save object handler */
function save_object_to(self) {
	if(self instanceof NoPgObject) {
		return function(doc) {
			return self.update(doc);
		};
	}

	if(self._values) {
		return function(doc) {
			self._values.push( doc );
			return self;
		};
	}

	throw new TypeError("Unknown target");
}

/** Perform query */
function do_query(self, query, values) {
	return extend.promise( [NoPG], self._db._query(query, values) );
}

/** Commit transaction */
NoPG.prototype.commit = function() {
	return extend.promise( [NoPG], this._db.commit() );
};

/** Initialize the database */
NoPG.prototype.init = function() {
	var self = this;
	var builders = require('./schema/');
	return builders.reduce(function (so_far, f) {
	    return so_far.then(function(db) {
			db.fetchAll();
			return db;
		}).then(f);
	}, Q(self._db)).then(function() { return self; });
};

/** Create object by type: `db.create([TYPE])([OPT(S)])`. */
NoPG.prototype.create = function(type) {
	debug.log('at create(', type, ')');
	var self = this;
	function create2(data) {
		debug.log('at create2(', data, ')');

		var query, params;

		if(type !== undefined) {
			if(typeof type === 'string') {
				query = "INSERT INTO objects (content, types_id) VALUES ($1, get_type($2)) RETURNING *";
				params = [data, type];
			} else if(type instanceof NoPgType) {
				query = "INSERT INTO objects (content, types_id) VALUES ($1, $2) RETURNING *";
				params = [data, type.id];
			} else {
				throw new TypeError("unknown type: " + type);
			}
		} else {
			query = "INSERT INTO objects (content) VALUES ($1) RETURNING *";
			params = [data];
		}

		return do_query(self, query, params).then(get_object).then(save_object_to(self));
	}
	return create2;
};

/** Update object */
NoPG.prototype.update = function(doc, data) {
	var self = this;
	assert_type(doc, NoPgObject, "doc is not NoPg.Object");
	return do_query(self, "UPDATE objects SET content = $1 RETURNING *", [data]).then(get_object).then(save_object_to(doc)).then(function() { return self; });
};

/** Create type object by type: `db.createType([TYPE-NAME])([OPT(S)])`. */
NoPG.prototype.createType = function(name) {
	debug.log('at createType(', name, ')');
	var self = this;
	function create2(opts) {
		opts = opts || {};
		debug.log('at createType2(', opts, ')');
		var schema = opts.schema || {};
		var validator = (''+opts.validator) || null;
		var meta = opts.meta || {};
		return do_query(self, "INSERT INTO types (name, schema, validator, meta) VALUES ($1, $2, $3, $4) RETURNING *", [name, schema, validator, meta]).then(get_object).then(save_object_to(self));
	}
	return create2;
};

/* EOF */
