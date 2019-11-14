/**
 * @file nor-nopg -- NoSQL database library for PostgreSQL
 * @Copyright 2014-2019 Sendanor <info@sendanor.fi>,
 *            2014-2019 Jaakko-Heikki Heusala <jheusala@iki.fi>
 */

import _ from 'lodash';
import moment from 'moment';
import pg from '@norjs/pg';
import { types } from '@norjs/pg';
import orm from './orm';
import merge from 'merge';
import pghelpers from './pghelpers.js';
import Predicate from './Predicate.js';
import EventEmitter from 'events';
import pg_escape from 'pg-escape';
import {
	DEBUG_NOPG,
	DEBUG_NOPG_EVENT_TIMES,
	NOPG_EVENT_TIMES,
	NOPG_TIMEOUT,
	NOPG_TYPE_AWARENESS,
	PGCONFIG
} from "./nopg-env";
import NoPgUtils from "./NoPgUtils";
import LogUtils from "@norjs/utils/Log";
import AssertUtils from "@norjs/utils/src/AssertUtils";
import first_letter_is_dollar from './first_letter_is_dollar.js';

pg.setTypeParser(pg.OID.TIMESTAMP, (timestamp) => moment(timestamp).toISOString());

pg.setTypeParser(pg.OID.TIMESTAMPTZ, (timestamp) => moment(timestamp).toISOString());

/** @typedef {Object} NoPgDefaultsObject
 * @property {string} [pgconfig] -
 * @property {number} [timeout] - The default timeout for transactions to automatically rollback. If this is `undefined`, there will be no timeout.
 *                                 Defaults to 30 seconds.
 * @property {boolean} [enableTypeAwareness] - If enabled NoPg will be more aware of the properties of types.
 *                                            When a user provides a type as a string, it will be converted as
 *                                            a type object. This will enable additional features like optional
 *                                            `traits.documents` support as a predefined in type.
 */

/** @type {object:string} Maps `<table>,<I|U|D>` into NoPg event name
 */
let TCN_EVENT_MAPPING = {
	'documents,I': 'create',
	'documents,U': 'update',
	'documents,D': 'delete',
	'types,I': 'createType',
	'types,U': 'updateType',
	'types,D': 'deleteType',
	'attachments,I': 'createAttachment',
	'attachments,U': 'updateAttachment',
	'attachments,D': 'deleteAttachment',
	'libs,I': 'createLib',
	'libs,U': 'updateLib',
	'libs,D': 'deleteLib',
	'methods,I': 'createMethod',
	'methods,U': 'updateMethod',
	'methods,D': 'deleteMethod',
	'views,I': 'createView',
	'views,U': 'updateView',
	'views,D': 'deleteView'
};

/** {array:string} Each tcn event name */
let TCN_EVENT_NAMES = Object.keys(TCN_EVENT_MAPPING).map(function(key) {
	return TCN_EVENT_MAPPING[key];
});

/** {array:string} Internal event names used by local NoPg connection */
let LOCAL_EVENT_NAMES = [
	'timeout',
	'commit',
	'rollback',
	'disconnect'
];


// Pre-define function
let parse_predicate_document_relations;

/** Convert NoPg keywords to internal PostgreSQL name paths for PostgreSQL get_documents() function
 * Note! documents might contain data like `['user|name,email']` which tells the field list and should be converted to
 * PostgreSQL names here. Format is either:
 *  - `<expression>|<field-list>` -- Fetch external documents matching UUID(s) found by <expression> with properties
 * specified by <field-list>
 *  - where `<field-list>` is either `*` for all properties or `<field#1>[,<field#2>[,...]]`
 *  - where `<expression>` is either:
 *     - `[<Type>#]<property-name>` -- The local property by name `<property-name>` is used as an UUID or if it is an
 * array, as a list of UUIDs to fetch these documents, optionally only documents with type `<Type>` are searched.
 *     - `<property-name>{<Type>#<field-name>}` -- Documents matching `<Type>` with a property named `<field-name>`
 * matching the UUID of the local document are fetched and UUIDs are saved as an array in a local property named
 * `<property-name>`.
 */
parse_predicate_document_relations = function parse_predicate_document_relations(ObjType, documents, traits) {
	return ARRAY(documents).map(function(d) {

		let parts = d.split('|');
		let expression = parts.shift();
		let fields = parts.join('|') || '*';

		let prop, type_name;
		let external_index = expression.indexOf('{');
		let type_index = expression.indexOf('#');
		if( (type_index >= 0) && ( (external_index < 0) || (type_index < external_index) ) ) {
			type_name = expression.substr(0, type_index);
			prop = expression.substr(type_index+1);
		} else {
			prop = expression;
		}

		fields = ARRAY(fields.split(',')).map(function(f) {
			if(f === '*') { return {'query':'*'}; }
			let p = _parse_predicate_key(ObjType, {'traits': traits, 'epoch':false}, f);
			return {
				'name': f,
				'datakey': p.getMeta('datakey'),
				'key': p.getMeta('key'),
				'query': p.getString()
			};
		}).valueOf();

		//nrLog.debug('fields = ', JSON.stringify(fields, null, 2) );

		if(prop && (prop.length >= 1) && (prop[0] === '$')) {
			return {
				'type': type_name,
				'prop': prop.substr(1),
				'fields': fields
			};
		}

		return {
			'type': type_name,
			'prop': get_predicate_datakey(ObjType) + '.' + prop,
			'fields': fields
		};
	}).valueOf();
};

/** Functions to build casts for different types */
let _pgcasts = {
	'direct': function pgcast_direct(x) { return '' + x; },
	'boolean': function pgcast_boolean(x) { return '(((' + x + ')::text)::boolean IS TRUE)'; },
	'numeric': function pgcast_numeric(x) { return '((' + x + ')::text)::numeric'; },
	'text': function pgcast_text(x) {
		if(x.indexOf(' -> ') !== -1) {
			return replace_last(x, ' -> ', ' ->> ');
		}

		if(x.indexOf(' #> ') !== -1) {
			return replace_last(x, ' #> ', ' #>> ');
		}

		//throw new TypeError('Cannot cast expression to text: ' + x);

		if(x.substr(0 - '::text'.length) === '::text') {
			return x;
		}

		if(x.substr(0 - '::text)'.length) === '::text)') {
			return x;
		}

		return '' + x + '::text';
	}
};

/* This object is because these functions need each other at the same time and must be defined before use. */
let _parsers = {};

/** Parse array predicate */
_parsers.parse_array_predicate = function _parse_array_predicate(ObjType, q, def_op, traits, o) {
	let op = 'AND';

	if(is_operator(o[0])) {
		o = [].concat(o);
		op = o.shift();
	}

	if(parse_operator_name(op) === 'BIND') {
		return _parse_function_predicate(ObjType, q, def_op, o, parse_operator_type(op), traits);
	}

	let parse_predicates = FUNCTION(_parsers.recursive_parse_predicates).curry(ObjType, q, def_op, traits);
	let predicates = ARRAY(o).map( parse_predicates ).filter(not_undefined).valueOf();
	return Predicate.join(predicates, op);
};

/** Recursively parse predicates */
_parsers.recursive_parse_predicates = function _recursive_parse_predicates(ObjType, q, def_op, traits, o) {

	if(o === undefined) { return; }

	if( is.array(o) ) {
		return _parsers.parse_array_predicate(ObjType, q, def_op, traits, o);
	}

	if( is.obj(o) ) {
		o = parse_predicates(ObjType)(o, ObjType.meta.datakey.substr(1) );
		let predicates = ARRAY(Object.keys(o)).map(function(k) {
			return new Predicate('' + k + ' = $', [o[k]]);
		}).valueOf();
		return Predicate.join(predicates, def_op);
	}

	return new Predicate(''+o);
};

// Exports as normal functions
let _recursive_parse_predicates = FUNCTION(_parsers.recursive_parse_predicates).curry();

/** Returns the keyword name without first letter */
let parse_keyword_name = require('./parse_keyword_name.js');

/** The constructor
 *
 * @param db
 * @constructor
 */
function NoPg (db) {

	let self = this;

	if(!db) { throw new TypeError("db invalid: " + LogUtils.getAsString(db) ); }

	self._db = db;
	self._values = [];
	self._tr_state = 'open';
	self._stats = [];
	self._cache = {
		'types': {},
		'objects': {}
	};
	self._events = new EventEmitter();

}

/** NoPg has event `timeout` -- when automatic timeout happens, and rollback issued, and connection is freed */
/** NoPg has event `rollback` -- when rollback hapens */
/** NoPg has event `commit` -- when commit hapens */

NoPg.debug = DEBUG_NOPG;

// Addons
NoPg.merge = require('./merge.js');
NoPg.scan = require('./scan.js');
NoPg.expand = require('./expand.js');
NoPg.compact = require('./compact.js');
NoPg.strip = require('./strip.js');
NoPg.types = require('./types.js');
NoPg.ResourceView = require('./ResourceView.js');

// Object constructors
NoPg.Document = orm.Document;
NoPg.Type = orm.Type;
NoPg.Attachment = orm.Attachment;
NoPg.Lib = orm.Lib;
NoPg.Method = orm.Method;
NoPg.View = orm.View;
NoPg.DBVersion = orm.DBVersion;

/* ------------- PUBLIC FUNCTIONS --------------- */

/** Returns the NoPg constructor type of `doc`, otherwise returns undefined.
 * @param doc
 * @return {*}
 */
NoPg._getObjectType = function(doc) {
	if(doc instanceof NoPg.Document  ) { return NoPg.Document;   }
	if(doc instanceof NoPg.Type      ) { return NoPg.Type;       }
	if(doc instanceof NoPg.Attachment) { return NoPg.Attachment; }
	if(doc instanceof NoPg.Lib       ) { return NoPg.Lib;        }
	if(doc instanceof NoPg.Method    ) { return NoPg.Method;     }
	if(doc instanceof NoPg.View    ) { return NoPg.View;     }
	if(doc instanceof NoPg.DBVersion ) { return NoPg.DBVersion;  }
};

/** Returns the NoPg constructor type of `doc`, otherwise throws an exception of `TypeError`.
 * @param doc
 * @return {*}
 */
NoPg.getObjectType = function(doc) {
	let ObjType = NoPg._getObjectType(doc);
	if(!ObjType) {
		throw new TypeError("doc is unknown type: {" + typeof doc + "} " + JSON.stringify(doc, null, 2) );
	}
	return ObjType;
};

/** Record internal timing statistic object
 * @param data
 */
NoPg.prototype._record_sample = function(data) {
	let self = this;

	let stats_enabled = is.array(self._stats);
	let log_times = process.env.DEBUG_NOPG_EVENT_TIMES !== undefined;

	if( (!stats_enabled) && (!log_times) ) {
		return;
	}

	debug.assert(data).is('object');
	debug.assert(data.event).is('string');
	debug.assert(data.start).is('date');
	debug.assert(data.end).is('date');
	debug.assert(data.duration).ignore(undefined).is('number');
	debug.assert(data.query).ignore(undefined).is('string');
	debug.assert(data.params).ignore(undefined).is('array');

	if(data.duration === undefined) {
		data.duration = _get_ms(data.start, data.end);
	}

	if(stats_enabled) {
		self._stats.push(data);
	}

	if(log_times) {
		_log_time(data);
	}
};

/** Record internal timing statistic object */
NoPg.prototype._finish_samples = function() {
	let self = this;

	let stats_enabled = is.array(self._stats);
	let log_times = process.env.NOPG_EVENT_TIMES !== undefined;

	if( (!stats_enabled) && (!log_times) ) {
		return;
	}

	let start = self._stats[0];
	let end = self._stats[self._stats.length-1];

	debug.assert(start).is('object');
	debug.assert(start.event).is('string').equals('start');

	debug.assert(end).is('object');
	debug.assert(end.event).is('string');

	let server_duration = 0;
	ARRAY(self._stats).forEach(function(sample) {
		server_duration += sample.duration;
	});

	self._record_sample({
		'event': 'transaction',
		'start': start.start,
		'end': end.end
	});

	self._record_sample({
		'event': 'transaction:server',
		'start': start.start,
		'end': end.end,
		'duration': server_duration
	});

};

/** Defaults
 * @type {NoPgDefaultsObject}
 */
NoPg.defaults = {

	pgconfig: PGCONFIG,

	timeout: NOPG_TIMEOUT !== undefined ? NOPG_TIMEOUT : 30000,

	/** If enabled NoPg will be more aware of the properties of types.
	 * When a user provides a type as a string, it will be converted as
	 * a type object. This will enable additional features like optional
	 * `traits.documents` support as a predefined in type.
	 * @type {boolean}
	 */

	enableTypeAwareness: NOPG_TYPE_AWARENESS !== undefined ? NOPG_TYPE_AWARENESS : false

};

/** Start a transaction
 * @param pgconfig {string} PostgreSQL database configuration. Example:
                            `"postgres://user:pw@localhost:5432/test"`
 * @param opts {object} Optional options.
 * @param opts.timeout {number} The timeout, default is
                                from `NoPg.defaults.timeout`.
 * @param opts.pgconfig {string} See param `pgconfig`.
 * @return {*}
 */
NoPg.start = function(pgconfig, opts = undefined) {
	return extend.promise( [NoPg], nr_fcall("nopg:start", function() {
		let start_time = new Date();
		let w;

		let args = ARRAY([pgconfig, opts]);
		pgconfig = args.find(is.string);
		opts = args.find(is.object);
		debug.assert(opts).ignore(undefined).is('object');

		if(!opts) {
			opts = {};
		}

		debug.assert(opts).is('object');

		if(!pgconfig) {
			pgconfig = opts.pgconfig || NoPg.defaults.pgconfig;
		}

		let timeout = opts.timeout || NoPg.defaults.timeout;
		debug.assert(timeout).ignore(undefined).is('number');
		debug.assert(pgconfig).is('string');

		return pg.start(pgconfig).then(function(db) {

			if(!db) { throw new TypeError("invalid db: " + LogUtils.getAsString(db) ); }
			if(timeout !== undefined) {
				w = create_watchdog(db, {"timeout": timeout});
			}
			let nopg_db = new NoPg(db);

			let end_time = new Date();
			nopg_db._record_sample({
				'event': 'start',
				'start': start_time,
				'end': end_time
			});

			return nopg_db;
		}).then(function(db) {
			if(w) {
				w.reset(db);
				db._watchdog = w;
			}
			return pg_query("SET plv8.start_proc = 'plv8_init'")(db);
		});
	}));
};

/** Start a connection (without transaction)
 * @param pgconfig {string} PostgreSQL database configuration. Example:
 `"postgres://user:pw@localhost:5432/test"`
 * @param opts {object} Optional options.
 * @param opts.pgconfig {string} See param `pgconfig`.
 * @return {*}
 */
NoPg.connect = function(pgconfig, opts) {
	return extend.promise( [NoPg], nr_fcall("nopg:connect", function() {
		let start_time = new Date();

		let args = ARRAY([pgconfig, opts]);
		pgconfig = args.find(is.string);
		opts = args.find(is.object);
		debug.assert(opts).ignore(undefined).is('object');
		if(!opts) {
			opts = {};
		}

		debug.assert(opts).is('object');

		if(!pgconfig) {
			pgconfig = opts.pgconfig || NoPg.defaults.pgconfig;
		}

		debug.assert(pgconfig).is('string');

		return pg.connect(pgconfig).then(function(db) {
			if(!db) { throw new TypeError("invalid db: " + LogUtils.getAsString(db) ); }
			let nopg_db = new NoPg(db);
			let end_time = new Date();
			nopg_db._record_sample({
				'event': 'connect',
				'start': start_time,
				'end': end_time
			});
			return nopg_db;
		}).then(function(db) {
			return pg_query("SET plv8.start_proc = 'plv8_init'")(db);
		});
	}));
};

/** Execute a function in a transaction with automatic commit / rollback.
 * @param pgconfig {string} PostgreSQL database configuration. Example:
 `"postgres://user:pw@localhost:5432/test"`
 * @param opts {object} Optional options.
 * @param opts.timeout {number} The timeout, default is
 from `NoPg.defaults.timeout`
 * @param opts.pgconfig {string} See param `pgconfig`.
 * @param fn {function} The function to be called.
 * @return {*}
 */
NoPg.transaction = function(pgconfig, opts, fn) {
	return extend.promise( [NoPg], nr_fcall("nopg:transaction", function() {
		let args = ARRAY([pgconfig, opts, fn]);
		pgconfig = args.find(is.string);
		opts = args.find(is.object);
		fn = args.find(is.func);

		debug.assert(pgconfig).ignore(undefined).is('string');
		debug.assert(opts).ignore(undefined).is('object');
		debug.assert(fn).is('function');

		let _db;
		return NoPg.start(pgconfig, opts).then(function(db) {
			_db = db;
			return db;
		}).then(fn).then(function(res) {
			return _db.commit().then(function() {
				_db = undefined;
				return res;
			});
		}).fail(function(err) {
			if(!_db) {
				//debug.error('Passing on error: ', err);
				return Q.reject(err);
			}
			return _db.rollback().fail(function(err2) {
				debug.error("rollback failed: ", err2);
				//debug.error('Passing on error: ', err);
				return Q.reject(err);
			}).then(function() {
				//debug.error('Passing on error: ', err);
				return Q.reject(err);
			});
		});

	}));
};

// Aliases
NoPg.fcall = NoPg.transaction;

/** Fetch next value from queue
 * @return {*}
 */
NoPg.prototype.fetch = function() {
	return this._values.shift();
};

/** Assume the next value in the queue is an array with single value, and throws an exception if it has more than one value.
 * @throws an error if the value is not an array or it has two or more values
 * @returns {* | undefined} The first value in the array or otherwise `undefined`
 */
NoPg.prototype.fetchSingle = function() {
	let db = this;
	let items = db.fetch();
	debug.assert(items).is('array').maxLength(1);
	return items.shift();
};

/** Assume the next value in the queue is an array with single value, and prints an warning if it has more than one value.
 * @throws an error if the value is not an array
 * @returns The first value in the array or otherwise `undefined`
 */
NoPg.prototype.fetchFirst = function() {
	let db = this;
	let items = db.fetch();
	debug.assert(items).is('array');
	if(items.length >= 2) {
		nrLog.warn('nopg.fetchSingle() got an array with too many results (' + items.length + ')');
	}
	return items.shift();
};

/** Push `value` to the queue. It makes it possible to implement your own functions.
 * @param value
 * @return {NoPg}
 */
NoPg.prototype.push = function(value) {
	this._values.push(value);
	return this;
};

/** Returns the latest value in the queue but does not remove it
 * @return {*}
 */
NoPg.prototype._getLastValue = function() {
	return this._values[this._values.length - 1];
};

/** Commit transaction
 * @return {*}
 */
NoPg.prototype.commit = function() {
	let self = this;
	let start_time = new Date();
	return extend.promise( [NoPg], nr_fcall("nopg:commit", function() {
		return self._db.commit().then(function() {
			let end_time = new Date();
			self._record_sample({
				'event': 'commit',
				'start': start_time,
				'end': end_time
			});

			self._finish_samples();

			self._tr_state = 'commit';
			if(is.obj(self._watchdog)) {
				self._watchdog.clear();
			}
			self._events.emit('commit');
			return self;
		});
	}));
};

/** Rollback transaction
 * @return {*}
 */
NoPg.prototype.rollback = function() {
	let self = this;
	let start_time = new Date();
	return extend.promise( [NoPg], nr_fcall("nopg:rollback", function() {
		return self._db.rollback().then(function() {
			let end_time = new Date();
			self._record_sample({
				'event': 'rollback',
				'start': start_time,
				'end': end_time
			});

			self._finish_samples();

			self._tr_state = 'rollback';
			if(is.obj(self._watchdog)) {
				self._watchdog.clear();
			}

			self._events.emit('rollback');
			return self;
		});
	}) );
};

/** Disconnect
 * @return {*}
 */
NoPg.prototype.disconnect = function() {
	let self = this;
	let start_time = new Date();
	return extend.promise( [NoPg], nr_fcall("nopg:disconnect", function() {
		return self._db.disconnect().then(function() {
			let end_time = new Date();
			self._record_sample({
				'event': 'disconnect',
				'start': start_time,
				'end': end_time
			});
			self._finish_samples();
			self._tr_state = 'disconnect';
			if(is.obj(self._watchdog)) {
				self._watchdog.clear();
			}
			self._events.emit('disconnect');
			return self;
		});
	}));
};

/** Returns CREATE TRIGGER query for Type specific TCN
 * @param type
 * @param op
 * @return {*}
 */
NoPg.createTriggerQueriesForType = function create_tcn_queries(type, op) {
	debug.assert(type).is('string');
	debug.assert(op).ignore(undefined).is('string');

	if(op === undefined) {
		return [].concat(create_tcn_queries(type, "insert"))
			.concat(create_tcn_queries(type, "update"))
			.concat(create_tcn_queries(type, "delete"));
	}

	if(['insert', 'delete', 'update'].indexOf(op) < 0) {
		throw new TypeError("op is invalid: " + op);
	}
	op = op.toLowerCase();

	let table_name = 'documents';
	let trigger_name = table_name + '_' + op + '_' + type + '_tcn_trigger';
	let channel_name = 'tcn' + type.toLowerCase();

	if(op === 'insert') {
		return [
			pg_escape('DROP TRIGGER IF EXISTS %I ON %I', trigger_name, table_name),
			pg_escape(
				'CREATE TRIGGER %I'+
				' AFTER '+op.toUpperCase()+' ON %I FOR EACH ROW'+
				' WHEN (NEW.type = %L)'+
				' EXECUTE PROCEDURE triggered_change_notification(%L)',
				trigger_name,
				table_name,
				type,
				channel_name
			)
		];
	}

	if(op === 'update') {
		return [
			pg_escape('DROP TRIGGER IF EXISTS %I ON %I', trigger_name, table_name),
			pg_escape(
				'CREATE TRIGGER %I'+
				' AFTER '+op.toUpperCase()+' ON %I FOR EACH ROW'+
				' WHEN (NEW.type = %L OR OLD.type = %L)'+
				' EXECUTE PROCEDURE triggered_change_notification(%L)',
				trigger_name,
				table_name,
				type,
				type,
				channel_name
			)
		];
	}

	if(op === 'delete') {
		return [
			pg_escape('DROP TRIGGER IF EXISTS %I ON %I', trigger_name, table_name),
			pg_escape(
				'CREATE TRIGGER %I'+
				' AFTER '+op.toUpperCase()+' ON %I FOR EACH ROW'+
				' WHEN (OLD.type = %L)'+
				' EXECUTE PROCEDURE triggered_change_notification(%L)',
				trigger_name,
				table_name,
				type,
				channel_name
			)
		];
	}

	throw new TypeError("op invalid: "+ op);
};

/** Setup triggers for specific type
 * @param type
 * @return {*}
 */
NoPg.prototype.setupTriggersForType = function(type) {
	let self = this;
	return extend.promise( [NoPg], nr_fcall("nopg:setupTriggersForType", function() {
		return ARRAY(NoPg.createTriggerQueriesForType(type)).map(function step_builder(query) {
			return function step() {
				return do_query(self, query);
			};
		}).reduce(Q.when, Q()).then(function() {
			return self;
		});
	}));
};

/** Returns true if argument is an event name
 * @param name
 * @return {* | boolean}
 */
NoPg.isTCNEventName = function is_event_name(name) {
	return _.isString(name) && (TCN_EVENT_NAMES.indexOf(name) >= 0);
};

/** Returns true if argument is an event name
 * @param name
 * @return {* | boolean}
 */
NoPg.isLocalEventName = function is_event_name(name) {
	return _.isString(name) && (LOCAL_EVENT_NAMES.indexOf(name) >= 0);
};

/** Returns true if argument is an event name
 * @param name
 * @return {* | boolean}
 */
NoPg.isEventName = function is_event_name(name) {
	return NoPg.isTCNEventName(name) || NoPg.isLocalEventName(name);
};

/** Convert event name from object to string
 * @param name {string} The name of an event, eg. `[(type_id|type)#][id@][(eventName|id|type)]`. [See
 *     more](https://trello.com/c/qrSpMOfk/6-event-support).
 * @returns {object} Parsed values, eg. `{"type":"User", "name":"create", "id":
 *     "b6913d79-d37a-5977-94b5-95bdfe5cccda"}`, where only available properties are defined. If type_id was used, the
 *     type will have an UUID and you should convert it to string presentation if necessary.
 */
NoPg.stringifyEventName = function parse_event_name(event) {
	debug.assert(event).is('object');
	debug.assert(event.type).ignore(undefined).is('string');
	debug.assert(event.name).ignore(undefined).is('string');
	debug.assert(event.id).ignore(undefined).is('string');

	let has_name = event.hasOwnProperty('name');

	if(has_name && NoPg.isLocalEventName(event.name) ) {
		return event.name;
	}

	let name = '';
	if(event.hasOwnProperty('type')) {
		name += event.type + '#';
	}
	if(event.hasOwnProperty('id')) {
		name += event.id + '@';
	}
	if(has_name) {
		name += event.name;
	}
	return name;
};

/** Parse event name from string into object.
 * @param name {string} The name of an event, eg. `[(type_id|type)#][id@][(eventName|id|type)]`. [See
 *     more](https://trello.com/c/qrSpMOfk/6-event-support).
 * @returns {object} Parsed values, eg. `{"type":"User", "name":"create", "id":
 *     "b6913d79-d37a-5977-94b5-95bdfe5cccda"}`, where only available properties are defined. If type_id was used, the
 *     type will have an UUID and you should convert it to string presentation if necessary.
 */
NoPg.parseEventName = function parse_event_name(name) {
	debug.assert(name).is('string');

	return merge.apply(undefined, ARRAY(name.replace(/([#@])/g, "$1\n").split("\n")).map(function(arg) {
		arg = arg.trim();
		let key;
		let is_type = arg[arg.length-1] === '#';
		let is_id = arg[arg.length-1] === '@';

		if(is_type || is_id) {
			arg = arg.substr(0, arg.length-1).trim();
			key = is_type ? 'type' : 'id';
			let result = {};
			result[key] = arg;
			return result;
		}

		if(is.uuid(arg)) {
			return {'id': arg};
		}

		if(NoPg.isEventName(arg)) {
			return {'name': arg};
		}

		if(arg) {
			return {'type':arg};
		}

		return {};
	}).valueOf());
};

/** Parse tcn channel name to listen from NoPg event name
 * @param event {string|object} The name of an event, eg. `[(type_id|type)#][id@][(eventName|id|type)]`. [See
 *     more](https://trello.com/c/qrSpMOfk/6-event-support).
 * @returns {string} Channel name for TCN, eg. `tcn_User` or `tcn` for non-typed events.
 * @todo Hmm, should we throw an exception if it isn't TCN event?
 */
NoPg.parseTCNChannelName = function parse_tcn_channel_name(event) {
	if(is.string(event)) {
		event = NoPg.parseEventName(event);
	}
	debug.assert(event).is('object');
	if(event.hasOwnProperty('type')) {
		return 'tcn' + event.type.toLowerCase();
	}
	return 'tcn';
};

/** Parse TCN payload string
 * @param payload {string} The payload string from PostgreSQL's tcn extension
 * @returns {object} 
 */
NoPg.parseTCNPayload = function nopg_parse_tcn_payload(payload) {

	debug.assert(payload).is('string');

	let parts = payload.split(',');

	let table = parts.shift(); // eg. `"documents"`
	debug.assert(table).is('string').minLength(2);
	debug.assert(table.charAt(0)).equals('"');
	debug.assert(table.charAt(table.length-1)).equals('"');
	table = table.substr(1, table.length-2);

	let op = parts.shift(); // eg. `I`
	debug.assert(op).is('string');

	let opts = parts.join(','); // eg. `"id"='b6913d79-d37a-5977-94b5-95bdfe5cccda'...`
	let i = opts.indexOf('=');
	if(i < 0) { throw new TypeError("No primary key!"); }

	let key = opts.substr(0, i); // eg. `"id"`
	debug.assert(key).is('string').minLength(2);
	debug.assert(key.charAt(0)).equals('"');
	debug.assert(key.charAt(key.length-1)).equals('"');
	key = key.substr(1, key.length-2);

	let value = opts.substr(i+1); // eg. `'b6913d79-d37a-5977-94b5-95bdfe5cccda'...`
	debug.assert(value).is('string').minLength(2);
	debug.assert(value.charAt(0)).equals("'");
	i = value.indexOf("'", 1);
	if(i < 0) { throw new TypeError("Parse error! Could not find end of input."); }
	value = value.substr(1, i-1);

	let keys = {};
	keys[key] = value;

	return {
		'table': table,
		'op': op,
		'keys': keys
	};
};

/** Start listening new event listener
 * @return {*}
 */
NoPg.prototype.setupTCN = function() {
	let self = this;
	return extend.promise( [NoPg], nr_fcall("nopg:setupTCN", function() {

		// Only setup TCN once
		if(self._tcn_setup) {
			return;
		}

		self._tcn_setup = true;

		/** {object:number} An object which contains counters by normalized event name. The counts how many times we are listening specific channel, so we can stop listening it when the counter goes 0. */
		let counter = {};

		/* Listeners for each normalized event name */
		let tcn_listeners = {};

		/** Listener for new listeners */
		function new_listener(event/*, listener*/) {

			// Ignore if not tcn event
			if(NoPg.isLocalEventName(event)) {
				return;
			}

			event = NoPg.parseEventName(event);

			// Stringifying back the event normalizes the original event name
			let event_name = NoPg.stringifyEventName(event);

			let channel_name = NoPg.parseTCNChannelName(event);

			// If we are already listening, just increase the counter.
			if(counter.hasOwnProperty(event_name)) {
				counter[event_name] += 1;
				return;
			}

			// Create the listener if necessary
			let tcn_listener;
			if(!tcn_listeners.hasOwnProperty(event_name)) {
				tcn_listener = tcn_listeners[event_name] = create_tcn_listener(self._events, event);
			} else {
				tcn_listener = tcn_listeners[event_name];
			}

			// Start listening tcn events for this channel
			//nrLog.debug('Listening channel ' + channel_name + ' for ' + event_name);
			self._db.on(channel_name, tcn_listener);
			counter[event_name] = 1;
		}

		/** Listener for removing listeners */
		function remove_listener(event/*, listener*/) {

			// Ignore if not tcn event
			if(NoPg.isLocalEventName(event)) {
				return;
			}

			event = NoPg.parseEventName(event);

			// Stringifying back the event normalizes the original event name
			let event_name = NoPg.stringifyEventName(event);

			let channel_name = NoPg.parseTCNChannelName(event);

			counter[event_name] -= 1;
			if(counter[event_name] === 0) {
				//nrLog.debug('Stopped listening channel ' + channel_name + ' for ' + event_name);
				self._db.removeListener(channel_name, tcn_listeners[event_name]);
				delete counter[event_name];
			}

		}

		self._events.on('newListener', new_listener);
		self._events.on('removeListener', remove_listener);
		return self;
	}));
};

/** Build wrappers for event methods */
['addListener', 'on', 'once', 'removeListener'].forEach(function(fn) {
	NoPg.prototype[fn] = function(event, listener) {
		let self = this;
		return extend.promise( [NoPg], nr_fcall("nopg:"+fn, function() {
			return Q.fcall(function() {
				event = NoPg.parseEventName(event);
				debug.assert(event).is('object');

				// Setup tcn listeners only if necessary
				if( event.hasOwnProperty('event') && NoPg.isLocalEventName(event.name) ) {
					return;
				}

				// FIXME: If type is declared as an uuid, we should convert it to string. But for now, just throw an exception.
				if(event.hasOwnProperty('type') && is.uuid(event.type)) {
					throw new TypeError("Types as UUID in events not supported yet!");
				}

				return self.setupTCN();

			}).then(function() {
				self._events[fn](NoPg.stringifyEventName(event), listener);
				return self;
			});
		}));
	};
});

/** Checks if server has compatible version
 * @return {*}
 */
NoPg.prototype.testServerVersion = function() {
	let self = this;
	return extend.promise( [NoPg], nr_fcall("nopg:testServerVersion", function() {
		return do_query(self, 'show server_version_num').then(function(rows) {
			//nrLog.debug('PostgreSQL server version (before parse): ', rows);
			let num = rows.shift().server_version_num;
			num = parseInt(num, 10);
			//nrLog.debug('PostgreSQL server version: ', num);
			if(num >= 90300) {
				return self;
			} else {
				throw new TypeError("PostgreSQL server must be v9.3 or newer (detected "+ num +")");
			}
		});
	}));
};

/** Checks if server has compatible version
 * @param name
 * @return {*}
 */
NoPg.prototype.testExtension = function(name) {
	let self = this;
	return extend.promise( [NoPg], nr_fcall("nopg:testExtension", function() {
		return do_query(self, 'SELECT COUNT(*) AS count FROM pg_catalog.pg_extension WHERE extname = $1', [name]).then(function(rows) {
			let row = rows.shift();
			let count = parseInt(row.count, 10);
			if(count === 1) {
				return self;
			} else {
				throw new TypeError("PostgreSQL server does not have extension: " + name);
			}
		});
	}));
};

/** Tests if the server is compatible
 * @return {*}
 */
NoPg.prototype.test = function() {
	let self = this;
	return extend.promise( [NoPg], nr_fcall("nopg:test", function() {
		return self.testServerVersion().testExtension('plv8').testExtension('uuid-ossp').testExtension('moddatetime').testExtension('tcn');
	}));
};

/** Initialize the database
 * @return {*}
 */
NoPg.prototype.init = function() {
	let self = this;
	return extend.promise( [NoPg], nr_fcall("nopg:init", function() {

		return self.test().latestDBVersion().then(function(db) {
			let code_version = require('./schema/latest.js');
			let db_version = db.fetch();
			if(! ((db_version >= -1) && (db_version<=code_version)) ) {
				throw new TypeError("Database version " + db_version + " is not between accepted range (-1 .. " + code_version + ")");
			}
			let builders = [];

			let i = db_version, file;
			while(i < code_version) {
				i += 1;
				file = './schema/v' + pad(i, 4) + '.js';
				try {
					//nrLog.debug('Loading database version ', i, " from ", file);
					//FUNCTION(builders.push).apply(builders, require(file) );
					push_file(builders, file);
				} catch(err) {
					debug.error("Exception: ", err);
					throw new TypeError("Failed to load: "+ file + ": " + err);
				}
			}

			// Skip upgrade if we have nothing to do
			if(builders.length === 0) {
				return self;
			}

			// Call upgrade steps
			return ARRAY(builders).reduce(function(so_far, f) {
				return so_far.then(function(db) {
					db.fetchAll();
					return db;
				}).then(f);
			}, Q(self._db)).then(function() {
				return db._addDBVersion({'$version': code_version});
			}).then(function() {
				//nrLog.debug('Successfully upgraded database from v' + db_version + ' to v' + code_version);
				return self;
			});

		}).then(function() {
			return self._importLib( require.resolve('tv4') ).then(function() { return self; });
		}).then(pg_query("SET plv8.start_proc = 'plv8_init'"));

	}));
};

/** Create document by type: `db.create([TYPE])([OPT(S)])`.
 * @param type
 * @return {create2}
 */
NoPg.prototype.create = function(type) {
	let self = this;

	function create2(data) {
		return extend.promise( [NoPg], nr_fcall("nopg:create", function() {

			if(type && (type instanceof NoPg.Type)) {
				data.$types_id = type.$id;
				data.$type = type.$name;
			} else if(type) {
				return self._getType(type).then(function(t) {
					if(!(t instanceof NoPg.Type)) {
						throw new TypeError("invalid type received: " + LogUtils.getAsString(t) );
					}
					type = t;
					return create2(data);
				});
			}

			let builder;
			if(self._documentBuilders && is.string(type) && self._documentBuilders.hasOwnProperty(type) && is.func(self._documentBuilders[type])) {
				builder = self._documentBuilders[type];
			} else if(self._documentBuilders && type && is.string(type.$name) && self._documentBuilders.hasOwnProperty(type.$name) && is.func(self._documentBuilders[type.$name])) {
				builder = self._documentBuilders[type.$name];
			}
			debug.assert(builder).ignore(undefined).is('function');

			return do_insert(self, NoPg.Document, data).then(_get_result(NoPg.Document)).then(function(result) {
				if(builder) {
					return builder(result);
				}
				return result;
			}).then(save_result_to(self));
		}));
	}

	return create2;
};

/** Add new DBVersion record
 * @param data
 * @return {Promise.<TResult>}
 */
NoPg.prototype._addDBVersion = function(data) {
	let self = this;
	return do_insert(self, NoPg.DBVersion, data).then(_get_result(NoPg.DBVersion));
};

/** Search documents
 * @param type
 * @return {search2}
 */
NoPg.prototype.search = function(type) {
	let self = this;
	let ObjType = NoPg.Document;
	function search2(opts, traits) {
		return extend.promise( [NoPg], nr_fcall("nopg:search", function() {
			return do_select(self, [ObjType, type], opts, traits).then(save_result_to_queue(self)).then(function() { return self; });
		}));
	}
	return search2;
};

/** Count documents
 * @param type
 * @return {count2}
 */
NoPg.prototype.count = function(type) {
	let self = this;
	let ObjType = NoPg.Document;
	function count2(opts, traits) {
		return extend.promise( [NoPg], nr_fcall("nopg:count", function() {
			traits = merge(traits, {'count':true, 'order':null});
			return do_count(self, [ObjType, type], opts, traits).then(save_result_to_queue(self)).then(function() { return self; });
		}));
	}
	return count2;
};

/** Search single document
 * @param type
 * @return {searchSingle2}
 */
NoPg.prototype.searchSingle = function(type) {
	let self = this;
	let ObjType = NoPg.Document;
	function searchSingle2(opts, traits) {
		return extend.promise( [NoPg], nr_fcall("nopg:search", function() {
			return do_select(self, [ObjType, type], opts, traits)
				.then(_get_result(ObjType))
				.then(save_result_to_queue(self))
				.then(function() { return self; });
		}));
	}
	return searchSingle2;
};

/** Update document
 * @param obj
 * @param data
 * @return {Promise.<TResult>}
 */
NoPg.prototype._update = function(obj, data) {
	let self = this;
	let ObjType = NoPg._getObjectType(obj) || NoPg.Document;
	return do_update(self, ObjType, obj, data).then(_get_result(ObjType));
};

/** Update document
 * @param obj
 * @param data
 * @return {*}
 */
NoPg.prototype.update = function(obj, data) {
	let self = this;
	let ObjType = NoPg._getObjectType(obj) || NoPg.Document;
	return extend.promise( [NoPg], nr_fcall("nopg:update", function() {

		let builder;
		if(self._documentBuilders && obj && is.string(obj.$type) && self._documentBuilders.hasOwnProperty(obj.$type) && is.func(self._documentBuilders[obj.$type])) {
			builder = self._documentBuilders[obj.$type];
		}
		debug.assert(builder).ignore(undefined).is('function');

		return do_update(self, ObjType, obj, data).then(_get_result(ObjType)).then(function(result) {
			if(builder) {
				return builder(result);
			}
			return result;
		}).then(save_result_to(self));
	}));
};

/** Delete resource
 * @param obj
 * @return {*}
 */
NoPg.prototype.del = function(obj) {
	if(!obj.$id) { throw new TypeError("opts.$id invalid: " + LogUtils.getAsString(obj) ); }
	let self = this;
	let ObjType = NoPg._getObjectType(obj) || NoPg.Document;
	return extend.promise( [NoPg], nr_fcall("nopg:del", function() {
		return do_delete(self, ObjType, obj).then(function() { return self; });
	}));
};

NoPg.prototype['delete'] = NoPg.prototype.del;

/** Delete type */
NoPg.prototype.delType = function(name) {
	debug.assert(name).is('string');
	let self = this;
	return extend.promise( [NoPg], nr_fcall("nopg:delType", function() {
		return self._getType(name).then(function(type) {
			if(!(type instanceof NoPg.Type)) {
				throw new TypeError("invalid type received: " + LogUtils.getAsString(type) );
			}
			return do_delete(self, NoPg.Type, type).then(function() { return self; });
		});
	}));
};

NoPg.prototype.deleteType = NoPg.prototype.delType;

/** Create a new type. We recommend using `._declareType()` instead unless you want an error if the type exists already. Use like `db._createType([TYPE-NAME])([OPT(S)])`. Returns the result instead of saving it to `self` queue. */
NoPg.prototype._createType = function(name) {
	let self = this;
	debug.assert(name).ignore(undefined).is('string');
	function createType2(data) {
		return extend.promise( [NoPg], nr_fcall("nopg:_createType", function() {
			data = data || {};
			if(name !== undefined) {
				data.$name = ''+name;
			}
			return do_insert(self, NoPg.Type, data).then(_get_result(NoPg.Type)).then(function(result) {
				return Q.fcall(function() {
					if(name !== undefined) {
						return self.setupTriggersForType(name);
					}
				}).then(function() {
					return result;
				});
			});
		}));
	}
	return createType2;
};

/** Create a new type. We recommend using `.declareType()` instead unless you want an error if the type exists already. Use like `db.createType([TYPE-NAME])([OPT(S)])`. */
NoPg.prototype.createType = function(name) {
	let self = this;
	function createType2(data) {
		return extend.promise( [NoPg], nr_fcall("nopg:createType", function() {
			return self._createType(name)(data).then(save_result_to(self));
		}));
	}
	return createType2;
};

/** Create a new type or replace existing type with the new values. Use like `db.declareType([TYPE-NAME])([OPT(S)])`. */
NoPg.prototype.declareType = function(name, opts) {
	opts = opts || {};
	debug.assert(opts).is('object');
	let opts_declare_indexes = opts.hasOwnProperty('declareIndexes') ? (opts.declareIndexes === true) : true;
	let self = this;
	function declareType2(data) {
		return extend.promise( [NoPg], nr_fcall("nopg:declareType", function() {
			data = data || {};

			debug.assert(data).is('object');
			debug.assert(data.indexes).ignore(undefined).is('array');
			debug.assert(data.uniqueIndexes).ignore(undefined).is('array');
			let indexes = data.indexes || [];
			let uniqueIndexes = data.uniqueIndexes || [];

			let where = {};
			if(name !== undefined) {
				if(name instanceof NoPg.Type) {
					where.$types_id = name.$id;
				} else {
					where.$name = ''+name;
				}
			}

			return self._getType(where).then(function(type) {
				if(type) {
					return self._updateTypeCache(type.$name, self._update(type, data));
				} else {
					return self._updateTypeCache(name, self._createType(name)(data));
				}
			}).then(function declare_indexes(type) {

				if(!opts_declare_indexes) {
					return self.push(type);
				}

				if(uniqueIndexes.length >= 1) {
					ARRAY(uniqueIndexes).forEach(function(i) {
						if(indexes.indexOf(i) < 0) {
							indexes.push(i);
						}
					});
				}

				if(indexes.indexOf('$id') < 0) {
					indexes.push('$id');
				}

				if(indexes.indexOf('$created') < 0) {
					indexes.push('$created');
				}

				if(indexes.indexOf('$modified') < 0) {
					indexes.push('$modified');
				}

				if(indexes.length === 0) {
					return self.push(type);
				}

				//let type = self.fetch();
				return indexes.map(function build_step(index) {
					return function step() {
						return pg_declare_index(self, NoPg.Document, type, index).then(function() {
							return pg_declare_index(self, NoPg.Document, type, index, "types_id", uniqueIndexes.indexOf(index) >= 0);
						}).then(function() {
							return pg_declare_index(self, NoPg.Document, type, index, "type", uniqueIndexes.indexOf(index) >= 0);
						});
					};
				}).reduce(Q.when, Q()).then(function() {
					return self.push(type);
				});
			});
		}));
	}
	return declareType2;
};

/* Start of Method Implementation */

/** Delete method */
NoPg.prototype.delMethod = function(type) {
	debug.assert(type).is('string');
	let self = this;
	let self_get_method = self._getMethod(type);
	return function(name) {
		debug.assert(name).is('string');
		return extend.promise( [NoPg], nr_fcall("nopg:delMethod", function() {
			return self_get_method(name).then(function(method) {
				if(!(method instanceof NoPg.Method)) {
					throw new TypeError("invalid method received: " + LogUtils.getAsString(method) );
				}
				return do_delete(self, NoPg.Method, method).then(function() { return self; });
			});
		}));
	};
};

NoPg.prototype.deleteMethod = NoPg.prototype.delMethod;

/** Search methods */
NoPg.prototype._searchMethods = function(type) {
	let self = this;
	let ObjType = NoPg.Method;
	debug.assert(type).is('string');
	return function nopg_prototype_search_methods_(opts, traits) {
		debug.assert(opts).ignore(undefined).is('object');
		opts = opts || {};
		opts.$type = type;
		if(!opts.hasOwnProperty('$active')) {
			opts.$active = true;
		}
		return do_select(self, ObjType, opts, traits).then(get_results(NoPg.Method));
	};
};

/** Search methods */
NoPg.prototype.searchMethods = function(type) {
	let self = this;
	debug.assert(type).is('string');
	let self_search_methods = self._searchMethods(type);
	return function nopg_prototype_search_methods_(opts, traits) {
		return extend.promise( [NoPg], nr_fcall("nopg:searchMethods", function() {
			return self_search_methods(opts, traits).then(save_result_to_queue(self)).then(function() { return self; });
		}));
	};
};

/** Get active method if it exists */
NoPg.prototype._getMethod = function nopg_prototype_get_method(type) {
	let self = this;
	debug.assert(type).is('string');
	return function nopg_prototype_get_method_(name) {
		debug.assert(name).is('string');
		let where = {
			'$type': type,
			'$name': name,
			'$active': true
		};
		let traits = {
			'order': ['$created']
		};
		return do_select(self, NoPg.Method, where, traits).then(_get_result(NoPg.Method));
	};
};

/** Create a new method. We recommend using `._declareMethod()` instead of this unless you want an error if the method exists already. Use like `db._createMethod([TYPE-NAME])(METHOD-NAME, METHOD-BODY, [OPT(S)])`. Returns the result instead of saving it to `self` queue. */
NoPg.prototype._createMethod = function(type) {
	let self = this;
	debug.assert(type).is('string');
	function createMethod2(name, body, data) {
		return extend.promise( [NoPg], nr_fcall("nopg:_createMethod", function() {

			debug.assert(data).ignore(undefined).is('object');
			data = data || {};

			debug.assert(name).is('string');

			if(is.func(body)) {
				body = '' + body;
			}
			debug.assert(body).ignore(undefined).is('string');
			body = body || "";

			data.$type = ''+type;
			data.$name = ''+name;
			data.$body = ''+body;

			return self._getType(type).then(function(type_obj) {
				data.$types_id = type_obj.$id;
				return do_insert(self, NoPg.Method, data).then(_get_result(NoPg.Method));
			});
		}));
	}
	return createMethod2;
};

/** Create a new method. We recommend using `.declareMethod()` instead unless you want an error if the type exists already. Use like `db.createMethod([TYPE-NAME])(METHOD-NAME, METHOD-BODY, [OPT(S)])`. */
NoPg.prototype.createMethod = function(type) {
	let self = this;
	let self_create_method = self._createMethod(type);
	function createMethod2(name, body, data) {
		return extend.promise( [NoPg], nr_fcall("nopg:createMethod", function() {
			return self_create_method(name, body, data).then(save_result_to(self));
		}));
	}
	return createMethod2;
};

/** Create a new method or replace existing with new values. Use like `db.declareMethod([TYPE-NAME])(METHOD-NAME, METHOD-BODY, [OPT(S)])`. */
NoPg.prototype.declareMethod = function(type) {
	let self = this;
	function declareMethod2(name, body, data) {
		return extend.promise( [NoPg], nr_fcall("nopg:declareMethod", function() {

			debug.assert(type).is('string');
			debug.assert(name).is('string');
			debug.assert(data).ignore(undefined).is('object');
			data = data || {};

			if(!data.hasOwnProperty('$active')) {
				data.$active = true;
			} else if(data.$active !== true) {
				data.$active = null;
			}

			return self._getMethod(type)(name).then(function(method) {
				if( method && (body === method.$body)) {
					return self._update(method, merge({}, data, {'$body':body}));
				}
				return self._createMethod(type)(name, body, data);
			}).then(function(method) {
				return self.push(method);
			});
		}));
	}
	return declareMethod2;
};

/** Returns a document builder function */
NoPg.prototype._createDocumentBuilder = function nopg_prototype_create_document_builder(type) {
	let self = this;
	debug.assert(type).is('string');
	let self_search_methods = self._searchMethods(type);
	return function nopg_prototype_create_document_builder_() {
		return self_search_methods({'$active': true}).then(function(methods) {

			/** Appends methods to doc */
			function _doc_builder(doc) {
				if(is.object(doc)) {
					ARRAY(methods).forEach(function(method) {
						doc[method.$name] = FUNCTION.parse(method.$body).bind(doc);
					});
				}
				return doc;
			}

			/** Appends methods to doc */
			function doc_builder(doc) {
				if(is.array(doc)) {
					return ARRAY(doc).map(_doc_builder).valueOf();
				}
				return _doc_builder(doc);
			}

			/** Removes methods from doc */
			function reset_methods(doc) {

				if(is.array(doc)) {
					return ARRAY(doc).map(reset_methods).valueOf();
				}

				if(is.object(doc)) {
					ARRAY(methods).forEach(function(method) {
						delete doc[method.$name];
					});
				}

				return doc;
			}

			doc_builder.reset = reset_methods;

			return doc_builder;
		});
	};
};

/** Returns a document builder function */
NoPg.prototype.createDocumentBuilder = function(type) {
	let self = this;
	debug.assert(type).is('string');
	let self_create_document_builder = self._createDocumentBuilder(type);
	return function nopg_prototype_create_document_builder_() {
		return extend.promise( [NoPg], nr_fcall("nopg:createDocumentBuilder", function() {
			return self_create_document_builder().then(save_result_to_queue(self)).then(function() { return self; });
		}));
	};
};

/** Setups a document builder function in session cache */
NoPg.prototype._initDocumentBuilder = function nopg_prototype_init_document_builder(type) {
	let self = this;
	debug.assert(type).is('string');
	if(!self._documentBuilders) {
		self._documentBuilders = {};
	}
	let create_document_builder = self._createDocumentBuilder(type);
	return function nopg_prototype_init_document_builder_() {
		if(self._documentBuilders.hasOwnProperty(type)) {
			return self;
		}
		return create_document_builder().then(function(builder) {
			self._documentBuilders[type] = builder;
			return self;
		});
	};
};

/** Setups a document builder function in session cache */
NoPg.prototype.initDocumentBuilder = function(type) {
	let self = this;
	debug.assert(type).is('string');
	let self_init_document_builder = self._initDocumentBuilder(type);
	return function nopg_prototype_init_document_builder_() {
		return extend.promise( [NoPg], nr_fcall("nopg:initDocumentBuilder", function() {
			return self_init_document_builder();
		}));
	};
};

/***** End of Method implementation *****/

/* Start of View Implementation */

/** Delete view */
NoPg.prototype.delView = function(type) {
	debug.assert(type).is('string');
	let self = this;
	let self_get_view = self._getView(type);
	return function(name) {
		debug.assert(name).is('string');
		return extend.promise( [NoPg], nr_fcall("nopg:delView", function() {
			return self_get_view(name).then(function(view) {
				if(!(view instanceof NoPg.View)) {
					throw new TypeError("invalid view received: " + LogUtils.getAsString(view) );
				}
				return do_delete(self, NoPg.View, view).then(function() { return self; });
			});
		}));
	};
};

NoPg.prototype.deleteView = NoPg.prototype.delView;

/** Search views */
NoPg.prototype._searchViews = function(type) {
	let self = this;
	let ObjType = NoPg.View;
	debug.assert(type).is('string');
	return function nopg_prototype_search_views_(opts, traits) {
		debug.assert(opts).ignore(undefined).is('object');
		opts = opts || {};
		opts.$type = type;
		return do_select(self, ObjType, opts, traits).then(get_results(NoPg.View));
	};
};

/** Search views */
NoPg.prototype.searchViews = function(type) {
	let self = this;
	debug.assert(type).is('string');
	let self_search_views = self._searchViews(type);
	return function nopg_prototype_search_views_(opts, traits) {
		return extend.promise( [NoPg], nr_fcall("nopg:searchViews", function() {
			return self_search_views(opts, traits).then(save_result_to_queue(self)).then(function() { return self; });
		}));
	};
};

/** Get view */
NoPg.prototype.getView = function(type) {
	let self = this;
	debug.assert(type).is('string');
	let self_get_view = self._getView(type);
	return function nopg_prototype_get_view_(opts, traits) {
		return extend.promise( [NoPg], nr_fcall("nopg:getView", function() {
			return self_get_view(opts, traits).then(save_result_to_queue(self)).then(function() { return self; });
		}));
	};
};

/** Get active view if it exists */
NoPg.prototype._getView = function nopg_prototype_get_view(type) {
	let self = this;
	debug.assert(type).is('string');
	return function nopg_prototype_get_view_(name) {
		debug.assert(name).is('string');
		let where = {
			'$type': type,
			'$name': name
		};
		let traits = {
			'order': ['$created']
		};
		return do_select(self, NoPg.View, where, traits).then(_get_result(NoPg.View));
	};
};

/** Create a new view. We recommend using `._declareView()` instead of this unless you want an error if the view exists already. Use like `db._createView([TYPE-NAME])(METHOD-NAME, METHOD-BODY, [OPT(S)])`. Returns the result instead of saving it to `self` queue. */
NoPg.prototype._createView = function(type) {
	let self = this;
	debug.assert(type).is('string');
	function createView2(name, data) {
		return extend.promise( [NoPg], nr_fcall("nopg:_createView", function() {

			debug.assert(data).ignore(undefined).is('object');
			data = data || {};

			debug.assert(name).is('string').minLength(1);

			data.$type = ''+type;
			data.$name = ''+name;

			return self._getType(type).then(function(type_obj) {
				debug.assert(type_obj).is('object');
				debug.assert(type_obj.$id).is('uuid');
				data.$types_id = type_obj.$id;
				return do_insert(self, NoPg.View, data).then(_get_result(NoPg.View));
			});
		}));
	}
	return createView2;
};

/** Create a new view. We recommend using `.declareView()` instead unless you want an error if the type exists already. Use like `db.createView([TYPE-NAME])(METHOD-NAME, METHOD-BODY, [OPT(S)])`. */
NoPg.prototype.createView = function(type) {
	let self = this;
	let self_create_view = self._createView(type);
	function createView2(name, data) {
		return extend.promise( [NoPg], nr_fcall("nopg:createView", function() {
			return self_create_view(name, data).then(save_result_to(self));
		}));
	}
	return createView2;
};

/** Create a new view or replace existing with new values. Use like `db.declareView([TYPE-NAME])(METHOD-NAME, METHOD-BODY, [OPT(S)])`. */
NoPg.prototype.declareView = function(type) {
	let self = this;
	function declareView2(name, data) {
		return extend.promise( [NoPg], nr_fcall("nopg:declareView", function() {

			debug.assert(type).is('string');
			debug.assert(name).is('string');
			debug.assert(data).ignore(undefined).is('object');
			data = data || {};

			if(!data.hasOwnProperty('$active')) {
				data.$active = true;
			} else if(data.$active !== true) {
				data.$active = false;
			}

			return self._getView(type)(name).then(function(view) {
				if(view) {
					return self._update(view, data);
				}
				return self._createView(type)(name, data);
			}).then(function(view) {
				return self.push(view);
			});
		}));
	}
	return declareView2;
};

/* End of views */

/** Create a new type or replace existing type with the new values. Use like `db.declareType([TYPE-NAME])([OPT(S)])`. */
NoPg.prototype.declareIndexes = function(name) {
	let self = this;
	function declareIndexes2(data) {
		return extend.promise( [NoPg], nr_fcall("nopg:declareIndexes", function() {
			data = data || {};
			debug.assert(data).is('object');
			debug.assert(data.indexes).ignore(undefined).is('array');
			debug.assert(data.uniqueIndexes).ignore(undefined).is('array');

			let indexes = data.indexes || [];
			let uniqueIndexes = data.uniqueIndexes || [];

			if(uniqueIndexes.length >= 1) {
				ARRAY(uniqueIndexes).forEach(function(i) {
					if(indexes.indexOf(i) < 0) {
						indexes.push(i);
					}
				});
			}

			if(indexes.indexOf('$id') < 0) {
				indexes.push('$id');
			}

			if(indexes.indexOf('$created') < 0) {
				indexes.push('$created');
			}

			if(indexes.indexOf('$modified') < 0) {
				indexes.push('$modified');
			}

			if(indexes.length === 0) {
				return self;
			}

			let where = {};
			if(name !== undefined) {
				if(name instanceof NoPg.Type) {
					where.$types_id = name.$id;
				} else {
					where.$name = ''+name;
				}
			}

			return self._getType(where).then(function(type) {
				return indexes.map(function build_step(index) {
					return function step() {
						return pg_declare_index(self, NoPg.Document, type, index).then(function() {
							return pg_declare_index(self, NoPg.Document, type, index, "types_id", uniqueIndexes.indexOf(index) >= 0);
						}).then(function() {
							return pg_declare_index(self, NoPg.Document, type, index, "type", uniqueIndexes.indexOf(index) >= 0);
						});
					};
				}).reduce(Q.when, Q()).then(function() {
					return self;
				});
			});
		}));
	}
	return declareIndexes2;
};

/** This is an alias for `.declareType()`. */
NoPg.prototype.createOrReplaceType = function(name) {
	return this.declareType(name);
};

/** Tests if type exists */
NoPg.prototype._typeExists = function(name) {
	let self = this;
	if(_.isString(name) && self._cache.types.hasOwnProperty(name)) {
		return true;
	}
	return do_select(self, NoPg.Type, name).then(function(types) {
		return (types.length >= 1) ? true : false;
	});
};

/** Tests if lib exists */
NoPg.prototype._libExists = function(name) {
	let self = this;
	return do_select(self, NoPg.Lib, name).then(function(types) {
		return (types.length >= 1) ? true : false;
	});
};

/** Get type and save it to result queue. */
NoPg.prototype.typeExists = function(name) {
	let self = this;
	return extend.promise( [NoPg], nr_fcall("nopg:typeExists", function() {
		return self._typeExists(name).then(save_result_to(self));
	}));
};

/** Update type cache
 * @param name {string} The name of the type
 * @param type {object|function} The promise of type object or type object instance
 */
NoPg.prototype._updateTypeCache = function(name, type) {
	let self = this;
	debug.assert(name).is('string');
	if(!is.func(type)) {
		debug.assert(type).is('object');
	}
	let cache = self._cache;
	debug.assert(cache).is('object');
	let objects = cache.objects;
	debug.assert(objects).is('object');
	let types = cache.types;
	debug.assert(types).is('object');
	let cached_type = types[name] = Q.when(type).then(function(result) {
		let result_id;
		if(is.obj(result)) {
			result_id = result.$id;
			types[name] = result;
			if(is.uuid(result_id)) {
				objects[result_id] = result;
			}
		}
		return result;
	});
	return cached_type;
};

/** Get type directly */
NoPg.prototype._getType = function(name, traits) {
	let self = this;
	if(!_.isString(name)) {
		return do_select(self, NoPg.Type, name, traits).then(_get_result(NoPg.Type));
	}

	if(self._cache.types.hasOwnProperty(name)) {
		return Q.when(self._cache.types[name]);
	}

	return self._updateTypeCache(name, do_select(self, NoPg.Type, name, traits).then(_get_result(NoPg.Type)));
};

/** Get type and save it to result queue. */
NoPg.prototype.getType = function(name) {
	let self = this;
	return extend.promise( [NoPg], nr_fcall("nopg:getType", function() {
		return self._getType(name).then(save_result_to(self));
	}));
};

/** Alias for `pghelpers.escapeFunction()` */
NoPg._escapeFunction = pghelpers.escapeFunction;

/** Returns the latest database server version as a integer number */
NoPg.prototype.latestDBVersion = function() {
	let self = this;
	return extend.promise( [NoPg], nr_fcall("nopg:latestDBVersion", function() {
		return _latestDBVersion(self).then(save_result_to(self));
	}));
};

/** Import javascript file into database as a library by calling `.importLib(FILE, [OPT(S)])` or `.importLib(OPT(S))` with `$content` property. */
NoPg.prototype._importLib = function(file, opts) {
	let self = this;
	opts = JSON.parse( JSON.stringify( opts || {} ));

	if( is.obj(file) && (opts === undefined) ) {
		opts = file;
		file = undefined;
	}

	return Q.fcall(function() {
		if(file) {
			return readFile(file, {'encoding':'utf8'});
		}
		if(opts.$content) {
			return;
		}
		throw new TypeError("NoPg.prototype.importLib() called without content or file");
	}).then(function importLib2(data) {
		opts.$name = opts.$name || require('path').basename(file, '.js');
		let name = '' + opts.$name;

		opts['content-type'] = '' + (opts['content-type'] || 'application/javascript');
		if(data) {
			opts.$content = ''+data;
		}

		return self._libExists(opts.$name).then(function(exists) {
			if(exists) {
				delete opts.$name;
				return do_update(self, NoPg.Lib, {"$name":name}, opts);
			} else {
				return do_insert(self, NoPg.Lib, opts);
			}
		});
	});

};

/** Import javascript file into database as a library by calling `.importLib(FILE, [OPT(S)])` or `.importLib(OPT(S))` with `$content` property. */
NoPg.prototype.importLib = function(file, opts) {
	let self = this;
	return extend.promise( [NoPg], nr_fcall("nopg:importLib", function() {
		return self._importLib(file, opts).then(_get_result(NoPg.Lib)).then(save_result_to(self));
	}));
};

/** Get specified object directly */
NoPg.prototype._getObject = function(ObjType) {
	let self = this;
	return function(opts, traits) {
		return do_select(self, ObjType, opts, traits).then(_get_result(ObjType));
	};
};

/** Get document directly */
NoPg.prototype._getDocument = function(opts) {
	let self = this;
	return self._getObject(NoPg.Document)(opts);
};

/** Get document and save it to result queue. */
NoPg.prototype.getDocument = function(opts) {
	let self = this;
	return extend.promise( [NoPg], nr_fcall("nopg:getDocument", function() {
		return self._getDocument(opts).then(save_result_to(self));
	}));
};

/** Search types */
NoPg.prototype.searchTypes = function(opts, traits) {
	let self = this;
	let ObjType = NoPg.Type;
	return extend.promise( [NoPg], nr_fcall("nopg:searchTypes", function() {
		return do_select(self, ObjType, opts, traits).then(save_result_to_queue(self)).then(function() { return self; });
	}));
};

/** Create an attachment from a file in the filesystem.
 * @param obj {object} The document object where the attachment will be placed.
 *          If it is an attachment object, it's parent will be used. If it is
 *          undefined, then last object in the queue will be used.
 */
NoPg.prototype.createAttachment = function(doc) {
	let self = this;
	let doc_id;

	function createAttachment2(file, opts) {
		return extend.promise( [NoPg], nr_fcall("nopg:createAttachment", function() {
			return Q.fcall(function() {
				opts = opts || {};

				let file_is_buffer = false;

				try {
					if(file && is.string(file)) {
						debug.assert(file).is('string');
					} else {
						debug.assert(file).typeOf('object').instanceOf(Buffer);
						file_is_buffer = true;
					}
				} catch(e) {
					throw new TypeError("Argument not String or Buffer: " + e);
				}
				debug.assert(opts).is('object');

				if(doc === undefined) {
					doc = self._getLastValue();
				}

				if(doc && (doc instanceof NoPg.Document)) {
					doc_id = doc.$id;
				} else if(doc && (doc instanceof NoPg.Attachment)) {
					doc_id = doc.$documents_id;
				} else if(doc && is.uuid(doc.$documents_id)) {
					doc_id = doc.$documents_id;
				} else if(doc && is.uuid(doc.$id)) {
					doc_id = doc.$id;
				} else {
					throw new TypeError("Could not detect document ID!");
				}

				debug.assert(doc_id).is('uuid');

				if(file_is_buffer) {
					return file;
				}

				return readFile(file, {'encoding':'hex'});

			}).then(function(buffer) {

				let data = {
					$documents_id: doc_id,
					$content: '\\x' + buffer,
					$meta: opts
				};

				debug.assert(data.$documents_id).is('string');

				return do_insert(self, NoPg.Attachment, data).then(_get_result(NoPg.Attachment)).then(save_result_to(self));
			}); // q_fcall
		})); // nr_fcall
	}
	return createAttachment2;
};

/** Search attachments */
NoPg.prototype.searchAttachments = function(doc) {
	let self = this;

	function get_documents_id(item) {
		if(item instanceof NoPg.Document) {
			return item.$id;
		} else if(item instanceof NoPg.Attachment) {
			return item.$documents_id;
		} else if(item && item.$documents_id) {
			return item.$documents_id;
		} else if(item && item.$id) {
			return item.$id;
		} else {
			return item;
		}
	}

	function searchAttachments2(opts, traits) {
		return extend.promise( [NoPg], nr_fcall("nopg:searchAttachments", function() {

			let ObjType = NoPg.Attachment;
			opts = opts || {};

			if(doc === undefined) {
				doc = self._getLastValue();
			}

			if(is.array(doc)) {
				opts = ARRAY(doc).map(get_documents_id).map(function(id) {
					if(is.uuid(id)) {
						return {'$documents_id': id};
					} else {
						return id;
					}
				}).valueOf();
			} else if(is.obj(doc)) {
				if(!is.obj(opts)) {
					opts = {};
				}
				opts.$documents_id = get_documents_id(doc);
			}

			return do_select(self, ObjType, opts, traits).then(save_result_to_queue(self)).then(function() { return self; });
		}));
	}

	return searchAttachments2;
};

/** Get value of internal PG connection */
NoPg.prototype.valueOf = function nopg_prototype_valueof() {
	return this._db;
};

// Exports
export default NoPg;
