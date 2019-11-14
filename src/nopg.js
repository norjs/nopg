/**
 * @file nor-nopg -- NoSQL database library for PostgreSQL
 * @Copyright 2014-2019 Sendanor <info@sendanor.fi>,
 *            2014-2019 Jaakko-Heikki Heusala <jheusala@iki.fi>
 */

import _ from 'lodash';
import moment from 'moment';
import pg from '@norjs/pg';
import orm from './orm';
import merge from 'merge';
import pghelpers from './pghelpers.js';
import EventEmitter from 'events';
import pg_escape from 'pg-escape';
import { DEBUG_NOPG } from "./nopg-env";
import NoPgUtils from "./NoPgUtils";
import LogUtils from "@norjs/utils/Log";
import AssertUtils from "@norjs/utils/src/AssertUtils";
import { NOPG_DEFAULTS } from "./nopg-constants";
import InsertQuery from "./insert_query";
import Query from "./query";
import NoPgParsers from "./NoPgParsers";
import first_letter_is_dollar from "./first_letter_is_dollar";

const nrLog = LogUtils.getLogger('@norjs/nopg');

pg.setTypeParser(pg.OID.TIMESTAMP, (timestamp) => moment(timestamp).toISOString());

pg.setTypeParser(pg.OID.TIMESTAMPTZ, (timestamp) => moment(timestamp).toISOString());

export class NoPg {

	/** The constructor
	 *
	 * @param db {NrPostgreSQL}
	 * @constructor
	 */
	constructor (db) {

		if (!db) {
			throw new TypeError("db invalid: " + LogUtils.getAsString(db) );
		}

		/**
		 * @member {NrPostgreSQL}
		 */
		this._db = db;

		this._values = [];

		this._tr_state = 'open';

		this._stats = [];

		this._cache = {
			'types': {},
			'objects': {}
		};

		this._events = new EventEmitter();

		this._watchdog = undefined;

	}

	/** Record internal timing statistic object
	 * @param data
	 */
	_record_sample (data) {

		let self = this;

		let stats_enabled = _.isArray(self._stats);
		let log_times = process.env.DEBUG_NOPG_EVENT_TIMES !== undefined;

		if ( (!stats_enabled) && (!log_times) ) {
			return;
		}

		AssertUtils.isObject(data);
		AssertUtils.isString(data.event);
		AssertUtils.isDate(data.start);
		AssertUtils.isDate(data.end);
		if ( data.duration !== undefined) AssertUtils.isNumber(data.duration);
		if ( data.query !== undefined) AssertUtils.isString(data.query);
		if ( data.params !== undefined) AssertUtils.isArray(data.params);

		if (data.duration === undefined) {
			data.duration = NoPgUtils.getMs(data.start, data.end);
		}

		if (stats_enabled) {
			self._stats.push(data);
		}

		if (log_times) {
			NoPgUtils.logTime(data);
		}

	}

	/** Record internal timing statistic object */
	_finish_samples () {

		let self = this;

		let stats_enabled = _.isArray(self._stats);
		let log_times = process.env.NOPG_EVENT_TIMES !== undefined;

		if ( (!stats_enabled) && (!log_times) ) {
			return;
		}

		let start = self._stats[0];
		let end = self._stats[self._stats.length-1];

		AssertUtils.isObject(start);
		AssertUtils.isEqual(start.event, 'start');

		AssertUtils.isObject(end);
		AssertUtils.isString(end.event);

		let server_duration = 0;
		_.forEach(self._stats, sample => {
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

	}

	/** Fetch next value from queue
	 * @return {*}
	 */
	fetch () {
		return this._values.shift();
	}

	// noinspection JSUnusedGlobalSymbols
	/** Assume the next value in the queue is an array with single value, and throws an exception if it has more than one value.
	 *
	 * @throws {TypeError} an error if the value is not an array or it has two or more values
	 * @returns {* | undefined} The first value in the array or otherwise `undefined`
	 */
	fetchSingle () {

		let items = this.fetch();

		AssertUtils.isArrayWithMaxLength(items, 1);

		return items.shift();

	}

	// noinspection JSUnusedGlobalSymbols
	/** Assume the next value in the queue is an array with single value, and prints an warning if it has more than one value.
	 * @throws an error if the value is not an array
	 * @returns The first value in the array or otherwise `undefined`
	 */
	fetchFirst () {

		let db = this;

		let items = db.fetch();

		AssertUtils.isArray(items);

		if (items.length >= 2) {
			nrLog.warn('nopg.fetchSingle() got an array with too many results (' + items.length + ')');
		}

		return items.shift();

	}

	/** Push `value` to the queue. It makes it possible to implement your own functions.
	 *
	 * @param value
	 * @return {NoPg}
	 */
	push (value) {

		this._save_result_to_queue(value);

		return this;

	}

	/** Returns the latest value in the queue but does not remove it
	 *
	 * @return {*}
	 * @private
	 */
	_getLastValue () {
		return this._values[this._values.length - 1];
	}

	/** Commit transaction
	 * @return {*}
	 */
	async commit () {

		let start_time = new Date();

		await this._db.commit();

		let end_time = new Date();

		this._record_sample({
			'event': 'commit',
			'start': start_time,
			'end': end_time
		});

		this._finish_samples();

		this._tr_state = 'commit';

		if ( NoPgUtils.isObjectNotArray(this._watchdog) ) {
			this._watchdog.clear();
		}

		this._events.emit('commit');

		return this;

	}

	/** Rollback transaction
	 * @return {*}
	 */
	async rollback () {

		let start_time = new Date();

		await this._db.rollback();

		let end_time = new Date();

		this._record_sample({
			'event': 'rollback',
			'start': start_time,
			'end': end_time
		});

		this._finish_samples();

		this._tr_state = 'rollback';

		if (NoPgUtils.isObjectNotArray(this._watchdog)) {
			this._watchdog.clear();
		}

		this._events.emit('rollback');

		return this;

	}

	/** Disconnect
	 * @return {*}
	 */
	async disconnect () {

		let start_time = new Date();

		await this._db.disconnect();

		let end_time = new Date();

		this._record_sample({
			'event': 'disconnect',
			'start': start_time,
			'end': end_time
		});

		this._finish_samples();

		this._tr_state = 'disconnect';

		if (NoPgUtils.isObjectNotArray(this._watchdog)) {
			this._watchdog.clear();
		}

		this._events.emit('disconnect');

		return this;

	}

	/** Setup triggers for specific type
	 * @param type
	 * @return {*}
	 */
	async setupTriggersForType(type) {

		await _.reduce(
			_.map(
				NoPg.createTriggerQueriesForType(type),
				query => () => this._doQuery(query)
			),
			(a, b) => a.then(b),
			Promise.resolve(undefined)
		);

		return this;

	}

	/** Perform generic query
	 *
	 * @param query
	 * @param values
	 * @returns {*}
	 */
	async _doQuery (query, values) {

		if (!query) {
			throw new TypeError("invalid: query: " + LogUtils.getAsString(query));
		}

		if (NoPg.debug) {
			nrLog.debug('query = ', query, '\nvalues = ', values);
		}

		AssertUtils.isObject(this);
		AssertUtils.isObject(this._db);
		AssertUtils.isFunction( this._db[pg.query] );

		let start_time = new Date();

		const res = await this._db[pg.query](query, values);

		let end_time = new Date();

		this._record_sample({
			'event': 'query',
			'start': start_time,
			'end': end_time,
			'query': query,
			'params': values
		});

		//nrLog.debug('res = ', res);

		return res;

	}

	/** Start listening new event listener
	 *
	 */
	setupTCN () {

		// Only setup TCN once
		if (this._tcn_setup) {
			return;
		}

		this._tcn_setup = true;

		/**
		 * An object which contains counters by normalized event name. The counts how many times we are listening
		 * specific channel, so we can stop listening it when the counter goes `0`.
		 *
		 * @type {Object<string,number>}
		 */
		let counter = {};

		/* Listeners for each normalized event name */
		let tcn_listeners = {};

		/** Listener for new listeners */
		const new_listener = event => {

			// Ignore if not tcn event
			if (NoPg.isLocalEventName(event)) {
				return;
			}

			event = NoPg.parseEventName(event);

			// Stringifying back the event normalizes the original event name
			let event_name = NoPg.stringifyEventName(event);

			let channel_name = NoPg.parseTCNChannelName(event);

			// If we are already listening, just increase the counter.
			if (counter.hasOwnProperty(event_name)) {
				counter[event_name] += 1;
				return;
			}

			// Create the listener if necessary
			let tcn_listener;
			if (!tcn_listeners.hasOwnProperty(event_name)) {
				tcn_listener = tcn_listeners[event_name] = NoPgUtils.create_tcn_listener(this._events, event);
			} else {
				tcn_listener = tcn_listeners[event_name];
			}

			// Start listening tcn events for this channel
			//nrLog.debug('Listening channel ' + channel_name + ' for ' + event_name);
			this._db.on(channel_name, tcn_listener);

			counter[event_name] = 1;

		};

		/** Listener for removing listeners */
		const remove_listener = event => {

			// Ignore if not tcn event
			if (NoPg.isLocalEventName(event)) {
				return;
			}

			event = NoPg.parseEventName(event);

			// Stringifying back the event normalizes the original event name
			let event_name = NoPg.stringifyEventName(event);

			let channel_name = NoPg.parseTCNChannelName(event);

			counter[event_name] -= 1;

			if (counter[event_name] === 0) {
				//nrLog.debug('Stopped listening channel ' + channel_name + ' for ' + event_name);
				this._db.removeListener(channel_name, tcn_listeners[event_name]);
				delete counter[event_name];
			}

		};

		this._events.on('newListener', new_listener);
		this._events.on('removeListener', remove_listener);

		return this;

	}

	/** Checks if server has compatible version
	 * @return {*}
	 */
	async testServerVersion () {

		const rows = await this._doQuery('show server_version_num');

		//nrLog.debug('PostgreSQL server version (before parse): ', rows);
		let num = rows.shift().server_version_num;

		num = parseInt(num, 10);

		//nrLog.debug('PostgreSQL server version: ', num);
		if (num >= 90300) {
			return this;
		} else {
			throw new TypeError("PostgreSQL server must be v9.3 or newer (detected "+ num +")");
		}

	}

	/** Checks if server has compatible version
	 *
	 * @param name
	 * @return {NoPg}
	 */
	async testExtension (name) {

		const rows = await this._doQuery('SELECT COUNT(*) AS count FROM pg_catalog.pg_extension WHERE extname = $1', [name]);

		const row = rows.shift();

		const count = parseInt(row.count, 10);

		if (count !== 1) {
			throw new TypeError("PostgreSQL server does not have extension: " + name);
		}

		return this;

	}

	/** Tests if the server is compatible
	 * @return {*}
	 */
	async test () {

		await this.testServerVersion();
		await this.testExtension('plv8');
		await this.testExtension('uuid-ossp');
		await this.testExtension('moddatetime');
		await this.testExtension('tcn');

	}

	/** Initialize the database
	 * @return {*}
	 */
	async init () {

		await this.test();

		const db = await this.latestDBVersion();

		let code_version = require('./schema/latest.js');

		let db_version = db.fetch();

		if (! ((db_version >= -1) && (db_version <= code_version)) ) {
			throw new TypeError("Database version " + db_version + " is not between accepted range (-1 .. " + code_version + ")");
		}

		let builders = [];

		let i = db_version;

		let file;

		while (i < code_version) {

			i += 1;

			file = './schema/v' + NoPgUtils.pad(i, 4) + '.js';

			try {
				NoPgUtils.push_file(builders, file);
			} catch(err) {
				nrLog.error("Exception: ", err);
				throw new TypeError("Failed to load: "+ file + ": " + err);
			}

		}

		// Skip upgrade if we have nothing to do
		if (builders.length === 0) {
			return this;
		}

		// Call upgrade steps
		await _.reduce(
			builders,
			async (so_far, f) => {

				/**
				 * @type {NrPostgreSQL}
				 */
				const db = await so_far;

				db.fetchAll();

				return f(db);
			},
			Promise.resolve(this._db)
		);

		await db._addDBVersion({'$version': code_version});

		await this._importLib(require.resolve('tv4'));

		await NoPgUtils.pg_query("SET plv8.start_proc = 'plv8_init'")(db);

		return this;

	}

	/** Internal INSERT query */
	_prepare_insert_query (self, ObjType, data) {
		return new InsertQuery({'ObjType': ObjType, 'data':data});
	}

	/** Internal INSERT query */
	async _doInsert (self, ObjType, data) {

		const q = this._prepare_insert_query(self, ObjType, data);

		const result = q.compile();

		return await this._doQuery(result.query, result.params);

	}

	/** Create document by type: `db.create([TYPE])([OPT(S)])`.
	 *
	 * @param type
	 * @return {function}
	 */
	create (type) {

		const create2 = async data => {

			if (type && (type instanceof NoPg.Type)) {

				data.$types_id = type.$id;
				data.$type = type.$name;

			} else if (type) {

				const t = await this._getType(type);

				if (!(t instanceof NoPg.Type)) {
					throw new TypeError("invalid type received: " + LogUtils.getAsString(t) );
				}

				type = t;

				return await create2(data);

			}

			let builder;

			if (this._documentBuilders && _.isString(type) && this._documentBuilders.hasOwnProperty(type) && _.isFunction(this._documentBuilders[type])) {
				builder = this._documentBuilders[type];
			} else if (this._documentBuilders && type && _.isString(type.$name) && this._documentBuilders.hasOwnProperty(type.$name) && _.isFunction(this._documentBuilders[type.$name])) {
				builder = this._documentBuilders[type.$name];
			}

			if ( builder !== undefined) AssertUtils.isFunction(builder);

			const rows = await this._doInsert(NoPg.Document, data);

			let result = NoPgUtils.get_result(NoPg.Document)(rows);

			if (builder) {
				result = await builder(result);
			}

			NoPgUtils.save_result_to(this)(result);

		};

		return create2;

	}

	/** Add new DBVersion record
	 * @param data
	 * @return {Promise.<*>}
	 */
	async _addDBVersion (data) {

		const fetchResults = NoPgUtils.get_result(NoPg.DBVersion);

		const rows = await this._doInsert(NoPg.DBVersion, data);

		return fetchResults(rows);

	}

	/** Get type object using only do_query()
	 *
	 * @param document_type
	 * @returns {Promise.<*>}
	 * @private
	 */
	async _get_type_by_name (document_type) {

		const fetchTypeResults = NoPgUtils.get_results(NoPg.Type);

		const rows = await this._doQuery("SELECT * FROM types WHERE name = $1", [document_type]);

		const results = await fetchTypeResults(rows);

		AssertUtils.isArray(results);

		if (results.length !== 1) {
			if (results.length === 0) {
				throw new TypeError("Database has no type: " + document_type);
			}
			throw new TypeError("Database has multiple types: " + document_type + " (" + results.length + ")");
		}

		let result = results.shift();

		AssertUtils.isObject(result);

		return result;

	}

	/** Returns the query object for SELECT queries
	 * @param types
	 * @param search_opts
	 * @param traits {object}
	 */
	async _prepare_select_query (types, search_opts, traits) {

		let ObjType, document_type, document_type_obj;

		// If true then this is recursive function call
		let _recursive = false;
		if (NoPgUtils.isObjectNotArray(traits) && traits._recursive) {
			_recursive = traits._recursive;
		}

		//
		if ( _.isArray(search_opts) && (search_opts.length === 1) && NoPgUtils.is_operator(search_opts[0]) ) {
			throw new TypeError('search_opts invalid: ' + LogUtils.getAsString(search_opts) );
		}

		// Object type and document type
		if (_.isArray(types)) {
			types = [].concat(types);
			ObjType = types.shift();
			document_type = types.shift();
		} else {
			ObjType = types;
		}

		// Create the initial query object
		let q = new Query({
			'method': 'select',
			'ObjType': ObjType,
			'document_type': document_type
		});

		// Traits for search operation
		traits = NoPgUtils.parse_search_traits(traits);

		if (traits.hasOwnProperty('count')) {
			q.count(traits.count);
		}

		// Search options for documents
		search_opts = NoPgUtils.parse_search_opts(search_opts, traits);

		/* Build `type_condition` */

		// If we have the document_type we can limit the results with it
		if (document_type) {
			NoPgUtils.parse_where_type_condition(q, document_type);
		}

		/* Parse `opts_condition` */

		let type_predicate = search_opts ? NoPgParsers.recursive_parse_predicates(ObjType, q, ((traits.match === 'any') ? 'OR' : 'AND'), traits, search_opts) : undefined;

		if (type_predicate) {
			q.where(type_predicate);
		}

		if (traits.limit) {
			q.limit(traits.limit);
		}

		if (traits.offset) {
			q.offset(traits.offset);
		}

		if ( NoPgUtils.isObjectNotArray(document_type) && (document_type instanceof NoPg.Type) ) {
			document_type_obj = document_type;
		}

		// Do not search type if recursive call
		if ( !(_recursive || document_type_obj || !_.isString(document_type)) ) {

			let order_enabled = !!(traits.order && NoPgUtils.has_property_names(traits.order));

			let group_enabled = !!(traits.group && NoPgUtils.has_property_names(traits.group));

			// Only search type if order has been enabled or traits.typeAwareness enabled
			if ( group_enabled || order_enabled || traits.typeAwareness ) {

				document_type_obj = await this._get_type_by_name(document_type);

			}

		}

		if (traits.order) {
			q.orders( NoPgUtils.parse_select_order(ObjType, document_type_obj, traits.order, q, traits) );
		}

		if (traits.group) {
			q.group( NoPgUtils.parse_select_order(ObjType, document_type_obj, traits.group, q, traits) );
		}

		//nrLog.debug('traits.typeAwareness = ', traits.typeAwareness);
		//if (document_type_obj) {
		//	nrLog.debug('document_type_obj = ', document_type_obj);
		//	nrLog.debug('document_type_obj.documents = ', document_type_obj.documents);
		//}

		if (traits.typeAwareness && document_type_obj && document_type_obj.hasOwnProperty('documents')) {

			// FIXME: Maybe we should make sure these documents settings do not collide?
			//nrLog.debug('traits.documents = ', traits.documents);
			//nrLog.debug('document_type_obj.documents = ', document_type_obj.documents);
			traits.documents = [].concat(traits.documents || []).concat(document_type_obj.documents);
			//nrLog.debug('traits.documents = ', traits.documents);

		}

		// Fields to search for
		let fields = NoPgUtils.parse_select_fields(ObjType, traits);

		q.fields(fields);

		return q;

	}

	/** Generic SELECT query
	 * @param self {object} The NoPg connection/transaction object
	 * @param types
	 * @param search_opts
	 * @param traits {object}
	 */
	async _doSelect(self, types, search_opts, traits) {

		const q = await this._prepare_select_query(types, search_opts, traits);

		let result = q.compile();

		// Unnecessary since do_query() does it too
		//if (NoPg.debug) {
		//	nrLog.debug('query = ', result.query);
		//	nrLog.debug('params = ', result.params);
		//}

		AssertUtils.isObject(result);
		AssertUtils.isString(result.query);
		AssertUtils.isArray(result.params);

		let builder;
		let type = result.documentType;

		if ( (result.ObjType === NoPg.Document) &&
			_.isString(type) &&
			self._documentBuilders &&
			self._documentBuilders.hasOwnProperty(type) &&
			_.isFunction(self._documentBuilders[type])
		) {
			builder = self._documentBuilders[type];
		}

		if (builder !== undefined) AssertUtils.isFunction(builder);

		const fetchData = NoPgUtils.get_results(result.ObjType, {
			'fieldMap': result.fieldMap
		});

		const rows = await this._doQuery(result.query, result.params);

		const data = await fetchData(rows);

		if (!builder) {
			return data;
		}

		nrLog.debug('data = ', data);

		return builder(data);

	}

	/** Takes the result and saves it into internal buffer `this._values`.
	 *
	 * Then the result can be fetched using `.fetch()`.
	 *
	 * @param value {*}
	 */
	_save_result_to_queue (value) {

		this._values.push( value );

	}

	/** Search documents
	 * @param type
	 * @return {function(*, *): NoPg}
	 */
	search (type) {

		return async (opts, traits) => {

			const objs = await this._doSelect([NoPg.Document, type], opts, traits);

			this._save_result_to_queue(objs);

			return this;

		};

	}

	/** Generic SELECT COUNT(*) query
	 *
	 * @param types
	 * @param search_opts
	 * @param traits {object}
	 * @fixme Split type specific code to factory function for optimization
	 */
	async _doCount (types, search_opts, traits) {

		const q = await this._prepare_select_query(types, search_opts, traits);

		const result = q.compile();

		nrLog.trace('query = ', result.query);
		nrLog.trace('params = ', result.params);

		const rows = await this._doQuery(result.query, result.params );

		if (!rows) { throw new TypeError("failed to parse result"); }

		if (rows.length !== 1) { throw new TypeError("result has too many rows: " + rows.length); }

		const row = rows.shift();

		if (!row.count) { throw new TypeError("failed to parse result"); }

		return parseInt(row.count, 10);

	}

	/** Count documents
	 * @param type
	 * @return {function(*, *): NoPg}
	 */
	count (type) {

		return async (opts, traits) => {

			traits = merge(traits, {'count': true, 'order': null});

			const result = await this._doCount([NoPg.Document, type], opts, traits);

			this._save_result_to_queue(result);

			return this;

		};

	}

	/** Search single document
	 * @param type
	 * @return {function(*,*): NoPg}
	 */
	searchSingle(type) {

		const ObjType = NoPg.Document;

		const getResult = NoPgUtils.get_result(ObjType);

		return async (opts, traits) => {

			const rows = await this._doSelect([ObjType, type], opts, traits);

			const result = getResult(rows);

			this._save_result_to_queue(result);

			return this;

		};

	}

	/** Internal UPDATE query
	 *
	 * @param self
	 * @param ObjType
	 * @param obj
	 * @param orig_data
	 * @returns {*}
	 */
	async _doUpdate (self, ObjType, obj, orig_data) {

		let query;
		let params;
		let data;
		let where = {};

		if (obj.$id) {
			where.$id = obj.$id;
		} else if (obj.$name) {
			where.$name = obj.$name;
		} else {
			throw new TypeError("Cannot know what to update!");
		}

		if (orig_data === undefined) {

			// FIXME: Check that `obj` is an ORM object
			data = obj.valueOf();

		} else {

			data = (new ObjType(obj)).update(orig_data).valueOf();

		}

		//nrLog.debug('data = ', data);

		// 1. Select only keys that start with $
		// 2. Remove leading '$' character from keys
		// 3. Ignore keys that aren't going to be changed
		// 4. Ignore keys that were not changed
		let keys = _.filter(ObjType.meta.keys, first_letter_is_dollar)
			.map( NoPgUtils.parse_keyword_name )
			.filter( key => data.hasOwnProperty(key) )
			.filter( key => !json_cmp(data[key], obj['$' + key]) );

		//nrLog.debug('keys = ', keys.valueOf());

		// Return with the current object if there is no keys to update
		const keysLength = keys.length;

		if (keysLength === 0) {
			return await this._doSelect(ObjType, where);
		}

		// FIXME: Implement binary content support
		query = "UPDATE " + (ObjType.meta.table) + " SET "+ keys.map((k, i) => k + ' = $' + (i + 1)).join(', ') +" WHERE ";

		if (where.$id) {
			query += "id = $"+ (keysLength+1);
		} else if (where.$name) {
			query += "name = $"+ (keysLength+1);
		} else {
			throw new TypeError("Cannot know what to update!");
		}

		query += " RETURNING *";

		params = keys.map(key => data[key]).valueOf();

		if (where.$id) {
			params.push(where.$id);
		} else if (where.$name){
			params.push(where.$name);
		}

		return await this._doQuery(query, params);

	}

	/** Update document
	 * @param obj
	 * @param data
	 * @return {Promise}
	 * @fixme Maybe this should also be a factory function
	 */
	async _update (obj, data) {

		const ObjType = NoPg._getObjectType(obj) || NoPg.Document;

		const getResult = NoPgUtils.get_result(ObjType);

		const rows = await this._doUpdate(ObjType, obj, data);

		return getResult(rows);

	}

	/** Update document
	 * @param obj
	 * @param data
	 * @return {*}
	 * @fixme This probably should be a async function
	 */
	async update (obj, data) {

		let self = this;

		let ObjType = NoPg._getObjectType(obj) || NoPg.Document;

		let builder;

		if ( self._documentBuilders
			&& obj
			&& _.isString(obj.$type)
			&& self._documentBuilders.hasOwnProperty(obj.$type)
			&& _.isFunction(self._documentBuilders[obj.$type])
		) {
			builder = self._documentBuilders[obj.$type];
		}

		if ( builder !== undefined) AssertUtils.isFunction(builder);

		const getResult = NoPgUtils.get_result(ObjType);

		const rows = await this._doUpdate(ObjType, obj, data);

		let result = getResult(rows);

		if (builder) {
			result = await builder(result);
		}

		this._save_result_to_queue(result);

		return self;

	}

	/** Internal DELETE query
	 *
	 * @param self
	 * @param ObjType
	 * @param obj
	 * @returns {*}
	 * @private
	 */
	async _doDelete (self, ObjType, obj) {

		if (!(obj && obj.$id)) {
			throw new TypeError("opts.$id invalid: " + LogUtils.getAsString(obj) );
		}

		const query = `DELETE FROM ${ ObjType.meta.table } WHERE id = $1`;

		const params = [obj.$id];

		return await this._doQuery(query, params);

	}

	/** Delete resource
	 * @param obj
	 * @return {*}
	 * @fixme This probably should be a factory function
	 */
	async del (obj) {

		if (!obj.$id) {
			throw new TypeError("opts.$id invalid: " + LogUtils.getAsString(obj) );
		}

		const ObjType = NoPg._getObjectType(obj) || NoPg.Document;

		await this._doDelete(ObjType, obj);

		return this;

	}

	/** Alias for `.del(obj)`
	 * @param obj
	 * @return {*}
	 */
	['delete'] (obj) {
		return this.del(obj);
	}

	/** Delete type */
	async delType (name) {

		AssertUtils.isString(name);

		const type = await this._getType(name);

		if (!(type instanceof NoPg.Type)) {
			throw new TypeError("invalid type received: " + LogUtils.getAsString(type) );
		}

		await this._doDelete(NoPg.Type, type);

		return this;

	}

	/**
	 * Alias for .delType()
	 *
	 * @param name
	 */
	deleteType(name) {
		return this.delType(name);
	}

	/** Create a new type. We recommend using `._declareType()` instead unless you want an error if the type exists
	 * already. Use like `db._createType([TYPE-NAME])([OPT(S)])`. Returns the result instead of saving it to `self`
	 * queue.
	 */
	_createType (name) {

		if ( name !== undefined) AssertUtils.isString(name);

		const getNoPgTypeResult = NoPgUtils.get_result(NoPg.Type);

		return async data => {

			data = data || {};

			if ( name !== undefined ) {
				data.$name = '' + name;
			}

			const rows = await this._doInsert(NoPg.Type, data);

			const result = getNoPgTypeResult(rows);

			if ( name !== undefined ) {
				await this.setupTriggersForType(name);
			}

			return result;

		};

	}

	/** Create a new type. We recommend using `.declareType()` instead unless you want an error if the type exists already. Use like `db.createType([TYPE-NAME])([OPT(S)])`.
	 *
	 * @param name
	 * @returns {function(*): NoPg}
	 */
	createType (name) {

		const createType = this._createType(name);

		return async data => {

			const type = await createType(data);

			this._save_result_to_queue(type);

			return this;

		};

	}

	/**
	 * Returns `true` if PostgreSQL database relation exists.
	 * @todo Implement this in nor-pg and use here.
	 */
	async _pg_relation_exists (name) {

		const rows = await this._doQuery('SELECT * FROM pg_class WHERE relname = $1 LIMIT 1', [name]);

		if (!rows) { throw new TypeError("Unexpected result from query: " + LogUtils.getAsString(rows)); }

		return rows.length !== 0;

	}

	/**
	 * Returns `true` if PostgreSQL database table has index like this one.
	 * @todo Implement this in nor-pg and use here.
	 * @returns {Promise.<*>}
	 */
	async _pg_get_indexdef (name) {

		const rows = await this._doQuery('SELECT indexdef FROM pg_indexes WHERE indexname = $1 LIMIT 1', [name]);

		if (!rows) { throw new TypeError("Unexpected result from query: " + LogUtils.getAsString(rows)); }

		if (rows.length === 0) {
			throw new TypeError("Index does not exist: " + name);
		}

		return (rows.shift()||{}).indexdef;

	}

	/** Create index
	 *
	 * @param ObjType
	 * @param type
	 * @param field
	 * @param typefield
	 * @param is_unique
	 * @return {*}
	 */
	async _pg_create_index (ObjType, type, field, typefield, is_unique) {

		const colName = NoPgUtils.parse_predicate_key(ObjType, {'epoch':false}, field);

		const name = NoPgUtils.pg_create_index_name(ObjType, type, field, typefield);

		let query = NoPgUtils.pg_create_index_query_internal_v1( ObjType, type, field, typefield, is_unique);

		const query_v2 = NoPgUtils.pg_create_index_query_internal_v2( ObjType, type, field, typefield, is_unique);

		query = Query.numerifyPlaceHolders(query);

		const params = colName.getParams();

		//nrLog.debug("params = ", params);

		const res = await this._doQuery(query, params);

		// Check that the index was created correctly
		const indexDef = await this._pg_get_indexdef(name);

		if (!( (indexDef === query) || (indexDef === query_v2) )) {
			nrLog.debug('attempted to use: ', query, '\n',
				'.but created as: ', indexDef);
			throw new TypeError("Failed to create index correctly!");
		}

		return res;

	}

	/** Internal DROP INDEX query
	 *
	 * @param ObjType
	 * @param type
	 * @param field
	 * @param typefield
	 * @return {*}
	 */
	_pg_drop_index (ObjType, type, field, typefield) {

		let colName = NoPgUtils.parse_predicate_key(ObjType, {'epoch':false}, field);

		let datakey = colName.getMeta('datakey');

		let field_name = (datakey ? datakey + '.' : '' ) + colName.getMeta('key');

		let name = NoPgUtils.pg_create_index_name( ObjType, type, field, typefield);

		let query = "DROP INDEX IF EXISTS "+name;

		return this._doQuery(query);

	}

	/** Internal CREATE INDEX query that will create the index only if the relation does not exists already
	 *
	 * @param ObjType
	 * @param type
	 * @param field
	 * @param typefield
	 * @param is_unique
	 * @return {Promise.<*>}
	 */
	async _pg_declare_index (ObjType, type, field, typefield, is_unique) {

		const colName = NoPgUtils.parse_predicate_key(ObjType, {'epoch':false}, field);

		const dataKey = colName.getMeta('dataKey');

		const field_name = (dataKey ? dataKey + '.' : '' ) + colName.getMeta('key');

		const name = NoPgUtils.pg_create_index_name(ObjType, type, field, typefield, is_unique);

		const exists = await this._pg_relation_exists(name);

		if (!exists) {
			return this._pg_create_index(ObjType, type, field, typefield, is_unique);
		}

		const old_indexdef = await this._pg_get_indexdef(name);

		const new_indexdef_v1 = NoPgUtils.pg_create_index_query_v1(ObjType, type, field, typefield, is_unique);
		if (new_indexdef_v1 === old_indexdef) return self;

		const new_indexdef_v2 = NoPgUtils.pg_create_index_query_v2(ObjType, type, field, typefield, is_unique);
		if (new_indexdef_v2 === old_indexdef) return self;

		if (NoPg.debug) {
			nrLog.info('Rebuilding index...');
			nrLog.debug('old index is: ', old_indexdef);
			nrLog.debug('new index is: ', new_indexdef_v1);
		}

		await self._pg_drop_index(ObjType, type, field, typefield);

		return await self._pg_create_index(ObjType, type, field, typefield, is_unique);

	}

	/** Create a new type or replace existing type with the new values. Use like `db.declareType([TYPE-NAME])([OPT(S)])`.
	 *
	 * @param name
	 * @param opts
	 * @returns {function(*): NoPg}
	 */
	declareType (name, opts) {

		opts = opts || {};

		AssertUtils.isObject(opts);

		const opts_declare_indexes = opts.hasOwnProperty('declareIndexes') ? (opts.declareIndexes === true) : true;

		const self = this;

		function declareType2(data) {

			data = data || {};

			AssertUtils.isObject(data);

			if ( data.indexes !== undefined) AssertUtils.isArray(data.indexes);

			if ( data.uniqueIndexes !== undefined) AssertUtils.isArray(data.uniqueIndexes);

			let indexes = data.indexes || [];

			let uniqueIndexes = data.uniqueIndexes || [];

			let where = {};

			if (name !== undefined) {
				if (name instanceof NoPg.Type) {
					where.$types_id = name.$id;
				} else {
					where.$name = ''+name;
				}
			}

			let type = await self._getType(where);

			if (type) {
				type = await self._updateTypeCache(type.$name, self._update(type, data));
			} else {
				type = await self._updateTypeCache(name, self._createType(name)(data));
			}

			if (!opts_declare_indexes) {

				self._save_result_to_queue(type);

				return self;

			}

			if (uniqueIndexes.length >= 1) {

				_.forEach(uniqueIndexes, i => {
					if (indexes.indexOf(i) < 0) {
						indexes.push(i);
					}
				});

			}

			if (indexes.indexOf('$id') < 0) {
				indexes.push('$id');
			}

			if (indexes.indexOf('$created') < 0) {
				indexes.push('$created');
			}

			if (indexes.indexOf('$modified') < 0) {
				indexes.push('$modified');
			}

			if (indexes.length === 0) {

				self._save_result_to_queue(type);

				return self;

			}

			//let type = self.fetch();
			await _.reduce(
				_.map(indexes, function build_step(index) {
					return function step() {
						return NoPgUtils.pg_declare_index(self, NoPg.Document, type, index).then(() => NoPgUtils.pg_declare_index(self, NoPg.Document, type, index, "types_id", uniqueIndexes.indexOf(index) >= 0)).then(() => pg_declare_index(self, NoPg.Document, type, index, "type", uniqueIndexes.indexOf(index) >= 0));
					};
				}),
				(a, b) => a.then(b),
				Promise.resolve(undefined)
			);

			self.push(type);

		}

		return declareType2;
	}

	/** Delete method */
	delMethod(type) {

		AssertUtils.isString(type);

		let self = this;

		let self_get_method = self._getMethod(type);

		return function(name) {
			AssertUtils.isString(name);
			return NoPgUtils.extendPromise( [NoPg], NoPgUtils.nr_fcall("nopg:delMethod", function() {
				return self_get_method(name).then(function(method) {
					if (!(method instanceof NoPg.Method)) {
						throw new TypeError("invalid method received: " + LogUtils.getAsString(method) );
					}
					return do_delete(self, NoPg.Method, method).then(function() { return self; });
				});
			}));
		};
	}

	/**
	 * Alias for .delMethod()
	 *
	 * @param name
	 */
	deleteMethod(name) {
		return this.delMethod(name);
	}

	/** Search methods */
	_searchMethods(type) {
		let self = this;
		let ObjType = NoPg.Method;
		AssertUtils.isString(type);
		return function nopg_prototype_search_methods_(opts, traits) {
			if ( opts !== undefined) AssertUtils.isObject(opts);
			opts = opts || {};
			opts.$type = type;
			if (!opts.hasOwnProperty('$active')) {
				opts.$active = true;
			}
			return do_select(self, ObjType, opts, traits).then(get_results(NoPg.Method));
		};
	}

	// noinspection JSUnusedGlobalSymbols
	/** Search methods */
	searchMethods(type) {
		let self = this;
		AssertUtils.isString(type);
		let self_search_methods = self._searchMethods(type);
		return function nopg_prototype_search_methods_(opts, traits) {
			return NoPgUtils.extendPromise( [NoPg], NoPgUtils.nr_fcall("nopg:searchMethods", function() {
				return self_search_methods(opts, traits).then(save_result_to_queue(self)).then(function() { return self; });
			}));
		};
	}

	/** Get active method if it exists */
	_getMethod(type) {
		let self = this;
		AssertUtils.isString(type);
		return function nopg_prototype_get_method_(name) {
			AssertUtils.isString(name);
			let where = {
				'$type': type,
				'$name': name,
				'$active': true
			};
			let traits = {
				'order': ['$created']
			};
			return do_select(self, NoPg.Method, where, traits).then(NoPgUtils.get_result(NoPg.Method));
		};
	}

	/** Create a new method. We recommend using `._declareMethod()` instead of this unless you want an error if the method exists already. Use like `db._createMethod([TYPE-NAME])(METHOD-NAME, METHOD-BODY, [OPT(S)])`. Returns the result instead of saving it to `self` queue. */
	_createMethod(type) {
		let self = this;
		AssertUtils.isString(type);

		function createMethod2(name, body, data) {
			return NoPgUtils.extendPromise( [NoPg], NoPgUtils.nr_fcall("nopg:_createMethod", function() {

				if ( data !== undefined) AssertUtils.isObject(data);
				data = data || {};

				AssertUtils.isString(name);

				if (_.isFunction(body)) {
					body = '' + body;
				}
				if ( body !== undefined) AssertUtils.isString(body);
				body = body || "";

				data.$type = ''+type;
				data.$name = ''+name;
				data.$body = ''+body;

				return self._getType(type).then(function(type_obj) {
					data.$types_id = type_obj.$id;
					return self._doInsert(NoPg.Method, data).then(NoPgUtils.get_result(NoPg.Method));
				});
			}));
		}

		return createMethod2;
	}

	/** Create a new method. We recommend using `.declareMethod()` instead unless you want an error if the type exists already. Use like `db.createMethod([TYPE-NAME])(METHOD-NAME, METHOD-BODY, [OPT(S)])`. */
	createMethod(type) {
		let self = this;
		let self_create_method = self._createMethod(type);

		function createMethod2(name, body, data) {
			return NoPgUtils.extendPromise( [NoPg], NoPgUtils.nr_fcall("nopg:createMethod", function() {
				return self_create_method(name, body, data).then(save_result_to(self));
			}));
		}

		return createMethod2;
	}

	// noinspection JSUnusedGlobalSymbols
	/** Create a new method or replace existing with new values. Use like `db.declareMethod([TYPE-NAME])(METHOD-NAME, METHOD-BODY, [OPT(S)])`. */
	declareMethod(type) {
		let self = this;

		function declareMethod2(name, body, data) {
			return NoPgUtils.extendPromise( [NoPg], NoPgUtils.nr_fcall("nopg:declareMethod", function() {

				AssertUtils.isString(type);
				AssertUtils.isString(name);
				if ( data !== undefined) AssertUtils.isObject(data);
				data = data || {};

				if (!data.hasOwnProperty('$active')) {
					data.$active = true;
				} else if (data.$active !== true) {
					data.$active = null;
				}

				return self._getMethod(type)(name).then(function(method) {
					if ( method && (body === method.$body)) {
						return self._update(method, merge({}, data, {'$body':body}));
					}
					return self._createMethod(type)(name, body, data);
				}).then(function(method) {
					return self.push(method);
				});
			}));
		}

		return declareMethod2;
	}

	/** Returns a document builder function */
	_createDocumentBuilder(type) {
		let self = this;
		AssertUtils.isString(type);
		let self_search_methods = self._searchMethods(type);
		return function nopg_prototype_create_document_builder_() {
			return self_search_methods({'$active': true}).then(function(methods) {

				/** Appends methods to doc */
				function _doc_builder(doc) {
					if (NoPgUtils.isObjectNotArray(doc)) {
						ARRAY(methods).forEach(method => {
							doc[method.$name] = FUNCTION.parse(method.$body).bind(doc);
						});
					}
					return doc;
				}

				/** Appends methods to doc */
				function doc_builder(doc) {
					if (_.isArray(doc)) {
						return ARRAY(doc).map(_doc_builder).valueOf();
					}
					return _doc_builder(doc);
				}

				/** Removes methods from doc */
				function reset_methods(doc) {

					if (_.isArray(doc)) {
						return _.map(doc).map(reset_methods);
					}

					if (NoPgUtils.isObjectNotArray(doc)) {
						_.forEach(methods, method => {
							delete doc[method.$name];
						});
					}

					return doc;
				}

				doc_builder.reset = reset_methods;

				return doc_builder;
			});
		};
	}

	/** Returns a document builder function */
	createDocumentBuilder(type) {
		let self = this;
		AssertUtils.isString(type);
		let self_create_document_builder = self._createDocumentBuilder(type);
		return function nopg_prototype_create_document_builder_() {
			return NoPgUtils.extendPromise( [NoPg], NoPgUtils.nr_fcall("nopg:createDocumentBuilder", function() {
				return self_create_document_builder().then(save_result_to_queue(self)).then(function() { return self; });
			}));
		};
	}

	/** Setups a document builder function in session cache */
	_initDocumentBuilder(type) {
		let self = this;
		AssertUtils.isString(type);
		if (!self._documentBuilders) {
			self._documentBuilders = {};
		}
		let create_document_builder = self._createDocumentBuilder(type);
		return function nopg_prototype_init_document_builder_() {
			if (self._documentBuilders.hasOwnProperty(type)) {
				return self;
			}
			return create_document_builder().then(function(builder) {
				self._documentBuilders[type] = builder;
				return self;
			});
		};
	}

	/** Setups a document builder function in session cache */
	initDocumentBuilder(type) {
		let self = this;
		AssertUtils.isString(type);
		let self_init_document_builder = self._initDocumentBuilder(type);
		return function nopg_prototype_init_document_builder_() {
			return NoPgUtils.extendPromise( [NoPg], NoPgUtils.nr_fcall("nopg:initDocumentBuilder", function() {
				return self_init_document_builder();
			}));
		};
	}

	/** Delete view */
	delView(type) {
		AssertUtils.isString(type);
		let self = this;
		let self_get_view = self._getView(type);
		return function(name) {
			AssertUtils.isString(name);
			return NoPgUtils.extendPromise( [NoPg], NoPgUtils.nr_fcall("nopg:delView", function() {
				return self_get_view(name).then(function(view) {
					if (!(view instanceof NoPg.View)) {
						throw new TypeError("invalid view received: " + LogUtils.getAsString(view) );
					}
					return do_delete(self, NoPg.View, view).then(function() { return self; });
				});
			}));
		};
	}

	/**
	 * Alias for .delView(type)
	 *
	 * @param type
	 * @returns {function(*=): *}
	 */
	deleteView (type) {
		return this.delView(type);
	}

	/** Search views */
	_searchViews(type) {
		let self = this;
		let ObjType = NoPg.View;
		AssertUtils.isString(type);
		return function nopg_prototype_search_views_(opts, traits) {
			if ( opts !== undefined) AssertUtils.isObject(opts);
			opts = opts || {};
			opts.$type = type;
			return do_select(self, ObjType, opts, traits).then(get_results(NoPg.View));
		};
	}

	// noinspection JSUnusedGlobalSymbols
	/** Search views */
	searchViews(type) {
		let self = this;
		AssertUtils.isString(type);
		let self_search_views = self._searchViews(type);
		return function nopg_prototype_search_views_(opts, traits) {
			return NoPgUtils.extendPromise( [NoPg], NoPgUtils.nr_fcall("nopg:searchViews", function() {
				return self_search_views(opts, traits).then(save_result_to_queue(self)).then(function() { return self; });
			}));
		};
	}

	/** Get view */
	getView(type) {
		let self = this;
		AssertUtils.isString(type);
		let self_get_view = self._getView(type);
		return function nopg_prototype_get_view_(opts, traits) {
			return NoPgUtils.extendPromise( [NoPg], NoPgUtils.nr_fcall("nopg:getView", function() {
				return self_get_view(opts, traits).then(save_result_to_queue(self)).then(function() { return self; });
			}));
		};
	}

	/** Get active view if it exists */
	_getView(type) {
		let self = this;
		AssertUtils.isString(type);
		return function nopg_prototype_get_view_(name) {
			AssertUtils.isString(name);
			let where = {
				'$type': type,
				'$name': name
			};
			let traits = {
				'order': ['$created']
			};
			return do_select(self, NoPg.View, where, traits).then(NoPgUtils.get_result(NoPg.View));
		};
	}

	/** Create a new view. We recommend using `._declareView()` instead of this unless you want an error if the view exists already. Use like `db._createView([TYPE-NAME])(METHOD-NAME, METHOD-BODY, [OPT(S)])`. Returns the result instead of saving it to `self` queue. */
	_createView(type) {
		let self = this;
		AssertUtils.isString(type);

		function createView2(name, data) {
			return NoPgUtils.extendPromise( [NoPg], NoPgUtils.nr_fcall("nopg:_createView", function() {

				if ( data !== undefined) AssertUtils.isObject(data);
				data = data || {};

				AssertUtils.isStringWithMinLength(name, 1);

				data.$type = ''+type;
				data.$name = ''+name;

				return self._getType(type).then(function(type_obj) {
					AssertUtils.isObject(type_obj);
					AssertUtils.isUuidString(type_obj.$id);
					data.$types_id = type_obj.$id;
					return self._doInsert(NoPg.View, data).then(NoPgUtils.get_result(NoPg.View));
				});
			}));
		}

		return createView2;
	}

	/** Create a new view. We recommend using `.declareView()` instead unless you want an error if the type exists already. Use like `db.createView([TYPE-NAME])(METHOD-NAME, METHOD-BODY, [OPT(S)])`. */
	createView(type) {
		let self = this;
		let self_create_view = self._createView(type);

		function createView2(name, data) {
			return NoPgUtils.extendPromise( [NoPg], NoPgUtils.nr_fcall("nopg:createView", function() {
				return self_create_view(name, data).then(save_result_to(self));
			}));
		}

		return createView2;
	}

	// noinspection JSUnusedGlobalSymbols
	/** Create a new view or replace existing with new values. Use like `db.declareView([TYPE-NAME])(METHOD-NAME, METHOD-BODY, [OPT(S)])`. */
	declareView(type) {
		let self = this;

		function declareView2(name, data) {
			return NoPgUtils.extendPromise( [NoPg], NoPgUtils.nr_fcall("nopg:declareView", function() {

				AssertUtils.isString(type);
				AssertUtils.isString(name);
				if ( data !== undefined) AssertUtils.isObject(data);
				data = data || {};

				if (!data.hasOwnProperty('$active')) {
					data.$active = true;
				} else if (data.$active !== true) {
					data.$active = false;
				}

				return self._getView(type)(name).then(function(view) {
					if (view) {
						return self._update(view, data);
					}
					return self._createView(type)(name, data);
				}).then(function(view) {
					return self.push(view);
				});
			}));
		}

		return declareView2;
	}

	/** Create a new type or replace existing type with the new values. Use like `db.declareType([TYPE-NAME])([OPT(S)])`. */
	declareIndexes(name) {
		let self = this;

		function declareIndexes2(data) {
			return NoPgUtils.extendPromise( [NoPg], NoPgUtils.nr_fcall("nopg:declareIndexes", function() {
				data = data || {};
				AssertUtils.isObject(data);
				if ( indexes !== undefined) AssertUtils.isArray(data.indexes);
				if ( uniqueIndexes !== undefined) AssertUtils.isArray(data.uniqueIndexes);

				let indexes = data.indexes || [];
				let uniqueIndexes = data.uniqueIndexes || [];

				if (uniqueIndexes.length >= 1) {
					ARRAY(uniqueIndexes).forEach(function(i) {
						if (indexes.indexOf(i) < 0) {
							indexes.push(i);
						}
					});
				}

				if (indexes.indexOf('$id') < 0) {
					indexes.push('$id');
				}

				if (indexes.indexOf('$created') < 0) {
					indexes.push('$created');
				}

				if (indexes.indexOf('$modified') < 0) {
					indexes.push('$modified');
				}

				if (indexes.length === 0) {
					return self;
				}

				let where = {};
				if (name !== undefined) {
					if (name instanceof NoPg.Type) {
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
	}

	/** This is an alias for `.declareType()`. */
	createOrReplaceType(name) {
		return this.declareType(name);
	}

	/** Tests if type exists */
	_typeExists(name) {
		let self = this;
		if (_.isString(name) && self._cache.types.hasOwnProperty(name)) {
			return true;
		}
		return do_select(self, NoPg.Type, name).then(function(types) {
			return (types.length >= 1) ? true : false;
		});
	}

	/** Tests if lib exists */
	_libExists(name) {
		let self = this;
		return do_select(self, NoPg.Lib, name).then(function(types) {
			return (types.length >= 1) ? true : false;
		});
	}

	/** Get type and save it to result queue. */
	typeExists(name) {
		let self = this;
		return NoPgUtils.extendPromise( [NoPg], NoPgUtils.nr_fcall("nopg:typeExists", function() {
			return self._typeExists(name).then(save_result_to(self));
		}));
	}

	/** Update type cache
	 * @param name {string} The name of the type
	 * @param type {Promise.<object|function>} The promise of type object or type object instance
	 */
	async _updateTypeCache (name, type) {

		AssertUtils.isString(name);

		if (!_.isFunction(type)) {
			AssertUtils.isObject(type);
		}

		const cache = this._cache;
		AssertUtils.isObject(cache);

		const objects = cache.objects;
		AssertUtils.isObject(objects);

		const types = cache.types;
		AssertUtils.isObject(types);

		return types[name] = Promise.resolve(type).then(result => {

			if ( NoPgUtils.isObjectNotArray(result) ) {

				const result_id = result.$id;

				types[name] = result;

				if ( NoPgUtils.isUuid(result_id) ) {
					objects[result_id] = result;
				}

			}

			return result;

		});

	}

	/** Get type directly
	 *
	 * @param name
	 * @param traits
	 * @returns {Promise}
	 * @private
	 */
	async _getType (name, traits) {

		const self = this;

		const getTypeResult = NoPgUtils.get_result(NoPg.Type);

		if (!_.isString(name)) {

			const rows = await this._doSelect(NoPg.Type, name, traits);

			return getTypeResult(rows);

		}

		if (self._cache.types.hasOwnProperty(name)) {
			return self._cache.types[name];
		}

		return await self._updateTypeCache(name, this._doSelect(NoPg.Type, name, traits).then(getTypeResult) );

	}

	/** Get type and save it to result queue. */
	getType(name) {
		let self = this;
		return NoPgUtils.extendPromise( [NoPg], NoPgUtils.nr_fcall("nopg:getType", function() {
			return self._getType(name).then(save_result_to(self));
		}));
	}

	/** Returns the latest database server version as a integer number */
	latestDBVersion () {
		let self = this;
		return NoPgUtils.extendPromise( [NoPg], NoPgUtils.nr_fcall("nopg:latestDBVersion", function() {
			return _latestDBVersion(self).then(save_result_to(self));
		}));
	}

	/** Import javascript file into database as a library by calling `.importLib(FILE, [OPT(S)])` or `.importLib(OPT(S))` with `$content` property. */
	_importLib(file, opts) {
		let self = this;
		opts = JSON.parse( JSON.stringify( opts || {} ));

		if ( NoPgUtils.isObjectNotArray(file) && (opts === undefined) ) {
			opts = file;
			file = undefined;
		}

		return Q.fcall(function() {
			if (file) {
				return readFile(file, {'encoding':'utf8'});
			}
			if (opts.$content) {
				return;
			}
			throw new TypeError("NoPg.prototype.importLib() called without content or file");
		}).then(function importLib2(data) {
			opts.$name = opts.$name || require('path').basename(file, '.js');
			let name = '' + opts.$name;

			opts['content-type'] = '' + (opts['content-type'] || 'application/javascript');
			if (data) {
				opts.$content = ''+data;
			}

			return self._libExists(opts.$name).then(function(exists) {
				if (exists) {
					delete opts.$name;
					return do_update(self, NoPg.Lib, {"$name":name}, opts);
				} else {
					return self._doInsert(NoPg.Lib, opts);
				}
			});
		});

	}

	// noinspection JSUnusedGlobalSymbols
	/** Import javascript file into database as a library by calling `.importLib(FILE, [OPT(S)])` or `.importLib(OPT(S))` with `$content` property. */
	importLib(file, opts) {
		let self = this;
		return NoPgUtils.extendPromise( [NoPg], NoPgUtils.nr_fcall("nopg:importLib", function() {
			return self._importLib(file, opts).then(NoPgUtils.get_result(NoPg.Lib)).then(save_result_to(self));
		}));
	}

	/** Get specified object directly */
	_getObject(ObjType) {
		let self = this;
		return function(opts, traits) {
			return do_select(self, ObjType, opts, traits).then(NoPgUtils.get_result(ObjType));
		};
	}

	/** Get document directly */
	_getDocument(opts) {
		let self = this;
		return self._getObject(NoPg.Document)(opts);
	}

	/** Get document and save it to result queue. */
	getDocument(opts) {
		let self = this;
		return NoPgUtils.extendPromise( [NoPg], NoPgUtils.nr_fcall("nopg:getDocument", function() {
			return self._getDocument(opts).then(save_result_to(self));
		}));
	}

	/** Search types */
	searchTypes(opts, traits) {
		let self = this;
		let ObjType = NoPg.Type;
		return NoPgUtils.extendPromise( [NoPg], NoPgUtils.nr_fcall("nopg:searchTypes", function() {
			return do_select(self, ObjType, opts, traits).then(save_result_to_queue(self)).then(function() { return self; });
		}));
	}

	/** Create an attachment from a file in the filesystem.
	 * @param obj {object} The document object where the attachment will be placed.
	 *          If it is an attachment object, it's parent will be used. If it is
	 *          undefined, then last object in the queue will be used.
	 */
	createAttachment(doc) {
		let self = this;
		let doc_id;

		function createAttachment2(file, opts) {
			return NoPgUtils.extendPromise( [NoPg], NoPgUtils.nr_fcall("nopg:createAttachment", function() {
				return Q.fcall(function() {
					opts = opts || {};

					let file_is_buffer = false;

					try {
						if (file && _.isString(file)) {
							AssertUtils.isString(file);
						} else {
							AssertUtils.isInstanceOf(file, Buffer);
							file_is_buffer = true;
						}
					} catch(e) {
						throw new TypeError("Argument not String or Buffer: " + e);
					}
					AssertUtils.isObject(opts);

					if (doc === undefined) {
						doc = self._getLastValue();
					}

					if (doc && (doc instanceof NoPg.Document)) {
						doc_id = doc.$id;
					} else if (doc && (doc instanceof NoPg.Attachment)) {
						doc_id = doc.$documents_id;
					} else if (doc && NoPgUtils.isUuid(doc.$documents_id)) {
						doc_id = doc.$documents_id;
					} else if (doc && NoPgUtils.isUuid(doc.$id)) {
						doc_id = doc.$id;
					} else {
						throw new TypeError("Could not detect document ID!");
					}

					AssertUtils.isUuidString(doc_id);

					if (file_is_buffer) {
						return file;
					}

					return readFile(file, {'encoding':'hex'});

				}).then(function(buffer) {

					let data = {
						$documents_id: doc_id,
						$content: '\\x' + buffer,
						$meta: opts
					};

					AssertUtils.isString(data.$documents_id);

					return self._doInsert(NoPg.Attachment, data).then(NoPgUtils.get_result(NoPg.Attachment)).then(save_result_to(self));
				}); // q_fcall
			})); // NoPgUtils.nr_fcall
		}

		return createAttachment2;
	}

	/** Search attachments */
	searchAttachments(doc) {
		let self = this;

		function get_documents_id(item) {
			if (item instanceof NoPg.Document) {
				return item.$id;
			} else if (item instanceof NoPg.Attachment) {
				return item.$documents_id;
			} else if (item && item.$documents_id) {
				return item.$documents_id;
			} else if (item && item.$id) {
				return item.$id;
			} else {
				return item;
			}
		}

		function searchAttachments2(opts, traits) {
			return NoPgUtils.extendPromise( [NoPg], NoPgUtils.nr_fcall("nopg:searchAttachments", function() {

				let ObjType = NoPg.Attachment;
				opts = opts || {};

				if (doc === undefined) {
					doc = self._getLastValue();
				}

				if (_.isArray(doc)) {
					opts = ARRAY(doc).map(get_documents_id).map(function(id) {
						if (NoPgUtils.isUuid(id)) {
							return {'$documents_id': id};
						} else {
							return id;
						}
					}).valueOf();
				} else if (NoPgUtils.isObjectNotArray(doc)) {
					if (!NoPgUtils.isObjectNotArray(opts)) {
						opts = {};
					}
					opts.$documents_id = get_documents_id(doc);
				}

				return do_select(self, ObjType, opts, traits).then(save_result_to_queue(self)).then(function() { return self; });
			}));
		}

		return searchAttachments2;
	}

	/** Get value of internal PG connection */
	valueOf () {
		return this._db;
	}

	/** Returns the NoPg constructor type of `doc`, otherwise returns undefined.
	 * @param doc
	 * @return {*}
	 */
	static _getObjectType(doc) {

		if (doc instanceof NoPg.Document  ) { return NoPg.Document;   }
		if (doc instanceof NoPg.Type      ) { return NoPg.Type;       }
		if (doc instanceof NoPg.Attachment) { return NoPg.Attachment; }
		if (doc instanceof NoPg.Lib       ) { return NoPg.Lib;        }
		if (doc instanceof NoPg.Method    ) { return NoPg.Method;     }
		if (doc instanceof NoPg.View    ) { return NoPg.View;     }
		if (doc instanceof NoPg.DBVersion ) { return NoPg.DBVersion;  }

	}

	// noinspection JSUnusedGlobalSymbols
	/** Returns the NoPg constructor type of `doc`, otherwise throws an exception of `TypeError`.
	 * @param doc
	 * @return {*}
	 */
	static getObjectType(doc) {
		let ObjType = NoPg._getObjectType(doc);
		if (!ObjType) {
			throw new TypeError("doc is unknown type: {" + typeof doc + "} " + JSON.stringify(doc, null, 2) );
		}
		return ObjType;
	}

	/** Start a transaction
	 * @param pgconfig {string} PostgreSQL database configuration. Example:
	 `"postgres://user:pw@localhost:5432/test"`
	 * @param opts {object} Optional options.
	 * @param opts.timeout {number} The timeout, default is
	 from `NoPg.defaults.timeout`.
	 * @param opts.pgconfig {string} See param `pgconfig`.
	 * @return {*}
	 */
	static start(pgconfig, opts = undefined) {
		return NoPgUtils.extendPromise( [NoPg], NoPgUtils.nr_fcall("nopg:start", function() {
			let start_time = new Date();
			let w;

			let args = ARRAY([pgconfig, opts]);
			pgconfig = _.find(args, arg => _.isString(arg));
			opts = _.find(args, arg => NoPgUtils.isObjectNotArray(arg));
			if ( opts !== undefined) AssertUtils.isObject(opts);

			if (!opts) {
				opts = {};
			}

			AssertUtils.isObject(opts);

			if (!pgconfig) {
				pgconfig = opts.pgconfig || NoPg.defaults.pgconfig;
			}

			let timeout = opts.timeout || NoPg.defaults.timeout;
			if ( timeout !== undefined) AssertUtils.isNumber(timeout);
			AssertUtils.isString(pgconfig);

			return pg.start(pgconfig).then(function(db) {

				if (!db) { throw new TypeError("invalid db: " + LogUtils.getAsString(db) ); }
				if (timeout !== undefined) {
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
				if (w) {
					w.reset(db);
					db._watchdog = w;
				}
				return pg_query("SET plv8.start_proc = 'plv8_init'")(db);
			});
		}));
	}

	/** Start a connection (without transaction)
	 * @param pgconfig {string} PostgreSQL database configuration. Example:
	 `"postgres://user:pw@localhost:5432/test"`
	 * @param opts {object} Optional options.
	 * @param opts.pgconfig {string} See param `pgconfig`.
	 * @return {*}
	 */
	static connect(pgconfig, opts) {
		return NoPgUtils.extendPromise( [NoPg], NoPgUtils.nr_fcall("nopg:connect", function() {
			let start_time = new Date();

			let args = ARRAY([pgconfig, opts]);
			pgconfig = _.find(args, arg => _.isString(arg));
			opts = _.find(args, arg => NoPgUtils.isObjectNotArray(arg));
			if ( opts !== undefined) AssertUtils.isObject(opts);
			if (!opts) {
				opts = {};
			}

			AssertUtils.isObject(opts);

			if (!pgconfig) {
				pgconfig = opts.pgconfig || NoPg.defaults.pgconfig;
			}

			AssertUtils.isString(pgconfig);

			return pg.connect(pgconfig).then(function(db) {
				if (!db) { throw new TypeError("invalid db: " + LogUtils.getAsString(db) ); }
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
	}

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
	static transaction(pgconfig, opts, fn) {
		return NoPgUtils.extendPromise( [NoPg], NoPgUtils.nr_fcall("nopg:transaction", function() {
			let args = ARRAY([pgconfig, opts, fn]);
			pgconfig = _.find(args, arg => _.isString(arg));
			opts = _.find(args, arg => NoPgUtils.isObjectNotArray(arg));
			fn = _.find(args, arg => _.isFunction(arg));

			if ( pgconfig !== undefined) AssertUtils.isString(pgconfig);
			if ( opts !== undefined) AssertUtils.isObject(opts);
			AssertUtils.isFunction(fn);

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
				if (!_db) {
					//nrLog.error('Passing on error: ', err);
					return Q.reject(err);
				}
				return _db.rollback().fail(function(err2) {
					nrLog.error("rollback failed: ", err2);
					//nrLog.error('Passing on error: ', err);
					return Q.reject(err);
				}).then(function() {
					//nrLog.error('Passing on error: ', err);
					return Q.reject(err);
				});
			});

		}));
	}

	/** Returns CREATE TRIGGER query for Type specific TCN
	 * @param type
	 * @param op
	 * @return {*}
	 */
	static createTriggerQueriesForType(type, op) {
		AssertUtils.isString(type);
		if ( op !== undefined) AssertUtils.isString(op);

		if (op === undefined) {
			return [].concat(create_tcn_queries(type, "insert"))
				.concat(create_tcn_queries(type, "update"))
				.concat(create_tcn_queries(type, "delete"));
		}

		if (['insert', 'delete', 'update'].indexOf(op) < 0) {
			throw new TypeError("op is invalid: " + op);
		}
		op = op.toLowerCase();

		let table_name = 'documents';
		let trigger_name = table_name + '_' + op + '_' + type + '_tcn_trigger';
		let channel_name = 'tcn' + type.toLowerCase();

		if (op === 'insert') {
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

		if (op === 'update') {
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

		if (op === 'delete') {
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
	}

	/** Returns true if argument is an event name
	 * @param name
	 * @return {* | boolean}
	 */
	static isTCNEventName(name) {
		return _.isString(name) && (TCN_EVENT_NAMES.indexOf(name) >= 0);
	}

	/** Returns true if argument is an event name
	 * @param name
	 * @return {* | boolean}
	 */
	static isLocalEventName(name) {
		return _.isString(name) && (LOCAL_EVENT_NAMES.indexOf(name) >= 0);
	}

	/** Returns true if argument is an event name
	 * @param name
	 * @return {* | boolean}
	 */
	static isEventName(name) {
		return NoPg.isTCNEventName(name) || NoPg.isLocalEventName(name);
	}

	/** Convert event name from object to string
	 * @param name {string} The name of an event, eg. `[(type_id|type)#][id@][(eventName|id|type)]`. [See
	 *     more](https://trello.com/c/qrSpMOfk/6-event-support).
	 * @returns {object} Parsed values, eg. `{"type":"User", "name":"create", "id":
	 *     "b6913d79-d37a-5977-94b5-95bdfe5cccda"}`, where only available properties are defined. If type_id was used, the
	 *     type will have an UUID and you should convert it to string presentation if necessary.
	 */
	static stringifyEventName(event) {
		AssertUtils.isObject(event);
		if ( type !== undefined) AssertUtils.isString(event.type);
		if ( name !== undefined) AssertUtils.isString(event.name);
		if ( id !== undefined) AssertUtils.isString(event.id);

		let has_name = event.hasOwnProperty('name');

		if (has_name && NoPg.isLocalEventName(event.name) ) {
			return event.name;
		}

		let name = '';
		if (event.hasOwnProperty('type')) {
			name += event.type + '#';
		}
		if (event.hasOwnProperty('id')) {
			name += event.id + '@';
		}
		if (has_name) {
			name += event.name;
		}
		return name;
	}

	/** Parse event name from string into object.
	 * @param name {string} The name of an event, eg. `[(type_id|type)#][id@][(eventName|id|type)]`. [See
	 *     more](https://trello.com/c/qrSpMOfk/6-event-support).
	 * @returns {object} Parsed values, eg. `{"type":"User", "name":"create", "id":
	 *     "b6913d79-d37a-5977-94b5-95bdfe5cccda"}`, where only available properties are defined. If type_id was used, the
	 *     type will have an UUID and you should convert it to string presentation if necessary.
	 */
	static parseEventName(name) {
		AssertUtils.isString(name);

		return merge.apply(undefined, ARRAY(name.replace(/([#@])/g, "$1\n").split("\n")).map(function(arg) {
			arg = arg.trim();
			let key;
			let is_type = arg[arg.length-1] === '#';
			let is_id = arg[arg.length-1] === '@';

			if (is_type || is_id) {
				arg = arg.substr(0, arg.length-1).trim();
				key = is_type ? 'type' : 'id';
				let result = {};
				result[key] = arg;
				return result;
			}

			if (NoPgUtils.isUuid(arg)) {
				return {'id': arg};
			}

			if (NoPg.isEventName(arg)) {
				return {'name': arg};
			}

			if (arg) {
				return {'type':arg};
			}

			return {};
		}).valueOf());
	}

	/** Parse tcn channel name to listen from NoPg event name
	 * @param event {string|object} The name of an event, eg. `[(type_id|type)#][id@][(eventName|id|type)]`. [See
	 *     more](https://trello.com/c/qrSpMOfk/6-event-support).
	 * @returns {string} Channel name for TCN, eg. `tcn_User` or `tcn` for non-typed events.
	 * @todo Hmm, should we throw an exception if it isn't TCN event?
	 */
	static parseTCNChannelName(event) {
		if (_.isString(event)) {
			event = NoPg.parseEventName(event);
		}
		AssertUtils.isObject(event);
		if (event.hasOwnProperty('type')) {
			return 'tcn' + event.type.toLowerCase();
		}
		return 'tcn';
	}

	/** Parse TCN payload string
	 * @param payload {string} The payload string from PostgreSQL's tcn extension
	 * @returns {object}
	 */
	static parseTCNPayload(payload) {

		AssertUtils.isString(payload);

		let parts = payload.split(',');

		let table = parts.shift(); // eg. `"documents"`
		AssertUtils.isStringWithMinLength(table, 2);
		AssertUtils.isEqual(table.charAt(0), '"');
		AssertUtils.isEqual(table.charAt(table.length-1), '"');
		table = table.substr(1, table.length-2);

		let op = parts.shift(); // eg. `I`
		AssertUtils.isString(op);

		let opts = parts.join(','); // eg. `"id"='b6913d79-d37a-5977-94b5-95bdfe5cccda'...`
		let i = opts.indexOf('=');
		if (i < 0) { throw new TypeError("No primary key!"); }

		let key = opts.substr(0, i); // eg. `"id"`
		AssertUtils.isStringWithMinLength(key, 2);
		AssertUtils.isEqual(key.charAt(0), '"');
		AssertUtils.isEqual(key.charAt(key.length-1), '"');
		key = key.substr(1, key.length-2);

		let value = opts.substr(i+1); // eg. `'b6913d79-d37a-5977-94b5-95bdfe5cccda'...`
		AssertUtils.isStringWithMinLength(value, 2);
		AssertUtils.isEqual(value.charAt(0), "'");
		i = value.indexOf("'", 1);
		if (i < 0) { throw new TypeError("Parse error! Could not find end of input."); }
		value = value.substr(1, i-1);

		let keys = {};
		keys[key] = value;

		return {
			'table': table,
			'op': op,
			'keys': keys
		};
	}

	static get debug () {
		return DEBUG_NOPG;
	}

	static get defaults () {
		return NOPG_DEFAULTS;
	}

}

/** NoPg has event `timeout` -- when automatic timeout happens, and rollback issued, and connection is freed */
/** NoPg has event `rollback` -- when rollback hapens */
/** NoPg has event `commit` -- when commit hapens */

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

// Aliases
NoPg.fcall = NoPg.transaction;

/** Build wrappers for event methods */
['addListener', 'on', 'once', 'removeListener'].forEach(function(fn) {
	NoPg.prototype[fn] = function(event, listener) {
		let self = this;
		return NoPgUtils.extendPromise( [NoPg], NoPgUtils.nr_fcall("nopg:"+fn, function() {
			return Q.fcall(function() {
				event = NoPg.parseEventName(event);
				AssertUtils.isObject(event);

				// Setup tcn listeners only if necessary
				if ( event.hasOwnProperty('event') && NoPg.isLocalEventName(event.name) ) {
					return;
				}

				// FIXME: If type is declared as an uuid, we should convert it to string. But for now, just throw an exception.
				if (event.hasOwnProperty('type') && NoPgUtils.isUuid(event.type)) {
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

NoPg.prototype['delete'] = NoPg.prototype.del;

/** Alias for `pghelpers.escapeFunction()` */
NoPg._escapeFunction = pghelpers.escapeFunction;

// Exports
export default NoPg;
