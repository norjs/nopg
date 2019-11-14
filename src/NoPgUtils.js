import FS from "fs";
import AssertUtils from "@norjs/utils/Assert";
import LogUtils from "@norjs/utils/Log";
import _ from "lodash";
import merge from "./merge";
import NoPg from "./nopg";
import Predicate from "./Predicate";
import pg from "@norjs/pg";
import Query from "./query";
import InsertQuery from "./insert_query";
import first_letter_is_dollar from "./first_letter_is_dollar";
import NrPgCast from "./NrPgCast";
import { TCN_EVENT_MAPPING } from "./nopg-constants";

/**
 * Functions to build casts for different types
 *
 * @enum {function}
 * @readonly
 */
export const PG_CASTS = {
    direct  : x => NrPgCast.castDirect(x),
    boolean : x => NrPgCast.castBoolean(x),
    numeric : x => NrPgCast.castNumeric(x),
    text    : x => NrPgCast.castText(x)
};

/**
 *
 */
export class NoPgUtils {

    static parseBoolean (value) {

        if (!value) { return false; }

        if (value === true) { return true; }

        value = ('' + value).toLowerCase();

        if (value === "false") { return false; }

        if (value === "off") { return false; }

        if (value === "no") { return false; }

        return value !== "0";

    }

    static async nr_fcall (desc, fn) {
        return await fn();
    }

    static async readFile (file, {encoding='utf8'} = {}) {
        return await new Promise((resolve, reject) => {
            try {
                FS.readFile(file, {encoding}, (err, data) => {
                    try {
                        if (err) {
                            reject(err);
                        } else {
                            resolve(data);
                        }
                    } catch (err) {
                        nrLog.error('Unexpected exception: ', err);
                        reject(err);
                    }
                });
            } catch (err) {
                reject(err);
            }
        });
    }

    /** Returns seconds between two date values
     * @returns {number} Time between two values (ms)
     */
    static getMs (a, b) {

        AssertUtils.isDate(a);

        AssertUtils.isDate(b);

        if (a < b) {
            return b.getTime() - a.getTime();
        }

        return a.getTime() - b.getTime();

    }

    /** Optionally log time */
    static logTime (sample) {

        AssertUtils.isObject(sample);
        AssertUtils.isString(sample.event);
        AssertUtils.isDate(sample.start);
        AssertUtils.isDate(sample.end);

        if (sample.duration !== undefined) {
            AssertUtils.isNumber(sample.duration);
        }

        if (sample.query !== undefined) {
            AssertUtils.isString(sample.query);
        }

        if (sample.params !== undefined) {
            AssertUtils.isArray(sample.params);
        }

        let msg = 'NoPg event ' + sample.event + ' in ' + sample.duration + ' ms';

        if (sample.query || sample.params) {
            msg += ': ';
        }

        if (sample.query) {
            msg += 'query=' + LogUtils.getAsString(sample.query);
            if (sample.params) {
                msg += ', ';
            }
        }

        if (sample.params) {
            msg += 'params=' + LogUtils.getAsString(sample.params);
        }

        nrLog.debug(msg);

    }


    /** Parse obj.$documents.expressions into the object */
    static _parse_object_expressions(obj) {

        AssertUtils.isObject(obj);

        if (!obj.$documents) {
            return;
        }

        let expressions = obj.$documents.expressions;
        if (!expressions) {
            return;
        }

        _.forEach(Object.keys(expressions), prop => {

            const value = expressions[prop];

            // FIXME: This code should understand things better
            const contentPrefix = 'content.';

            let key;
            if (prop.substr(0, contentPrefix.length) === contentPrefix) {
                key = prop.substr(contentPrefix.length);
            } else {
                key = '$' + prop;
            }

            obj[key] = value;

        });

    }

    /** Take first result from the database query and returns new instance of `Type`
     *
     * @param Type
     * @returns {Function}
     */
    static get_result (Type) {
        return rows => {

            if (!rows) { throw new TypeError("failed to parse result"); }

            let doc = rows.shift();

            if (!doc) { return; }

            if (doc instanceof Type) {
                return doc;
            }

            const obj = {};

            _.forEach(Object.keys(doc), key => {

                if (key === 'documents') {

                    obj['$'+key] = {};

                    _.forEach(Object.keys(doc[key]), k => {
                        if (NoPgUtils.isUuid(k)) {
                            obj['$'+key][k] = NoPgUtils.get_result(NoPg.Document)([doc[key][k]]);
                        } else {
                            obj['$'+key][k] = doc[key][k];
                        }
                    });

                    return;

                }

                obj['$'+key] = doc[key];

            });

            NoPgUtils._parse_object_expressions(obj);

            return new Type(obj);

        };
    }

    /** Take all results from the database query and return an array of new instances of `Type`
     *
     * @param Type
     * @param opts
     * @returns {function(*=): Object}
     */
    static get_results (Type, opts) {

        opts = opts || {};

        let field_map;
        if (_.isFunction(opts.fieldMap)) {
            field_map = opts.fieldMap;
        } else if (NoPgUtils.isObjectNotArray(opts.fieldMap)) {
            field_map = function(k) {
                return opts.fieldMap[k];
            };
        }

        /** Parse field */
        function parse_field(obj, key, value) {
            AssertUtils.isObject(obj);
            //nrLog.debug('obj = ', obj);
            //nrLog.debug('key = ', key);
            //nrLog.debug('value = ', value);

            /* Parse full top level field */
            function parse_field_top(obj, key, value) {
                if ( _.isArray(obj['$'+key]) ) {
                    obj['$'+key] = obj['$'+key].concat(value);
                } else if ( NoPgUtils.isObjectNotArray(obj['$'+key]) ) {
                    obj['$'+key] = merge(obj['$'+key], value);
                } else {
                    obj['$'+key] = value;
                }
            }

            /* Parse property in top level field based on a key as an array `[datakey, property_name]` */
            function parse_field_property(obj, key, value) {
                //nrLog.debug('key = ', key);
                let a = key[0];
                let b = key[1];
                //nrLog.debug('key_a = ', a);
                //nrLog.debug('key_b = ', b);

                if (!NoPgUtils.isObjectNotArray(obj['$'+a])) {
                    obj['$'+a] = {};
                }

                obj['$'+a][b] = value;
            }

            /* Parse property in top level field based on key in PostgreSQL JSON format */
            function parse_field_property_pg(obj, key, value) {
                //nrLog.debug('key = ', key);
                /*jslint regexp: false*/
                let matches = /^([a-z][a-z0-9\_]*)\-\>\>'([^\']+)'$/.exec(key);
                /*jslint regexp: true*/
                let a = matches[1];
                let b = matches[2];
                return parse_field_property_pg(obj, [a,b], value);
            }

            //
            let new_key;
            if ( _.isFunction(field_map) && (new_key = field_map(key)) ) {
                if ( (new_key) && (new_key !== key) ) {
                    return parse_field(obj, new_key, value);
                }
            }

            if ( _.isArray(key) ) {
                parse_field_property(obj, key, value);
            } else if ( _.isString(key) && (/^[a-z][a-z0-9\_]*$/.test(key)) ) {
                parse_field_top(obj, key, value);
                /*jslint regexp: false*/
            } else if ( _.isString(key) && (/^([a-z][a-z0-9\_]*)\-\>\>'([^\']+)'$/.test(key)) ) {
                /*jslint regexp: true*/
                parse_field_property_pg(obj, key, value);
            } else {
                //nrLog.debug('key = ', key);
                throw new TypeError("Unknown field name: " + key);
            }
        }

        /* Returns a function which will go through rows and convert them to NoPg format */
        return rows => _.map(rows, (row, i) => {
            if ( !row ) {
                throw new TypeError("failed to parse result #" + i + " from database!");
            }
            //nrLog.debug('input in row = ', row);

            //nrLog.debug('row = ', row);

            if ( row instanceof Type ) {
                return row;
            }

            let obj = {};
            _.forEach(Object.keys(row), key => {

                if ( key === 'documents' ) {

                    obj['$' + key] = {};
                    _.forEach(Object.keys(row[key]), uuid => {
                        if ( !NoPgUtils.isUuid(uuid) ) {
                            obj['$' + key][uuid] = row[key][uuid];
                            return;
                        }
                        let sub_doc = row[key][uuid];
                        let sub_obj = {};
                        _.forEach(Object.keys(sub_doc), k => {
                            parse_field(sub_obj, k, sub_doc[k]);
                        });
                        obj['$' + key][uuid] = new NoPg.Document(sub_obj);
                    });

                    return;

                }

                parse_field(obj, key, row[key]);

            });

            _parse_object_expressions(obj);

            //nrLog.debug('result in obj = ', obj);
            return new Type(obj);
        }).valueOf();
    }

    /** Takes the result and saves it into `self`. If `self` is one of `NoPg.Document`,
     * `NoPg.Type`, `NoPg.Attachment` or `NoPg.Lib`, then the content is updated into
     * that instance. If the `doc` is an instance of `NoPg` then the result can be
     * fetched using `self.fetch()`.
     *
     * **DEPRECATED:** You should know the type when you call and use it's method.
     *
     * @deprecated
     */
    static save_result_to(self) {

        if ( NoPgUtils.isObjectNotArray(self) && _.isFunction(self.nopg) ) {
            return doc => self.update(doc);
        }

        if ( NoPgUtils.isObjectNotArray(self) && _.isArray(self._values) ) {
            return doc => {
                self._values.push( doc );
                return self;
            };
        }

        throw new TypeError("Unknown target: " + (typeof self));

    }

    /** Returns the data key of Type */
    static get_predicate_datakey(Type) {
        return (Type.meta.datakey || '$meta').substr(1);
    }

    /** */
    static parse_keyref_json(datakey, key) {
        if (key.indexOf('.') === -1) {
            //      json_extract_path(content, VARIADIC ARRAY['["user"]'::json ->> 0])::text
            //return "json_extract_path(" + datakey + ", '" + JSON.stringify([key]) + "'::json ->> 0)::text";
            return "(" + datakey + " -> '" + key + "'::text)";
        } else {
            //return "json_extract_path(" + datakey + ", '" + JSON.stringify(key.split('.')) + "'::json ->> 0)::text";
            return "(" + datakey + " #> '{" + key.split('.').join(',') +"}')";
        }
    }

    /** */
    static parse_keyref_text(datakey, key) {
        if (key.indexOf('.') === -1) {
            //      json_extract_path(content, VARIADIC ARRAY['["user"]'::json ->> 0])::text
            //return "json_extract_path(" + datakey + ", '" + JSON.stringify([key]) + "'::json ->> 0)::text";
            return "(" + datakey + " ->> '" + key + "'::text)";
        } else {
            //return "json_extract_path(" + datakey + ", '" + JSON.stringify(key.split('.')) + "'::json ->> 0)::text";
            return "(" + datakey + " #>> '{" + key.split('.').join(',') +"}')";
        }
    }

    /** Returns PostgreSQL keyword for NoPg keyword. Converts `$foo` to `foo` and `foo` to `meta->'foo'` etc.
     * @param Type
     * @param key {string} The NoPg keyword
     * @param opts
     */
    static parse_predicate_key (Type, opts, key) {

        opts = opts || {};

        AssertUtils.isString(key);

        if (key[0] !== '$') {
            let datakey = get_predicate_datakey(Type);
            //return new Predicate( "json_extract_path("+datakey+", '"+JSON.stringify([key])+"'::json->>0)::text", [], {'datakey': datakey, 'key': key});
            return new Predicate( parse_keyref_json(datakey, key), [], {'datakey': datakey, 'key': key});
        }

        let _key = key.substr(1);

        if ( (opts.epoch === true) && ( (key === '$created') || (key === '$modified') ) ) {
            //return new Predicate("to_json(extract(epoch from "+_key+")*1000)", [], {'key':_key});
            return new Predicate("extract(epoch from "+_key+")*1000", [], {'key':_key});
        }

        if (key === '$documents') {
            let traits = (opts && opts.traits) || {};
            let documents = (traits && traits.documents) || [];
            AssertUtils.isArray(documents);
            return new Predicate("get_documents(row_to_json("+(Type.meta.table)+".*), $::json)", [
                JSON.stringify(parse_predicate_document_relations(Type, documents, traits))
            ], {'key':_key});
        }

        return new Predicate(_key, [], {'key':_key});

    }

    /** @FIXME Implement escape? */
    static is_valid_key(key) {
        let keyreg = /^[a-zA-Z0-9_\-\.]+$/;
        return keyreg.test(key);
    }

    /** */
    static parse_meta_properties(res, opts, datakey, key) {
        if (!is_valid_key(key)) { throw new TypeError("Invalid keyword: " + key); }
        let keyref = parse_keyref_text(datakey, key);
        // FIXME: This should use same code as indexes?
        if (is.boolean(opts[key])) {
            res["(("+keyref+")::boolean IS TRUE)"] = (opts[key] === true) ? 'true' : 'false';
        } else if (is.number(opts[key])) {
            res["("+keyref+")::numeric"] = opts[key];
        } else {
            res[keyref] = ''+opts[key];
        }
    }

    /** */
    static parse_top_level_properties(res, opts, key) {
        let k = key.substr(1);
        res[k] = opts[key];
    }

    /** Convert properties like {"$foo":123} -> "foo = 123" and {foo:123} -> "(meta->'foo')::numeric = 123" and {foo:"123"} -> "meta->'foo' = '123'"
     * Usage: `let where = parse_predicates(NoPg.Document)({"$foo":123})`
     */
    static parse_predicates(Type) {

        //function first_letter_not_dollar(k) {
        //	return k[0] !== '$';
        //}

        function parse_data(opts) {
            AssertUtils.isObject(opts).ignore(undefined);

            opts = opts || {};
            let datakey = get_predicate_datakey(Type);
            let res = {};
            ARRAY(Object.keys(opts)).forEach(function(i) {
                if (first_letter_is_dollar(i)) {
                    // Parse top level properties
                    parse_top_level_properties(res, opts, i);
                } else {
                    // Parse meta properties
                    parse_meta_properties(res, opts, datakey, i);
                }
            });
            return res;
        }

        return parse_data;
    }

    /* Returns the type condition and pushes new params to `params` */
    static parse_where_type_condition_array(query, type) {
        let predicates = ARRAY(type).map(function(t) {
            if (_.isString(t)) {
                return new Predicate("type = $", t);
            }
            if (type instanceof NoPg.Type) {
                return new Predicate("types_id = $", type.$id);
            }
            throw new TypeError("Unknown type: " + LogUtils.getAsString(t));
        }).valueOf();
        query.where( Predicate.join(predicates, 'OR') );
    }

    /* Returns the type condition and pushes new params to `params` */
    static parse_where_type_condition(query, type) {
        if (type === undefined) {
            return;
        }

        if (_.isArray(type)) {
            return parse_where_type_condition_array(query, type);
        }

        if (_.isString(type)) {
            // NOTE: We need to use `get_type_id()` until we fix the possibility that some
            // older rows do not have correct `type` field -- these are rows that were created
            // before their current validation schema and do not pass it.
            //query.where( new Predicate("types_id = get_type_id($)", type) );

            // schema v18 should have fixed type's for all documents
            query.where( new Predicate("type = $", type) );

            return;
        }

        if (type instanceof NoPg.Type) {
            query.where( new Predicate("types_id = $", type.$id) );
            return;
        }

        throw new TypeError("Unknown type: " + LogUtils.getAsString(type));
    }

    /** Returns true if `i` is not `undefined` */
    static not_undefined(i) {
        return i !== undefined;
    }

    /** */
    static replace_last(x, from, to) {
        let i = x.lastIndexOf(from);
        if (i === -1) {
            return x;
        }
        x = x.substr(0, i) + to + x.substr(i+from.length);
        return x;
    }

    /** Returns the correct cast from JSON to PostgreSQL type */
    static parse_predicate_pgcast_by_type(pgtype) {
        //nrLog.debug('pgtype = ', pgtype);
        if (_.isFunction(PG_CASTS[pgtype])) {
            return PG_CASTS[pgtype];
        }
        return function pgcast_default(x) { return '' + x + '::' + pgtype; };
    }

    /** Returns PostgreSQL type for key based on the schema
     * @FIXME Detect correct types for all keys
     */
    static parse_predicate_pgtype(ObjType, document_type, key) {

        AssertUtils.isFunction(ObjType);
        AssertUtils.isObject(document_type).ignore(undefined);

        let schema = (document_type && document_type.$schema) || {};
        AssertUtils.isObject(schema);

        if (key[0] === '$') {

            // FIXME: Change this to do direct for everything else but JSON types!

            if (key === '$version') { return 'direct'; }
            if (key === '$created') { return 'direct'; }
            if (key === '$modified') { return 'direct'; }
            if (key === '$name') { return 'direct'; }
            if (key === '$type') { return 'direct'; }
            if (key === '$validator') { return 'direct'; }
            if (key === '$id') { return 'direct'; }
            if (key === '$types_id') { return 'direct'; }
            if (key === '$documents_id') { return 'direct'; }

        } else {

            let type;
            if (schema && schema.properties && schema.properties.hasOwnProperty(key) && schema.properties[key].type) {
                type = schema.properties[key].type;
            }

            if (type === 'number') {
                return 'numeric';
            }

            if (type === 'boolean') {
                return 'boolean';
            }

        }

        return 'text';
    }

    /** Returns the correct cast from JSON to PostgreSQL type */
    static parse_predicate_pgcast(ObjType, document_type, key) {
        let pgtype = parse_predicate_pgtype(ObjType, document_type, key);
        return parse_predicate_pgcast_by_type(pgtype);
    }

    /** Parse array predicate */
    static parse_function_predicate(ObjType, q, def_op, o, ret_type, traits) {
        AssertUtils.isArray(o);

        ret_type = ret_type || 'boolean';

        let func = ARRAY(o).find(is.func);

        AssertUtils.isFunction(func);

        let i = o.indexOf(func);
        AssertUtils.isNumber(i);

        let input_nopg_keys = o.slice(0, i);
        let js_input_params = o.slice(i+1);

        AssertUtils.isArray(input_nopg_keys);
        AssertUtils.isArray(js_input_params);

        //nrLog.debug('input_nopg_keys = ', input_nopg_keys);
        //nrLog.debug('func = ', func);
        //nrLog.debug('js_input_params = ', js_input_params);

        let _parse_predicate_key_epoch = FUNCTION(NoPgUtils.parse_predicate_key).curry(ObjType, {'traits': traits, 'epoch':true});
        let input_pg_keys = ARRAY(input_nopg_keys).map(_parse_predicate_key_epoch);

        let pg_items = input_pg_keys.map(function(i) { return i.getString(); }).valueOf();
        let pg_params = input_pg_keys.map(function(i) { return i.getParams(); }).reduce(function(a, b) { return a.concat(b); });

        AssertUtils.isArray(pg_items);
        AssertUtils.isArray(pg_params);

        //nrLog.debug('input_pg_keys = ', input_pg_keys);

        //let n = arg_params.length;
        //arg_params.push(JSON.stringify(FUNCTION(func).stringify()));
        //arg_params.push(JSON.stringify(js_input_params));

        let call_func = 'nopg.call_func(array_to_json(ARRAY['+pg_items.join(', ')+"]), $::json, $::json)";

        let type_cast = parse_predicate_pgcast_by_type(ret_type);

        return new Predicate(type_cast(call_func), pg_params.concat( [JSON.stringify(FUNCTION(func).stringify()), JSON.stringify(js_input_params)] ));
    }

    /** Returns true if op is AND, OR or BIND */
    static parse_operator_name(op) {
        op = ''+op;
        op = op.split(':')[0];
        return op;
    }

    /** Returns true if op is AND, OR or BIND */
    static parse_operator_type(op, def) {
        op = ''+op;
        if (op.indexOf(':') === -1) {
            return def || 'boolean';
        }
        return op.split(':')[1];
    }

    /** Returns true if op is AND, OR or BIND */
    static is_operator(op) {
        op = parse_operator_name(op);
        return (op === 'AND') || (op === 'OR') || (op === 'BIND');
    }

    /** Returns true if array has values without leading $ */
    static has_property_names(a) {
        return ARRAY(a).some(function(k) {
            return k && (k[0] !== '$');
        });
    }

    /** Parse traits object */
    static parse_search_traits(traits) {
        traits = traits || {};

        // Initialize fields as all fields
        if (!traits.fields) {
            traits.fields = ['$*'];
        }

        // If fields was not an array (but is not negative -- check previous if clause), lets make it that.
        if (!_.isArray(traits.fields)) {
            traits.fields = [traits.fields];
        }

        AssertUtils.isArray(traits.fields);

        // Order by $created by default
        if (!traits.order) {
            // FIXME: Check if `$created` exists in the ObjType!
            traits.order = ['$created'];
        }

        // Enable group by
        if (traits.hasOwnProperty('group')) {
            traits.group = [].concat(traits.group);
        }

        if (!_.isArray(traits.order)) {
            traits.order = [traits.order];
        }

        AssertUtils.isArray(traits.order);

        if (traits.limit) {
            if (!traits.order) {
                nrLog.warn('Limit without ordering will yeald unpredictable results!');
            }

            if ((''+traits.limit).toLowerCase() === 'all') {
                traits.limit = 'ALL';
            } else {
                traits.limit = '' + parseInt(traits.limit, 10);
            }
        }

        if (traits.offset) {
            traits.offset = parseInt(traits.offset, 10);
        }

        if (traits.hasOwnProperty('prepareOnly')) {
            traits.prepareOnly = traits.prepareOnly === true;
        }

        if (traits.hasOwnProperty('typeAwareness')) {
            traits.typeAwareness = traits.typeAwareness === true;
        } else {
            traits.typeAwareness = NoPg.defaults.enableTypeAwareness === true;
        }

        // Append '$documents' to fields if traits.documents is specified and it is missing from there
        if ((traits.documents || traits.typeAwareness) && (traits.fields.indexOf('$documents') === -1) ) {
            traits.fields = traits.fields.concat(['$documents']);
        }

        if (traits.hasOwnProperty('count')) {
            traits.count = traits.count === true;
            traits.fields = ['count'];
            delete traits.order;
        }

        return traits;
    }

    /** Parses internal fields from nopg style fields
     *
     */
    static parse_select_fields(ObjType, traits) {
        AssertUtils.isFunction(ObjType);
        AssertUtils.isObject(traits).ignore(undefined);
        return ARRAY(traits.fields).map(function(f) {
            return NoPgUtils.parse_predicate_key(ObjType, {'traits': traits, 'epoch':false}, f);
        }).valueOf();
    }

    /** Parse opts object */
    static parse_search_opts(opts, traits) {

        if (opts === undefined) {
            return;
        }

        if (_.isArray(opts)) {
            if ( (opts.length >= 1) && NoPgUtils.isObjectNotArray(opts[0]) ) {
                return [ ((traits.match === 'any') ? 'OR' : 'AND') ].concat(opts);
            }
            return opts;
        }

        if (opts instanceof NoPg.Type) {
            return [ "AND", { "$id": opts.$id } ];
        }

        if (NoPgUtils.isObjectNotArray(opts)) {
            return [ ((traits.match === 'any') ? 'OR' : 'AND') , opts];
        }

        return [ "AND", {"$name": ''+opts} ];
    }

    /** Generate ORDER BY using `traits.order` */
    static parse_select_order(ObjType, document_type, order, q, traits) {

        AssertUtils.isFunction(ObjType);
        AssertUtils.isObject(document_type).ignore(undefined);
        AssertUtils.isArray(order);

        return ARRAY(order).map(function(o) {
            let key, type, rest;
            if (_.isArray(o)) {
                key = parse_operator_name(o[0]);
                type = parse_operator_type(o[0], 'text');
                rest = o.slice(1);
            } else {
                key = parse_operator_name(o);
                type = parse_operator_type(o, 'text');
                rest = [];
            }

            if (key === 'BIND') {
                return this.parse_function_predicate(ObjType, q, undefined, rest, type, traits);
            }

            //nrLog.debug('key = ', key);
            let parsed_key = NoPgUtils.parse_predicate_key(ObjType, {'traits': traits, 'epoch':true}, key);
            //nrLog.debug('parsed_key = ', parsed_key);
            let pgcast = parse_predicate_pgcast(ObjType, document_type, key);
            //nrLog.debug('pgcast = ', pgcast);

            return new Predicate( [pgcast(parsed_key.getString())].concat(rest).join(' '), parsed_key.getParams(), parsed_key.getMetaObject() );
        }).valueOf();
    }

    /** Compare two variables as JSON strings */
    static json_cmp(a, b) {
        a = JSON.stringify(a);
        b = JSON.stringify(b);
        let ret = (a === b) ? true : false;
        return ret;
    }


    /**
     * Returns `true` if PostgreSQL database table exists.
     * @todo Implement this in nor-pg and use here.
     */
    static pg_table_exists(self, name) {
        return do_query(self, 'SELECT * FROM information_schema.tables WHERE table_name = $1 LIMIT 1', [name]).then(function(rows) {
            if (!rows) { throw new TypeError("Unexpected result from query: " + LogUtils.getAsString(rows)); }
            return rows.length !== 0;
        });
    }

    /** Convert special characters in field name to "_" for index naming
     * @param field
     * @return {string}
     */
    static pg_convert_index_name(field) {
        return field.toLowerCase().replace(/[^a-z0-9]+/g, "_");
    }

    /** Returns index name
     *
     * @param ObjType
     * @param type
     * @param field
     * @param typefield
     * @return {string}
     */
    static pg_create_index_name (ObjType, type, field, typefield) {

        const colName = NoPgUtils.parse_predicate_key(ObjType, {'epoch':false}, field);

        const dataKey = colName.getMeta('dataKey');

        const field_name = (dataKey ? dataKey + '.' : '' ) + colName.getMeta('key');

        let name;

        if ( (ObjType === NoPg.Document) && (typefield !== undefined)) {

            if (!typefield) {
                throw new TypeError("No typefield set for NoPg.Document!");
            }

            name = NoPgUtils.pg_convert_index_name(ObjType.meta.table) + "_" + typefield + "_" + NoPgUtils.pg_convert_index_name(field_name) + "_index";

        } else {

            name = NoPgUtils.pg_convert_index_name(ObjType.meta.table) + "_" + NoPgUtils.pg_convert_index_name(field_name) + "_index";

        }

        return name;

    }

    /** Returns index query
     * @param self
     * @param ObjType
     * @param type
     * @param field
     * @param typefield
     * @param is_unique
     * @return {string}
     */
    static pg_create_index_query_internal_v1 (ObjType, type, field, typefield, is_unique) {

        let query;

        let pgcast = NoPgUtils.parse_predicate_pgcast(ObjType, type, field);

        let colname = NoPgUtils.parse_predicate_key(ObjType, {'epoch':false}, field);

        let name = NoPgUtils.pg_create_index_name(ObjType, type, field, typefield);

        query = "CREATE " + (is_unique?'UNIQUE ':'') + "INDEX "+name+" ON " + (ObjType.meta.table) + " USING btree ";

        if ( (ObjType === NoPg.Document) && (typefield !== undefined)) {

            if (!typefield) {
                throw new TypeError("No typefield set for NoPg.Document!");
            }

            query += "(" + typefield+", "+ NoPgUtils.wrap_casts(pgcast(colname.getString())) + ")";

        } else {

            query += "(" + NoPgUtils.wrap_casts(pgcast(colname.getString())) + ")";

        }

        return query;

    }

    /** Wrap parenthesis around casts
     * @param x
     * @return {*}
     */
    static wrap_casts(x) {
        x = '' + x;
        if (/^\(.+\)$/.test(x)) {
            return '(' + x + ')';
        }
        if (/::[a-z]+$/.test(x)) {
            if (/^[a-z]+ \->> /.test(x)) {
                return '((' + x + '))';
            }
            return '(' + x + ')';
        }
        return x;
    }

    /** Returns index query
     *
     * @param ObjType
     * @param type
     * @param field
     * @param typefield
     * @param is_unique
     * @return {string | *}
     */
    static pg_create_index_query_internal_v2 (ObjType, type, field, typefield, is_unique) {

        let query;

        let pgcast = NoPgUtils.parse_predicate_pgcast(ObjType, type, field);

        let colname = NoPgUtils.parse_predicate_key(ObjType, {'epoch':false}, field);

        let name = NoPgUtils.pg_create_index_name( ObjType, type, field, typefield);

        query = "CREATE " + (is_unique?'UNIQUE ':'') + "INDEX "+name+" ON public." + (ObjType.meta.table) + " USING btree ";

        if ( (ObjType === NoPg.Document) && (typefield !== undefined)) {

            if (!typefield) {
                throw new TypeError("No typefield set for NoPg.Document!");
            }

            query += "(" + typefield+", "+ NoPgUtils.wrap_casts(pgcast(colname.getString())) + ")";

        } else {

            query += "(" + NoPgUtils.wrap_casts(pgcast(colname.getString())) + ")";

        }

        return query;

    }

    /** Returns the create index query as string and throws an error if any parameters exists
     *
     * @param ObjType
     * @param type
     * @param field
     * @param typefield
     * @param is_unique
     * @return {string | *}
     */
    static pg_create_index_query_v1 (ObjType, type, field, typefield, is_unique) {

        let colname = NoPgUtils.parse_predicate_key(ObjType, {'epoch':false}, field);

        let query = NoPgUtils.pg_create_index_query_internal_v1(ObjType, type, field, typefield, is_unique);

        let params = colname.getParams();

        if (params.length !== 0) {
            throw new TypeError("pg_create_index_query_v1() does not support params!");
        }

        return query;

    }

    /** Returns the create index query as string and throws an error if any parameters exists
     *
     * @param ObjType
     * @param type
     * @param field
     * @param typefield
     * @param is_unique
     * @return {string | *}
     */
    static pg_create_index_query_v2 (ObjType, type, field, typefield, is_unique) {
        let colname = NoPgUtils.parse_predicate_key(ObjType, {'epoch':false}, field);
        let query = NoPgUtils.pg_create_index_query_internal_v2(ObjType, type, field, typefield, is_unique);
        let params = colname.getParams();
        if (params.length !== 0) {
            throw new TypeError("pg_create_index_query_v2() does not support params!");
        }
        return query;
    }

    /** Run query on the PostgreSQL server
     * @param query
     * @param params
     * @return {Function}
     */
    static pg_query(query, params) {
        return function(db) {
            let start_time = new Date();
            return do_query(db, query, params).then(function() {

                let end_time = new Date();

                db._record_sample({
                    'event': 'query',
                    'start': start_time,
                    'end': end_time,
                    'query': query,
                    'params': params
                });

                return db;
            });
        };
    }

    /** Create watchdog timer
     * @param db
     * @param opts
     * @return {{}}
     */
    static create_watchdog(db, opts) {
        AssertUtils.isObject(db);

        opts = opts || {};

        AssertUtils.isObject(opts);

        opts.timeout = opts.timeout || 30000;
        AssertUtils.isNumber(opts.timeout);

        let w = {};
        w.db = db;
        w.opts = opts;

        /* Setup */

        w.timeout = setTimeout(function() {
            nrLog.warn('Got timeout.');
            w.timeout = undefined;
            Q.fcall(function() {
                let tr_open, tr_commit, tr_rollback, state, tr_unknown, tr_disconnect;

                // NoPg instance
                if (w.db === undefined) {
                    nrLog.warn("Timeout exceeded and database instance undefined. Nothing done.");
                    return;
                }

                if (!(w.db && w.db._tr_state)) {
                    nrLog.warn("Timeout exceeded but db was not NoPg instance.");
                    return;
                }

                state = w.db._tr_state;
                tr_open = (state === 'open') ? true : false;
                tr_commit = (state === 'commit') ? true : false;
                tr_rollback = (state === 'rollback') ? true : false;
                tr_disconnect = (state === 'disconnect') ? true : false;
                tr_unknown = ((!tr_open) && (!tr_commit) && (!tr_rollback) && (!tr_disconnect)) ? true : false;

                if (tr_unknown) {
                    nrLog.warn("Timeout exceeded and transaction state was unknown ("+state+"). Nothing done.");
                    return;
                }

                if (tr_open) {
                    nrLog.warn("Timeout exceeded and transaction still open. Closing it by rollback.");
                    return w.db.rollback().fail(function(err) {
                        nrLog.error("Rollback failed: " + (err.stack || err) );
                    });
                }

                if (tr_disconnect) {
                    //nrLog.debug('...but .disconnect() was already done.');
                    return;
                }

                if (tr_commit) {
                    //nrLog.debug('...but .commit() was already done.');
                    return;
                }

                if (tr_rollback) {
                    //nrLog.debug('...but .rollback() was already done.');
                    return;
                }

            }).fin(function() {
                if (w && w.db) {
                    w.db._events.emit('timeout');
                }
            }).done();
        }, opts.timeout);

        /* Set object */
        w.reset = function(o) {
            AssertUtils.isObject(o);
            //nrLog.debug('Resetting the watchdog.');
            w.db = o;
        };

        /** Clear the timeout */
        w.clear = function() {
            if (w.timeout) {
                //nrLog.debug('Clearing the watchdog.');
                clearTimeout(w.timeout);
                w.timeout = undefined;
            }
        };

        return w;
    }

    /** Returns a number padded to specific width
     * @param num
     * @param size
     * @return {string}
     */
    static pad(num, size) {
        let s = num+"";
        while (s.length < size) {
            s = "0" + s;
        }
        return s;
    }

    /** Runs `require(file)` and push results to `builders` array
     * @param builders
     * @param file
     */
    static push_file (builders, file) {
        FUNCTION(builders.push).apply(builders, require(file) );
    }

    /** Returns the latest database server version
     * @param self
     * @return {Promise}
     * @private
     */
    static _latestDBVersion(self) {
        let table = NoPg.DBVersion.meta.table;
        return pg_table_exists(self, table).then(function(exists) {
            if (!exists) {
                return -1;
            }
            let query = 'SELECT COALESCE(MAX(version), 0) AS version FROM ' + table;
            return do_query(self, query).then(function(rows) {
                if (!(rows instanceof Array)) { throw new TypeError("Unexpected result from rows: " + LogUtils.getAsString(rows) ); }
                let obj = rows.shift();
                return parseInt(obj.version, 10);
            });
        }).then(function(db_version) {
            if (db_version < -1 ) {
                throw new TypeError("Database version " + db_version + " is not between accepted range (-1 ..)");
            }
            return db_version;
        });
    }


    /** Returns a listener for notifications from TCN extension
     * @param events {EventEmitter} The event emitter where we should trigger matching events.
     * @param when {object} We should only trigger events that match this specification. Object with optional properties
     *     `type`, `id` and `name`.
     */
    static create_tcn_listener (events, when) {

        AssertUtils.isObject(events);
        AssertUtils.isObject(when);

        // Normalize event object back to event name
        let when_str = NoPg.stringifyEventName(when);

        return function tcn_listener(payload) {

            payload = NoPg.parseTCNPayload(payload);

            let event = TCN_EVENT_MAPPING[''+payload.table+','+payload.op];

            if (!event) {
                nrLog.warn('Could not find event name for payload: ', payload);
                return;
            }

            // Verify we don't emit, if matching id enabled and does not match
            if ( when.hasOwnProperty('id') && (payload.keys.id !== when.id) ) {
                return;
            }

            // Verify we don't emit, if matching event name enabled and does not match
            if ( when.hasOwnProperty('name') && (event !== when.name) ) {
                return;
            }

            events.emit(when_str, payload.keys.id, event, when.type);

        };
    }

    /**
     *
     * @param value {string}
     * @returns {boolean}
     */
    static isUuid (value) {
        return _.isString(value) && /^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$/.test(value);
    }

    /**
     *
     * @param value
     * @returns {value is object | boolean}
     */
    static isObjectNotArray (value) {

        return _.isObject(value) && !_.isArray(value);

    }

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
    static parse_predicate_document_relations (ObjType, documents, traits) {
        return ARRAY(documents).map(function(d) {

            let parts = d.split('|');
            let expression = parts.shift();
            let fields = parts.join('|') || '*';

            let prop, type_name;
            let external_index = expression.indexOf('{');
            let type_index = expression.indexOf('#');
            if ( (type_index >= 0) && ( (external_index < 0) || (type_index < external_index) ) ) {
                type_name = expression.substr(0, type_index);
                prop = expression.substr(type_index+1);
            } else {
                prop = expression;
            }

            fields = ARRAY(fields.split(',')).map(function(f) {
                if (f === '*') { return {'query':'*'}; }
                let p = NoPgUtils.parse_predicate_key(ObjType, {'traits': traits, 'epoch':false}, f);
                return {
                    'name': f,
                    'datakey': p.getMeta('datakey'),
                    'key': p.getMeta('key'),
                    'query': p.getString()
                };
            }).valueOf();

            //nrLog.debug('fields = ', JSON.stringify(fields, null, 2) );

            if (prop && (prop.length >= 1) && (prop[0] === '$')) {
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
    }

    /**
     * Workaround until code is refactored not to use this function
     *
     * @param spec {*}
     * @param value {Promise}
     * @returns {*}
     */
    static extendPromise (spec, value) {
        return value;
    }

    /** Returns the keyword name without first letter */
    static parse_keyword_name (key) {
        return key.substr(1);
    }

}

// noinspection JSUnusedGlobalSymbols
export default NoPgUtils;
