/* nor-nopg -- Implementation of meta objects for `NoPg.Document`, `NoPg.Type`, `NoPg.Attachment` and `NoPg.Lib`. */

"use strict";

import _ from 'lodash';
import is from '@norjs/is';
import debug from '@norjs/debug';
import ARRAY from 'nor-array';
import FUNCTION from 'nor-function';

const copy = obj => _.cloneDeep(obj);

function meta(opts) {
	opts = opts || {};

	if(!opts.parsers) { opts.parsers = {}; }

	function builder(self) {
		debug.assert(self).is('object');

		//debug.log("meta::builder(self=", self, ")");
		var obj = {};

		/** Set meta keys */
		obj.set_meta_keys = function(data) {
			//debug.log("meta.set_meta_keys(data=", data, ")");

			//debug.log("builder.datakey = ", builder.datakey);
			//debug.log("self = ", self);

			// Search initial meta keys
			ARRAY(builder.keys).forEach(function(key) {
				if(data[key] === undefined) {
					return;
				}
				if(opts.parsers[key] === 'function') {
					if(data[key]) {
						self[key] = FUNCTION.parse(data[key]);
					}
				} else {
					self[key] = copy(data[key]);
				}
			});

			// Move normal keys to default JSON object ($content or $meta depending of type)

			debug.assert(builder.datakey).is('string');

			if(!is.obj(self[builder.datakey])) {
				self[builder.datakey] = {};
			}

			debug.assert(self[builder.datakey]).is('object');

			//debug.log('First: self[builder.datakey] = ', self[builder.datakey]);

			ARRAY(Object.keys(data)).filter(function(key) {
				return key[0] !== '$';
			}).forEach(function(key) {
				//debug.log('key = ', key);
				//debug.log('self = ', self);
				//debug.log('builder.datakey = ', builder.datakey);
				//debug.log('self[builder.datakey] = ', self[builder.datakey]);
				self[builder.datakey][key] = copy(data[key]);
				delete self[key];
			});

			//debug.log("object after set_meta_keys(", data, ") is: ", self);
			return obj;
		};

		/** Resolve single object key into top level */
		obj.resolve = function(datakey) {
			//debug.log("meta.resolve(datakey=", datakey, ")");
			datakey = datakey || builder.datakey;
			//debug.log("datakey = ", datakey);
			//debug.log("self = ", self);

			if(self[datakey]) {
				ARRAY(Object.keys(self[datakey])).forEach(function(key) {
					self[key] = copy(self[datakey][key]);
				});
				//debug.log("object after resolve(", datakey, ") is: ", self);
			}
			return obj;
		};

		/** Unresolve object back into internal database data */
		obj.unresolve = function(datakey) {
			//debug.log("meta.unresolve(datakey=", datakey, ")");
			datakey = (datakey || builder.datakey) .substr(1);

			//debug.log("datakey = ", datakey);
			//debug.log("self = ", self);

			var data = {};

			// FIXME: These test functions could be in internal shared helper module with nopg.js

			// Copy table columns
			ARRAY(builder.keys).filter(function(key) {
				return key[0] === '$';
			}).map(function(key) {
				return key.substr(1);
			}).forEach(function(key) {
				if(self['$'+key] === undefined) {
					return;
				}

				//if(self['$'+key] instanceof Function) {
				//	self['$'+key] = pghelpers.escapeFunction(self['$'+key]);
				//}

				if(self['$'+key] instanceof Function) {
					self['$'+key] = FUNCTION(self['$'+key]).stringify();
				}

				data[key] = copy(self['$'+key]);
			});

			// Copy plain data
			ARRAY(Object.keys(self)).filter(function(key) {
				return key[0] !== '$';
			}).forEach(function(key) {
				if(!data[datakey]) {
					data[datakey] = {};
				}
				data[datakey][key] = copy(self[key]);
			});

			//debug.log("data after unresolve: ", data);
			return data;
		};

		/** Unresolve to data presentation */
		obj.toData = function() {
			var data = {};

			// Copy table columns
			ARRAY(builder.keys).filter(function(key) {
				return key[0] === '$';
			}).map(function(key) {
				return key.substr(1);
			}).forEach(function(key) {
				if(self['$'+key] === undefined) {
					return;
				}

				if(is.func(self['$'+key])) {
					self['$'+key] = FUNCTION(self['$'+key]).stringify();
				}

				data['$'+key] = copy(self['$'+key]);
			});

			// Copy plain data
			ARRAY(Object.keys(self)).filter(function(key) {
				return key[0] !== '$';
			}).forEach(function(key) {
				if(is.func(self[key])) {
					data[key] = self[key].call(self);
				} else {
					data[key] = copy(self[key]);
				}
			});

			//debug.log("data after toData: ", data);
			return data;
		};

		return obj;
	}

	/** Internal meta values */

	builder.keys = opts.keys || [];
	builder.datakey = opts.datakey || '$meta';
	builder.table = opts.table;

	return builder;
}

module.exports = meta;

/* EOF */
