import NOPG from '../nopg.js';
import ProcessUtils from "@norjs/utils/Process";
import LogUtils from "@norjs/utils/Log";

const ARGV = ProcessUtils.getArguments();
const OPTIONS = ProcessUtils.filterOptions(ARGV);
const FREE_ARGUMENTS = ProcessUtils.filterFreeArguments(ARGV);
const ARGUMENTS = ProcessUtils.parseOptionsAsObject(OPTIONS);

// Set default timeout for these operations as 1 hour unless it is already longer
//if (NOPG.defaults.timeout < 3600000) {
//	NOPG.defaults.timeout = 3600000;
//}

/// Disable timeout
NOPG.defaults.timeout = 0;

let PGCONFIG = ARGUMENTS.pg || process.env.PGCONFIG || 'postgres://localhost:5432/test';

if (ARGUMENTS.v) {
	// Q.longStackSupport = true;
	// debug.setNodeENV('development');
} else {
	// debug.setNodeENV('production');
}

//nrLog.trace('PGCONFIG = ', PGCONFIG);

let actions = {

	help  : async () => await NoPgCommandActions.help(),

	init  : async () => await NoPgCommandActions.init(),

	types : async () => await NoPgCommandActions.types(),

	test  : async () => await NoPgCommandActions.test()

};

class NoPgCommandActions {

	/** Output usage information */
	static async help () {

		//nrLog.trace("Executing");
		console.log("USAGE: nopg [--pg='psql://localhost:5432/test'] help|test|init");
		console.log('where:');
		console.log('  help      -- print this help');
		console.log('  test      -- test server features');
		console.log('  init      -- initialize database');
		console.log('  types     -- list types');
		console.log('  documents -- list documents');

	}

	/** Initialize database */
	static async init () {

		//nrLog.trace("actions.init(): Executing");

		const db = await NOPG.start(PGCONFIG);

		try {

			await db.init();

			await db.commit();

			console.log('init: Successfully initialized database');

		} catch (err) {

			await db.rollback();

			throw err;
		}

	}

	/** List types */
	static async types () {

		//nrLog.trace("Executing");
		let keys = ['_', 'v', '$0'];
		let opts = {};

		_.forEach(
			Object.keys(ARGUMENTS).filter(k => keys.indexOf(k) === -1),
			key => {
				if  (key[0] === '-') {
					opts[ '$' + key.substr(1) ] = ARGUMENTS[key];
				} else {
					opts[key] = ARGUMENTS[key];
				}
			}
		);

		if (Object.keys(opts).length === 0) {
			opts = undefined;
		}

		//nrLog.trace('opts = ', opts);
		const db = await NOPG.start(PGCONFIG);
		try {

			await db.searchTypes(opts);

			await db.commit();

		} catch (err) {

			await db.rollback();

			throw err;

		}

		let types = db.fetch();

		let table = _.map(types, type => [type.$id, type.$name]);

		console.log( this._getMarkdownTable([ "$id", "$name" ], table) );

	}

	/** Test server features */
	static async test () {

		//nrLog.trace("Executing");
		const db = NOPG.start(PGCONFIG);

		try {

			await db.test();

			await db.commit();

		} catch (err) {

			await db.rollback();

			throw err;

		}

		console.log("test: OK");

	}

	/** Returns markdown formated table
	 *
	 * @param headers
	 * @param table
	 * @returns {string}
	 */
	static _getMarkdownTable (headers, table) {

		// FIXME: Implement better markdown table formating
		return _.concat([headers, [ "---", "---" ]], table).map(cols => '| ' + _.join(cols, ' | ') + ' |').join('\n');

	}

}

export class NoPgCommand {

	/* Do actions */
	static async main () {

		/* Test arguments */
		if  (FREE_ARGUMENTS.length === 0) {
			return await actions.help();
		}

		await _.reduce(
			_.map(ARGUMENTS, action => {

				if ( actions[action] === undefined ) {
					throw new Error(`Unknown action: ${action}`);
				}

				//nrLog.trace("Scheduled action for ", action);

				return async () => {

					try {

						await actions[action]();

					} catch (err) {

						if (ARGUMENTS.v) {
							throw err;
						}

						throw new TypeError(`Failed action "${LogUtils.getAsString(action)}": ${LogUtils.getAsString(err)}`);

					}

				};

			}),

			( a, f ) => a.then(f),

			Promise.resolve(undefined)
		);

		// FIXME: Implement automatic shutdown, now pg still listens.
		//nrLog.trace("Returning with exit status 0");
		process.exit(0);

	}

}

NoPgCommand.main().catch(err => {

	console.error(''+err);

	if  (ARGUMENTS.v) {
		nrLog.error(err);
	}

	process.exit(1);

});
