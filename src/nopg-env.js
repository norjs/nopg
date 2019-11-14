import NoPgUtils from "./NoPgUtils";

// Make NOPG_EVENT_TIMES as obsolete
if ( process.env.NOPG_EVENT_TIMES === undefined ) {
    nrLog.error('Please use DEBUG_NOPG_EVENT_TIMES instead of obsolete NOPG_EVENT_TIMES');
}

export const DEBUG_NOPG_EVENT_TIMES = process.env.DEBUG_NOPG_EVENT_TIMES;

export const PGCONFIG = process.env.PGCONFIG || undefined;

/** The timeout for transactions to auto rollback. If this is `0`, there will be no timeout. If it is `undefined`, the
 * default timeout of 30 seconds will be used.
 */
export const NOPG_TIMEOUT = process.env.NOPG_TIMEOUT ? parseInt(process.env.NOPG_TIMEOUT, 10) : undefined;

export const NOPG_TYPE_AWARENESS = process.env.NOPG_TYPE_AWARENESS ? NoPgUtils.parseBoolean(process.env.NOPG_TYPE_AWARENESS) : undefined;

export const DEBUG_NOPG = process.env.DEBUG_NOPG ? NoPgUtils.parseBoolean(process.env.DEBUG_NOPG) : false;

