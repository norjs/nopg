import { NOPG_TIMEOUT, NOPG_TYPE_AWARENESS, PGCONFIG } from "./nopg-env";

/**
 * Maps `<table>,<I|U|D>` into NoPg event name
 *
 * @type {Object<string, string>}
 */
export const TCN_EVENT_MAPPING = {
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

/** Each tcn event name
 *
 * @type {array.<string>}
 */
export const TCN_EVENT_NAMES = Object.keys(TCN_EVENT_MAPPING).map(key => TCN_EVENT_MAPPING[key]);

/** Internal event names used by local NoPg connection
 *
 * @type {array.<string>}
 */
export const LOCAL_EVENT_NAMES = [
    'timeout',
    'commit',
    'rollback',
    'disconnect'
];

/** Defaults
 * @type {NoPgDefaultsObject}
 */
export const NOPG_DEFAULTS = {

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
