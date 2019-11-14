
/** @typedef {Object} NoPgDefaultsObject
 * @property {string} [pgconfig] -
 * @property {number} [timeout] - The default timeout for transactions to automatically rollback. If this is `undefined`, there will be no timeout.
 *                                 Defaults to 30 seconds.
 * @property {boolean} [enableTypeAwareness] - If enabled NoPg will be more aware of the properties of types.
 *                                            When a user provides a type as a string, it will be converted as
 *                                            a type object. This will enable additional features like optional
 *                                            `traits.documents` support as a predefined in type.
 */
