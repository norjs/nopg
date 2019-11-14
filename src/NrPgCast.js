import NoPgUtils from "./NoPgUtils";

/**
 * PostgreSQL cast functions
 */
export class NrPgCast {

    /**
     *
     * @param x {string}
     * @returns {string}
     */
    static castDirect (x) {
        return '' + x;
    }

    /**
     *
     * @param x {string}
     * @returns {string}
     */
    static castBoolean (x) {
        return '(((' + x + ')::text)::boolean IS TRUE)';
    }

    /**
     *
     * @param x {string}
     * @returns {string}
     */
    static castNumeric (x) {
        return '((' + x + ')::text)::numeric';
    }

    /**
     *
     * @param x {string}
     * @returns {string|string|*}
     */
    static castText (x) {

        if (x.indexOf(' -> ') !== -1) {
            return NoPgUtils.replace_last(x, ' -> ', ' ->> ');
        }

        if (x.indexOf(' #> ') !== -1) {
            return NoPgUtils.replace_last(x, ' #> ', ' #>> ');
        }

        //throw new TypeError('Cannot cast expression to text: ' + x);

        if (x.substr(0 - '::text'.length) === '::text') {
            return x;
        }

        if (x.substr(0 - '::text)'.length) === '::text)') {
            return x;
        }

        return '' + x + '::text';

    }

}

export default NrPgCast;
