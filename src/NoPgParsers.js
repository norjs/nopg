import Predicate from "./Predicate";
import _ from "lodash";
import NoPgUtils from "./NoPgUtils";

export class NoPgParsers {

    static parse_array_predicate (ObjType, q, def_op, traits, o) {

        let op = 'AND';

        if (NoPgUtils.is_operator(o[0])) {
            o = [].concat(o);
            op = o.shift();
        }

        if (NoPgUtils.parse_operator_name(op) === 'BIND') {
            return NoPgUtils.parse_function_predicate(ObjType, q, def_op, o, NoPgUtils.parse_operator_type(op), traits);
        }

        let predicates = _.map(o, item => NoPgParsers.recursive_parse_predicates(ObjType, q, def_op, traits, item) ).filter( NoPgUtils.not_undefined );

        return Predicate.join(predicates, op);

    }

    /** Recursively parse predicates */
    static recursive_parse_predicates (ObjType, q, def_op, traits, o) {

        if (o === undefined) {
            return;
        }

        if ( _.isArray(o) ) {
            return NoPgParsers.parse_array_predicate(ObjType, q, def_op, traits, o);
        }

        if ( NoPgUtils.isObjectNotArray(o) ) {

            o = NoPgUtils.parse_predicates(ObjType)(o, ObjType.meta.datakey.substr(1) );

            let predicates = _.map(Object.keys(o), k => new Predicate('' + k + ' = $', [o[k]]));

            return Predicate.join(predicates, def_op);

        }

        return new Predicate(''+o);

    }

}

// noinspection JSUnusedGlobalSymbols
export default NoPgParsers;
