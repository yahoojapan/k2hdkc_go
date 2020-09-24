//
// k2hdkc_go
//
// Copyright 2018 Yahoo Japan Corporation.
//
// Go driver for k2hdkc that is a highly available and scalable distributed
// KVS clustering system. For k2hdkc, see
// https://github.com/yahoojapan/k2hdkc for the details.
//
// For the full copyright and license information, please view
// the license file that was distributed with this source code.
//
// AUTHOR:   Hirotaka Wakabayashi
// CREATE:   Fri, 14 Sep 2018
// REVISION:
//

#include <k2hdkc/k2hdkc.h>
#include <k2hash/k2hash.h>

const char* str_dkcres_result_type(dkcres_type_t res) {
    return STR_DKCRES_RESULT_TYPE(res);

}

const char* str_dkcres_subcode_type(dkcres_type_t res) {
    return STR_DKCRES_SUBCODE_TYPE(res);
}

bool dkc_free_keypack(PK2HKEYPCK pkeys, int keycnt) {
    return k2h_free_keypack(pkeys, keycnt);
}

bool dkc_free_keyarray(char** pkeys) {
    return k2h_free_keyarray(pkeys);
}

bool dkc_free_attrpack(PK2HATTRPCK pattrs, int attrcnt) {
    return k2h_free_attrpack(pattrs, attrcnt);
}

// Local Variables:
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: t
// End:
// vim600: noexpandtab sw=4 ts=4 fdm=marker
// vim<600: noexpandtab sw=4 ts=4
