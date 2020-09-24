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

#ifndef _K2H_MACRO_H_
#define _K2H_MACRO_H_

const char* str_dkcres_result_type(dkcres_type_t res);
const char* str_dkcres_subcode_type(dkcres_type_t res);
bool dkc_free_keypack(PK2HKEYPCK pkeys, int keycnt);
bool dkc_free_keyarray(char** pkeys);
bool dkc_free_attrpack(PK2HATTRPCK pattrs, int attrcnt);

#endif // _K2H_MACRO_H_

// Local Variables:
// c-basic-offset: 4
// tab-width: 4
// indent-tabs-mode: t
// End:
// vim600: noexpandtab sw=4 ts=4 fdm=marker
// vim<600: noexpandtab sw=4 ts=4
