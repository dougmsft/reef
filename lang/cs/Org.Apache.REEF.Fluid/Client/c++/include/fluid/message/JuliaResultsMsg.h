/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#ifndef E__FLUID_LANG_CS_ORG_APACHE_REEF_FLUID_CLIENT_C___INCLUDE_FLUID_MESSAGE_JULIARESULTSMSG_H_1544362699__H_
#define E__FLUID_LANG_CS_ORG_APACHE_REEF_FLUID_CLIENT_C___INCLUDE_FLUID_MESSAGE_JULIARESULTSMSG_H_1544362699__H_


#include <sstream>
#include "boost/any.hpp"
#include "Avro/Specific.hh"
#include "Avro/Encoder.hh"
#include "Avro/Decoder.hh"

namespace fluid {
struct JuliaResultsMsg {
    std::string uuid;
    std::string value;
    JuliaResultsMsg() :
        uuid(std::string()),
        value(std::string())
        { }
};

struct _JuliaResultsMsg_avsc_Union__0__ {
private:
    size_t idx_;
    boost::any value_;
public:
    size_t idx() const { return idx_; }
    JuliaResultsMsg get_JuliaResultsMsg() const;
    void set_JuliaResultsMsg(const JuliaResultsMsg& v);
    _JuliaResultsMsg_avsc_Union__0__();
};

inline
JuliaResultsMsg _JuliaResultsMsg_avsc_Union__0__::get_JuliaResultsMsg() const {
    if (idx_ != 0) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<JuliaResultsMsg >(value_);
}

inline
void _JuliaResultsMsg_avsc_Union__0__::set_JuliaResultsMsg(const JuliaResultsMsg& v) {
    idx_ = 0;
    value_ = v;
}

inline _JuliaResultsMsg_avsc_Union__0__::_JuliaResultsMsg_avsc_Union__0__() : idx_(0), value_(JuliaResultsMsg()) { }
}
namespace avro {
template<> struct codec_traits<fluid::JuliaResultsMsg> {
    static void encode(Encoder& e, const fluid::JuliaResultsMsg& v) {
        avro::encode(e, v.uuid);
        avro::encode(e, v.value);
    }
    static void decode(Decoder& d, fluid::JuliaResultsMsg& v) {
        if (avro::ResolvingDecoder *rd =
            dynamic_cast<avro::ResolvingDecoder *>(&d)) {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (std::vector<size_t>::const_iterator it = fo.begin();
                it != fo.end(); ++it) {
                switch (*it) {
                case 0:
                    avro::decode(d, v.uuid);
                    break;
                case 1:
                    avro::decode(d, v.value);
                    break;
                default:
                    break;
                }
            }
        } else {
            avro::decode(d, v.uuid);
            avro::decode(d, v.value);
        }
    }
};

template<> struct codec_traits<fluid::_JuliaResultsMsg_avsc_Union__0__> {
    static void encode(Encoder& e, fluid::_JuliaResultsMsg_avsc_Union__0__ v) {
        e.encodeUnionIndex(v.idx());
        switch (v.idx()) {
        case 0:
            avro::encode(e, v.get_JuliaResultsMsg());
            break;
        }
    }
    static void decode(Decoder& d, fluid::_JuliaResultsMsg_avsc_Union__0__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 1) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            {
                fluid::JuliaResultsMsg vv;
                avro::decode(d, vv);
                v.set_JuliaResultsMsg(vv);
            }
            break;
        }
    }
};

}
#endif
