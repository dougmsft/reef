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


#ifndef E__FLUID_LANG_CS_ORG_APACHE_REEF_FLUID_CLIENT_C___INCLUDE_FLUID_MESSAGE_SHUTDOWNMSG_H_1544362699__H_
#define E__FLUID_LANG_CS_ORG_APACHE_REEF_FLUID_CLIENT_C___INCLUDE_FLUID_MESSAGE_SHUTDOWNMSG_H_1544362699__H_


#include <sstream>
#include "boost/any.hpp"
#include "Avro/Specific.hh"
#include "Avro/Encoder.hh"
#include "Avro/Decoder.hh"

namespace fluid {
struct ShutdownMsg {
    int32_t command;
    ShutdownMsg() :
        command(int32_t())
        { }
};

struct _ShutdownMsg_avsc_Union__0__ {
private:
    size_t idx_;
    boost::any value_;
public:
    size_t idx() const { return idx_; }
    ShutdownMsg get_ShutdownMsg() const;
    void set_ShutdownMsg(const ShutdownMsg& v);
    _ShutdownMsg_avsc_Union__0__();
};

inline
ShutdownMsg _ShutdownMsg_avsc_Union__0__::get_ShutdownMsg() const {
    if (idx_ != 0) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<ShutdownMsg >(value_);
}

inline
void _ShutdownMsg_avsc_Union__0__::set_ShutdownMsg(const ShutdownMsg& v) {
    idx_ = 0;
    value_ = v;
}

inline _ShutdownMsg_avsc_Union__0__::_ShutdownMsg_avsc_Union__0__() : idx_(0), value_(ShutdownMsg()) { }
}
namespace avro {
template<> struct codec_traits<fluid::ShutdownMsg> {
    static void encode(Encoder& e, const fluid::ShutdownMsg& v) {
        avro::encode(e, v.command);
    }
    static void decode(Decoder& d, fluid::ShutdownMsg& v) {
        if (avro::ResolvingDecoder *rd =
            dynamic_cast<avro::ResolvingDecoder *>(&d)) {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (std::vector<size_t>::const_iterator it = fo.begin();
                it != fo.end(); ++it) {
                switch (*it) {
                case 0:
                    avro::decode(d, v.command);
                    break;
                default:
                    break;
                }
            }
        } else {
            avro::decode(d, v.command);
        }
    }
};

template<> struct codec_traits<fluid::_ShutdownMsg_avsc_Union__0__> {
    static void encode(Encoder& e, fluid::_ShutdownMsg_avsc_Union__0__ v) {
        e.encodeUnionIndex(v.idx());
        switch (v.idx()) {
        case 0:
            avro::encode(e, v.get_ShutdownMsg());
            break;
        }
    }
    static void decode(Decoder& d, fluid::_ShutdownMsg_avsc_Union__0__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 1) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            {
                fluid::ShutdownMsg vv;
                avro::decode(d, vv);
                v.set_ShutdownMsg(vv);
            }
            break;
        }
    }
};

}
#endif
