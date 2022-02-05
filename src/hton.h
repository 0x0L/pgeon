#include "endianness.h"

static inline void pack_int16(char* buf, int16_t x) {
    uint16_t nx = hton16((uint16_t)x);
    /* NOTE: the memcpy below is _important_ to support systems
       which disallow unaligned access.  On systems, which do
       allow unaligned access it will be optimized away by the
       compiler
    */
    memcpy(buf, &nx, sizeof(uint16_t));
}

static inline void pack_int32(char* buf, int64_t x) {
    uint32_t nx = hton32((uint32_t)x);
    memcpy(buf, &nx, sizeof(uint32_t));
}

static inline void pack_int64(char* buf, int64_t x) {
    uint64_t nx = hton64((uint64_t)x);
    memcpy(buf, &nx, sizeof(uint64_t));
}

static inline uint16_t unpack_uint16(const char* buf) {
    uint16_t nx;
    memcpy((char*)&nx, buf, sizeof(uint16_t));
    return ntoh16(nx);
}

static inline int16_t unpack_int16(const char* buf) {
    return (int16_t)unpack_uint16(buf);
}

static inline uint32_t unpack_uint32(const char* buf) {
    uint32_t nx;
    memcpy((char*)&nx, buf, sizeof(uint32_t));
    return ntoh32(nx);
}

static inline int32_t unpack_int32(const char* buf) {
    return (int32_t)unpack_uint32(buf);
}

static inline uint64_t unpack_uint64(const char* buf) {
    uint64_t nx;
    memcpy((char*)&nx, buf, sizeof(uint64_t));
    return ntoh64(nx);
}

static inline int64_t unpack_int64(const char* buf) {
    return (int64_t)unpack_uint64(buf);
}

union _floatconv {
    uint32_t i;
    float f;
};

static inline void pack_float(char* buf, float f) {
    union _floatconv v;
    v.f = f;
    pack_int32(buf, (int32_t)v.i);
}

static inline float unpack_float(const char* buf) {
    union _floatconv v;
    v.i = (uint32_t)unpack_int32(buf);
    return v.f;
}

union _doubleconv {
    uint64_t i;
    double f;
};

static inline void pack_double(char* buf, double f) {
    union _doubleconv v;
    v.f = f;
    pack_int64(buf, (int64_t)v.i);
}

static inline double unpack_double(const char* buf) {
    union _doubleconv v;
    v.i = (uint64_t)unpack_int64(buf);
    return v.f;
}
