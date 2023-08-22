#pragma once

#include <cassert>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <type_traits>




template <typename T>
inline uint32_t getLeadingZeroBitsUnsafe(T x)
{
    assert(x != 0);

    if constexpr (sizeof(T) <= sizeof(unsigned int))
    {
        return __builtin_clz(x);
    }
    else if constexpr (sizeof(T) <= sizeof(unsigned long int)) /// NOLINT
    {
        return __builtin_clzl(x);
    }
    else
    {
        return __builtin_clzll(x);
    }
}


template <typename T>
inline size_t getLeadingZeroBits(T x)
{
    if (!x)
        return sizeof(x) * 8;

    return getLeadingZeroBitsUnsafe(x);
}

// Unsafe since __builtin_ctz()-family explicitly state that result is undefined on x == 0
template <typename T>
inline size_t getTrailingZeroBitsUnsafe(T x)
{
    assert(x != 0);

    if constexpr (sizeof(T) <= sizeof(unsigned int))
    {
        return __builtin_ctz(x);
    }
    else if constexpr (sizeof(T) <= sizeof(unsigned long int)) /// NOLINT
    {
        return __builtin_ctzl(x);
    }
    else
    {
        return __builtin_ctzll(x);
    }
}

template <typename T>
inline size_t getTrailingZeroBits(T x)
{
    if (!x)
        return sizeof(x) * 8;

    return getTrailingZeroBitsUnsafe(x);
}

/** Returns a mask that has '1' for `bits` LSB set:
  * maskLowBits<UInt8>(3) => 00000111
  * maskLowBits<Int8>(3) => 00000111
  */
template <typename T>
inline T maskLowBits(unsigned char bits)
{
    using UnsignedT = std::make_unsigned_t<T>;
    if (bits == 0)
    {
        return 0;
    }

    UnsignedT result = static_cast<UnsignedT>(~UnsignedT{0});
    if (bits < sizeof(T) * 8)
    {
        result = static_cast<UnsignedT>(result >> (sizeof(UnsignedT) * 8 - bits));
    }

    return static_cast<T>(result);
}
