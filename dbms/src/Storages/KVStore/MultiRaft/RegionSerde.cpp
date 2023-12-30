// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/Utils/SerializationHelper.h>


namespace DB
{

namespace RegionPersistFormat
{
static constexpr UInt32 HAS_EAGER_TRUNCATE_INDEX = 0x01;
// The upper bits are used to store length of extensions. DO NOT USE!
} // namespace RegionPersistFormat

/// The flexible pattern
/// The `payload 1` is of length defined by `length 1`
/// |--------- 32 bits ----------|
/// |- 31b exts -|- 1b eager gc -|
/// |--------- eager gc ---------|
/// |--------- eager gc ---------|
/// |-------- ext type 1 --------|
/// |--------- length 1 ---------|
/// |--------- payload 1 --------|
/// |--------- ......... --------|
/// |-------- ext type n --------|
/// |--------- length n ---------|
/// |--------- payload n --------|


constexpr UInt32 EXTENSION_LENGTH_MAP[][2] = {
    {0, 0}, // not used
    {static_cast<UInt32>(RegionPersistVersion::V1), 0},
    {static_cast<UInt32>(RegionPersistVersion::V2), 0},
    {static_cast<UInt32>(RegionPersistVersion::V3), 1},
};

std::pair<MaybeRegionPersistExtension, UInt32> getPersistExtensionTypeAndLength(ReadBuffer & buf)
{
    auto ext_type = readBinary2<MaybeRegionPersistExtension>(buf);
    auto size = readBinary2<UInt32>(buf);
    // Note `ext_type` may not valid in RegionPersistExtension
    return std::make_pair(ext_type, size);
}

size_t writePersistExtension(
    UInt32 & cnt,
    WriteBuffer & wb,
    MaybeRegionPersistExtension ext_type,
    const char * data,
    UInt32 size)
{
    auto total_size = writeBinary2(ext_type, wb);
    total_size += writeBinary2(size, wb);
    wb.write(data, size);
    total_size += size;
    cnt++;
    return total_size;
}

template <UInt32 num_ext>
size_t serializeExtension(WriteBuffer & buf, [[maybe_unused]] UInt32 & actual_extension_count)
{
    auto total_size = 0;
#ifdef DBMS_PUBLIC_GTEST
    // only serialize ReservedForTest extension in test
    if constexpr (num_ext >= 1)
    {
        std::string s = "abcd";
        total_size += writePersistExtension(
            actual_extension_count,
            buf,
            magic_enum::enum_underlying(RegionPersistExtension::ReservedForTest),
            s.data(),
            s.size());
    }
#endif
    return total_size;
}

template <UInt32 num_ext>
void deserializeExtension(ReadBuffer & buf, [[maybe_unused]] MaybeRegionPersistExtension extension_type, UInt32 length)
{
    bool is_valid_extension = false;
#ifdef DBMS_PUBLIC_GTEST
    // only deserialize ReservedForTest extension in test
    if constexpr (num_ext >= 1)
    {
        if (extension_type == magic_enum::enum_underlying(RegionPersistExtension::ReservedForTest))
        {
            RUNTIME_CHECK(length == 4);
            RUNTIME_CHECK(readStringWithLength(buf, 4) == "abcd");
            is_valid_extension = true;
        }
    }
#endif
    if (!is_valid_extension)
        buf.ignore(length);
}

template <UInt32 version>
std::tuple<size_t, UInt64> Region::serialize(WriteBuffer & buf) const
{
    constexpr UInt32 binary_version = version;
    constexpr UInt32 expected_extension_count = EXTENSION_LENGTH_MAP[binary_version][1];

    // Serialize version
    size_t total_size = writeBinary2(binary_version, buf);
    UInt64 applied_index = -1;

    std::shared_lock<std::shared_mutex> lock(mutex);

    // Serialize meta
    const auto [meta_size, index] = meta.serialize(buf);
    total_size += meta_size;
    applied_index = index;

    if (binary_version >= 2)
    {
        // Serialize extra flags
        static_assert(sizeof(eager_truncated_index) == sizeof(UInt64));
        // The upper 31 bits are used to store the length of extensions, and the lowest bit is flag of eager gc.
        UInt32 flags = (expected_extension_count << 1) | RegionPersistFormat::HAS_EAGER_TRUNCATE_INDEX;
        total_size += writeBinary2(flags, buf);
        total_size += writeBinary2(eager_truncated_index, buf);

        // Serialize extensions
        UInt32 actual_extension_count = 0;
        total_size += serializeExtension<expected_extension_count>(buf, actual_extension_count);
        RUNTIME_CHECK(
            expected_extension_count == actual_extension_count,
            expected_extension_count,
            actual_extension_count);
    }

    // serialize data
    total_size += data.serialize(buf);

    return {total_size, applied_index};
}

/// Currently supports:
/// 1. Vx -> Vy where x >= 2, y >= 3
/// 2. Vx -> V2 where x >= 2, in 7.5.0
/// 3. Vx -> V2 where x >= 2, in later 7.5
template <UInt32 version>
RegionPtr Region::deserialize(ReadBuffer & buf, const TiFlashRaftProxyHelper * proxy_helper)
{
    // Deserialize version
    const auto binary_version = readBinary2<UInt32>(buf);
    constexpr auto current_version = version;
    constexpr UInt32 expected_extension_count = EXTENSION_LENGTH_MAP[current_version][1];

    if (current_version <= 1 && binary_version > current_version)
    {
        // Conform to https://github.com/pingcap/tiflash/blob/43f809fffde22d0af4c519be4546a5bf4dde30a2/dbms/src/Storages/KVStore/Region.cpp#L197
        // When downgrade from x(where x > 1) -> 1, the old version will throw with "unexpected version".
        // So we will also throw here.
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Don't support downgrading from {} to {}",
            binary_version,
            current_version);
    }
    const auto binary_version_decoded = magic_enum::enum_cast<RegionPersistVersion>(binary_version);
    if (!binary_version_decoded.has_value())
    {
        LOG_DEBUG(DB::Logger::get(), "Maybe downgrade from {} to {}", binary_version, current_version);
    }

    // Deserialize meta
    RegionPtr region = std::make_shared<Region>(RegionMeta::deserialize(buf), proxy_helper);

    if (binary_version >= 2)
    {
        // Deserialize flags
        auto flags = readBinary2<UInt32>(buf);
        if ((flags & RegionPersistFormat::HAS_EAGER_TRUNCATE_INDEX) != 0)
        {
            region->eager_truncated_index = readBinary2<UInt64>(buf);
        }
        // Deserialize extensions
        UInt32 extension_cnt = flags >> 1;
        for (UInt32 i = 0; i < extension_cnt; ++i)
        {
            auto [extension_type, length] = getPersistExtensionTypeAndLength(buf);
            deserializeExtension<expected_extension_count>(buf, extension_type, length);
        }
    }

    // deserialize data
    RegionData::deserialize(buf, region->data);
    region->data.reportAlloc(region->data.cf_data_size);

    // restore other var according to meta
    region->last_restart_log_applied = region->appliedIndex();
    region->setLastCompactLogApplied(region->appliedIndex());
    return region;
}

template RegionPtr Region::deserialize<static_cast<UInt32>(RegionPersistVersion::V3)>(
    ReadBuffer & buf,
    const TiFlashRaftProxyHelper * proxy_helper);
template RegionPtr Region::deserialize<static_cast<UInt32>(RegionPersistVersion::V2)>(
    ReadBuffer & buf,
    const TiFlashRaftProxyHelper * proxy_helper);
template RegionPtr Region::deserialize<static_cast<UInt32>(RegionPersistVersion::V1)>(
    ReadBuffer & buf,
    const TiFlashRaftProxyHelper * proxy_helper);
template std::tuple<size_t, UInt64> Region::serialize<static_cast<UInt32>(RegionPersistVersion::V3)>(
    WriteBuffer & buf) const;
template std::tuple<size_t, UInt64> Region::serialize<static_cast<UInt32>(RegionPersistVersion::V2)>(
    WriteBuffer & buf) const;
template std::tuple<size_t, UInt64> Region::serialize<static_cast<UInt32>(RegionPersistVersion::V1)>(
    WriteBuffer & buf) const;

} // namespace DB