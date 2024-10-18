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

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/TiFlashMetrics.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/S3/FileCache.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <Storages/S3/S3RandomAccessFile.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <common/likely.h>

#include <optional>

namespace ProfileEvents
{
extern const Event S3GetObject;
extern const Event S3ReadBytes;
extern const Event S3GetObjectRetry;
extern const Event S3CachedReadBytes;
extern const Event S3CachedSkipBytes;
extern const Event S3CachedRead;
extern const Event S3CachedSkip;
extern const Event S3IORead;
extern const Event S3IOSeek;
} // namespace ProfileEvents

namespace DB::S3
{
PrefetchCache::PrefetchRes PrefetchCache::maybePrefetch()
{
    if (eof)
    {
        return PrefetchRes::NeedNot;
    }
    if (pos >= buffer_limit)
    {
        write_buffer.reserve(buffer_size);
        // TODO Check if it is OK to read when the rest of the chars are less than size.
        auto res = read_func(write_buffer.data(), buffer_size);
        if (res < 0)
        {
            // Error state.
            eof = true;
            pos = 0;
            buffer_limit = 0;
        }
        else
        {
            // If we actually got some data.
            pos = 0;
            buffer_limit = res;
        }
        return PrefetchRes::Ok;
    }
    return PrefetchRes::NeedNot;
}

// - If the read is filled entirely by cache, then we will return the "gcount" of the cache.
// - If the read is filled by both the cache and `read_func`,
//   + If the read_func returns a positive number, we will add the contribution of the cache, and then return.
//   + Otherwise, we will return what `read)func` returns.
ssize_t PrefetchCache::read(char * buf, size_t size)
{
    if (hit_count++ < hit_limit)
    {
        // Do not use the cache.
        return read_func(buf, size);
    }
    if (size == 0) {
        return read_func(buf, size);
    }
    maybePrefetch();
    if (pos + size > buffer_limit)
    {
        // No enough data in cache.
        auto read_from_cache = buffer_limit - pos;
        ::memcpy(buf, write_buffer.data() + pos, read_from_cache);
        cache_read += read_from_cache;
        pos = buffer_limit;
        auto expected_direct_read_bytes = size - read_from_cache;
        auto res = read_func(buf + read_from_cache, expected_direct_read_bytes);
        ProfileEvents::increment(ProfileEvents::S3CachedReadBytes, read_from_cache);

        if (res < 0)
            return res;
        direct_read += res;
        // We may not read `size` data.
        return res + read_from_cache;
    }
    else
    {
        ::memcpy(buf, write_buffer.data() + pos, size);
        cache_read += size;
        ProfileEvents::increment(ProfileEvents::S3CachedReadBytes, size);
        ProfileEvents::increment(ProfileEvents::S3CachedRead, 1);
        pos += size;
        return size;
    }
}

size_t PrefetchCache::skip(size_t ignore_count) {
    if (hit_count++ < hit_limit)
    {
        return ignore_count;
    }
    if (ignore_count == 0) return 0;
    maybePrefetch();
    if (pos + ignore_count > buffer_limit)
    {
        // No enough data in cache.
        auto read_from_cache = buffer_limit - pos;
        pos = buffer_limit;
        auto expected_direct_read_bytes = ignore_count - read_from_cache;
        ProfileEvents::increment(ProfileEvents::S3CachedSkipBytes, read_from_cache);
        return expected_direct_read_bytes;
    }
    else
    {
        pos += ignore_count;
        ProfileEvents::increment(ProfileEvents::S3CachedSkipBytes, ignore_count);
        ProfileEvents::increment(ProfileEvents::S3CachedSkip, 1);
        return 0;
    }
}

String PrefetchCache::summary() const {
    return fmt::format("hit_count={} hit_limit={} buffer_limit={} direct_read={} cache_read={} pos={}", hit_count, hit_limit, buffer_limit, direct_read, cache_read, pos);
}

String S3RandomAccessFile::summary() const {
    return fmt::format("prefetch=({}) remote_fname={} cur_offset={} cur_retry={}", prefetch == nullptr ? "" : prefetch->summary(), remote_fname, cur_offset, cur_retry);
}

S3RandomAccessFile::S3RandomAccessFile(std::shared_ptr<TiFlashS3Client> client_ptr_, const String & remote_fname_)
    : client_ptr(std::move(client_ptr_))
    , remote_fname(remote_fname_)
    , cur_offset(0)
    , log(Logger::get(remote_fname))
{
    RUNTIME_CHECK(client_ptr != nullptr);
    RUNTIME_CHECK(initialize(), remote_fname);
    // TODO Update parameters
}

std::string S3RandomAccessFile::getFileName() const
{
    return fmt::format("{}/{}", client_ptr->bucket(), remote_fname);
}

std::string S3RandomAccessFile::getRemoteFileName() const
{
    return remote_fname;
}

bool isRetryableError(int e)
{
    return e == ECONNRESET || e == EAGAIN;
}

size_t S3RandomAccessFile::getPos() const {
    if (prefetch != nullptr) {
        return cur_offset - prefetch->unreadBytes();
    }
    return cur_offset;
}

size_t S3RandomAccessFile::getPrefetchedSize() const {
    if (prefetch != nullptr) {
        return prefetch->getCachedSize();
    }
    return 0;
}


ssize_t S3RandomAccessFile::read(char * buf, size_t size)
{
    while (true)
    {
        auto n = prefetch->read(buf, size);
        if (unlikely(n < 0 && isRetryableError(errno)))
        {
            // If it is a retryable error, then initialize again
            if (initialize())
            {
                continue;
            }
        }
        return n;
    }
}

ssize_t S3RandomAccessFile::readImpl(char * buf, size_t size)
{
    Stopwatch sw;
    ProfileEvents::increment(ProfileEvents::S3IORead, 1);
    auto & istr = read_result.GetBody();
    istr.read(buf, size);
    size_t gcount = istr.gcount();
    // Theoretically, `istr.eof()` is equivalent to `cur_offset + gcount != static_cast<size_t>(content_length)`.
    // It's just a double check for more safty.
    if (gcount < size && (!istr.eof() || cur_offset + gcount != static_cast<size_t>(content_length)))
    {
        LOG_ERROR(
            log,
            "Cannot read from istream, size={} gcount={} state=0x{:02X} cur_offset={} content_length={} errmsg={} "
            "cost={}ns",
            size,
            gcount,
            istr.rdstate(),
            cur_offset,
            content_length,
            strerror(errno),
            sw.elapsed());
        return -1;
    }
    auto elapsed_ns = sw.elapsed();
    GET_METRIC(tiflash_storage_s3_request_seconds, type_read_stream).Observe(elapsed_ns / 1000000000.0);
    if (elapsed_ns > 10000000) // 10ms
    {
        LOG_DEBUG(
            log,
            "gcount={} cur_offset={} content_length={} cost={}ns",
            gcount,
            cur_offset,
            content_length,
            elapsed_ns);
    }
    cur_offset += gcount;
    ProfileEvents::increment(ProfileEvents::S3ReadBytes, gcount);
    return gcount;
}

off_t S3RandomAccessFile::seek(off_t offset_, int whence)
{
    while (true)
    {
        auto off = seekImpl(offset_, whence);
        if (unlikely(off < 0 && isRetryableError(errno)))
        {
            // If it is a retryable error, then initialize again
            if (initialize())
            {
                continue;
            }
        }
        return off;
    }
}

off_t S3RandomAccessFile::seekImpl(off_t offset_, int whence)
{
    RUNTIME_CHECK_MSG(whence == SEEK_SET, "Only SEEK_SET mode is allowed, but {} is received", whence);
    RUNTIME_CHECK_MSG(
        offset_ >= cur_offset && offset_ <= content_length,
        "Seek position is out of bounds: offset={}, cur_offset={}, content_length={}",
        offset_,
        cur_offset,
        content_length);

    if (offset_ == cur_offset)
    {
        return cur_offset;
    }
    Stopwatch sw;
    ProfileEvents::increment(ProfileEvents::S3IOSeek, 1);
    auto & istr = read_result.GetBody();
    auto ignore_count = offset_ - cur_offset;
    auto direct_ignore_count = prefetch->skip(ignore_count);
    if (!istr.ignore(direct_ignore_count))
    {
        LOG_ERROR(log, "Cannot ignore from istream, errmsg={}, cost={}ns", strerror(errno), sw.elapsed());
        return -1;
    }
    auto elapsed_ns = sw.elapsed();
    GET_METRIC(tiflash_storage_s3_request_seconds, type_read_stream).Observe(elapsed_ns / 1000000000.0);
    if (elapsed_ns > 10000000) // 10ms
    {
        LOG_DEBUG(
            log,
            "ignore_count={} direct_ignore_count={} cur_offset={} content_length={} cost={}ns",
            ignore_count,
            direct_ignore_count,
            cur_offset,
            content_length,
            elapsed_ns);
    }
    ProfileEvents::increment(ProfileEvents::S3ReadBytes, offset_ - cur_offset);
    cur_offset = offset_;
    return cur_offset;
}

String S3RandomAccessFile::readRangeOfObject()
{
    return fmt::format("bytes={}-", cur_offset);
}

bool S3RandomAccessFile::initialize()
{
    Stopwatch sw;
    bool request_succ = false;
    if (prefetch != nullptr) {
        auto to_revert = prefetch->getRevertCount();
        LOG_INFO(log, "S3 revert cache {}", to_revert);
        cur_offset -= to_revert;
    }
    prefetch = std::make_unique<PrefetchCache>(10, std::bind(&S3RandomAccessFile::readImpl, this, std::placeholders::_1, std::placeholders::_2), 5 * 1024 * 1024);
    Aws::S3::Model::GetObjectRequest req;
    req.SetRange(readRangeOfObject());
    client_ptr->setBucketAndKeyWithRoot(req, remote_fname);
    while (cur_retry < max_retry)
    {
        cur_retry += 1;
        ProfileEvents::increment(ProfileEvents::S3GetObject);
        if (cur_retry > 1)
        {
            ProfileEvents::increment(ProfileEvents::S3GetObjectRetry);
        }
        auto outcome = client_ptr->GetObject(req);
        if (!outcome.IsSuccess())
        {
            auto el = sw.elapsedSeconds();
            LOG_ERROR(
                log,
                "S3 GetObject failed: {}, cur_retry={}, key={}, elapsed{}={:.3f}s",
                S3::S3ErrorMessage(outcome.GetError()),
                cur_retry,
                req.GetKey(),
                el > 60.0 ? "(long)" : "",
                el);
            continue;
        }

        request_succ = true;
        if (content_length == 0)
        {
            content_length = outcome.GetResult().GetContentLength();
        }
        read_result = outcome.GetResultWithOwnership();
        RUNTIME_CHECK(read_result.GetBody(), remote_fname, strerror(errno));
        GET_METRIC(tiflash_storage_s3_request_seconds, type_get_object).Observe(sw.elapsedSeconds());
        break;
    }
    if (cur_retry >= max_retry && !request_succ)
    {
        auto el = sw.elapsedSeconds();
        LOG_INFO(
            log,
            "S3 GetObject timeout: max_retry={}, key={}, elapsed{}={:.3f}s",
            max_retry,
            req.GetKey(),
            el > 60.0 ? "(long)" : "",
            el);
    }
    return request_succ;
}

inline static RandomAccessFilePtr tryOpenCachedFile(const String & remote_fname, std::optional<UInt64> filesize)
{
    try
    {
        auto * file_cache = FileCache::instance();
        return file_cache != nullptr
            ? file_cache->getRandomAccessFile(S3::S3FilenameView::fromKey(remote_fname), filesize)
            : nullptr;
    }
    catch (...)
    {
        tryLogCurrentException("tryOpenCachedFile", remote_fname);
        return nullptr;
    }
}

inline static RandomAccessFilePtr createFromNormalFile(
    const String & remote_fname,
    std::optional<UInt64> filesize,
    std::optional<DM::ScanContextPtr> scan_context)
{
    auto file = tryOpenCachedFile(remote_fname, filesize);
    if (file != nullptr)
    {
        if (scan_context.has_value())
            scan_context.value()->disagg_read_cache_hit_size += filesize.value();
        return file;
    }
    if (scan_context.has_value())
        scan_context.value()->disagg_read_cache_miss_size += filesize.value();
    auto & ins = S3::ClientFactory::instance();
    return std::make_shared<S3RandomAccessFile>(ins.sharedTiFlashClient(), remote_fname);
}

RandomAccessFilePtr S3RandomAccessFile::create(const String & remote_fname)
{
    if (read_file_info)
        return createFromNormalFile(
            remote_fname,
            std::optional<UInt64>(read_file_info->size),
            read_file_info->scan_context != nullptr ? std::optional<DM::ScanContextPtr>(read_file_info->scan_context)
                                                    : std::nullopt);
    else
        return createFromNormalFile(remote_fname, std::nullopt, std::nullopt);
}
} // namespace DB::S3
