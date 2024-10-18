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

#pragma once

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Encryption/RandomAccessFile.h>
#include <Storages/DeltaMerge/ScanContext_fwd.h>
#include <aws/s3/model/GetObjectResult.h>
#include <common/types.h>

#include <ext/scope_guard.h>

/// Remove the population of thread_local from Poco
#ifdef thread_local
#undef thread_local
#endif

namespace DB::S3
{
class TiFlashS3Client;
}

namespace DB::ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

namespace DB::S3
{
struct PrefetchCache
{
    using ReadFunc = std::function<ssize_t(char *, size_t)>;

    PrefetchCache(UInt32 hit_limit_, ReadFunc read_func_, size_t buffer_size_)
        : hit_limit(hit_limit_)
        , hit_count(0)
        , read_func(read_func_)
        , buffer_size(buffer_size_)
    {
        pos = buffer_limit;
    }

    ssize_t read(char * buf, size_t size);
    size_t skip(size_t ignore_count);

    enum class PrefetchRes
    {
        NeedNot,
        Ok,
    };

    PrefetchRes maybePrefetch();

    size_t getCacheRead() const { return cache_read; }
    size_t getDirectRead() const { return direct_read; }
    size_t getRevertCount() const {
        if (hit_count < hit_limit) return 0;
        return buffer_limit - pos;
    }
    size_t getCurrent() const { return pos; }
    bool needsRefill() const { return pos >= buffer_limit; }
    size_t unreadBytes() const { 
        RUNTIME_CHECK(buffer_limit >= pos);
        return buffer_limit - pos;
    }
    size_t getCachedSize() const { return buffer_limit; }
    bool activated() const { return hit_count >= hit_limit; }
    String summary() const;

private:
    UInt32 hit_limit;
    std::atomic<UInt32> hit_count;
    bool eof = false;
    ReadFunc read_func;
    // Equal to size of `write_buffer`.
    size_t buffer_size;
    size_t pos;
    // How many data is actually in the buffer.
    size_t buffer_limit = 0;
    std::vector<char> write_buffer;
    size_t direct_read = 0;
    size_t cache_read = 0;
};
class S3RandomAccessFile final : public RandomAccessFile
{
public:
    static RandomAccessFilePtr create(const String & remote_fname);

    S3RandomAccessFile(std::shared_ptr<TiFlashS3Client> client_ptr_, const String & remote_fname_, size_t prefetch_limit_ = 10);

    size_t getPos() const;
    size_t getPrefetchedSize() const;

    // Can only seek forward.
    off_t seek(off_t offset, int whence) override;

    ssize_t read(char * buf, size_t size) override;

    std::string getFileName() const override;
    std::string getRemoteFileName() const;

    ssize_t pread(char * /*buf*/, size_t /*size*/, off_t /*offset*/) const override
    {
        throw Exception("S3RandomAccessFile not support pread", ErrorCodes::NOT_IMPLEMENTED);
    }

    int getFd() const override { return -1; }

    bool isClosed() const override { return is_close; }

    void close() override { is_close = true; }

    struct ReadFileInfo
    {
        UInt64 size = 0; // File size of `remote_fname` or `merged_filename`, mainly used for FileCache.
        DB::DM::ScanContextPtr scan_context;
    };

    [[nodiscard]] static auto setReadFileInfo(ReadFileInfo && read_file_info_)
    {
        read_file_info = std::move(read_file_info_);
        return ext::make_scope_guard([]() { read_file_info.reset(); });
    }

    String summary() const;
    const PrefetchCache * debugPrefetchCache() const { return prefetch.get(); }

private:
    bool initialize();
    off_t seekImpl(off_t offset, int whence);
    ssize_t readImpl(char * buf, size_t size);
    String readRangeOfObject();

    // When reading, it is necessary to pass the extra information of file, such file size, to S3RandomAccessFile::create.
    // It is troublesome to pass parameters layer by layer. So currently, use thread_local global variable to pass parameters.
    // TODO: refine these codes later.
    inline static thread_local std::optional<ReadFileInfo> read_file_info;

    std::shared_ptr<TiFlashS3Client> client_ptr;
    String remote_fname;

    off_t cur_offset;
    Aws::S3::Model::GetObjectResult read_result;
    Int64 content_length = 0;

    DB::LoggerPtr log;
    bool is_close = false;

    Int32 cur_retry = 0;
    static constexpr Int32 max_retry = 3;

    std::unique_ptr<PrefetchCache> prefetch;
    size_t prefetch_limit;
};

using S3RandomAccessFilePtr = std::shared_ptr<S3RandomAccessFile>;

} // namespace DB::S3
