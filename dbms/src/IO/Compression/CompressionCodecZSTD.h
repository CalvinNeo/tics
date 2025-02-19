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

#include <IO/Compression/ICompressionCodec.h>

namespace DB
{

class CompressionCodecZSTD : public ICompressionCodec
{
public:
    explicit CompressionCodecZSTD(int level_);

    UInt8 getMethodByte() const override;

    bool isCompression() const override { return true; }

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size)
        const override;

private:
    const int level;
};

} // namespace DB
