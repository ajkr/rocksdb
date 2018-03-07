// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
#pragma once

#include <algorithm>
#include <limits>
#include <string>

#include "rocksdb/options.h"
#include "util/coding.h"

#ifdef SNAPPY
#include <snappy.h>
#endif

#ifdef ZLIB
#include <zlib.h>
#endif

#ifdef BZIP2
#include <bzlib.h>
#endif

#if defined(LZ4)
#include <lz4.h>
#include <lz4hc.h>
#endif

#if defined(ZSTD)
#include <zstd.h>
#if ZSTD_VERSION_NUMBER >= 800  // v0.8.0+
#include <zdict.h>
#endif  // ZSTD_VERSION_NUMBER >= 800
#endif  // ZSTD

#if defined(XPRESS)
#include "port/xpress.h"
#endif

namespace rocksdb {

// Data associated with compression dictionary, e.g., to avoid repetitive
// preprocessing work.
struct DictContext {
#if defined(ZSTD) && ZSTD_VERSION_NUMBER >= 700  // v0.7.0+
  // we cannot use unique_ptr as ZSTD_CDict is an incomplete type so its
  // destructor is unknown. That's unfortunate because it means we have to
  // implement move semantics ourselves.
  ZSTD_CDict* zstd_cdict;
#endif
  Slice raw_dict_bytes;

  DictContext() : zstd_cdict(nullptr) {}

  DictContext(DictContext&& other) : zstd_cdict(nullptr) {
    *this = std::move(other);
  }

  DictContext& operator=(DictContext&& other) {
    raw_dict_bytes = std::move(other.raw_dict_bytes);
#if defined(ZSTD) && ZSTD_VERSION_NUMBER >= 700  // v0.7.0+
    size_t res = ZSTD_freeCDict(zstd_cdict);
    assert(res == 0);  // Last I checked it can't fail
    (void) res;  // prevent unused var warning
    zstd_cdict = other.zstd_cdict;
    other.zstd_cdict = nullptr;
#endif
    return *this;
  }

  DictContext& operator=(const DictContext& other) = delete;
  DictContext(const DictContext& other) = delete;

  // @param raw_dict_bytes If non-nullptr, the pointed-to string must outlive
  //    the DictContext
  Status Init(CompressionType _compression_type,
              const CompressionOptions& _compression_opts,
              const std::string* _raw_dict_bytes) {
    if (_raw_dict_bytes != nullptr && _raw_dict_bytes->size()) {
      raw_dict_bytes = *_raw_dict_bytes;
#if defined(ZSTD) && ZSTD_VERSION_NUMBER >= 700  // v0.7.0+
      if (_compression_type == kZSTD ||
          _compression_type == kZSTDNotFinalCompression) {
        zstd_cdict =
            ZSTD_createCDict(_raw_dict_bytes->data(), _raw_dict_bytes->size(),
                             _compression_opts.level);
        if (zstd_cdict == nullptr) {
          char msg[255];
          snprintf(msg, sizeof(msg),
                   "ZSTD_createCDict failed on dictionary with size "
                   "%" ROCKSDB_PRIszt,
                   _raw_dict_bytes->size());
          // it has no error code so we can't really know why it failed.
          return Status::InvalidArgument(msg);
        }
      }
#endif
    }
    return Status::OK();
  }

  ~DictContext() {
#if defined(ZSTD) && ZSTD_VERSION_NUMBER >= 700  // v0.7.0+
    size_t res = ZSTD_freeCDict(zstd_cdict);
    assert(res == 0);  // Last I checked it can't fail
    (void) res;  // prevent unused var warning
#endif
  }
};

inline bool Snappy_Supported() {
#ifdef SNAPPY
  return true;
#else
  return false;
#endif
}

inline bool Zlib_Supported() {
#ifdef ZLIB
  return true;
#else
  return false;
#endif
}

inline bool BZip2_Supported() {
#ifdef BZIP2
  return true;
#else
  return false;
#endif
}

inline bool LZ4_Supported() {
#ifdef LZ4
  return true;
#else
  return false;
#endif
}

inline bool XPRESS_Supported() {
#ifdef XPRESS
  return true;
#else
  return false;
#endif
}

inline bool ZSTD_Supported() {
#ifdef ZSTD
  // ZSTD format is finalized since version 0.8.0.
  return (ZSTD_versionNumber() >= 800);
#else
  return false;
#endif
}

inline bool ZSTDNotFinal_Supported() {
#ifdef ZSTD
  return true;
#else
  return false;
#endif
}

inline bool CompressionTypeSupported(CompressionType compression_type) {
  switch (compression_type) {
    case kNoCompression:
      return true;
    case kSnappyCompression:
      return Snappy_Supported();
    case kZlibCompression:
      return Zlib_Supported();
    case kBZip2Compression:
      return BZip2_Supported();
    case kLZ4Compression:
      return LZ4_Supported();
    case kLZ4HCCompression:
      return LZ4_Supported();
    case kXpressCompression:
      return XPRESS_Supported();
    case kZSTDNotFinalCompression:
      return ZSTDNotFinal_Supported();
    case kZSTD:
      return ZSTD_Supported();
    default:
      assert(false);
      return false;
  }
}

inline std::string CompressionTypeToString(CompressionType compression_type) {
  switch (compression_type) {
    case kNoCompression:
      return "NoCompression";
    case kSnappyCompression:
      return "Snappy";
    case kZlibCompression:
      return "Zlib";
    case kBZip2Compression:
      return "BZip2";
    case kLZ4Compression:
      return "LZ4";
    case kLZ4HCCompression:
      return "LZ4HC";
    case kXpressCompression:
      return "Xpress";
    case kZSTD:
      return "ZSTD";
    case kZSTDNotFinalCompression:
      return "ZSTDNotFinal";
    default:
      assert(false);
      return "";
  }
}

// compress_format_version can have two values:
// 1 -- decompressed sizes for BZip2 and Zlib are not included in the compressed
// block. Also, decompressed sizes for LZ4 are encoded in platform-dependent
// way.
// 2 -- Zlib, BZip2 and LZ4 encode decompressed size as Varint32 just before the
// start of compressed block. Snappy format is the same as version 1.

inline bool Snappy_Compress(const CompressionOptions& /*opts*/,
                            const char* input, size_t length,
                            ::std::string* output) {
#ifdef SNAPPY
  output->resize(snappy::MaxCompressedLength(length));
  size_t outlen;
  snappy::RawCompress(input, length, &(*output)[0], &outlen);
  output->resize(outlen);
  return true;
#else
  return false;
#endif
}

inline bool Snappy_GetUncompressedLength(const char* input, size_t length,
                                         size_t* result) {
#ifdef SNAPPY
  return snappy::GetUncompressedLength(input, length, result);
#else
  return false;
#endif
}

inline bool Snappy_Uncompress(const char* input, size_t length,
                              char* output) {
#ifdef SNAPPY
  return snappy::RawUncompress(input, length, output);
#else
  return false;
#endif
}

namespace compression {
// returns size
inline size_t PutDecompressedSizeInfo(std::string* output, uint32_t length) {
  PutVarint32(output, length);
  return output->size();
}

inline bool GetDecompressedSizeInfo(const char** input_data,
                                    size_t* input_length,
                                    uint32_t* output_len) {
  auto new_input_data =
      GetVarint32Ptr(*input_data, *input_data + *input_length, output_len);
  if (new_input_data == nullptr) {
    return false;
  }
  *input_length -= (new_input_data - *input_data);
  *input_data = new_input_data;
  return true;
}
}  // namespace compression

// compress_format_version == 1 -- decompressed size is not included in the
// block header
// compress_format_version == 2 -- decompressed size is included in the block
// header in varint32 format
// @param compression_dict Data for presetting the compression library's
//    dictionary.
inline bool Zlib_Compress(const CompressionOptions& opts,
                          uint32_t compress_format_version, const char* input,
                          size_t length, ::std::string* output,
                          const Slice& compression_dict = Slice()) {
#ifdef ZLIB
  if (length > std::numeric_limits<uint32_t>::max()) {
    // Can't compress more than 4GB
    return false;
  }

  size_t output_header_len = 0;
  if (compress_format_version == 2) {
    output_header_len = compression::PutDecompressedSizeInfo(
        output, static_cast<uint32_t>(length));
  }
  // Resize output to be the plain data length.
  // This may not be big enough if the compression actually expands data.
  output->resize(output_header_len + length);

  // The memLevel parameter specifies how much memory should be allocated for
  // the internal compression state.
  // memLevel=1 uses minimum memory but is slow and reduces compression ratio.
  // memLevel=9 uses maximum memory for optimal speed.
  // The default value is 8. See zconf.h for more details.
  static const int memLevel = 8;
  z_stream _stream;
  memset(&_stream, 0, sizeof(z_stream));
  int st = deflateInit2(&_stream, opts.level, Z_DEFLATED, opts.window_bits,
                        memLevel, opts.strategy);
  if (st != Z_OK) {
    return false;
  }

  if (compression_dict.size()) {
    // Initialize the compression library's dictionary
    st = deflateSetDictionary(
        &_stream, reinterpret_cast<const Bytef*>(compression_dict.data()),
        static_cast<unsigned int>(compression_dict.size()));
    if (st != Z_OK) {
      deflateEnd(&_stream);
      return false;
    }
  }

  // Compress the input, and put compressed data in output.
  _stream.next_in = (Bytef *)input;
  _stream.avail_in = static_cast<unsigned int>(length);

  // Initialize the output size.
  _stream.avail_out = static_cast<unsigned int>(length);
  _stream.next_out = reinterpret_cast<Bytef*>(&(*output)[output_header_len]);

  bool compressed = false;
  st = deflate(&_stream, Z_FINISH);
  if (st == Z_STREAM_END) {
    compressed = true;
    output->resize(output->size() - _stream.avail_out);
  }
  // The only return value we really care about is Z_STREAM_END.
  // Z_OK means insufficient output space. This means the compression is
  // bigger than decompressed size. Just fail the compression in that case.

  deflateEnd(&_stream);
  return compressed;
#else
  return false;
#endif
}

// compress_format_version == 1 -- decompressed size is not included in the
// block header
// compress_format_version == 2 -- decompressed size is included in the block
// header in varint32 format
// @param compression_dict Data for presetting the compression library's
//    dictionary.
inline char* Zlib_Uncompress(const char* input_data, size_t input_length,
                             int* decompress_size,
                             uint32_t compress_format_version,
                             const Slice& compression_dict = Slice(),
                             int windowBits = -14) {
#ifdef ZLIB
  uint32_t output_len = 0;
  if (compress_format_version == 2) {
    if (!compression::GetDecompressedSizeInfo(&input_data, &input_length,
                                              &output_len)) {
      return nullptr;
    }
  } else {
    // Assume the decompressed data size will 5x of compressed size, but round
    // to the page size
    size_t proposed_output_len = ((input_length * 5) & (~(4096 - 1))) + 4096;
    output_len = static_cast<uint32_t>(
        std::min(proposed_output_len,
                 static_cast<size_t>(std::numeric_limits<uint32_t>::max())));
  }

  z_stream _stream;
  memset(&_stream, 0, sizeof(z_stream));

  // For raw inflate, the windowBits should be -8..-15.
  // If windowBits is bigger than zero, it will use either zlib
  // header or gzip header. Adding 32 to it will do automatic detection.
  int st = inflateInit2(&_stream,
      windowBits > 0 ? windowBits + 32 : windowBits);
  if (st != Z_OK) {
    return nullptr;
  }

  if (compression_dict.size()) {
    // Initialize the compression library's dictionary
    st = inflateSetDictionary(
        &_stream, reinterpret_cast<const Bytef*>(compression_dict.data()),
        static_cast<unsigned int>(compression_dict.size()));
    if (st != Z_OK) {
      return nullptr;
    }
  }

  _stream.next_in = (Bytef *)input_data;
  _stream.avail_in = static_cast<unsigned int>(input_length);

  char* output = new char[output_len];

  _stream.next_out = (Bytef *)output;
  _stream.avail_out = static_cast<unsigned int>(output_len);

  bool done = false;
  while (!done) {
    st = inflate(&_stream, Z_SYNC_FLUSH);
    switch (st) {
      case Z_STREAM_END:
        done = true;
        break;
      case Z_OK: {
        // No output space. Increase the output space by 20%.
        // We should never run out of output space if
        // compress_format_version == 2
        assert(compress_format_version != 2);
        size_t old_sz = output_len;
        uint32_t output_len_delta = output_len/5;
        output_len += output_len_delta < 10 ? 10 : output_len_delta;
        char* tmp = new char[output_len];
        memcpy(tmp, output, old_sz);
        delete[] output;
        output = tmp;

        // Set more output.
        _stream.next_out = (Bytef *)(output + old_sz);
        _stream.avail_out = static_cast<unsigned int>(output_len - old_sz);
        break;
      }
      case Z_BUF_ERROR:
      default:
        delete[] output;
        inflateEnd(&_stream);
        return nullptr;
    }
  }

  // If we encoded decompressed block size, we should have no bytes left
  assert(compress_format_version != 2 || _stream.avail_out == 0);
  *decompress_size = static_cast<int>(output_len - _stream.avail_out);
  inflateEnd(&_stream);
  return output;
#else
  return nullptr;
#endif
}

// compress_format_version == 1 -- decompressed size is not included in the
// block header
// compress_format_version == 2 -- decompressed size is included in the block
// header in varint32 format
inline bool BZip2_Compress(const CompressionOptions& /*opts*/,
                           uint32_t compress_format_version, const char* input,
                           size_t length, ::std::string* output) {
#ifdef BZIP2
  if (length > std::numeric_limits<uint32_t>::max()) {
    // Can't compress more than 4GB
    return false;
  }
  size_t output_header_len = 0;
  if (compress_format_version == 2) {
    output_header_len = compression::PutDecompressedSizeInfo(
        output, static_cast<uint32_t>(length));
  }
  // Resize output to be the plain data length.
  // This may not be big enough if the compression actually expands data.
  output->resize(output_header_len + length);


  bz_stream _stream;
  memset(&_stream, 0, sizeof(bz_stream));

  // Block size 1 is 100K.
  // 0 is for silent.
  // 30 is the default workFactor
  int st = BZ2_bzCompressInit(&_stream, 1, 0, 30);
  if (st != BZ_OK) {
    return false;
  }

  // Compress the input, and put compressed data in output.
  _stream.next_in = (char *)input;
  _stream.avail_in = static_cast<unsigned int>(length);

  // Initialize the output size.
  _stream.avail_out = static_cast<unsigned int>(length);
  _stream.next_out = reinterpret_cast<char*>(&(*output)[output_header_len]);

  bool compressed = false;
  st = BZ2_bzCompress(&_stream, BZ_FINISH);
  if (st == BZ_STREAM_END) {
    compressed = true;
    output->resize(output->size() - _stream.avail_out);
  }
  // The only return value we really care about is BZ_STREAM_END.
  // BZ_FINISH_OK means insufficient output space. This means the compression
  // is bigger than decompressed size. Just fail the compression in that case.

  BZ2_bzCompressEnd(&_stream);
  return compressed;
#else
  return false;
#endif
}

// compress_format_version == 1 -- decompressed size is not included in the
// block header
// compress_format_version == 2 -- decompressed size is included in the block
// header in varint32 format
inline char* BZip2_Uncompress(const char* input_data, size_t input_length,
                              int* decompress_size,
                              uint32_t compress_format_version) {
#ifdef BZIP2
  uint32_t output_len = 0;
  if (compress_format_version == 2) {
    if (!compression::GetDecompressedSizeInfo(&input_data, &input_length,
                                              &output_len)) {
      return nullptr;
    }
  } else {
    // Assume the decompressed data size will 5x of compressed size, but round
    // to the next page size
    size_t proposed_output_len = ((input_length * 5) & (~(4096 - 1))) + 4096;
    output_len = static_cast<uint32_t>(
        std::min(proposed_output_len,
                 static_cast<size_t>(std::numeric_limits<uint32_t>::max())));
  }

  bz_stream _stream;
  memset(&_stream, 0, sizeof(bz_stream));

  int st = BZ2_bzDecompressInit(&_stream, 0, 0);
  if (st != BZ_OK) {
    return nullptr;
  }

  _stream.next_in = (char *)input_data;
  _stream.avail_in = static_cast<unsigned int>(input_length);

  char* output = new char[output_len];

  _stream.next_out = (char *)output;
  _stream.avail_out = static_cast<unsigned int>(output_len);

  bool done = false;
  while (!done) {
    st = BZ2_bzDecompress(&_stream);
    switch (st) {
      case BZ_STREAM_END:
        done = true;
        break;
      case BZ_OK: {
        // No output space. Increase the output space by 20%.
        // We should never run out of output space if
        // compress_format_version == 2
        assert(compress_format_version != 2);
        uint32_t old_sz = output_len;
        output_len = output_len * 1.2;
        char* tmp = new char[output_len];
        memcpy(tmp, output, old_sz);
        delete[] output;
        output = tmp;

        // Set more output.
        _stream.next_out = (char *)(output + old_sz);
        _stream.avail_out = static_cast<unsigned int>(output_len - old_sz);
        break;
      }
      default:
        delete[] output;
        BZ2_bzDecompressEnd(&_stream);
        return nullptr;
    }
  }

  // If we encoded decompressed block size, we should have no bytes left
  assert(compress_format_version != 2 || _stream.avail_out == 0);
  *decompress_size = static_cast<int>(output_len - _stream.avail_out);
  BZ2_bzDecompressEnd(&_stream);
  return output;
#else
  return nullptr;
#endif
}

// compress_format_version == 1 -- decompressed size is included in the
// block header using memcpy, which makes database non-portable)
// compress_format_version == 2 -- decompressed size is included in the block
// header in varint32 format
// @param compression_dict Data for presetting the compression library's
//    dictionary.
inline bool LZ4_Compress(const CompressionOptions& /*opts*/,
                         uint32_t compress_format_version, const char* input,
                         size_t length, ::std::string* output,
                         const Slice compression_dict = Slice()) {
#ifdef LZ4
  if (length > std::numeric_limits<uint32_t>::max()) {
    // Can't compress more than 4GB
    return false;
  }

  size_t output_header_len = 0;
  if (compress_format_version == 2) {
    // new encoding, using varint32 to store size information
    output_header_len = compression::PutDecompressedSizeInfo(
        output, static_cast<uint32_t>(length));
  } else {
    // legacy encoding, which is not really portable (depends on big/little
    // endianness)
    output_header_len = 8;
    output->resize(output_header_len);
    char* p = const_cast<char*>(output->c_str());
    memcpy(p, &length, sizeof(length));
  }
  int compress_bound = LZ4_compressBound(static_cast<int>(length));
  output->resize(static_cast<size_t>(output_header_len + compress_bound));

  int outlen;
#if LZ4_VERSION_NUMBER >= 10400  // r124+
  LZ4_stream_t* stream = LZ4_createStream();
  if (compression_dict.size()) {
    LZ4_loadDict(stream, compression_dict.data(),
                 static_cast<int>(compression_dict.size()));
  }
#if LZ4_VERSION_NUMBER >= 10700  // r129+
  outlen = LZ4_compress_fast_continue(
      stream, input, &(*output)[output_header_len], static_cast<int>(length),
      compress_bound, 1);
#else  // up to r128
  outlen = LZ4_compress_limitedOutput_continue(
      stream, input, &(*output)[output_header_len], static_cast<int>(length),
      compress_bound);
#endif
  LZ4_freeStream(stream);
#else   // up to r123
  outlen = LZ4_compress_limitedOutput(input, &(*output)[output_header_len],
                                      static_cast<int>(length), compress_bound);
#endif  // LZ4_VERSION_NUMBER >= 10400

  if (outlen == 0) {
    return false;
  }
  output->resize(static_cast<size_t>(output_header_len + outlen));
  return true;
#else  // LZ4
  return false;
#endif
}

// compress_format_version == 1 -- decompressed size is included in the
// block header using memcpy, which makes database non-portable)
// compress_format_version == 2 -- decompressed size is included in the block
// header in varint32 format
// @param compression_dict Data for presetting the compression library's
//    dictionary.
inline char* LZ4_Uncompress(const char* input_data, size_t input_length,
                            int* decompress_size,
                            uint32_t compress_format_version,
                            const Slice& compression_dict = Slice()) {
#ifdef LZ4
  uint32_t output_len = 0;
  if (compress_format_version == 2) {
    // new encoding, using varint32 to store size information
    if (!compression::GetDecompressedSizeInfo(&input_data, &input_length,
                                              &output_len)) {
      return nullptr;
    }
  } else {
    // legacy encoding, which is not really portable (depends on big/little
    // endianness)
    if (input_length < 8) {
      return nullptr;
    }
    memcpy(&output_len, input_data, sizeof(output_len));
    input_length -= 8;
    input_data += 8;
  }

  char* output = new char[output_len];
#if LZ4_VERSION_NUMBER >= 10400  // r124+
  LZ4_streamDecode_t* stream = LZ4_createStreamDecode();
  if (compression_dict.size()) {
    LZ4_setStreamDecode(stream, compression_dict.data(),
                        static_cast<int>(compression_dict.size()));
  }
  *decompress_size = LZ4_decompress_safe_continue(
      stream, input_data, output, static_cast<int>(input_length),
      static_cast<int>(output_len));
  LZ4_freeStreamDecode(stream);
#else   // up to r123
  *decompress_size =
      LZ4_decompress_safe(input_data, output, static_cast<int>(input_length),
                          static_cast<int>(output_len));
#endif  // LZ4_VERSION_NUMBER >= 10400

  if (*decompress_size < 0) {
    delete[] output;
    return nullptr;
  }
  assert(*decompress_size == static_cast<int>(output_len));
  return output;
#else  // LZ4
  return nullptr;
#endif
}

// compress_format_version == 1 -- decompressed size is included in the
// block header using memcpy, which makes database non-portable)
// compress_format_version == 2 -- decompressed size is included in the block
// header in varint32 format
// @param compression_dict Data for presetting the compression library's
//    dictionary.
inline bool LZ4HC_Compress(const CompressionOptions& opts,
                           uint32_t compress_format_version, const char* input,
                           size_t length, ::std::string* output,
                           const Slice& compression_dict = Slice()) {
#ifdef LZ4
  if (length > std::numeric_limits<uint32_t>::max()) {
    // Can't compress more than 4GB
    return false;
  }

  size_t output_header_len = 0;
  if (compress_format_version == 2) {
    // new encoding, using varint32 to store size information
    output_header_len = compression::PutDecompressedSizeInfo(
        output, static_cast<uint32_t>(length));
  } else {
    // legacy encoding, which is not really portable (depends on big/little
    // endianness)
    output_header_len = 8;
    output->resize(output_header_len);
    char* p = const_cast<char*>(output->c_str());
    memcpy(p, &length, sizeof(length));
  }
  int compress_bound = LZ4_compressBound(static_cast<int>(length));
  output->resize(static_cast<size_t>(output_header_len + compress_bound));

  int outlen;
#if LZ4_VERSION_NUMBER >= 10400  // r124+
  LZ4_streamHC_t* stream = LZ4_createStreamHC();
  LZ4_resetStreamHC(stream, opts.level);
  const char* compression_dict_data =
      compression_dict.size() > 0 ? compression_dict.data() : nullptr;
  size_t compression_dict_size = compression_dict.size();
  LZ4_loadDictHC(stream, compression_dict_data,
                 static_cast<int>(compression_dict_size));

#if LZ4_VERSION_NUMBER >= 10700  // r129+
  outlen =
      LZ4_compress_HC_continue(stream, input, &(*output)[output_header_len],
                               static_cast<int>(length), compress_bound);
#else   // r124-r128
  outlen = LZ4_compressHC_limitedOutput_continue(
      stream, input, &(*output)[output_header_len], static_cast<int>(length),
      compress_bound);
#endif  // LZ4_VERSION_NUMBER >= 10700
  LZ4_freeStreamHC(stream);

#elif LZ4_VERSION_MAJOR  // r113-r123
  outlen = LZ4_compressHC2_limitedOutput(input, &(*output)[output_header_len],
                                         static_cast<int>(length),
                                         compress_bound, opts.level);
#else                    // up to r112
  outlen =
      LZ4_compressHC_limitedOutput(input, &(*output)[output_header_len],
                                   static_cast<int>(length), compress_bound);
#endif                   // LZ4_VERSION_NUMBER >= 10400

  if (outlen == 0) {
    return false;
  }
  output->resize(static_cast<size_t>(output_header_len + outlen));
  return true;
#else  // LZ4
  return false;
#endif
}

#ifdef XPRESS
inline bool XPRESS_Compress(const char* input, size_t length,
                            std::string* output) {
  return port::xpress::Compress(input, length, output);
}
#else
inline bool XPRESS_Compress(const char* /*input*/, size_t /*length*/,
                            std::string* /*output*/) {
  return false;
}
#endif

#ifdef XPRESS
inline char* XPRESS_Uncompress(const char* input_data, size_t input_length,
                               int* decompress_size) {
  return port::xpress::Decompress(input_data, input_length, decompress_size);
}
#else
inline char* XPRESS_Uncompress(const char* /*input_data*/,
                               size_t /*input_length*/,
                               int* /*decompress_size*/) {
  return nullptr;
}
#endif

// @param dict_context Data for presetting the compression library's
//    dictionary.
inline bool ZSTD_Compress(const CompressionOptions& opts, const char* input,
                          size_t length, ::std::string* output,
                          const DictContext& dict_context) {
#ifdef ZSTD
  if (length > std::numeric_limits<uint32_t>::max()) {
    // Can't compress more than 4GB
    return false;
  }

  size_t output_header_len = compression::PutDecompressedSizeInfo(
      output, static_cast<uint32_t>(length));

  size_t compressBound = ZSTD_compressBound(length);
  output->resize(static_cast<size_t>(output_header_len + compressBound));
  size_t outlen = 0;
#if ZSTD_VERSION_NUMBER >= 500  // v0.5.0+
  ZSTD_CCtx* context = ZSTD_createCCtx();
#if ZSTD_VERSION_NUMBER >= 700  // v0.7.0+
  if (dict_context.zstd_cdict != nullptr) {
    outlen = ZSTD_compress_usingCDict(context, &(*output)[output_header_len],
                                      compressBound, input, length,
                                      dict_context.zstd_cdict);
  }
#endif  // ZSTD_VERSION_NUMBER >= 700
  if (outlen == 0) {
    outlen = ZSTD_compress_usingDict(
        context, &(*output)[output_header_len], compressBound, input, length,
        dict_context.raw_dict_bytes.data(), dict_context.raw_dict_bytes.size(),
        opts.level);
  }
  ZSTD_freeCCtx(context);
#else  // up to v0.4.x
  outlen = ZSTD_compress(&(*output)[output_header_len], compressBound, input,
                         length, opts.level);
#endif  // ZSTD_VERSION_NUMBER >= 500
  if (outlen == 0) {
    return false;
  }
  output->resize(output_header_len + outlen);
  return true;
#else // ZSTD
  return false;
#endif
}

// @param compression_dict Data for presetting the compression library's
//    dictionary.
inline char* ZSTD_Uncompress(const char* input_data, size_t input_length,
                             int* decompress_size,
                             const Slice& compression_dict = Slice()) {
#ifdef ZSTD
  uint32_t output_len = 0;
  if (!compression::GetDecompressedSizeInfo(&input_data, &input_length,
                                            &output_len)) {
    return nullptr;
  }

  char* output = new char[output_len];
  size_t actual_output_length;
#if ZSTD_VERSION_NUMBER >= 500  // v0.5.0+
  ZSTD_DCtx* context = ZSTD_createDCtx();
  actual_output_length = ZSTD_decompress_usingDict(
      context, output, output_len, input_data, input_length,
      compression_dict.data(), compression_dict.size());
  ZSTD_freeDCtx(context);
#else  // up to v0.4.x
  actual_output_length =
      ZSTD_decompress(output, output_len, input_data, input_length);
#endif  // ZSTD_VERSION_NUMBER >= 500
  assert(actual_output_length == output_len);
  *decompress_size = static_cast<int>(actual_output_length);
  return output;
#else // ZSTD
  return nullptr;
#endif
}

inline std::string ZSTD_TrainDictionary(const std::string& samples,
                                        const std::vector<size_t>& sample_lens,
                                        size_t max_dict_bytes) {
  // Dictionary trainer is available since v0.6.1, but ZSTD was marked stable
  // only since v0.8.0. For now we enable the feature in stable versions only.
#if ZSTD_VERSION_NUMBER >= 800  // v0.8.0+
  std::string dict_data(max_dict_bytes, '\0');
  size_t dict_len = ZDICT_trainFromBuffer(
      &dict_data[0], max_dict_bytes, &samples[0], &sample_lens[0],
      static_cast<unsigned>(sample_lens.size()));
  if (ZDICT_isError(dict_len)) {
    return "";
  }
  assert(dict_len <= max_dict_bytes);
  dict_data.resize(dict_len);
  return dict_data;
#else   // up to v0.7.x
  assert(false);
  return "";
#endif  // ZSTD_VERSION_NUMBER >= 800
}

inline std::string ZSTD_TrainDictionary(const std::string& samples,
                                        size_t sample_len_shift,
                                        size_t max_dict_bytes) {
  // Dictionary trainer is available since v0.6.1, but ZSTD was marked stable
  // only since v0.8.0. For now we enable the feature in stable versions only.
#if ZSTD_VERSION_NUMBER >= 800  // v0.8.0+
  // skips potential partial sample at the end of "samples"
  size_t num_samples = samples.size() >> sample_len_shift;
  std::vector<size_t> sample_lens(num_samples, 1 << sample_len_shift);
  return ZSTD_TrainDictionary(samples, sample_lens, max_dict_bytes);
#else   // up to v0.7.x
  assert(false);
  return "";
#endif  // ZSTD_VERSION_NUMBER >= 800
}

}  // namespace rocksdb
