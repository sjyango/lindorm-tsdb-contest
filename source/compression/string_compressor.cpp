#include "compression/string_compressor.h"

namespace LindormContest::compression {

    uint32_t CompressionCodecZSTD::compress(const char *source, uint32_t source_size, char *dest) const {
        ZSTD_CCtx *cctx = ZSTD_createCCtx();
        ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel, 1);
        ZSTD_CCtx_setParameter(cctx, ZSTD_c_strategy, ZSTD_greedy);
        size_t compressed_size = ZSTD_compress2(cctx, dest, ZSTD_compressBound(source_size), source, source_size);
        ZSTD_freeCCtx(cctx);

        if (ZSTD_isError(compressed_size)) {
            throw "Error on compressing";
        }
        return static_cast<uint32_t>(compressed_size);
    }


    void CompressionCodecZSTD::decompress(const char *source, uint32_t source_size, char *dest,
                                          uint32_t uncompressed_size) const {
        size_t res = ZSTD_decompress(dest, uncompressed_size, source, source_size);

        if (ZSTD_isError(res)) {
            throw "Error on decompressing";
        }
    }

    uint32_t CompressionCodecBrotli::compress(const char *source, uint32_t source_size, char *dest) const {
        size_t encode_size;
        bool encode_res = BrotliEncoderCompress(5, BROTLI_DEFAULT_WINDOW, BROTLI_DEFAULT_MODE,
                                                source_size, reinterpret_cast<const uint8_t *>(source), &encode_size,
                                                reinterpret_cast<uint8_t *>(dest));
        assert(encode_res);
        return encode_size;
    }

    void CompressionCodecBrotli::decompress(const char *source, uint32_t source_size,
                                            char *dest, uint32_t uncompressed_size) const {
        size_t decode_size;
        auto decode_res = BrotliDecoderDecompress(source_size, reinterpret_cast<const uint8_t *>(source), &decode_size,
                                                  reinterpret_cast<uint8_t *>(dest));
        assert(decode_size == uncompressed_size);
        assert(decode_res == BROTLI_DECODER_RESULT_SUCCESS);
    }
    
    /// Store the compressed corpus. Returns the compressed size
    uint32_t CompressionCodecFsst::compress(const std::vector<std::string> &source, uint32_t source_size, char* dest) {
        /// The decode
        fsst_decoder_t decoder;
        /// The compressed data
        std::vector<unsigned char> compressedData;
        /// The offsets
        std::vector<unsigned> offsets;
    
        std::vector<unsigned long> rowLens, compressedRowLens;
        std::vector<unsigned char*> rowPtrs, compressedRowPtrs;
        rowLens.reserve(source.size());
        compressedRowLens.resize(source.size());
        rowPtrs.reserve(source.size());
        compressedRowPtrs.resize(source.size() + 1);
        unsigned totalLen = 0;
        for (auto& d : source) {
            totalLen += d.size();
            rowLens.push_back(d.size());
            rowPtrs.push_back(reinterpret_cast<unsigned char*>(const_cast<char*>(d.data())));
        }
        
        auto encoder = fsst_create(source.size(), rowLens.data(), rowPtrs.data(), false);

        std::vector<unsigned char> compressionBuffer;
        compressionBuffer.resize(16 + 2 * totalLen);

        fsst_compress(encoder, source.size(), rowLens.data(), rowPtrs.data(), compressionBuffer.size(), compressionBuffer.data(), compressedRowLens.data(), compressedRowPtrs.data());

        unsigned long compressedLen = source.empty() ? 0 : (compressedRowPtrs[source.size() - 1] + compressedRowLens[source.size() - 1] - compressionBuffer.data());

        compressedData.resize(compressedLen + 8192);
        memcpy(compressedData.data(), compressionBuffer.data(), compressedLen);
        offsets.reserve(source.size());
        compressedRowPtrs[source.size()] = compressionBuffer.data() + compressedLen;
        for (unsigned index = 0, limit = source.size(); index != limit; ++index){
            offsets.push_back(compressedRowPtrs[index + 1] - compressionBuffer.data());
        }
        uint64_t result = compressedData.size() /*+ (offsets.size() * sizeof(unsigned))*/;
        {
            unsigned char buffer[sizeof(fsst_decoder_t)];
            unsigned dictLen = fsst_export(encoder, buffer);
            fsst_destroy(encoder);
            result += dictLen;

            fsst_import(&decoder, buffer);
        }
        // layout: |offset1 offset2 | decoder | internal_offsets | compressedData |
        size_t of1, of2, init = 2 * sizeof(size_t), dataSize = compressedLen;
        of1 = init + sizeof(decoder);
        of2 = of1 + (offsets.size() * sizeof(offsets[0]));

        std::cout << "data size: " << result;
        // dump to dest
        memcpy(dest, &of1, sizeof(of1));
        memcpy(dest + sizeof(of1), &of2, sizeof(of2));
        memcpy(dest + init, &decoder, sizeof(decoder));
        memcpy(dest + of1, offsets.data(), offsets.size() * sizeof(offsets[0]));
        memcpy(dest + of2, compressedData.data(), dataSize);
        return of2 + dataSize;
    }

    /// Decompress a single row. The target buffer is guaranteed to be large enough
    void CompressionCodecFsst::decompress(const char *source, uint32_t source_size, std::vector<std::string> &dest, uint32_t uncompressed_size) const {
        
        /// The decode
        fsst_decoder_t decoder;
        /// The compressed data
        std::vector<unsigned char> compressedData;
        /// The offsets
        std::vector<unsigned> offsets;
        // Recover meta data
        size_t of1, of2, init = 2 * sizeof(size_t);
        memcpy(&of1,source, sizeof(size_t));
        memcpy(&of2,source + sizeof(size_t), sizeof(size_t));
        memcpy(&decoder, source + init, sizeof(fsst_decoder_t));
        
        offsets.reserve(of2 - of1);
        memcpy(offsets.data(), source + of1, of2 - of1);
        compressedData.reserve(source_size - of2);
        memcpy(compressedData.data(), source + of2, source_size - of2);
        
        char* writer = reinterpret_cast<char*>(dest.data());
        auto limit = writer + uncompressed_size;
        auto data = compressedData.data();
        unsigned len = 0, lenSum = 0;
        unsigned start, end = 0;
        size_t i = 0;

        for(auto numPtr = offsets.data(); lenSum < uncompressed_size ;numPtr++) {
            start = end;
            end = *numPtr;
            
            std::string tmp;
            // expand to maximum
            tmp.reserve(100 + 1);
            len = fsst_decompress(&decoder, end - start, data + start, limit - writer,
                                  reinterpret_cast<unsigned char*>(tmp.data()));

            dest[i].resize(len);
            strcpy(dest[i].data(),tmp.data());
            i++;
            lenSum += len;
        }
        
    }
}