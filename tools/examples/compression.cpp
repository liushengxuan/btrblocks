// ------------------------------------------------------------------------------
// Simple Example: Compress Columns with btrblocks
// ------------------------------------------------------------------------------
#include "btrblocks.hpp"
#include "common/Log.hpp"
#include "storage/MMapVector.hpp"
#include "storage/Relation.hpp"

#include <chrono>
#include <iostream>
#include <fstream>
#include <filesystem>

// ------------------------------------------------------------------------------
template<typename T>
btrblocks::Vector<T> generateData(size_t size, size_t unique, size_t runlength, int seed = 42) {
    btrblocks::Vector<T> data(size);
    std::mt19937 gen(seed);
    for (auto i = 0u; i < size - runlength; ++i) {
        auto number = static_cast<T>(gen() % unique);
        for (auto j = 0u; j != runlength; ++j,++i) {
            data[i] = number;
        }
    }
    return data;
}

template<typename T>
btrblocks::Vector<T> loadData(const char* filepath) {
  btrblocks::Vector<T> data("/data00/velox_reader_benchmark/parquet_playground/6289ac/orderkey/_6289ac_0.txt", sizeof(T));
  return data;
}

// ------------------------------------------------------------------------------
template <typename T>
bool validateData(size_t size, T* input, T* output) {
    for (auto i = 0u; i != size; ++i) {
        if (input[i] != output[i]) {
            std::cout << "value @" << i << " does not match; in " << input[i] << " vs out"
                      << output[i] << std::endl;
            return false;
        }
    }
    return true;
}
// ------------------------------------------------------------------------------
int main(int argc, char *argv[]) {
    using namespace btrblocks;
    std::vector<std::string> fullFilePaths;
    std::string column;
    std::string folder_path_base = "/data00/velox_reader_benchmark/parquet_playground/6289ac/";

    std::unordered_map<std::string, uint64_t> uncompressedSizes, compressedSizes, compressTimes, uncompressTimes;
    // required before interacting with btrblocks
    // the passed function is optional and can be used to modify the
    // configuration before BtrBlocks initializes itself
    BtrBlocksConfig::configure([&](BtrBlocksConfig &config){
        if (argc > 1) {
//            auto max_depth  = std::atoi(argv[1]);
//            std::cout << "setting max cascade depth to " << max_depth << std::endl;
//            config.integers.max_cascade_depth = max_depth;
//            config.doubles.max_cascade_depth = max_depth;
//            config.strings.max_cascade_depth = max_depth;
            column = argv[1];
        }
        config.doubles.schemes.enable(DoubleSchemeType::DOUBLE_BP);
    });

    // If compiled with BTR_FLAG_LOGGING (cmake -DWITH_LOGGING=ON ..),
    // this will set the log level to info; otherwise, it is a no-op.
    // For even more info, set it to 'debug'.
    Log::set_level(Log::level::info);

    // -------------------------------------------------------------------------------------
    // compression
    // -------------------------------------------------------------------------------------


    std::string folder_path = folder_path_base + column + "/";
    for (const auto &entry : std::filesystem::directory_iterator(folder_path)) {
      if (!entry.is_regular_file()) continue;
      std::ifstream file(entry.path());
      fullFilePaths.push_back(folder_path + entry.path().filename().string());
    }

    Relation to_compress;

    for(auto file: fullFilePaths) {
      to_compress.addColumn({file, loadData<int>(file.c_str())});
    }


//    // usually we would split up the data into multiple chunks here using Relation::getRanges
//    // and then compress each one individually (in parallel).
//    // Here, we just compress the whole column at once.
    Range range(0, to_compress.tuple_count);
    Chunk input = to_compress.getChunk({range}, 0);
    Datablock compressor(to_compress);
//
//    // allocate some memory for the output; if this is passed as null,
//    // the compressor will allocate the memory itself, estimating required space
//    // passing too little memory here can lead to a crash/UB; memory bounds are not checked.
    std::unique_ptr<uint8_t[]> output(new uint8_t[input.tuple_count * sizeof(double) * 2]);
//
//    // compress the data; return value contains some statistics about the
//    // overall compression, used schemes and individual columns
    auto stats = compressor.compress(input, output);
//
//    // compile with BTR_FLAG_LOGGING (cmake -DWITH_LOGGING=ON ..) to
//    // get more insights into the compression process
//    // the
    std::cout << "Stats:" <<  std::endl
        << "- input size " << input.size_bytes() << std::endl
        << "- output size " << stats.total_data_size << std::endl
        << "- compression ratio " << stats.compression_ratio << std::endl
        ;


    // -------------------------------------------------------------------------------------
    // decompression
    // -------------------------------------------------------------------------------------
    auto startTime = std::chrono::high_resolution_clock::now();
    Chunk decompressed = compressor.decompress(output);

    // check if the decompressed data is the same as the original data
    bool check;

    for (auto col = 0u; col != to_compress.columns.size(); ++col) {
        auto& orig = input.columns[col];
        auto& decomp = decompressed.columns[col];
//        switch (to_compress.columns[col].type) {
//            case ColumnType::INTEGER:
//              check = validateData(size, reinterpret_cast<int32_t*>(orig.get()),
//                                   reinterpret_cast<int32_t*>(decomp.get()));
//              break;
//            case ColumnType::DOUBLE:
//              check = validateData(size, reinterpret_cast<double*>(orig.get()),
//                                   reinterpret_cast<double*>(decomp.get()));
//              break;
//            default:
//              UNREACHABLE();
//        }
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    auto time = std::chrono::duration_cast<std::chrono::microseconds>(
                    endTime - startTime)
                    .count();
    std::cout << "decompression time: " << time << " us" <<std::endl;
    std::cout << "decompression throughput: " << (double) stats.total_data_size/1024/1024/(double)time *1000*1000 << " MB/s" << std::endl;



    return !check;

}
// ------------------------------------------------------------------------------
