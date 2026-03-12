#pragma once

#include <cstdint>
#include <cstddef>
#include <type_traits>
#include <string>
#include <string_view>
#include <fstream>
#include <filesystem>
#include <vector>
#include <tuple>
#include <initializer_list>
#include <utility>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdexcept>
#include <cstring>
#include <cerrno>
#include <cassert>
#include <span>
#ifdef MMAPPET_USE_UNIX_FILEOPS
#include <sys/types.h>
#endif


template<typename T, typename U>
constexpr bool is_compatible_type()
{
    using T_plain = std::decay_t<T>;
    using U_plain = std::decay_t<U>;
    return sizeof(T_plain) == sizeof(U_plain) && (
           (std::is_integral<T_plain>::value && std::is_integral<U_plain>::value &&
            (std::is_signed<T_plain>::value == std::is_signed<U_plain>::value)) ||
           (std::is_floating_point<T_plain>::value && std::is_floating_point<U_plain>::value)
           );
}

template<typename T>
#if defined(__cpp_lib_constexpr_string) && __cpp_lib_constexpr_string >= 201907L
constexpr
#endif
std::string get_type_str()
{
    if constexpr (is_compatible_type<T, uint8_t>()) return "uint8";
    else if constexpr (is_compatible_type<T, int8_t>()) return "int8";
    else if constexpr (is_compatible_type<T, uint16_t>()) return "uint16";
    else if constexpr (is_compatible_type<T, int16_t>()) return "int16";
    else if constexpr (is_compatible_type<T, uint32_t>()) return "uint32";
    else if constexpr (is_compatible_type<T, int32_t>()) return "int32";
    else if constexpr (is_compatible_type<T, uint64_t>()) return "uint64";
    else if constexpr (is_compatible_type<T, int64_t>()) return "int64";
    else if constexpr (is_compatible_type<T, float>()) return "float32";
    else if constexpr (is_compatible_type<T, double>()) return "float64";
    else return "bytes" + std::to_string(sizeof(T));
}

template<typename T>
class MMappedData {
    T* mappedData = nullptr;
    int fileDescriptor = -1;
    size_t dataSize = 0;
    size_t no_elements = 0;
    const std::filesystem::path filepath;
    int open_flags;
    int mmap_prot;
    int mmap_flags;

public:
    MMappedData(const std::filesystem::path& filepath, int open_flags = O_RDONLY, int mmap_prot = PROT_READ, int mmap_flags = MAP_SHARED) :
        filepath(filepath),
        open_flags(open_flags),
        mmap_prot(mmap_prot),
        mmap_flags(mmap_flags)
    {
        open_and_map(open_flags, mmap_prot, mmap_flags);
    }

    void open_and_map(int open_flags, int mmap_prot, int mmap_flags)
    {
        dataSize = std::filesystem::file_size(filepath);
        if(dataSize % sizeof(T) != 0)
            throw std::runtime_error("File size is not a multiple of element size for file: " + filepath.string());
        no_elements = dataSize / sizeof(T);

        fileDescriptor = open(filepath.c_str(), open_flags);
        if (fileDescriptor == -1)
            throw std::runtime_error("Failed to open file: " + filepath.string() + ", error: " + std::strerror(errno));

        if (no_elements == 0) {
            // Empty dataset, avoid mmap call, which would fail
            mappedData = nullptr;
            return;
        }

        void* raw = mmap(nullptr, dataSize, mmap_prot, mmap_flags, fileDescriptor, 0);
        if (raw == MAP_FAILED)
        {
            close(fileDescriptor);
            fileDescriptor = -1;
            throw std::runtime_error("Failed to mmap file: " + filepath.string() + ", error: " + std::strerror(errno));
        }
        mappedData = static_cast<T*>(raw);
    }

    void close_and_unmap() noexcept
    {
        if (mappedData)
        {
            munmap(mappedData, dataSize);
            mappedData = nullptr;
        }
        if (fileDescriptor != -1)
        {
            close(fileDescriptor);
            fileDescriptor = -1;
        }
    }

    void resize(size_t new_no_elements)
    {
        close_and_unmap();
        dataSize = new_no_elements * sizeof(T);
        std::filesystem::resize_file(filepath, dataSize);
        open_and_map(open_flags, mmap_prot, mmap_flags);
    }

    ~MMappedData() noexcept
    {
        close_and_unmap();
    }

    MMappedData(const MMappedData&) = delete;
    MMappedData& operator=(const MMappedData&) = delete;
    MMappedData(MMappedData&& other) noexcept :
        mappedData(other.mappedData),
        fileDescriptor(other.fileDescriptor),
        dataSize(other.dataSize),
        no_elements(other.no_elements)
    {
        other.mappedData = nullptr;
        other.fileDescriptor = -1;
        other.dataSize = 0;
        other.no_elements = 0;
    }
    MMappedData& operator=(MMappedData&&) noexcept = delete;

    inline T& operator[](size_t index) const {

        return mappedData[index];
    }

    inline size_t size() const noexcept {
        return no_elements;
    }

    inline T* data() const noexcept {
        return mappedData;
    }
};


template<typename... Args>
class Dataset {
public:
    Dataset(const std::filesystem::path&, std::vector<std::pair<std::string,std::string>>, size_t, int, int, int)
    {
        // Base case: do nothing
    }

    std::tuple<> move_columns()
    {
        return std::tuple<>();
    }

    std::tuple<> operator[](size_t)
    {
        return std::tuple<>();
    }

    void resize(size_t)
    {
        // Base case: do nothing
    }
};

static inline std::pair<std::string, std::string>
split_first_space(const std::string& s);

template<typename T>
MMappedData<T> OpenColumn(const std::filesystem::path& filepath, const std::string column_name, int open_flags = O_RDONLY, int mmap_prot = PROT_READ, int mmap_flags = MAP_SHARED)
{
    std::ifstream file;
    file.open(filepath / "schema.txt", std::ios::in | std::ios::binary);
    if(!file.is_open())
        throw std::runtime_error("Failed to open schema file: " + (filepath / "schema.txt").string() + ", error: " + std::strerror(errno));

    std::string s;
    size_t col_nr = 0;
    bool found = false;
    while(std::getline(file, s))
    {
        if(s.empty())
            continue;
        auto [type_str, col_name] = split_first_space(s);
        if(col_name == column_name)
        {
            if(type_str != get_type_str<T>())
                throw std::runtime_error("Type mismatch for column '" + column_name +
                                         "': expected " + get_type_str<T>() +
                                         ", got " + type_str);
            found = true;
            break;
        }
        ++col_nr;
    }
    file.close();

    if(!found)
        throw std::runtime_error("Column '" + column_name + "' not found in schema file: " + (filepath / "schema.txt").string());

    return MMappedData<T>(filepath / (std::to_string(col_nr) + ".bin"), open_flags, mmap_prot, mmap_flags);
}

template<typename T, typename... Args>
class Dataset<T, Args...>
{
    const std::string type_str;
    const std::string column_name;
    const size_t column_number;
    MMappedData<T> data;
    Dataset<Args...> next_dataset;

public:

    Dataset(const std::filesystem::path& filepath,
            const std::vector<std::pair<std::string, std::string>>& type_strs,
            size_t col_nr,
            int open_flags = O_RDONLY,
            int mmap_prot = PROT_READ,
            int mmap_flags = MAP_SHARED
        ) :
        type_str(type_strs[col_nr].first),
        column_name(type_strs[col_nr].second),
        column_number(col_nr),
        data(filepath / (std::to_string(col_nr) + ".bin"), open_flags, mmap_prot, mmap_flags),
        next_dataset(filepath, type_strs, col_nr + 1, open_flags, mmap_prot, mmap_flags)
    {
        if(type_str != get_type_str<T>())
            throw std::runtime_error("Type mismatch for column " + std::to_string(column_number) +
                                     ": expected " + get_type_str<T>() +
                                     ", got " + type_str);
        if constexpr (sizeof...(Args) > 0)
            if(next_dataset.size() != data.size())
                throw std::runtime_error("Column size mismatch between column " + std::to_string(column_number) +
                                     " and column " + std::to_string(column_number + 1));
    }

    template <size_t colnr>
    auto& get_column()
    {
        if constexpr (colnr == 0) {
            return data;
        } else {
            return next_dataset.template get_column<colnr - 1>();
        }
    }

    inline size_t size() const noexcept
    {
        return data.size();
    }

    void resize(size_t new_size)
    {
        data.resize(new_size);
        next_dataset.resize(new_size);
    }

    auto move_columns()
    {
        return std::tuple_cat(std::make_tuple(std::move(data)), next_dataset.move_columns());
    }

    std::tuple<T, Args...> operator[](size_t index)
    {
        return std::tuple_cat(std::make_tuple(data[index]), next_dataset[index]);
    }

    class Iterator {
        size_t index;
        Dataset<T, Args...>* dataset;
    public:
        Iterator(size_t idx, Dataset<T, Args...>* ds) : index(idx), dataset(ds) {};
        inline std::tuple<T, Args...> operator*() {
            return (*dataset)[index];
        }
        inline Iterator& operator++() {
            ++index;
            return *this;
        }
        inline bool operator!=(const Iterator& other) const {
            return index != other.index;
        }
    };

    Iterator begin() {
        return Iterator(0, this);
    }
    Iterator end() {
        return Iterator(data.size(), this);
    }
};

static inline std::pair<std::string, std::string>
split_first_space(const std::string& s) {
    size_t pos = s.find(' ');
    if (pos == std::string::npos) {
        return {s, ""};
    }
    return { s.substr(0, pos), s.substr(pos + 1) };
}


template<typename T, typename... Args>
auto OpenDataset(const std::filesystem::path& filepath, std::initializer_list<std::string> column_names, int open_flags = O_RDONLY, int mmap_prot = PROT_READ, int mmap_flags = MAP_SHARED)
{
    std::ifstream file;
    file.open(filepath / "schema.txt", std::ios::in | std::ios::binary);
    if(!file.is_open())
        throw std::runtime_error("Failed to open schema file: " + (filepath / "schema.txt").string() + ", error: " + std::strerror(errno));

    std::vector<std::pair<std::string, std::string>> tmp_type_strs;
    std::string s;
    while(std::getline(file, s))
    {
        if(s.empty())
            continue;
        size_t pos = s.find(' ');
        if (pos == std::string::npos)
            tmp_type_strs.emplace_back(s, "");
        else
            tmp_type_strs.emplace_back(s.substr(0, pos), s.substr(pos + 1));
    }
    file.close();

    if(column_names.size() != tmp_type_strs.size())
        throw std::runtime_error("Number of column names provided as argument does not match number of columns in file.");

    size_t ii = 0;
    for(auto it = column_names.begin(); it != column_names.end(); ++it, ++ii)
    {
        if(*it != tmp_type_strs[ii].second)
            throw std::runtime_error("Column name mismatch at column " + std::to_string(ii) +
                                     ": expected '" + *it +
                                     "', got '" + tmp_type_strs[ii].second + "'");
    }
    return Dataset<T, Args...>(filepath, tmp_type_strs, 0, open_flags, mmap_prot, mmap_flags);
}



template<size_t idx, typename T, typename... Args>
std::string schema_string_impl(const std::vector<std::string>& column_names)
{
    std::string result = get_type_str<T>() + " " + column_names[idx] + "\n";
    if constexpr (sizeof...(Args) == 0)
    {
        return result;
    }
    else
    {
        result += schema_string_impl<idx + 1, Args...>(column_names);
        return result;
    }
}

template<typename... Args>
class DatasetWriter {
public:
    DatasetWriter(const std::filesystem::path&, size_t) {}
    void write_row() {}
    void write_rows(size_t) {}
};

template<typename T, typename... Args>
class DatasetWriter<T, Args...> {
    #ifdef MMAPPET_USE_UNIX_FILEOPS
    int file_descriptor;
    #else
    std::ofstream file;
    #endif
    DatasetWriter<Args...> next_writer;

public:
    DatasetWriter(const std::filesystem::path& filepath, size_t col_nr) :
        #if !defined(MMAPPET_USE_UNIX_FILEOPS)
        file(),
        #endif
        next_writer(filepath, col_nr + 1)
    {
        #ifdef MMAPPET_USE_UNIX_FILEOPS
        file_descriptor = open((filepath / (std::to_string(col_nr) + ".bin")).c_str(), O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
        if (file_descriptor == -1)
            throw std::runtime_error("Failed to open file for writing: " + (filepath / (std::to_string(col_nr) + ".bin")).string() + ", error: " + std::strerror(errno));
        #else
        file.open(filepath / (std::to_string(col_nr) + ".bin"), std::ios::out | std::ios::binary | std::ios::trunc);
        if(!file.is_open())
            throw std::runtime_error("Failed to open file for writing: " + (filepath / (std::to_string(col_nr) + ".bin")).string() + ", error: " + std::strerror(errno));
        file.exceptions(std::ofstream::failbit | std::ofstream::badbit);
        #endif
    }

    void write_row(const T& value, const Args&... args)
    {
        #ifdef MMAPPET_USE_UNIX_FILEOPS
        ssize_t bytes_written = write(file_descriptor, reinterpret_cast<const char*>(&value), sizeof(T));
        if (bytes_written != sizeof(T))
            throw std::runtime_error("Failed to write data to file: " + std::to_string(file_descriptor) + ", error: " + std::strerror(errno));
        #else
        file.write(reinterpret_cast<const char*>(&value), sizeof(T));
        #endif
        next_writer.write_row(args...);
    }

    void write_rows(size_t n, const T* values, const Args*... args)
    {
        #ifdef MMAPPET_USE_UNIX_FILEOPS
        ssize_t bytes_written = write(file_descriptor, reinterpret_cast<const char*>(values), n * sizeof(T));
        if (bytes_written != static_cast<ssize_t>(n * sizeof(T)))
            throw std::runtime_error("Failed to write data to file: " + std::to_string(file_descriptor) + ", error: " + std::strerror(errno));
        #else
        file.write(reinterpret_cast<const char*>(values), n * sizeof(T));
        #endif
        next_writer.write_rows(n, args...);
    }
};


template<typename T, typename... Args>
class IndexedDataset {
    Dataset<T, Args...> dataset;
    Dataset<size_t> index_data;
    size_t* index_ptr;
public:
    IndexedDataset(Dataset<T, Args...>&& ds,
                   Dataset<size_t>&& idx_data) :
        dataset(std::move(ds)),
        index_data(std::move(idx_data)),
        index_ptr(index_data.template get_column<0>().data())
    {}

    std::tuple<std::span<T>, std::span<Args>...> get_group(size_t group_index)
    {
        if(group_index >= number_of_groups())
            throw std::out_of_range("Group index out of range in IndexedDataset::get_group");
        size_t start = index_ptr[group_index];
        size_t end = index_ptr[group_index + 1];
        return get_group_impl<0, T, Args...>(start, end);
    }

    size_t number_of_groups() const noexcept {
        return index_data.size() > 0 ? index_data.size() - 1 : 0;
    }

private:
    template<size_t idx, typename U, typename... Rest>
    auto get_group_impl(size_t start, size_t end)
    {
        if constexpr (sizeof...(Rest) == 0)
        {
            return std::make_tuple(std::span<U>(dataset.template get_column<idx>().data() + start, end - start));
        }
        else
        {
            return std::tuple_cat(std::make_tuple(std::span<U>(dataset.template get_column<idx>().data() + start, end - start)),
                                  get_group_impl<idx + 1, Rest...>(start, end));
        }
    }
};



template<typename T, typename... Args>
class IndexedWriter {
    DatasetWriter<T, Args...> writer;
    DatasetWriter<size_t> index_writer;
    size_t current_index = 0;
public:
    IndexedWriter(DatasetWriter<T, Args...>&& w,
                  DatasetWriter<size_t>&& idx_w) :
        writer(std::move(w)),
        index_writer(std::move(idx_w))
    {
        // Start with index 0
        index_writer.write_row(0);
    }
    void write_group(size_t n, const T* values, const Args*... args)
    {
        writer.write_rows(n, values, args...);
        current_index += n;
        index_writer.write_row(current_index);
    }
    void write_group(const std::span<T>& values, const std::span<Args>&... args)
    {
        write_group(values.size(), values.data(), args.data()...);
    }
};


template<typename T, typename... Args>
class Schema
{
    std::vector<std::string> column_names;


    template<size_t idx, typename U, typename... Rest>
    std::string schema_string_impl() const
    {
        std::string result = get_type_str<U>() + " " + column_names[idx] + "\n";
        if constexpr (sizeof...(Rest) == 0)
        {
            return result;
        }
        else
        {
            result += schema_string_impl<idx + 1, Rest...>();
            return result;
        }
    }

    template<size_t... Is>
    auto open_dataset_impl(const std::filesystem::path& filepath,
                           int open_flags,
                           int mmap_prot,
                           int mmap_flags,
                           std::index_sequence<Is...>)
    {
        return OpenDataset<T, Args...>(filepath, {column_names[Is]...}, open_flags, mmap_prot, mmap_flags);
    }

    public:
    template<typename... Strings>
    Schema(const Strings&... col_names)
    { (column_names.push_back(col_names), ...);}

    auto open_dataset(const std::filesystem::path& filepath, bool readonly = true)
    {
        int open_flags = readonly ? O_RDONLY : O_RDWR;
        int mmap_prot = readonly ? PROT_READ : (PROT_READ | PROT_WRITE);
        int mmap_flags = MAP_SHARED;
        return open_dataset_flags(filepath, open_flags, mmap_prot, mmap_flags);
    }

    auto open_dataset_flags(const std::filesystem::path& filepath,
                           int open_flags,
                           int mmap_prot,
                           int mmap_flags)
    {
        return open_dataset_impl(filepath, open_flags, mmap_prot, mmap_flags, std::make_index_sequence<sizeof...(Args)+1>{});
    }

    auto open_indexed_dataset(const std::filesystem::path& filepath, bool readonly = true)
    {
        int open_flags = readonly ? O_RDONLY : O_RDWR;
        int mmap_prot = readonly ? PROT_READ : (PROT_READ | PROT_WRITE);
        int mmap_flags = MAP_SHARED;
        auto ds = open_dataset_flags(filepath, open_flags, mmap_prot, mmap_flags);
        auto index_ds = OpenDataset<size_t>(filepath / "index.mmappet", {"Index"}, O_RDONLY, PROT_READ, MAP_SHARED);
        return IndexedDataset<T, Args...>(std::move(ds), std::move(index_ds));
    }


    auto get_columns(const std::filesystem::path& filepath, bool readonly = true)
    {
        auto dataset = open_dataset(filepath, readonly);
        return dataset.move_columns();
    }

    std::string schema_string() const
    {
        return schema_string_impl<0, T, Args...>();
    }

    void write_schema_file(const std::filesystem::path& filepath) const
    {
        std::ofstream file;
        file.open(filepath, std::ios::out |  std::ios::trunc | std::ios::binary);
        if(!file.is_open())
            throw std::runtime_error("Failed to open schema file for writing: " + filepath.string() + ", error: " + std::strerror(errno));
        file << schema_string();
        file.close();
    }

    DatasetWriter<T, Args...> create_writer(const std::filesystem::path& filepath)
    {
        std::filesystem::create_directories(filepath);
        write_schema_file(filepath / "schema.txt");
        return DatasetWriter<T, Args...>(filepath, 0);
    }

    IndexedWriter<T, Args...> create_indexed_writer(const std::filesystem::path& filepath)
    {
        auto writer = create_writer(filepath);
        Schema<size_t> index_schema("Index");
        DatasetWriter<size_t> index_writer = index_schema.create_writer(filepath / "index.mmappet");
        return IndexedWriter<T, Args...>(std::move(writer), std::move(index_writer));
    }

};
