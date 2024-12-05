#pragma once
// clang-format off
#include <cstdint>
#include <vector>
#include <string>
#include <iostream>
#include <utility>
#include <tuple>
#include <functional>
#include <fstream>
#include <sstream>
#include <cmath>
#include <type_traits>
#include <cstdio>
#include <filesystem>

namespace distribsys{

using std::min;
using std::max;
using std::ceil;
using i64 = int64_t;
using u64 = uint64_t;
using u32 = uint32_t;
using i32 = int32_t;
using u16 = uint16_t;
using i16 = int16_t;

using u8 = uint8_t;
using i8 = int8_t;
template<typename T>
using uptr = std::unique_ptr<T>;
template<typename T>
using sptr = std::shared_ptr<T>;



inline i64 mod(i64 a, i64 b) {
    return (a % b + b) % b;
}
inline i64 ceil_div(i64 a, i64 b) {
    return (a + b - 1) / b;
}
inline i64 ceil_mult_of(i64 a, i64 b) {
    return ceil_div(a, b) * b;
}
constexpr i64 ceil_div_const(i64 a, i64 b) {
    return (a + b - 1) / b;
}
constexpr i64 ceil_mult_of_const(i64 a, i64 b) {
    return ceil_div_const(a, b) * b;
}
inline i64 ceil_double_div(double a, double b) {
    return std::ceil(a / b);
}
constexpr double ceil_const(double x) {
    i64 x_int = static_cast<i64>(x);
    return (x > x_int) ? x_int + 1 : x_int;
}
constexpr i64 ceil_double_div_const(double a, double b) {
    return ceil_const(a / b);
}




const u64 u64_bits = 64;
const u64 bits_in_byte = 8;
const u64 full_u64_mask = ~(0ULL);



//handles the case when b>=64
inline u64 safe_lbitshift(u64 a, u64 b) {
    return (b >= 64) ? 0 : a << b;
}
//gets bitmask of length len, safe for len>=64
inline u64 bitmask_len(u64 len) {
    return safe_lbitshift(1, len) - 1;
}
//ORs the bit at position bit in bitvector with state
template <template <typename> typename V>
inline void set_bitvector_bit(V<u64>& bitvector, u64 bit,bool state) {
    bitvector[bit / 64] |= (1ULL << (bit % 64))*state;
}
template <template <typename> typename V>
inline bool get_bitvector_bit(V<u64>& bitvector, u64 bit) {
    return (bitvector[bit / 64] >> (bit % 64)) & 1;
}
//copies len bits from `from` to `to`, starting at `start_from` and `start_to` respectively
//~ no check that it fits in V_to / exists fully in V_from
template<template <typename> typename V_from, template <typename> typename V_to>
inline void copy_bitvector(V_from<u64>& from,V_to<u64>& to, u64 start_from, i64 len, u64 start_to){
    if(len<=0){
        return;
    }
    u64 off_from = start_from%u64_bits;
    u64 off_to = start_to%u64_bits;
    i64 end_from = start_from+len;
    if(off_to!=0){//make sure off_to is 0
        to[start_to/u64_bits] &= bitmask_len(off_to);//leave only lower bits
        to[start_to/u64_bits] |= (
                (from[start_from/u64_bits]>>off_from)//only top bits
                & bitmask_len(len)//only as much bits as len
            )<<off_to;//shift to correct position

        u64 num=min(64-off_from,64-off_to);//actual amount of bits copied
        //update all of these
        start_from+=num;
        start_to+=num;
        len-=num;
        off_from = start_from%u64_bits;
        off_to = start_to%u64_bits;

        //if off_to is still not 0, then we consumed 64-off_from bits (or len), 
        //get more until off_to is 0
        if(off_to!=0 && len>0){
            to[start_to/u64_bits] &= bitmask_len(off_to);
            to[start_to/u64_bits] |= (//repeat like before
                    (from[start_from/u64_bits]>>off_from)
                    & bitmask_len(len)
                )<<off_to;

            start_from+=64-off_to;
            start_to+=64-off_to;
            len-=64-off_to;
            off_from = start_from%u64_bits;
            //off_to is now known to be 0, unused from here on
        }
    }
    if(len<=0){
        return;
    }
    u64 j=start_to;
    if(off_from==0){
        //only len-64 are guaranteed to be full, so no bitmask is needed
        //end_from-64 == start_from+len-64
        i64 i=start_from;
        for(; i < end_from-64; i+=64){
            to[j/u64_bits] = from[i/u64_bits];
            j+=64;
        }
        //j/u64_bits is guaranteed to point to the last u64 in "to"
        //bitshift by len%64 since start_from%64==0 and i%64==0 and we want (start_from+len-i)
        //could also bitshift by mod(len-1,64)+1, but end_from-i is simpler
        to[j/u64_bits] = from[i/u64_bits] & bitmask_len(end_from-i);
    }else{
        i64 i=start_from;
        //case when off_from!=0, so need to do two accesses per each "to" u64
        //off_from and off_from2 never change
        off_from = i%u64_bits;//offset for first access; length of second access
        u64 off_from2 = 64-off_from;//offset for second access; length of first access
        //like before, only len-64 are guaranteed to be full, so no bitmask is needed
        for(; i < end_from-64;){
            to[j/u64_bits] = (from[i/u64_bits]>>off_from);//put top "from" bits into bottom of "to"
            i+=off_from2;
            // to[j/u64_bits] |= (from[i/u64_bits]&(full_u64_mask>>off_from2))<<off_from2;
            //no bitmask needed since bitshift already removes top bits
            to[j/u64_bits] |= (from[i/u64_bits]<<off_from2);
            i+=off_from;//by here i+=64
            j+=64;
        }
        //do one more iteration but with len masking and second access only if len>0
        //j/u64_bits is guaranteed to point to the last u64 in "to"
        //start+len-i == len from position i
        to[j/u64_bits] = (from[i/u64_bits]>>off_from) & bitmask_len(end_from-i);
        i+=off_from2;
        if(i<end_from){//only do second access if there are bits left
            to[j/u64_bits] |= (from[i/u64_bits] & bitmask_len(end_from-i))<<off_from2;
        }
    }
}




//todo: c++20 add conditional copy/move constructors based on T
template <typename T> 
class FCVector { // Fixed Capacity Vector
  public:
    using value_type = T;

    explicit FCVector(u64 size)
        : size_(size), count_(0), data_(static_cast<T *>(::operator new(size * sizeof(T)))) {}
    template <typename... Args>
    explicit FCVector(u64 size, Args&&... args)
        : size_(size), count_(0), data_(static_cast<T *>(::operator new(size * sizeof(T)))) {
        for (u64 i = 0; i < size; ++i) {
            emplace_back(std::forward<Args>(args)...);
        }
    }

    ~FCVector() {
        clear();
        ::operator delete(data_);
    }

    template <typename... Args>
    inline void emplace_back(Args &&...args) {
        if (count_ >= size_) {
            throw std::out_of_range("FCVector capacity exceeded: "+std::to_string(count_)+" >= "+std::to_string(size_));
        }
        new (&data_[count_]) T(std::forward<Args>(args)...);
        ++count_;
    }

    // inline T &operator[](i64 index) {//}
    //     if (index >= count_ || index < 0) {
    //}
    inline T &operator[](u64 index) {
        if (index >= count_) {//automatically checks if index is negative
            throw std::out_of_range("FCV Index [] out of range: "+std::to_string(index)+" >= "+std::to_string(count_));
        }
        return data_[index];
    }

    inline const T &operator[](u64 index) const {
        if (index >= count_) {//automatically checks if index is negative
            throw std::out_of_range("FCV Index [] out of range: "+std::to_string(index)+" >= "+std::to_string(count_));
        }
        return data_[index];
    }

    inline u64 size() const { return count_; }

    inline u64 capacity() const { return size_; }
    inline T &back() {
        if (count_ == 0) {
            throw std::out_of_range("FCVector: back() called on empty vector");
        }
        return data_[count_ - 1];
    }
    inline void pop_back() {
        if (count_ == 0) {
            throw std::out_of_range("FCVector: pop_back() called on empty vector");
        }
        data_[count_ - 1].~T();
        --count_;
    }

    inline void clear() {
        for (u64 i = 0; i < count_; ++i) {
            data_[i].~T();
        }
        count_ = 0;
    }
    inline T *data() { return data_; }
    inline const T *data() const { return data_; }

  private:
    u64 size_;
    u64 count_;
    T *data_;
};

template <typename V_type>
class ContinuousVector: public V_type {
    public:
    ContinuousVector(u64 size): V_type(size) {}
    inline void remove(u64 index) {
        if (index >= this->size()) {
            throw std::out_of_range("ContinuousVector: Remove out of range: "+std::to_string(index)+" >= "+std::to_string(this->size()));
        }
        if (index!=this->size()-1) {
            std::swap((*this)[index], this->back());
        }
        this->pop_back();
    }
};


enum OffsetVectorOpts{
    DEFAULT,
    SET_MAX_SIZE
};
//non-owning vector
//!Note: always sets size to 0 unless constructed from rvalue of another vector or given SET_MAX_SIZE
template <typename T> class OffsetVector { // Fixed Capacity Vector
    static_assert(std::is_trivially_copyable_v<T>,"OffsetVector T must be trivially copyable");
    static_assert(std::is_standard_layout_v<T>, "OffsetVector T must be standard layout");
  private:
    T* ptr_;
    u64 max_cap_;
    u64 size_;
    const u64 in_v_off_;//within the input vector
  public:
//   enum ConstructOptions{//~gave up on this
//     DEFAULT,
//     SET_MAX_SIZE,
//   }

    using value_type = T;


    // explicit OffsetVector(u64 cap, u64 off, V_type &v) : max_cap_(cap), offset_(off), size_(0), vec_(v) {
    //     if (offset_ >= vec_.size()) {
    //         throw std::out_of_range("OffsetVector: offset out of range");
    //     }
    //     if (offset_ + max_cap_ > vec_.size()){
    //         throw std::out_of_range("OffsetVector: offset + max_cap_ out of range");
    //     }
    // }
    template <typename V_type>
    constexpr void assert_compatible(){
        // static_assert(sizeof(T)%sizeof(V_type::value_type)==0,"OffsetVector size must be multiple of V_type::value_type for safe reinterpret_cast");
        static_assert(alignof(typename V_type::value_type)%alignof(T)==0,"OffsetVector must align with V_type::value_type");
        static_assert(std::is_trivially_copyable_v<typename V_type::value_type>,"OffsetVector V_type::value_type must be trivially copyable");
        static_assert(std::is_standard_layout_v<typename V_type::value_type>, "OffsetVector V_type::value_type must be standard layout");
    }
    // template <ConstructOptions opt=DEFAULT,typename V_type>//~gave up
    template <typename V_type>
    explicit OffsetVector(V_type &v) :
    max_cap_(v.size()*sizeof(typename V_type::value_type)/sizeof(T)),
    size_(0),
    ptr_(reinterpret_cast<T*>(v.data())),
    in_v_off_(0) {
        assert_compatible<V_type>();
    }
    template <typename V_type>
    explicit OffsetVector(V_type &v, const OffsetVectorOpts opt) :
    max_cap_(v.size()*sizeof(typename V_type::value_type)/sizeof(T)),
    size_(opt==SET_MAX_SIZE?v.size()*sizeof(typename V_type::value_type)/sizeof(T):0),
    ptr_(reinterpret_cast<T*>(v.data())),
    in_v_off_(0) {
        assert_compatible<V_type>();
    }

    // template <ConstructOptions opt=DEFAULT,typename V_type>//~gave up
    template <typename V_type>
    explicit OffsetVector(V_type &in_v, u64 in_v_off, u64 in_v_cap) :
    max_cap_(in_v_cap*sizeof(typename V_type::value_type)/sizeof(T)),
    // size_(opt==SET_MAX_SIZE?in_v_cap:0),
    size_(0),
    ptr_(reinterpret_cast<T*>(in_v.data()+in_v_off)),
    in_v_off_(in_v_off) {
        assert_compatible<V_type>();
        if (in_v_off + in_v_cap > in_v.size()){
            throw std::out_of_range("OffsetVector: offset + in_v_cap out of range: "+std::to_string(in_v_off)+" + "+std::to_string(in_v_cap)+" > "+std::to_string(in_v.size()));
        }
    }
        // template <ConstructOptions opt=DEFAULT,typename V_type>//~gave up
    template <typename V_type>
    explicit OffsetVector(V_type &in_v, u64 in_v_off, u64 in_v_cap, const OffsetVectorOpts opt) :
    max_cap_(in_v_cap*sizeof(typename V_type::value_type)/sizeof(T)),
    // size_(opt==SET_MAX_SIZE?in_v_cap:0),
    size_(opt==SET_MAX_SIZE?in_v_cap*sizeof(typename V_type::value_type)/sizeof(T):0),
    ptr_(reinterpret_cast<T*>(in_v.data()+in_v_off)),
    in_v_off_(in_v_off) {
        assert_compatible<V_type>();
        if (in_v_off + in_v_cap > in_v.size()){
            throw std::out_of_range("OffsetVector: offset + in_v_cap out of range: "+std::to_string(in_v_off)+" + "+std::to_string(in_v_cap)+" > "+std::to_string(in_v.size()));
        }
    }
    // explicit OffsetVector(u64 cap, T* ptr) : max_cap_(cap), size_(0), ptr_(ptr) {
    // }
    explicit OffsetVector(OffsetVector&& other) : 
    max_cap_(other.max_cap_), size_(other.size_), ptr_(other.ptr_), in_v_off_(other.in_v_off_) {
        other.size_ = 0;
        other.max_cap_ = 0;
    } 
    inline void resize(u64 new_size) {
        if (new_size > max_cap_) {
            throw std::out_of_range("OffsetVector resize capacity exceeded: "+std::to_string(new_size)+" > "+std::to_string(max_cap_));
        }
        size_ = new_size;
    }
    inline void reserve(u64 new_cap) {
        if (new_cap > max_cap_) {
            throw std::out_of_range("OffsetVector reserve capacity exceeded: "+std::to_string(new_cap)+" > "+std::to_string(max_cap_));
        }
    }
    inline T &operator[](u64 index) {
        if (index >= size_) {
            throw std::out_of_range("OV Index [] out of range: "+std::to_string(index)+" >= "+std::to_string(size_));
        }
        // return vec_[index + offset_];
        return ptr_[index];
    }

    inline const T &operator[](u64 index) const {
        if (index >= size_) { // automatically checks if index is negative
            throw std::out_of_range("OV Index [] out of range: "+std::to_string(index)+" >= "+std::to_string(size_));
        }
        // return vec_[index + offset_];
        return ptr_[index];
    }

    inline u64 size() const { return size_; }

    inline u64 capacity() const { return max_cap_; }
    inline u64 offset() const { return in_v_off_; }
    inline u64 in_v_offset() const { return in_v_off_; }
    inline T &back() {
        if (size_ == 0) {
            throw std::out_of_range("OffsetVector: back() called on empty vector");
        }
        return ptr_[size_ - 1];
    }
    inline void pop_back() {
        if (size_ == 0) {
            throw std::out_of_range("OffsetVector: pop_back() called on empty vector");
        }
        // vec[count_ - 1].~T();
        --size_;
    }

    inline void clear() {
        // for (u64 i = 0; i < count_; ++i) {
        //     vec[i].~T();
        // }
        size_ = 0;
    }
    inline void push_back(const T &value) {
        if (size_ >= max_cap_) {
            throw std::out_of_range("OffsetVector push_back capacity exceeded: "+std::to_string(size_)+" >= "+std::to_string(max_cap_));
        }
        ptr_[size_] = value;
        ++size_;
    }
    inline void push_back(T &&value) {
        if (size_ >= max_cap_) {
            throw std::out_of_range("OffsetVector push_back capacity exceeded: "+std::to_string(size_)+" >= "+std::to_string(max_cap_));
        }
        ptr_[size_] = std::move(value);
        ++size_;
    }
    inline T *data() { return ptr_; }
    inline const T *data() const { return ptr_; }
};
// template <typename T, typename V_type> 
// OffsetVector<T> full_offset_vector(V_type &in_v, u64 in_v_off, u64 in_v_cap){
//     OffsetVector<T> temp(in_v, in_v_off, in_v_cap);
//     temp.resize(in_v_cap);
//     return temp;
// }

template <typename V>
inline i32 find_map(const V& map, i64 id){
    for(i32 i=0; i<map.size(); i++){
        if(map[i].first==id){
            return map[i].second;
        }
    }
    return -1;
}
template <typename V, typename Any_Func>
inline bool any_of(const V& vec, Any_Func func){
    for(i32 i=0; i<vec.size(); i++){
        if(func(vec, i)){
            return true;
        }
    }
    return false;
}
template <typename V, typename Any_Func>
inline i32 first_of(const V& vec, Any_Func func){
    for(i32 i=0; i<vec.size(); i++){
        if(func(vec, i)){
            return i;
        }
    }
    return -1;
}
template <typename V, typename All_Func>
inline bool all_of(const V& vec, All_Func func){
    for(i32 i=0; i<vec.size(); i++){
        if(!func(vec, i)){
            return false;
        }
    }
    return true;
}

template <typename V, typename T>
inline void fill_vector(V& vec, T val){
    for(i32 i=0; i<vec.size(); i++){
        vec[i]=val;
    }
}
//~container.find(val)==container.end();
template <typename V, typename T>
inline bool not_in(const T& val, const V& container){
    return container.find(val)==container.end();
}
template <typename V, typename T>
inline bool in_vec(const T& val, const V& vec){
    return std::find(vec.begin(), vec.end(), val) != vec.end();
}
template <typename V, typename T>
inline i64 index_of(const T& val, const V& vec){
    for(i64 i=0; i<vec.size(); i++){
        if(vec[i]==val){
            return i;
        }
    }
    return -1;
}

template<typename f_type>
class OnDelete {
    f_type f;
public:
    OnDelete(f_type f) : f(f) {}
    ~OnDelete() {
        f();
    }
};

template<typename T, typename... Args>
void prints(std::stringstream &ss, T arg, Args&&... args) {
    ss<<arg;
    ((ss << " " << args), ...); // C++17 fold expression to print all arguments with a space in between
}
template<typename T, typename... Args>
void prints_newline(std::stringstream &ss, T arg, Args&&... args) {
    ss<<arg;
    ((ss << " " << args), ...); // C++17 fold expression to print all arguments with a space in between
    ss<<"\n";
}
template<typename T, typename... Args>
std::string prints_new(T arg, Args&&... args) {
    std::stringstream ss;
    ss<<arg;
    ((ss << " " << args), ...); // C++17 fold expression to print all arguments with a space in between
    return ss.str();
}
template <typename V>
std::string print_vec_new(V& vec){
    std::stringstream ss;
    ss<<"[";
    for(i32 i=0; i<vec.size(); i++){
        ss<<vec[i];
        if(i!=vec.size()-1){
            ss<<",";
        }
    }
    ss<<"]";
    return ss.str();
}
template <typename V>
std::string print_arr_new(V& arr, i32 size){
    std::stringstream ss;
    ss<<"[";
    for(i32 i=0; i<size; i++){
        ss<<arr[i];
        if(i!=size-1){
            ss<<",";
        }
    }
    ss<<"]";
    return ss.str();
}
void printcout(const std::string &s) {
    std::cout << s << std::endl;
}
void printcout(std::string &&s) {
    std::cout << s << std::endl;
}
void printcout_inline(std::string &&s) {
    std::cout << s;
}
void printcout_inline(const std::string &s) {
    std::cout << s;
}


struct CompareFirst{
    inline bool operator()(std::pair<i64,i32> const& a, std::pair<i64,i32> const& b) const noexcept{
        return a.first>b.first;
    }
};


class ThrowingOfstream : public std::ofstream {
  public:

    static bool check_path_valid(const std::string &filepath) {
        // ThrowingOfstream(filepath, std::ios::out);
        // std::ofstream file(filepath);
        // return file.good();
        try {
            ThrowingOfstream(filepath, std::ios::out);
            return true;
        } catch (...) {
            return false;
        }
    }

    static bool check_file_exists(const std::string &filepath) {
        return std::filesystem::exists(filepath);
    }


    ThrowingOfstream(const std::string &filepath, std::ios::openmode mode)
        : std::ofstream(filepath, mode) {
        if (this->fail()) {
            throw std::ios::failure(
                prints_new("The path", filepath,
                           "cannot be opened. Check that all the folders in the path is "
                           "correct and that you have permission to create files in this path folder"));
        }
    }
    static bool make_recursive_dir(const std::string &dirpath) {
        return std::filesystem::create_directories(dirpath);
    }
    static bool make_recursive_file(const std::string &file) {
        if(!check_path_valid(file)){
            return false;
        }
        std::filesystem::path p(file);
        if (p.has_parent_path()) {
            if(!make_recursive_dir(p.parent_path().string())){
                return false;
            }
        }
        std::ofstream of(file);
        return of.good();
    }
    static bool remove_file(const std::string &file) {
        return std::filesystem::remove(file);
    }
    static std::vector<std::string> list_files(const std::string &dir) {
        std::vector<std::string> files;
        for (const auto &entry : std::filesystem::directory_iterator(dir)) {
            files.push_back(entry.path().string());
        }
        return files;
    }

    void write_line(const std::string &s) {
        (*this) << s << '\n';
    }
    using std::ofstream::write;
    template <typename T> 
    void write(T t) {
        write(reinterpret_cast<char *>(&t), sizeof(t));
    }

    template <typename V_type>
    void write_v(const V_type &v) {
        write(reinterpret_cast<const char *>(v.data()), v.size() * sizeof(typename V_type::value_type));
    }


    void write_string_with_size(const std::string &s) {
        size_t size = reinterpret_cast<size_t>(s.size());
        std::ofstream::write(reinterpret_cast<char *>(&size), sizeof(size_t));
        (*this) << s;
    }
};

class ThrowingIfstream : public std::ifstream {
  public:
    ThrowingIfstream(const std::string &filename, std::ios::openmode mode)
        : std::ifstream(filename, mode) {
        if (this->fail()) {
            throw std::ios::failure(prints_new("The input file", filename, "cannot be opened"));
        }
    }

    static bool check_file_exists(const std::string &filename) {
        // ThrowingIfstream(filename, std::ios::in);
        std::ifstream file(filename);
        return file.good();
    }
    static u64 check_filesize(const std::string &filename) {
        return std::filesystem::file_size(filename);
    }

    //bool for not end of file
    bool read_line(std::string &s) {
        std::getline(*this, s);
        return !(this->eof());
    }

    using std::ifstream::read;
    std::string read_string_with_size() {
        u64 size = 0;
        read(reinterpret_cast<char *>(&size), sizeof(u64));
        std::string s;
        s.resize(size);
        read(reinterpret_cast<char *>(s.data()), static_cast<std::streamsize>(size));
        return s;
    }

    template <typename T> 
    T read() {
        T t = 0;
        read(reinterpret_cast<char *>(&t), sizeof(T));
        return t;
    }

};

// clang-format on

}//namespace distribsys