//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (!IsReadable(i)) {
      if (!IsOccupied(i)) {
        break;
      }
      continue;
    }
    if (cmp(key, KeyAt(i)) == 0) {
      result->push_back(ValueAt(i));
    }
  }
  return !result->empty();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  if (IsFull()) {
    return false;
  }
  std::vector<ValueType> result;
  GetValue(key, cmp, &result);
  if (std::find(result.cbegin(), result.cend(), value) != result.cend()) {
    return false;
  }

  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (!IsReadable(i)) {
      array_[i] = MappingType(key, value);
      SetReadable(i, 1);
      SetOccupied(i, 1);
      break;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  bool success = false;
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (!IsReadable(i)) {
      continue;
    }
    if (cmp(key, KeyAt(i)) == 0 && ValueAt(i) == value) {
      SetReadable(i, 0);
      success = true;
      break;
    }
  }
  return success;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  SetOccupied(bucket_idx, 1);
  SetReadable(bucket_idx, 0);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  auto location = GetLocation(bucket_idx);
  return (occupied_[location.first] >> location.second) & 1;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx, int bit) {
  auto location = GetLocation(bucket_idx);
  char mask = GetMask(location.second, bit);
  if (bit == 0) {
    occupied_[location.first] &= mask;
  } else {
    occupied_[location.first] |= mask;
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  auto location = GetLocation(bucket_idx);
  return (readable_[location.first] >> location.second) & 1;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx, int bit) {
  auto location = GetLocation(bucket_idx);
  char mask = GetMask(location.second, bit);
  if (bit == 0) {
    readable_[location.first] &= mask;
  } else {
    readable_[location.first] |= mask;
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  return NumReadable() == BUCKET_ARRAY_SIZE;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  uint32_t readable_num = 0;
  int size = (BUCKET_ARRAY_SIZE - 1) / 8 + 1;
  for (int i = 0; i < size; ++i) {
    uint8_t readable = readable_[i];
    while (readable != 0) {
      readable &= (readable - 1);
      readable_num++;
    }
  }
  return readable_num;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  return NumReadable() == 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
std::pair<int, int> HASH_TABLE_BUCKET_TYPE::GetLocation(uint32_t bucket_idx) const {
  auto ret = std::pair<int, int>(0, 0);
  ret.first = bucket_idx / 8;
  ret.second = bucket_idx % 8;
  return ret;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
char HASH_TABLE_BUCKET_TYPE::GetMask(int which, int bit) {
  char mask = 0;
  if (bit == 0) {
    mask = static_cast<char>(~(1 << which));
  } else {
    mask = static_cast<char>(1 << which);
  }
  return mask;
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
