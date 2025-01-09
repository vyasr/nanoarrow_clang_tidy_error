#include <vector>
#include <tuple>
#include <algorithm>
#include <random>

#include <nanoarrow/nanoarrow.hpp>

std::tuple<std::vector<int64_t>, std::vector<uint8_t>, nanoarrow::UniqueSchema>
get_table(int length)
{
  std::vector<int64_t> int64_data(length);
  std::vector<uint8_t> validity(length);
  std::generate(int64_data.begin(), int64_data.end(), []() { return rand() % 500000; });
  auto validity_generator = []() { return rand() % 7 != 0; };
  std::generate(validity.begin(), validity.end(), validity_generator);

  nanoarrow::UniqueSchema schema;
  ArrowSchemaInit(schema.get());
  NANOARROW_THROW_NOT_OK(ArrowSchemaSetTypeStruct(schema.get(), 6));

  NANOARROW_THROW_NOT_OK(ArrowSchemaInitFromType(schema->children[0], NANOARROW_TYPE_INT64));
  NANOARROW_THROW_NOT_OK(ArrowSchemaSetName(schema->children[0], "a"));
  schema->children[0]->flags |= ARROW_FLAG_NULLABLE;

  NANOARROW_THROW_NOT_OK(ArrowSchemaInitFromType(schema->children[1], NANOARROW_TYPE_STRING));
  NANOARROW_THROW_NOT_OK(ArrowSchemaSetName(schema->children[1], "b"));
  schema->children[1]->flags |= ARROW_FLAG_NULLABLE;

  NANOARROW_THROW_NOT_OK(ArrowSchemaInitFromType(schema->children[2], NANOARROW_TYPE_INT32));
  NANOARROW_THROW_NOT_OK(ArrowSchemaAllocateDictionary(schema->children[2]));
  NANOARROW_THROW_NOT_OK(
    ArrowSchemaInitFromType(schema->children[2]->dictionary, NANOARROW_TYPE_INT64));
  NANOARROW_THROW_NOT_OK(ArrowSchemaSetName(schema->children[2], "c"));
  schema->children[2]->flags |= ARROW_FLAG_NULLABLE;

  NANOARROW_THROW_NOT_OK(ArrowSchemaInitFromType(schema->children[3], NANOARROW_TYPE_BOOL));
  NANOARROW_THROW_NOT_OK(ArrowSchemaSetName(schema->children[3], "d"));
  schema->children[3]->flags |= ARROW_FLAG_NULLABLE;

  NANOARROW_THROW_NOT_OK(ArrowSchemaInitFromType(schema->children[4], NANOARROW_TYPE_LIST));
  NANOARROW_THROW_NOT_OK(
    ArrowSchemaInitFromType(schema->children[4]->children[0], NANOARROW_TYPE_INT64));
  NANOARROW_THROW_NOT_OK(ArrowSchemaSetName(schema->children[4]->children[0], "element"));
  schema->children[4]->children[0]->flags |= ARROW_FLAG_NULLABLE;

  NANOARROW_THROW_NOT_OK(ArrowSchemaSetName(schema->children[4], "e"));
  schema->children[4]->flags |= ARROW_FLAG_NULLABLE;

  ArrowSchemaInit(schema->children[5]);
  NANOARROW_THROW_NOT_OK(ArrowSchemaSetTypeStruct(schema->children[5], 2));
  NANOARROW_THROW_NOT_OK(
    ArrowSchemaInitFromType(schema->children[5]->children[0], NANOARROW_TYPE_INT64));
  NANOARROW_THROW_NOT_OK(ArrowSchemaSetName(schema->children[5]->children[0], "integral"));
  schema->children[5]->children[0]->flags |= ARROW_FLAG_NULLABLE;

  NANOARROW_THROW_NOT_OK(
    ArrowSchemaInitFromType(schema->children[5]->children[1], NANOARROW_TYPE_STRING));
  NANOARROW_THROW_NOT_OK(ArrowSchemaSetName(schema->children[5]->children[1], "string"));
  schema->children[5]->children[1]->flags |= ARROW_FLAG_NULLABLE;

  NANOARROW_THROW_NOT_OK(ArrowSchemaSetName(schema->children[5], "f"));
  schema->children[5]->flags |= ARROW_FLAG_NULLABLE;

  return std::make_tuple(
    std::move(int64_data), std::move(validity), std::move(schema));
}

int main()
{
  auto [data, mask, test_data] = get_table(10000);

  nanoarrow::UniqueArray tmp;
  NANOARROW_THROW_NOT_OK(ArrowArrayInitFromType(tmp.get(), NANOARROW_TYPE_INT64));

  ArrowBitmap bitmap;
  ArrowBitmapInit(&bitmap);
  NANOARROW_THROW_NOT_OK(ArrowBitmapReserve(&bitmap, mask.size()));
  ArrowBitmapAppendInt8Unsafe(&bitmap, reinterpret_cast<int8_t const*>(mask.data()), mask.size());

  tmp->length = data.size();
}
