// Copyright (c) YugaByte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//

#include "yb/master/snapshot_coordinator_context.h"

#include "yb/common/schema.h"

#include "yb/docdb/doc_key.h"
#include "yb/docdb/key_bytes.h"
#include "yb/docdb/primitive_value.h"

#include "yb/master/sys_catalog_constants.h"

#include "yb/util/result.h"

namespace yb {
namespace master {

namespace {

Result<ColumnId> MetadataColumnId(SnapshotCoordinatorContext* context) {
  return context->schema().ColumnIdByName(kSysCatalogTableColMetadata);
}

} // namespace

Result<docdb::KeyBytes> EncodedKey(
    SysRowEntryType type, const Slice& id, SnapshotCoordinatorContext* context) {
  docdb::DocKey doc_key({ docdb::PrimitiveValue::Int32(type),
                          docdb::PrimitiveValue(id.ToBuffer()) });
  docdb::SubDocKey sub_doc_key(
      doc_key, docdb::PrimitiveValue(VERIFY_RESULT(MetadataColumnId(context))));
  return sub_doc_key.Encode();
}

} // namespace master
} // namespace yb
