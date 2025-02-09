// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// The following only applies to changes made to this file as part of YugaByte development.
//
// Portions Copyright (c) YugaByte, Inc.
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
#ifndef YB_ROCKSDB_SST_DUMP_TOOL_H
#define YB_ROCKSDB_SST_DUMP_TOOL_H

#ifndef ROCKSDB_LITE
#pragma once

#include <glog/logging.h>

#include "yb/docdb/docdb_types.h"
#include "yb/util/enums.h"
#include "yb/util/slice.h"

namespace rocksdb {

YB_DEFINE_ENUM(OutputFormat, (kRaw)(kHex)(kDecodedRegularDB)(kDecodedIntentsDB));

class DocDBKVFormatter {
 public:
  DocDBKVFormatter() {}
  virtual ~DocDBKVFormatter() {}
  virtual std::string Format(const yb::Slice&, const yb::Slice&, yb::docdb::StorageDbType) const;
};

class SSTDumpTool {
 public:
  explicit SSTDumpTool(const rocksdb::DocDBKVFormatter& formatter = rocksdb::DocDBKVFormatter())
      : formatter_(formatter) {}

  int Run(int argc, char** argv);

 private:
  const rocksdb::DocDBKVFormatter& formatter_;
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE

#endif  // YB_ROCKSDB_SST_DUMP_TOOL_H
