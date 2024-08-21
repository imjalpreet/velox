/*
* Copyright (c) Facebook, Inc. and its affiliates.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

#include "velox/connectors/hive/iceberg/IcebergDataSink.h"

#include "velox/common/base/Counters.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/HivePartitionFunction.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/core/ITypedExpr.h"
#include <utility>

namespace facebook::velox::connector::hive::iceberg {

namespace {
#define WRITER_NON_RECLAIMABLE_SECTION_GUARD(index)       \
  memory::NonReclaimableSectionGuard nonReclaimableGuard( \
      writerInfo_[(index)]->nonReclaimableSectionHolder.get())
} // namespace

IcebergDataSink::IcebergDataSink(
    facebook::velox::RowTypePtr inputType,
    std::shared_ptr<const IcebergInsertTableHandle> insertTableHandle,
    const facebook::velox::connector::ConnectorQueryCtx* connectorQueryCtx,
    facebook::velox::connector::CommitStrategy commitStrategy,
    const std::shared_ptr<const HiveConfig>& hiveConfig)
    : HiveDataSink(
          std::move(inputType),
          std::move(insertTableHandle),
          connectorQueryCtx,
          commitStrategy,
          hiveConfig) {}

void IcebergDataSink::appendData(facebook::velox::RowVectorPtr input) {
  checkRunning();

  // Write to unpartitioned table.
  if (!isPartitioned()) {
    const auto index = ensureWriter(HiveWriterId::unpartitionedId());
    write(index, input);
    return;
  }
}

void IcebergDataSink::write(size_t index, RowVectorPtr input) {
  WRITER_NON_RECLAIMABLE_SECTION_GUARD(index);

  writers_[index]->write(input);
  writerInfo_[index]->numWrittenRows += input->size();
}

std::vector<std::string> IcebergDataSink::close() {
  checkRunning();
  state_ = State::kClosed;
  closeInternal();

  auto icebergInsertTableHandle = std::dynamic_pointer_cast<const IcebergInsertTableHandle>(insertTableHandle_);

  std::vector<std::string> commitTasks;
  commitTasks.reserve(writerInfo_.size());
  auto fileFormat = toString(insertTableHandle_->tableStorageFormat());
  std::string finalFileFormat(fileFormat);
  std::transform(finalFileFormat.begin(), finalFileFormat.end(), finalFileFormat.begin(), ::toupper);

  for (int i = 0; i < writerInfo_.size(); ++i) {
    const auto& info = writerInfo_.at(i);
    VELOX_CHECK_NOT_NULL(info);
    // TODO(imjalpreet): Collect Iceberg file format metrics required by com.facebook.presto.iceberg.MetricsWrapper->org.apache.iceberg.Metrics#Metrics
    // clang-format off
      auto commitDataJson = folly::toJson(
       folly::dynamic::object
          ("path", info->writerParameters.writeDirectory() + "/" + info->writerParameters.writeFileName())
          ("fileSizeInBytes", ioStats_.at(i)->rawBytesWritten())
          ("metrics", folly::dynamic::object
            ("recordCount", info->numWrittenRows))
          ("partitionSpecId", icebergInsertTableHandle->partitionSpec()->specId)
          ("partitionDataJson", nullptr)
          ("fileFormat", finalFileFormat)
          ("referencedDataFile", nullptr));
    // clang-format on
    commitTasks.push_back(commitDataJson);
  }
  return commitTasks;
}
}