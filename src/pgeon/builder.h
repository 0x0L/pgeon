// Copyright 2022 nullptr

#pragma once

#include <memory>
#include <string>

#include "pgeon/builder/base.h"

namespace pgeon {

std::shared_ptr<ArrayBuilder> MakeBuilder(const std::string& typreceive,
                                          const SqlTypeInfo& info,
                                          const UserOptions& options);

}  // namespace pgeon
