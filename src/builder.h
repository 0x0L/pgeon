// Copyright 2022 nullptr

#pragma once

#include <memory>

#include "builder/base.h"

namespace pgeon {

std::shared_ptr<ArrayBuilder> MakeBuilder(const SqlTypeInfo& info,
                                          const UserOptions& options);

}  // namespace pgeon
