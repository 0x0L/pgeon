// MIT License

// Copyright (c) 2022 nullptr

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

#pragma once

#include <arrow/api.h>

#include <memory>

namespace pgeon {

struct UserOptions {
  bool string_as_dictionaries = false;
  // TODO(xav) max precision of 128 decimal ?
  int default_numeric_precision = 22;
  int default_numeric_scale = 6;
  // TODO(xav) lc_monetary
  int monetary_fractional_precision = 2;

  static UserOptions Defaults();
  arrow::Status Validate() const;
};

arrow::Result<std::shared_ptr<arrow::Table>> CopyQuery(
    const char* conninfo, const char* query,
    const UserOptions& options = UserOptions::Defaults());

}  // namespace pgeon
