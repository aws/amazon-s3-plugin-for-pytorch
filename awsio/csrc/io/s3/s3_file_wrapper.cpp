//   Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
  
//   Licensed under the Apache License, Version 2.0 (the "License").
//   You may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
  
//       http://www.apache.org/licenses/LICENSE-2.0
  
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

#include <pybind11/stl.h>

#include <string>
#include <vector>

#include "pybind11/pybind11.h"
#include "s3_io.h"

namespace {
namespace py = pybind11;
using awsio::S3Init;
PYBIND11_MODULE(_pywrap_s3_io, m) {
    py::class_<S3Init>(m, "S3Init")
        .def(py::init<>())
        .def("s3_read",
             [](S3Init* self, const std::string& file_url) {
                 std::string result;
                 self->s3_read(file_url, &result);
                 return py::bytes(result);
             })
        .def("list_files",
             [](S3Init* self, const std::string& file_url) {
                 std::vector<std::string> filenames;
                 self->list_files(file_url, &filenames);
                 return filenames;
             })
        .def("file_exists",
             [](S3Init* self, const std::string& file_url) {
                 return self->file_exists(file_url);
             })
        .def("get_file_size",
             [](S3Init* self, const std::string& file_url) {
                 return self->get_file_size(file_url);
        });
}
}  // namespace
