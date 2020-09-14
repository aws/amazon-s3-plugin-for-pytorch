//
// Created by Nagmote, Roshani on 5/19/20.
//

#include <pybind11/stl.h>

#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "pybind11/pybind11.h"
#include "s3_io.h"

namespace {
namespace py = pybind11;
using awsio::S3Init;
PYBIND11_MODULE(_pywrap_s3_io, m) {
    py::class_<S3Init>(m, "S3Init")
        .def(py::init<>())
        .def("s3_read",
             [](S3Init* self, const std::string& file_url, bool use_tm) {
                 std::string result;
                 self->s3_read(file_url, &result, use_tm);
                 return py::bytes(result);
             })
        .def("list_files", [](S3Init* self, const std::string& file_url) {
            std::vector<std::string> filenames;
            self->list_files(file_url, &filenames);
            return filenames;
        })
        .def("file_exists", [](S3Init* self, const std::string& bucket, const std::string& object) {
            return self->file_exists(bucket, object);
    });
}
}  // namespace
