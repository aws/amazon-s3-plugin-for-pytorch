//
// Created by Nagmote, Roshani on 5/19/20.
//

#include <string>
#include <vector>

#include "s3_io.h"
#include "pybind11/pybind11.h"
#include <pybind11/stl.h>
#include "absl/strings/string_view.h"

namespace {
    namespace py = pybind11;
    using awsio::S3Init;
    PYBIND11_MODULE(_pywrap_s3_io, m){
        py::class_<S3Init>(m, "S3Init")
                .def(py::init<>())
                .def("s3_read", [](S3Init* self, const std::string &file_url, bool use_tm) {
		    std::string result;
		    self->s3_read(file_url, &result,  use_tm);
		    return py::bytes(result);
                });
    }
}
