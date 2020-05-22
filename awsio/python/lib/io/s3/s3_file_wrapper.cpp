//
// Created by Nagmote, Roshani on 5/19/20.
//

#include <string>
#include <vector>

#include "csrc/io/s3/s3_io.h"
#include "pybind11/pybind11.h"

namespace {
    namespace py = pybind11;
    using awsio::S3Init;
    PYBIND11_MODULE(_pywrap_s3_io, m){
        py::class_<S3Init>(m, "S3Init")
                .def(py::init<>())
                .def("s3_read", [](const std::string& file_url, bool use_tm) {
                    awsio::S3Init s3caller;
		    s3caller.s3_read(file_url, use_tm);
                });
    }
}
