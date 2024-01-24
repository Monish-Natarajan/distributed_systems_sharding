#include "consistent_hasher.h"
#include <boost/python.hpp>

#include <iostream>

BOOST_PYTHON_MODULE(consistent_hasher) {
    using namespace boost::python;
    using namespace Hashing;

    class_<consistent_hasher, boost::noncopyable>("consistent_hasher")
            .def("add_server", &consistent_hasher::add_server)
            .def("get_nearest_server", &consistent_hasher::get_server)
            .def("remove_server", &consistent_hasher::remove_server)
            .def("get_servers", &consistent_hasher::get_server_list);
}
