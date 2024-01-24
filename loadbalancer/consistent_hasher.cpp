#include "consistent_hasher.h"
#include <boost/python.hpp>
#include <boost/python/suite/indexing/vector_indexing_suite.hpp>
#include <iostream>

BOOST_PYTHON_MODULE(consistent_hasher) {
    using namespace boost::python;
    using namespace Hashing;
    using ListType = std::vector<std::string>;
    class_<ListType>("ListType")
            .def(vector_indexing_suite<ListType>());

    class_<consistent_hasher, boost::noncopyable>("ConsistentHashing")
            .def("add_server", &consistent_hasher::add_server)
            .def("get_nearest_server", &consistent_hasher::get_server)
            .def("remove_server", &consistent_hasher::remove_server)
            .def("get_servers", &consistent_hasher::get_server_list);
}
