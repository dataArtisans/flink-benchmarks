/*
 * If you want to create updated Java Thrift classes, you need to install the
 * thrift binary with a matching version to the dependency inside the pom.xml or
 * override it and generate classes via
 *
 * > mvn generate-sources -Pgenerate-thrift -Dthrift.version=0.13.0
 *
 * or more dynamically:
 *
 * > mvn generate-sources -Pgenerate-thrift -Dthrift.version=$(thrift --version | rev | cut -d' ' -f 1 | rev)
 *
 * Be sure to use the same thrift version when compiling and running
 * tests/benchmarks as well to avoid potential conflicts.
 */

namespace java org.apache.flink.benchmark.thrift

    typedef i32 int

    struct MyPojo {
        1: int id;
        2: string name;
        3: list<string> operationName;
        4: list<MyOperation> operations;
        5: int otherId1;
        6: int otherId2;
        7: int otherId3;
        8: optional string someObject;
    }

    struct MyOperation {
        1: int id;
        2: string name;
    }
