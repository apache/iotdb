export THRIFT_EXE=thrift
export CMAKE_EXE=cmake
export MAKE_EXE=make
export SH_DIR=$(dirname $0)/
export THRIFT_SCRIPT=${SH_DIR}rpc.thrift
export THRIFT_OUT=${SH_DIR}gen-cpp
export USELESS_FILE=${SH_DIR}/gen-cpp/TSIService_server.skeleton.cpp
export SUB_CMAKELISTS=${SH_DIR}/gen-cpp/CMakeLists.txt
export BUILD_FOLDER=${SH_DIR}/build

rm -rf ${THRIFT_OUT}
${THRIFT_EXE} -gen cpp ${THRIFT_SCRIPT}
rm -rf ${USELESS_FILE}
touch ${SUB_CMAKELISTS}
cd ${BUILD_FOLDER}
${CMAKE_EXE} ../
${MAKE_EXE}
mv client-cpp ../