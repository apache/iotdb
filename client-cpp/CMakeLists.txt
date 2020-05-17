project(client-cpp)
cmake_minimum_required(VERSION 3.16)
add_subdirectory(Session)
add_subdirectory(gen-cpp)

aux_source_directory(./Session dir_Session_srcs)
add_library(Session ${dir_Session_srcs})

aux_source_directory(./gen-cpp dir_GenCPP_srcs)
add_library(GenCPP ${dir_GenCPP_srcs})

# if you already have thrift, just delete this code
include_directories(./thrift_0.13.0_src)

aux_source_directory(. EXE_SRCS)
add_executable(client-cpp ${EXE_SRCS})
target_link_libraries(client-cpp Session)
target_link_libraries(client-cpp GenCPP)

target_link_libraries(client-cpp libthrift.a)
target_link_libraries(client-cpp libthriftnb.a)
