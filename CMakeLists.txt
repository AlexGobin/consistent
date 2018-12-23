

cmake_minimum_required(VERSION 3.0)

project(consistent)

aux_source_directory(. SRC)
add_executable(consistent_test ${SRC} consistent.hpp WfirstRWLock.hpp)
