cmake_minimum_required(VERSION 3.26.4)
project(ClangTidyError)

set(NANOARROW_NAMESPACE "placeholder")

include(FetchContent)
FetchContent_Declare(nanoarrow
                     GIT_REPOSITORY https://github.com/apache/arrow-nanoarrow.git
                     GIT_TAG main
                     GIT_SHALLOW TRUE)

FetchContent_MakeAvailable(nanoarrow)

find_program(
  CLANG_TIDY_EXE
  NAMES "clang-tidy"
  DOC "Path to clang-tidy executable" REQUIRED
)


add_executable(ct_test main.cpp)
set_target_properties(
  ct_test PROPERTIES CXX_CLANG_TIDY "${CLANG_TIDY_EXE}"
)
target_link_libraries(ct_test PRIVATE nanoarrow)
target_compile_definitions(nanoarrow PUBLIC NANOARROW_DEBUG=1)
target_compile_definitions(ct_test PUBLIC NANOARROW_DEBUG=1)
