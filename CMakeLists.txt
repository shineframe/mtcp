PROJECT(mtcp_examples)

CMAKE_MINIMUM_REQUIRED(VERSION 2.4)

INCLUDE_DIRECTORIES(
  ${PROJECT_SOURCE_DIR}
)

IF(NOT WIN32)
 SET(CMAKE_CXX_FLAGS "-std=c++11 -Wall -O3")

ENDIF()


ADD_EXECUTABLE(echo_server examples/echo_server.cpp)

ADD_EXECUTABLE(echo_client examples/echo_client.cpp)
