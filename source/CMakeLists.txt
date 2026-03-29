
include_directories(${PROJECT_SOURCE_DIR}/include)

set(BASE_SOURCE zQP.cc ib.cc)

add_library(zQP STATIC ${BASE_SOURCE})
target_link_libraries(zQP ibverbs rdmacm pthread rt mlx5 tbb profiler)

add_executable(zServer zServer.cc)
target_link_libraries(zServer zQP )

add_executable(zClient zClient.cc)
target_link_libraries(zClient zQP )

add_executable(latency_test latency_test.cc)
target_link_libraries(latency_test zQP )

install(TARGETS zQP
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib
        RUNTIME DESTINATION bin
        PUBLIC_HEADER DESTINATION include)
