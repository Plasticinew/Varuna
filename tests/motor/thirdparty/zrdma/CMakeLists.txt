if(EXISTS "${CMAKE_SOURCE_DIR}/include/zQP.h")
    set(VARUNA_ROOT "${CMAKE_SOURCE_DIR}")
else()
    get_filename_component(VARUNA_ROOT "${CMAKE_SOURCE_DIR}/../.." ABSOLUTE)
endif()

set(VARUNA_ZQP_LIB "${VARUNA_ROOT}/build/source/libzQP.a")
if(NOT EXISTS "${VARUNA_ZQP_LIB}")
    message(FATAL_ERROR "Varuna libzQP.a not found at ${VARUNA_ZQP_LIB}. Build Varuna first: cmake -S ${VARUNA_ROOT} -B ${VARUNA_ROOT}/build && cmake --build ${VARUNA_ROOT}/build -j")
endif()

add_library(zrdma INTERFACE)
target_include_directories(zrdma INTERFACE
    ${VARUNA_ROOT}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/..
)
target_link_libraries(zrdma INTERFACE
    ${VARUNA_ZQP_LIB}
    ibverbs rdmacm pthread rt mlx5 tbb profiler
)
