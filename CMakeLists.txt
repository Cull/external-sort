cmake_minimum_required(VERSION 2.6)
project(threadsort)

set(Boost_USE_STATIC_LIBS OFF) 
set(Boost_USE_MULTITHREADED ON)  
set(Boost_USE_STATIC_RUNTIME OFF) 
find_package(Boost 1.45.0 COMPONENTS thread timer) 

if(Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS}) 
    add_executable(threadsort main.cpp runctrl.cpp externalsortcontroler.cpp cachingiofilecontroler.cpp cachingiofilepartctrl.cpp)
    target_link_libraries(threadsort ${Boost_LIBRARIES})
    install(TARGETS threadsort RUNTIME DESTINATION bin)
endif()
