cmake_minimum_required(VERSION 3.20)

# Project name
project(minilog_db_project)

# Set C++ standard (C++17 recommended to support newer features)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Windows MSVC specific configuration
if(MSVC)
    add_compile_options(/utf-8)
endif()

# ==========================================
# Define source file list
# Note: Since CMakeLists.txt is in the root directory,
# need to add "minilog_db/" prefix when referencing source files
# ==========================================

# 1. Database core logic sources (excluding main.cpp and test_recovery.cpp)
set(DB_CORE_SOURCES
    minilog_db/BPTree.cpp
    minilog_db/rwdata.cpp
    minilog_db/BufferPool.cpp
    minilog_db/WAL.cpp
    minilog_db/DataBase.cpp
    minilog_db/sqlparser.cpp
)

# ===================
# Define executables
# ===================

# 1. Main program (minilog_db)
# Composed of main.cpp + core logic
add_executable(minilog_db 
    minilog_db/main.cpp 
    ${DB_CORE_SOURCES}
)

# 2. Recovery test program (test_recovery)
# Composed of test_recovery.cpp + core logic
add_executable(test_recovery 
    minilog_db/test_recovery.cpp 
    ${DB_CORE_SOURCES}
)

# Set include directory paths
# PUBLIC minilog_db tells the compiler to look for .h files in the minilog_db directory
target_include_directories(minilog_db PUBLIC minilog_db)
target_include_directories(test_recovery PUBLIC minilog_db)