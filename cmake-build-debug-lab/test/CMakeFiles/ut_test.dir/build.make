# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.23

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:

#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:

# Disable VCS-based implicit rules.
% : %,v

# Disable VCS-based implicit rules.
% : RCS/%

# Disable VCS-based implicit rules.
% : RCS/%,v

# Disable VCS-based implicit rules.
% : SCCS/s.%

# Disable VCS-based implicit rules.
% : s.%

.SUFFIXES: .hpux_make_needs_suffix_list

# Command-line flag to silence nested $(MAKE).
$(VERBOSE)MAKESILENT = -s

#Suppress display of executed commands.
$(VERBOSE).SILENT:

# A target that is always out of date.
cmake_force:
.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/local/bin/cmake

# The command to remove a file.
RM = /usr/local/bin/cmake -E rm -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /tmp/tmp.Ru5KUPcvI2

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /tmp/tmp.Ru5KUPcvI2/cmake-build-debug-lab

# Include any dependencies generated for this target.
include test/CMakeFiles/ut_test.dir/depend.make
# Include any dependencies generated by the compiler for this target.
include test/CMakeFiles/ut_test.dir/compiler_depend.make

# Include the progress variables for this target.
include test/CMakeFiles/ut_test.dir/progress.make

# Include the compile flags for this target's objects.
include test/CMakeFiles/ut_test.dir/flags.make

test/CMakeFiles/ut_test.dir/compression_test.cpp.o: test/CMakeFiles/ut_test.dir/flags.make
test/CMakeFiles/ut_test.dir/compression_test.cpp.o: ../test/compression_test.cpp
test/CMakeFiles/ut_test.dir/compression_test.cpp.o: test/CMakeFiles/ut_test.dir/compiler_depend.ts
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/tmp/tmp.Ru5KUPcvI2/cmake-build-debug-lab/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object test/CMakeFiles/ut_test.dir/compression_test.cpp.o"
	cd /tmp/tmp.Ru5KUPcvI2/cmake-build-debug-lab/test && g++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -MD -MT test/CMakeFiles/ut_test.dir/compression_test.cpp.o -MF CMakeFiles/ut_test.dir/compression_test.cpp.o.d -o CMakeFiles/ut_test.dir/compression_test.cpp.o -c /tmp/tmp.Ru5KUPcvI2/test/compression_test.cpp

test/CMakeFiles/ut_test.dir/compression_test.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/ut_test.dir/compression_test.cpp.i"
	cd /tmp/tmp.Ru5KUPcvI2/cmake-build-debug-lab/test && g++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /tmp/tmp.Ru5KUPcvI2/test/compression_test.cpp > CMakeFiles/ut_test.dir/compression_test.cpp.i

test/CMakeFiles/ut_test.dir/compression_test.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/ut_test.dir/compression_test.cpp.s"
	cd /tmp/tmp.Ru5KUPcvI2/cmake-build-debug-lab/test && g++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /tmp/tmp.Ru5KUPcvI2/test/compression_test.cpp -o CMakeFiles/ut_test.dir/compression_test.cpp.s

test/CMakeFiles/ut_test.dir/multi_thread_test.cpp.o: test/CMakeFiles/ut_test.dir/flags.make
test/CMakeFiles/ut_test.dir/multi_thread_test.cpp.o: ../test/multi_thread_test.cpp
test/CMakeFiles/ut_test.dir/multi_thread_test.cpp.o: test/CMakeFiles/ut_test.dir/compiler_depend.ts
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/tmp/tmp.Ru5KUPcvI2/cmake-build-debug-lab/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Building CXX object test/CMakeFiles/ut_test.dir/multi_thread_test.cpp.o"
	cd /tmp/tmp.Ru5KUPcvI2/cmake-build-debug-lab/test && g++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -MD -MT test/CMakeFiles/ut_test.dir/multi_thread_test.cpp.o -MF CMakeFiles/ut_test.dir/multi_thread_test.cpp.o.d -o CMakeFiles/ut_test.dir/multi_thread_test.cpp.o -c /tmp/tmp.Ru5KUPcvI2/test/multi_thread_test.cpp

test/CMakeFiles/ut_test.dir/multi_thread_test.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/ut_test.dir/multi_thread_test.cpp.i"
	cd /tmp/tmp.Ru5KUPcvI2/cmake-build-debug-lab/test && g++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /tmp/tmp.Ru5KUPcvI2/test/multi_thread_test.cpp > CMakeFiles/ut_test.dir/multi_thread_test.cpp.i

test/CMakeFiles/ut_test.dir/multi_thread_test.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/ut_test.dir/multi_thread_test.cpp.s"
	cd /tmp/tmp.Ru5KUPcvI2/cmake-build-debug-lab/test && g++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /tmp/tmp.Ru5KUPcvI2/test/multi_thread_test.cpp -o CMakeFiles/ut_test.dir/multi_thread_test.cpp.s

# Object files for target ut_test
ut_test_OBJECTS = \
"CMakeFiles/ut_test.dir/compression_test.cpp.o" \
"CMakeFiles/ut_test.dir/multi_thread_test.cpp.o"

# External object files for target ut_test
ut_test_EXTERNAL_OBJECTS =

test/ut_test: test/CMakeFiles/ut_test.dir/compression_test.cpp.o
test/ut_test: test/CMakeFiles/ut_test.dir/multi_thread_test.cpp.o
test/ut_test: test/CMakeFiles/ut_test.dir/build.make
test/ut_test: libDBLib.a
test/ut_test: /usr/local/lib64/libzstd.a
test/ut_test: test/CMakeFiles/ut_test.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/tmp/tmp.Ru5KUPcvI2/cmake-build-debug-lab/CMakeFiles --progress-num=$(CMAKE_PROGRESS_3) "Linking CXX executable ut_test"
	cd /tmp/tmp.Ru5KUPcvI2/cmake-build-debug-lab/test && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/ut_test.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
test/CMakeFiles/ut_test.dir/build: test/ut_test
.PHONY : test/CMakeFiles/ut_test.dir/build

test/CMakeFiles/ut_test.dir/clean:
	cd /tmp/tmp.Ru5KUPcvI2/cmake-build-debug-lab/test && $(CMAKE_COMMAND) -P CMakeFiles/ut_test.dir/cmake_clean.cmake
.PHONY : test/CMakeFiles/ut_test.dir/clean

test/CMakeFiles/ut_test.dir/depend:
	cd /tmp/tmp.Ru5KUPcvI2/cmake-build-debug-lab && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /tmp/tmp.Ru5KUPcvI2 /tmp/tmp.Ru5KUPcvI2/test /tmp/tmp.Ru5KUPcvI2/cmake-build-debug-lab /tmp/tmp.Ru5KUPcvI2/cmake-build-debug-lab/test /tmp/tmp.Ru5KUPcvI2/cmake-build-debug-lab/test/CMakeFiles/ut_test.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : test/CMakeFiles/ut_test.dir/depend

