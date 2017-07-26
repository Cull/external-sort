/*
 * Copyright 2017 <copyright holder> <email>
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */

#ifndef SORTCRTL_H
#define SORTCRTL_H

#include<boost/thread/thread.hpp>
#include<boost/shared_ptr.hpp>
#include"externalsortcontroler.h"

class RunCtrl
{
private:
    RunCtrl();
    RunCtrl(const RunCtrl &other);
    RunCtrl& operator=(const RunCtrl &other);
    static void runThreadSort(boost::shared_ptr<externalSortControler> sortCntrl, const std::string  input,
		              const std::string result_file_name, long long part_start_pos,
		              long long part_end_pos);
    unsigned int _avail_memory_mbytes;
    bool is_all_file_viewed(std::vector<boost::shared_ptr<cachingIOFileControler> > &sorted_parts);
    void merge_sorted_parts(boost::shared_ptr<std::string> file_names, unsigned file_count);
    void threads_sort(std::string &file_to_sort);
    void one_thread_sort(std::string &file_to_sort);
public:
    RunCtrl(unsigned int avail_memory_mbytes);
    ~RunCtrl();
    void run_sort();
    void test_result();
};

#endif // SORTCRTL_H
