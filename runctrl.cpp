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

#include "runctrl.h"

template< typename T >
struct array_deleter
{
  void operator ()( T const * p)
  { 
    delete[] p; 
  }
};

RunCtrl::RunCtrl(unsigned int avail_memory_mbytes = 320)
{
    _avail_memory_mbytes = avail_memory_mbytes;
}

RunCtrl::~RunCtrl()
{

}

void RunCtrl::runThreadSort(boost::shared_ptr<externalSortControler> sortCntrl, const std::string  input,
		            const std::string result_file_name, long long part_start_pos,
		            long long part_end_pos)
{
    sortCntrl.get()->part_sort(input, result_file_name, part_start_pos, part_end_pos);
}

bool RunCtrl::is_all_file_viewed(std::vector<boost::shared_ptr<cachingIOFileControler> > &sorted_parts)
{
    unsigned size = sorted_parts.size();
    bool result = false;
    for (unsigned i = 0; i < size; ++i) {
	result |= sorted_parts.at(i).get()->is_file_ended();
    }
    return result;
}

void RunCtrl::merge_sorted_parts(boost::shared_ptr<std::string> file_names, unsigned file_count) //saves origin byte order
{
    std::vector<boost::shared_ptr<cachingIOFileControler> > sorted_parts(file_count);
    std::vector<uint8_t> cur_bytes(file_count);
    
    for (unsigned i = 0; i < file_count; ++i) {
	sorted_parts[i] = boost::shared_ptr<cachingIOFileControler>(new cachingIOFileControler(file_names.get()[i],
											       fstream::in | fstream::binary,
											       _avail_memory_mbytes/(file_count+1)));
    }
    for (unsigned i = 0; i < file_count; ++i) {
	if(sorted_parts[i].get()->is_file_ended()) cur_bytes[i] = sorted_parts[i].get()->get_next();
    }
    
    unsigned min_byte_file = 0;
    cachingIOFileControler result("result", fstream::out | fstream::trunc | fstream::binary, _avail_memory_mbytes/(file_count+1));
    bool is_equals = true; //to correct order
    while (!is_all_file_viewed(sorted_parts)) {
	for (unsigned i = 0; i < file_count; ++i) {
	    if (sorted_parts[i].get()->is_file_ended()) continue;
	    if (cur_bytes[i] < cur_bytes[min_byte_file]) {
		min_byte_file = i;
		is_equals = false;
	    }
	}
	if (is_equals) {
	    min_byte_file = 0;
	    while(sorted_parts[min_byte_file].get()->is_file_ended()) min_byte_file++;
	    is_equals = true;
	}
	result.put(cur_bytes[min_byte_file]);
	cur_bytes[min_byte_file] = sorted_parts[min_byte_file].get()->get_next();
    }
}

void RunCtrl::threads_sort(std::string &file_to_sort)
{
    std::fstream check_size(file_to_sort.c_str(), std::fstream::in | std::fstream::binary); 
    long long file_size = (check_size.seekg(0, check_size.end) <= 0) ? 0 : static_cast<long long>(check_size.tellg());
    check_size.close();
    
    unsigned num_of_threads = 0;
    std::cout << "Enter number of threads for execution:" << std::endl;
    std::cin >> num_of_threads;
    while (_avail_memory_mbytes / num_of_threads < 5 || !num_of_threads) { //5 is some intuite value
	if (!num_of_threads) {
	    std::cout << " entered value less or equal to zero!" << std::endl;
	} else {
	    std::cout << " warning: too low memory for single thread " << std::endl;
	    std::cout << " warning: please enter less threads count ... " << std::endl;
	    }
	std::cin >> num_of_threads;
    }
    

    boost::shared_ptr<boost::thread> threads (new boost::thread[num_of_threads], array_deleter<boost::thread>()) ;
    boost::shared_ptr<std::string> result_filenames (new std::string[num_of_threads], array_deleter<std::string>());
    std::vector<boost::shared_ptr<externalSortControler> > threadSortCrtls;
    for (unsigned i = 0; i < num_of_threads; ++i) {
	threadSortCrtls.push_back(boost::shared_ptr<externalSortControler>(
	                                                new externalSortControler(_avail_memory_mbytes/num_of_threads)));
	std::ostringstream stringStream;
        stringStream << i;
        std::string file_suf = stringStream.str();
	result_filenames.get()[i] = std::string("result_th" + file_suf);
    }
    

    for (unsigned i = 0; i < num_of_threads; ++i) {
	//nasty if else for correcting threads file segments
	if (i == 0) {
	    threads.get()[0] = boost::thread(runThreadSort, threadSortCrtls[0], file_to_sort, result_filenames.get()[0],
					                                                     0, file_size/num_of_threads);
	} else if (i == 1) {
	    threads.get()[1] = boost::thread(runThreadSort, threadSortCrtls[1], file_to_sort, result_filenames.get()[1],
							      file_size/num_of_threads, 2*(file_size/num_of_threads) + 1);
	} else if (i == num_of_threads - 1){
	    threads.get()[i] = boost::thread(runThreadSort, threadSortCrtls[i], file_to_sort, result_filenames.get()[i],
					                                     i*(file_size/num_of_threads) + 1, file_size);
	} else {
	    threads.get()[i] = boost::thread(runThreadSort, threadSortCrtls[i], file_to_sort, result_filenames.get()[i], 
					          i*(file_size/num_of_threads) + 1, (i+1)*(file_size/num_of_threads) + 1);
	}
    }
    
    
    for (unsigned i = 0; i < num_of_threads; ++i) {
	threads.get()[i].join();
    }
    
    merge_sorted_parts(result_filenames, num_of_threads);
}

void RunCtrl::one_thread_sort(std::string &file_to_sort)
{
    externalSortControler threadSortCrtls(_avail_memory_mbytes);
    threadSortCrtls.sort(file_to_sort, "result");
}

void RunCtrl::run_sort()
{
    std::string file_to_sort;
    std::cout << "file to sort:" << std::endl;
    std::cin >> file_to_sort;
    std::cout << "sort with threads?(1/0):" << std::endl;
    bool is_parallel;
    std::cin >> is_parallel;
    if (is_parallel) {
	threads_sort(file_to_sort);
    } else {
	one_thread_sort(file_to_sort);
    }
}

void RunCtrl::test_result()
{
    bool is_sorted = true;
    uint8_t cur_byte = 0;
    uint8_t prev_byte = 0;
    cachingIOFileControler input_file("result", fstream::in | fstream::binary, _avail_memory_mbytes);
    unsigned long long file_length = input_file.size();
    unsigned long long file_pos = 0;
    while (file_pos < file_length) {
	cur_byte = input_file.get_next();
	if (cur_byte < prev_byte) {
	    is_sorted = false;
	}
	file_pos++;
	prev_byte = cur_byte;
    }
    std::cout << "is result sorted - " << (is_sorted ? "yes" : "no") << std::endl;
}


