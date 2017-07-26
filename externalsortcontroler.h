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

#ifndef EXTERNALSORTCONTROLER_H
#define EXTERNALSORTCONTROLER_H

#include<fstream>
#include<iostream>
#include<vector>
#include<iterator>
#include<stdint.h>
#include<sstream>
#include<boost/thread/thread.hpp>
#include"cachingiofilecontroler.h"
#include"cachingiofilepartctrl.h"

class externalSortControler
{
private:
    externalSortControler();
    externalSortControler(const externalSortControler& other);
    externalSortControler& operator=(const externalSortControler& other);
    static unsigned _count; //count of existed controllers used to generate uniq _tmp_file_names
    std::vector<std::string> _tmp_file_names;
    unsigned int _avail_memory;
    unsigned int _max(unsigned int first, unsigned int second);
    bool _stream_split_file(std::istream &input);
    bool _split_file(const std::string &input);
    bool _split_file_part(const std::string &input, long long part_start_pos, long long part_end_pos);
    void _merge(const std::string &output);
    void _stream_merge(std::ostream &output_file);
    void _stream_sort(const std::string  &input, const std::string &result_file_name);
    void _cache_sort(const std::string &input_file_name, const std::string &result_file_name);
public:
    externalSortControler(unsigned int avail_mem_size); //in mbytes
    ~externalSortControler();
    void sort(const std::string  input, const std::string result_file_name);
    void part_sort(const string &input_file_name, const string &result_file_name, long long part_start_pos,
		   long long part_end_pos);
};

#endif // EXTERNALSORTCONTROLEL_H
