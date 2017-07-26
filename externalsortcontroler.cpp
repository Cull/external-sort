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

#include"externalsortcontroler.h"

using namespace std;

unsigned externalSortControler::_count = 0;

unsigned int externalSortControler::_max(unsigned int first, unsigned int second) {
    return first < second ? first : second;
}


externalSortControler::externalSortControler(unsigned int avail_mem_size) :
                      _avail_memory(avail_mem_size*1000*1000) {
    std::ostringstream stringStream;
    stringStream << externalSortControler::_count++;
    std::string file_suf = stringStream.str();
    _tmp_file_names.push_back(string("tmp_file1_" + file_suf));
    _tmp_file_names.push_back(string("tmp_file2_" + file_suf));
}

externalSortControler::~externalSortControler() {
    externalSortControler::_count--;
}

bool externalSortControler::_stream_split_file(istream &input_file) {
    uint8_t prev_byte = 0;
    uint8_t cur_byte = 0;
    bool is_file_splited = false;
    istream_iterator<uint8_t> input_file_stream(input_file);
    istream_iterator<uint8_t> eos;
    ofstream first_file(_tmp_file_names.at(0).c_str(), std::ofstream::out | std::ofstream::trunc);
    ofstream second_file(_tmp_file_names.at(1).c_str(), std::ofstream::out | std::ofstream::trunc);
    ofstream *cur_file = &first_file;
    while(input_file_stream != eos) {
	cur_byte = *input_file_stream;
	if (cur_byte < prev_byte) {
	    *cur_file << "* ";
	    is_file_splited = true;
	    cur_file = cur_file == &first_file ? &second_file : &first_file;
	}
	input_file_stream++;
	*cur_file << cur_byte << " ";
	prev_byte = cur_byte;
    }
    first_file.close();
    second_file.close();
    return is_file_splited;
}


void externalSortControler::_stream_merge(ostream &output_file) {
    ifstream first_file(_tmp_file_names.at(0).c_str(), std::ofstream::in);
    ifstream second_file(_tmp_file_names.at(1).c_str(), std::ofstream::in);
    istream_iterator<uint8_t> input_file1_stream(first_file);
    istream_iterator<uint8_t> input_file2_stream(second_file);
    istream_iterator<uint8_t> eos;
    while (input_file1_stream != eos && input_file2_stream != eos) { //if else shit to merge sorted sequences ended by "*"
	if (*input_file1_stream == '*') {
	    while (*input_file2_stream != '*' && input_file2_stream != eos) {
		output_file << *input_file2_stream << " ";
		input_file2_stream++;
	    }
	    input_file1_stream++;
	    if (input_file2_stream != eos) input_file2_stream++;
	} else {
	    if (*input_file2_stream == '*') {
	        while (*input_file1_stream !=  '*' && input_file1_stream != eos) {
		    output_file << *input_file1_stream << " ";
		    input_file1_stream++;
	        }
	        input_file2_stream++;
	        if (input_file1_stream != eos) input_file1_stream++;
	    } else {
	        if (*input_file1_stream <= *input_file2_stream) {
		    output_file << *input_file1_stream << " ";
		    input_file1_stream++;
		} else {
		    output_file << *input_file2_stream << " ";
		    input_file2_stream++;
		}
	    }
	}
    }
    while (input_file1_stream != eos) {
	if (*input_file1_stream != '*') output_file << *input_file1_stream << endl;
	input_file1_stream++;
    }
    while (input_file2_stream != eos) {
	if (*input_file2_stream != '*') output_file << *input_file2_stream << endl;
	input_file2_stream++;
    }
    first_file.close();
    second_file.close();
}

void externalSortControler::_stream_sort(const string &input_file_name, const string &result_file_name) {
    fstream input(input_file_name.c_str(), std::fstream::in);
    fstream tmp_merge(result_file_name.c_str(), std::fstream::out | std::fstream::in | std::ofstream::trunc);
    if (!_stream_split_file(input)) { // just copy file(its sorted already)
	istream_iterator<int> input_stream(input);
	istream_iterator<int> eos;
	while (input_stream != eos) {
	    tmp_merge << *input_stream;
	    input_stream++;
	}
	return;
    }
    _stream_merge(tmp_merge);
    tmp_merge.close();
    tmp_merge.open(result_file_name.c_str(), std::fstream::in);
    while (_stream_split_file(tmp_merge)) {
	tmp_merge.close();
	tmp_merge.open(result_file_name.c_str(), std::fstream::out | std::ofstream::trunc);
        _stream_merge(tmp_merge);
	tmp_merge.close();
	tmp_merge.open(result_file_name.c_str(), std::fstream::in);
    }
    tmp_merge.close();
}

bool externalSortControler::_split_file(const string &input_filename)
{
    uint8_t prev_byte = 0;
    uint8_t cur_byte = 0;
    bool is_file_splited = false;
    cachingIOFileControler input_file(input_filename, fstream::in | fstream::binary, _avail_memory/3);
    cachingIOFileControler first_file(_tmp_file_names.at(0).c_str(),
				      fstream::out | fstream::trunc | fstream::binary, _avail_memory/3);
    cachingIOFileControler second_file(_tmp_file_names.at(1).c_str(), fstream::out | fstream::trunc | fstream::binary,
				       _avail_memory - 2*_avail_memory/3);
    cachingIOFileControler *cur_file = &first_file;
    unsigned long long file_length = input_file.size();
    unsigned long long file_pos = 0;
    while (file_pos < file_length) {
	cur_byte = input_file.get_next();
	if (cur_byte < prev_byte) {
	    is_file_splited = true;
	    cur_file = cur_file == &first_file ? &second_file : &first_file;
	}
	file_pos++;
	prev_byte = cur_byte;
	cur_file->put(cur_byte);
    }
    return is_file_splited;
}

bool externalSortControler::_split_file_part(const string &input_filename, long long part_start_pos,
					     long long part_end_pos)
{
    uint8_t prev_byte = 0;
    uint8_t cur_byte = 0;
    bool is_file_splited = false;
    cachingIOFilePartCtrl input_file(input_filename, fstream::in | fstream::binary, _avail_memory/3,
				     part_start_pos, part_end_pos);
    cachingIOFileControler first_file(_tmp_file_names.at(0).c_str(), fstream::out | fstream::trunc | fstream::binary,
				      _avail_memory/3);
    cachingIOFileControler second_file(_tmp_file_names.at(1).c_str(), fstream::out | fstream::trunc | fstream::binary,
				       _avail_memory - 2*_avail_memory/3);
    cachingIOFileControler *cur_file = &first_file;
    unsigned long long file_length = input_file.size();
    unsigned long long file_pos = 0;
    
    while (file_pos < file_length) {
	cur_byte = input_file.get_next();
	if (cur_byte < prev_byte) {
	    is_file_splited = true;
	    cur_file = cur_file == &first_file ? &second_file : &first_file;
	}
	file_pos++;
	prev_byte = cur_byte;
	cur_file->put(cur_byte);
    }
    return is_file_splited;
}

void externalSortControler::_merge(const string &output_filename) {
    uint8_t cur_byte_f1 = 0;
    uint8_t cur_byte_f2 = 0;
    uint8_t prev_byte_f1 = 0;
    uint8_t prev_byte_f2 = 0;
    cachingIOFileControler output_file(output_filename, fstream::out | fstream::trunc | fstream::binary,
				       _avail_memory/3);
    cachingIOFileControler first_file(_tmp_file_names.at(0).c_str(), fstream::in | fstream::binary,
				      _avail_memory/3);
    cachingIOFileControler second_file(_tmp_file_names.at(1).c_str(), fstream::in | fstream::binary,
				       _avail_memory - 2*_avail_memory/3);
    unsigned long long first_file_length = first_file.size();
    unsigned long long second_file_length = second_file.size();
    unsigned long long first_file_pos = 0;
    unsigned long long second_file_pos = 0;
    while(first_file_pos < first_file_length && second_file_pos < second_file_length) {
        if (cur_byte_f1 < prev_byte_f1 && cur_byte_f2 >= prev_byte_f2) {
	    while (cur_byte_f2 >= prev_byte_f2 && second_file_pos < second_file_length) {
		output_file.put(cur_byte_f2);
		prev_byte_f2 = cur_byte_f2;
		cur_byte_f2 = second_file.get_next();
		second_file_pos++;
	    }
	}
	if (cur_byte_f2 < prev_byte_f2 && cur_byte_f1 >= prev_byte_f1) {
	    while (cur_byte_f1 >= prev_byte_f1 && first_file_pos < first_file_length) {
		output_file.put(cur_byte_f1);
		prev_byte_f1 = cur_byte_f1;
		cur_byte_f1 = first_file.get_next();
		first_file_pos++;
	    }
	}
	if (first_file_pos >= first_file_length || second_file_pos >= second_file_length) break;
	if (cur_byte_f1 < cur_byte_f2) {
	    output_file.put(cur_byte_f1);
	    prev_byte_f1 = cur_byte_f1;
	    cur_byte_f1 = first_file.get_next();
	    first_file_pos++;
	} else {
	    output_file.put(cur_byte_f2);
	    prev_byte_f2 = cur_byte_f2;
	    cur_byte_f2 = second_file.get_next();
	    second_file_pos++;
	}
    }
    while(first_file_pos < first_file_length) {
	output_file.put(cur_byte_f1);
	cur_byte_f1 = first_file.get_next();
	first_file_pos++;
    }
    while(second_file_pos < second_file_length) {
	output_file.put(cur_byte_f2);
	cur_byte_f2 = second_file.get_next();
	second_file_pos++;
    }
}

void externalSortControler::_cache_sort(const string &input_file_name, const string &result_file_name) {
    if (!_split_file(input_file_name)) { // just copy file(its sorted already)
	cachingIOFileControler input(input_file_name, fstream::in | fstream::binary, _avail_memory/2);
	cachingIOFileControler output(result_file_name, fstream::out | fstream::trunc | fstream::binary,
				      _avail_memory - _avail_memory/2);
	unsigned long long file_size = input.size();
	unsigned long long cur_pos = 0;
	while (cur_pos < file_size) {
	    uint8_t cur = input.get_next();
	    cur_pos++;
	    output.put(cur);
	}
	return;
    }
    _merge(result_file_name);
    while (_split_file(result_file_name)) {
        _merge(result_file_name);
    }
}

void externalSortControler::part_sort(const string &input_file_name, const string &result_file_name,
				      long long part_start_pos, long long part_end_pos)
{
    if (!_split_file_part(input_file_name, part_start_pos, part_end_pos)) { // just copy file(its sorted already)
	cachingIOFilePartCtrl input(input_file_name, fstream::in | fstream::binary, _avail_memory/2,
				    part_start_pos, part_end_pos);
	cachingIOFileControler output(result_file_name, fstream::out | fstream::trunc | fstream::binary,
				      _avail_memory - _avail_memory/2);
	unsigned long long file_size = input.size();
	unsigned long long cur_pos = 0;
	while (cur_pos < file_size) {
	    uint8_t cur = input.get_next();
	    cur_pos++;
	    output.put(cur);
	}
	return;
    }
    _merge(result_file_name);
    while (_split_file(result_file_name)) {
        _merge(result_file_name);
    }
}

void externalSortControler::sort(const string input, const string result_file_name)
{
    _cache_sort(input,result_file_name);
}
