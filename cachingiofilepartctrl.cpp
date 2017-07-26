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

#include "cachingiofilepartctrl.h"

cachingIOFilePartCtrl::cachingIOFilePartCtrl(const string& file_name, const ios_base::openmode& flags,
					     const long unsigned int cache_mem_size, unsigned int start_pos,
					     unsigned int end_pos): 
					     cachingIOFileControler(file_name, flags, cache_mem_size), _part_start_pos(start_pos),
					     _part_end_pos(end_pos), _part_cur_pos(start_pos)
{
    _file_pos = _part_start_pos;
}

cachingIOFilePartCtrl::~cachingIOFilePartCtrl()
{
}

uint8_t cachingIOFilePartCtrl::get_next()
{
    if (!(_flags & fstream::in)) {
	fputs("file open only to write\n", stderr);
	abort();
    }
    if (_part_cur_pos >= _part_end_pos) {
	fputs("error: reading anavailable file part\n", stderr);
	abort();
    }
    _part_cur_pos++;
    return cachingIOFileControler::get_next();
}

void cachingIOFilePartCtrl::put(uint8_t tmp)  
{
    
    if (!(_flags & fstream::out)) {
	fputs("file open only to read\n", stderr);
	abort();
    }
    if (_part_cur_pos >= _part_end_pos) {
	fputs("error: write to anavailable file part\n", stderr);
	abort();
    }
    _part_cur_pos++;
    cachingIOFileControler::put(tmp);
}

unsigned long long cachingIOFilePartCtrl::size()
{
    return _part_end_pos - _part_start_pos;
}

bool cachingIOFilePartCtrl::is_file_ended()
{
    return _part_cur_pos == _part_end_pos;
}