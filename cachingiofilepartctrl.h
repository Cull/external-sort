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

#ifndef CACHINGIOFILEPARTCTRL_H
#define CACHINGIOFILEPARTCTRL_H

#include"cachingiofilecontroler.h"

class cachingIOFilePartCtrl : public cachingIOFileControler
{
private:
    cachingIOFilePartCtrl();
    cachingIOFilePartCtrl(const cachingIOFileControler &other);
    cachingIOFileControler& operator=(const cachingIOFileControler &other);
    unsigned long long _part_start_pos; //start of streaming file part
    unsigned long long _part_cur_pos; //current_pos in file
    unsigned long long _part_end_pos; //end of streaming file part
public:
    ~cachingIOFilePartCtrl();
    cachingIOFilePartCtrl(const string &file_name, const ios_base::openmode &flags, const unsigned long cache_mem_size,
			  unsigned start_pos, unsigned end_pos);
    uint8_t get_next(); //get value from cur file position
    void put(uint8_t); //put value to cur file position
    unsigned long long size();
    bool is_file_ended();
};

#endif // CACHINGIOFILEPARTCTRL_H
