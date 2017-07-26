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

#include "cachingiofilecontroler.h"

cachingIOFileControler::cachingIOFileControler(const string &file_name, const ios_base::openmode &flags,
					       const unsigned long cache_mem_size) :
                                               _cache_size(cache_mem_size), _file_pos(0), _cache_pos(0),
                                               _is_modified(false), _is_last_block_cached(false),
                                               _flags(flags), _is_cache_initialized(false),
                                               _cache_end_pos(_cache_size)
{
    _caching_stream.open(file_name.c_str(), flags);
    _cache_buf = new uint8_t[_cache_size];
}

void cachingIOFileControler::_flush()
{
    if (!_is_modified) return;
    _caching_stream.seekp(_file_pos);
    _caching_stream.write((char*)_cache_buf, _cache_pos/sizeof(char));
    _cache_pos = 0;
}

void cachingIOFileControler::_get_next_block()
{
    _flush();
    _caching_stream.read((char*)_cache_buf, _cache_size/sizeof(char));
    if ((_caching_stream.rdstate() & std::fstream::eofbit) != 0) {
	_is_last_block_cached = true;
	unsigned long symb_readed = _caching_stream.gcount();
	_cache_end_pos = symb_readed == 0  ? _cache_size : symb_readed;
    }
    _cache_pos = 0;
    _file_pos += _cache_size;
    _is_modified = false;
    _is_cache_initialized = true;
}

uint8_t cachingIOFileControler::get_next()
{
    if (!(_flags & fstream::in)) {
	fputs("file open only to write", stderr);
	abort();
    }
    
    if (_is_last_block_cached && _cache_pos >= _cache_end_pos) {
	cout << "cache_pos: " << _cache_pos << " file_pos: " << _file_pos << " cache_size: " << _cache_size;
	cout << " cache_end_pos: " << _cache_end_pos << " size: " << size() << endl;
	fputs("ERROR reading after eof\n ", stderr);
	abort();
    }
    if (_cache_pos >= _cache_size || !_is_cache_initialized) {
	_get_next_block();
    }
    return _cache_buf[_cache_pos++];
}

void cachingIOFileControler::put(uint8_t tmp)  
{
    
    if (!(_flags & fstream::out)) {
	fputs("file open only to read", stderr);
	abort();
    }
    if (_cache_pos >= _cache_size) {
	_flush();
	_file_pos += _cache_size;
    }
    _cache_buf[_cache_pos++] = tmp;
    _is_modified = true;
}

unsigned long long cachingIOFileControler::size()
{
    unsigned long long cur_pos =  _caching_stream.tellg();
    if(_caching_stream.seekg(0, _caching_stream.end) <= 0) return 0;
    unsigned long long size = _caching_stream.tellg();
    _caching_stream.seekg(cur_pos, _caching_stream.beg);
    return size;
}

bool cachingIOFileControler::is_file_ended()
{
    return _is_last_block_cached;
}


cachingIOFileControler::~cachingIOFileControler()
{
    _flush();
    _caching_stream.close();
    delete _cache_buf;
}
