#include "rvm.h"
#include <unordered_map>
#include <unordered_set>
#include <sys/stat.h>
#include <iostream>
#include <unistd.h>
#include <fcntl.h>
#include <list>
#include <cstring>

using namespace std;

#define CERR_AND_RETURN(text, val)\
  cerr <<  text << endl;\
  return val;\

#define CERR_AND_EXIT(text)\
  cerr <<  text << endl;\
  exit(EXIT_FAILURE);\

namespace {
  enum class Status {MAPPED, UNMAPPED, ACQUIRED};
  
  struct Segment{
    void* segbase = nullptr;
    int size = -1;
    int mapped_size = -1;
    int fd = -1;
    Status st = Status::UNMAPPED;
    string filepath;
  };

  struct RVM {
    const string                    _backing_store;
    unordered_map<string, Segment*> _name_seg_map;
    unordered_map<void*, Segment*>  _base_seg_map;
    
    RVM(const char* directory) : _backing_store(directory){}

    Segment* find_seg_by_name(const char* name) {
      auto itr = _name_seg_map.find(name);
      if(itr == _name_seg_map.end()){
        return nullptr;
      }
      return itr->second;
    } 
    
    Segment* find_seg_by_base(void* base) {
      auto itr = _base_seg_map.find(base);
      if(itr == _base_seg_map.end()){
        return nullptr;
      }
      return itr->second;
    } 

    std::string get_filepath(const char* segname) { return std::move("./" + _backing_store + "/" + segname); }
  };


  struct Transcation {
    struct Region {
      Segment* _seg;
      int _offset;
      int _size;
      void* _undo_log;
      Region(Segment* seg, int offset, int size) : _seg(seg), _offset(offset), _size(size), _undo_log(::operator new(size)) {
        memcpy(_undo_log, (char*)_seg->segbase + offset, _size);
      }
    };

    std::list<Region> _regions;
    std::unordered_map<void*, Segment*> _segs;

    void modify(void *segbase, int offset, int size) {
      auto itr = _segs.find(segbase);
      if (_segs.end() == itr) { CERR_AND_EXIT("Error. Transaction tried modifying a segment for which it was not registered.") }
      Segment* seg = itr->second;
      if (seg->mapped_size < offset + size) { CERR_AND_EXIT("Error. Offset and size exceeds the segment's mapped size") }
      _regions.push_back(std::move(Region(seg, offset, size)));
    }
    
    void release_hold_segs() {
      for(const auto& seg_pair: _segs) { seg_pair.second->st = Status::MAPPED; }
    }

    void commit() {
      for(const auto& region : _regions) {
        Segment* seg = region._seg;
        if (-1 == lseek(seg->fd, region._offset, SEEK_SET)) { CERR_AND_EXIT("Error while setting offset during commit.") } 
        if (region._size != write(seg->fd, (char*)seg->segbase + region._offset, region._size))
          { CERR_AND_EXIT("Error writing to the file during commit.") }
        if (0 != fsync(seg->fd)) { CERR_AND_EXIT("Error while fsync-ing file") }
      }
    }
    
    void abort(){
      for(const auto& region : _regions) 
        { memcpy((char*)region._seg->segbase + region._offset, region._undo_log, region._size); }
    }
  };

  struct gen_id{
    int current = 0;
    int operator()() { return ++current; }
  } rvm_id, trans_id;

  unordered_map<rvm_t, RVM> rvms;
  unordered_set<string> rvm_dirs;
  unordered_map<trans_t, Transcation> transs;
}

#define CHECK_RVM_VALIDITY(rvm_ref, rvm)\
  auto itr_rvm = rvms.find(rvm);  \
  if(rvms.end() == itr_rvm) {\
    cerr <<  "Not a valid rmv passed." << endl;  \
    exit(EXIT_FAILURE); \
  }\
  RVM& rvm_ref = itr_rvm->second;\

#define CHECK_TRANS_VALIDITY(trans_ref, tid)\
  auto itr_trans = transs.find(tid);  \
  if(transs.end() == itr_trans) {\
    cerr <<  "Not a valid tid passed." << endl;  \
    exit(EXIT_FAILURE); \
  }\
  Transcation& trans_ref = itr_trans->second;\

#define CLOSE_FILE_AND_EXIT(fd, err_text) \
  close(fd);\
  CERR_AND_EXIT(err_text << ", for segment: " << segname)\

rvm_t rvm_init(const char *directory) {
  if(!rvm_dirs.insert(directory).second) { CERR_AND_EXIT("Error. RVM already initialized with this directory: " << directory) }
  mkdir(directory, S_IRWXU|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH);
  return rvms.insert(make_pair(rvm_id(), move(RVM(directory)))).first->first;
}

void *rvm_map(rvm_t rvm, const char *segname, int size_to_create) {
  CHECK_RVM_VALIDITY(rvm_ref, rvm);
  
  Segment* seg = rvm_ref.find_seg_by_name(segname);
  if (seg != nullptr) {
    if (seg->st != Status::UNMAPPED) { CERR_AND_RETURN("Error: Mapping segment: " << segname << " twice.", (void*)-1) }
  }
  else{ seg = new Segment; }
  seg->filepath = rvm_ref.get_filepath(segname); 
  seg->fd = open(seg->filepath.c_str(), O_RDWR | O_CREAT, S_IRWXU|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH);
  if (seg->fd == -1) { CLOSE_FILE_AND_EXIT(seg->fd, "Error opening file in rvm_map") }

  seg->mapped_size = size_to_create;
  seg->size = lseek(seg->fd, 0, SEEK_END);
  if (seg->size == -1) { CLOSE_FILE_AND_EXIT(seg->fd, "Error getting file size. in rvm_map") }  

  if (size_to_create > seg->size) { 
    if (-1 == lseek(seg->fd, size_to_create - 1, SEEK_SET)) { CLOSE_FILE_AND_EXIT(seg->fd, "Error changing file size in rvm_map") }
    if (1 != write(seg->fd, "", 1)) { CLOSE_FILE_AND_EXIT(seg->fd, "Error writing to the file in rvm_map") }
  }
  
  seg->segbase = ::operator new(size_to_create);  
  if (0 != lseek(seg->fd, 0, SEEK_SET)) { CLOSE_FILE_AND_EXIT(seg->fd, "Error setting offset to beginning of the file in rvm_map") }
  if (size_to_create != read(seg->fd, seg->segbase, size_to_create))
  { CLOSE_FILE_AND_EXIT(seg->fd, "Error copying segment content to memory in rvm_map") }

  seg->st = Status::MAPPED;
  rvm_ref._name_seg_map[segname] = seg;
  rvm_ref._base_seg_map[seg->segbase] = seg;
  return seg->segbase;
}

void rvm_unmap(rvm_t rvm, void *segbase) {
  CHECK_RVM_VALIDITY(rvm_ref, rvm);
  
  Segment* seg = rvm_ref.find_seg_by_base(segbase);
  if (seg == nullptr || seg->st == Status::UNMAPPED) { return; }
  if (seg->st != Status::MAPPED){ CERR_AND_EXIT("Error. Trying to unmap a segment which in use by a live transaction.") }
  close(seg->fd);
  ::operator delete(seg->segbase);
  seg->st = Status::UNMAPPED;
  rvm_ref._base_seg_map.erase(segbase);
}

void rvm_destroy(rvm_t rvm, const char *segname) {
  CHECK_RVM_VALIDITY(rvm_ref, rvm);
  
  Segment* seg = rvm_ref.find_seg_by_name(segname);
  if (seg != nullptr) {
    if(seg->st != Status::UNMAPPED) { return; }
    rvm_ref._name_seg_map.erase(segname);
    delete seg;
  }
  remove(rvm_ref.get_filepath(segname).c_str());
}

trans_t rvm_begin_trans(rvm_t rvm, int numsegs, void **segbases) {
  CHECK_RVM_VALIDITY(rvm_ref, rvm);

  for(int i=0; i<numsegs; ++i) {
    Segment* seg = rvm_ref.find_seg_by_base(segbases[i]);
    if (seg == nullptr) { CERR_AND_RETURN("Couldn't find a mapped segbase passed in rvm_begin_trans.", -1) }
    if (seg->st != Status::MAPPED) { CERR_AND_RETURN("Status of a segment should have been mapped.", -1) } 
  }
  
  auto itr = transs.insert(std::make_pair(trans_id(), std::move(Transcation()))).first;
  for(int i=0; i<numsegs; ++i) { 
    Segment* seg = rvm_ref.find_seg_by_base(segbases[i]);
    seg->st = Status::ACQUIRED; 
    itr->second._segs[seg->segbase] = seg;
  }
  return itr->first;  
}

void rvm_about_to_modify(trans_t tid, void *segbase, int offset, int size) {
  CHECK_TRANS_VALIDITY(trans_ref, tid);  
  trans_ref.modify(segbase,offset, size);
}

void rvm_commit_trans(trans_t tid) {
  CHECK_TRANS_VALIDITY(trans_ref, tid);  
  trans_ref.commit();
  trans_ref.release_hold_segs();
  transs.erase(tid);
}

void rvm_abort_trans(trans_t tid){
  CHECK_TRANS_VALIDITY(trans_ref, tid);  
  trans_ref.abort();
  trans_ref.release_hold_segs();
  transs.erase(tid);
}

void rvm_truncate_log(rvm_t rvm) {
  //nothing to do as directly written to disk during commit and non undo logs in hard disk.
}