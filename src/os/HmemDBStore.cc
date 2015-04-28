// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "HmemDBStore.h"

#include <set>
#include <map>
#include <string>
#include "include/memory.h"
#include <errno.h>
using std::string;
#include "common/perf_counters.h"

#include <uftl/kvs.h>

int HmemDBStore::init()
{
  // init defaults.  caller can override these if they want
  // prior to calling open.
  derr << __FILE__ << " " << __FUNCTION__ << " " << __LINE__ << dendl;
  options.write_buffer_size = g_conf->leveldb_write_buffer_size;
  options.cache_size = g_conf->leveldb_cache_size;
  options.block_size = g_conf->leveldb_block_size;
  options.bloom_size = g_conf->leveldb_bloom_size;
  options.compression_enabled = g_conf->leveldb_compression;
  options.paranoid_checks = g_conf->leveldb_paranoid;
  options.max_open_files = g_conf->leveldb_max_open_files;
  options.log_file = g_conf->leveldb_log;
  return 0;
}

int HmemDBStore::do_open(ostream &out, bool create_if_missing)
{
  derr << __FILE__ << " " << __FUNCTION__ << " " << __LINE__ << dendl;
  leveldb::Options ldoptions;

  if (options.write_buffer_size)
    ldoptions.write_buffer_size = options.write_buffer_size;
  if (options.max_open_files)
    ldoptions.max_open_files = options.max_open_files;
  if (options.cache_size) {
    leveldb::Cache *_db_cache = leveldb::NewLRUCache(options.cache_size);
    db_cache.reset(_db_cache);
    ldoptions.block_cache = db_cache.get();
  }
  if (options.block_size)
    ldoptions.block_size = options.block_size;
  if (options.bloom_size) {
#ifdef HAVE_LEVELDB_FILTER_POLICY
    const leveldb::FilterPolicy *_filterpolicy =
	leveldb::NewBloomFilterPolicy(options.bloom_size);
    filterpolicy.reset(_filterpolicy);
    ldoptions.filter_policy = filterpolicy.get();
#else
    assert(0 == "bloom size set but installed leveldb doesn't support bloom filters");
#endif
  }
  if (options.compression_enabled)
    ldoptions.compression = leveldb::kSnappyCompression;
  else
    ldoptions.compression = leveldb::kNoCompression;
  if (options.block_restart_interval)
    ldoptions.block_restart_interval = options.block_restart_interval;

  ldoptions.error_if_exists = options.error_if_exists;
  ldoptions.paranoid_checks = options.paranoid_checks;
  ldoptions.create_if_missing = create_if_missing;

  if (options.log_file.length()) {
    leveldb::Env *env = leveldb::Env::Default();
    env->NewLogger(options.log_file, &ldoptions.info_log);
  }

  leveldb::DB *_db;
  leveldb::Status status = leveldb::DB::Open(ldoptions, path, &_db);
  db.reset(_db);
  if (!status.ok()) {
    out << status.ToString() << std::endl;
    return -EINVAL;
  }
  string kvs_path = path + "/hmem_store";
  if (kvs_open((char *)kvs_path.c_str(), (char *)"hmem_store", &kvs_handle) != 0) {
      derr << "kvs_open failed " << dendl;
  } 

  if (g_conf->leveldb_compact_on_mount) {
    derr << "Compacting leveldb store..." << dendl;
    compact();
    derr << "Finished compacting leveldb store" << dendl;
  }

  PerfCountersBuilder plb(g_ceph_context, "leveldb", l_hmemdb_first, l_hmemdb_last);
  plb.add_u64_counter(l_hmemdb_gets, "leveldb_get", "Gets");
  plb.add_u64_counter(l_hmemdb_txns, "leveldb_transaction", "Transactions");
  plb.add_u64_counter(l_hmemdb_compact, "leveldb_compact", "Compactions");
  plb.add_u64_counter(l_hmemdb_compact_range, "leveldb_compact_range", "Compactions by range");
  plb.add_u64_counter(l_hmemdb_compact_queue_merge, "leveldb_compact_queue_merge", "Mergings of ranges in compaction queue");
  plb.add_u64(l_hmemdb_compact_queue_len, "leveldb_compact_queue_len", "Length of compaction queue");
  logger = plb.create_perf_counters();
  cct->get_perfcounters_collection()->add(logger);
  return 0;
}

int HmemDBStore::_test_init(const string& dir)
{
  derr << __FILE__ << " " << __FUNCTION__ << " " << __LINE__ << dendl;
  leveldb::Options options;
  options.create_if_missing = true;
  leveldb::DB *db;
  leveldb::Status status = leveldb::DB::Open(options, dir, &db);
  delete db;
  derr << "Open Status " << status.ToString() << dendl;

  int64 _kvs_handle;
  string kvs_path = dir + "/hmem_store";
  if (kvs_open((char *)kvs_path.c_str(), (char *)"hmem_store", &_kvs_handle) != 0) {
      derr << "kvs_open failed " << dendl;
  } else {
      kvs_close(_kvs_handle);
  }

  return status.ok() ? 0 : -EIO;
}

HmemDBStore::~HmemDBStore()
{
  derr << __FILE__ << " " << __FUNCTION__ << " " << __LINE__ << dendl;
  close();
  delete logger;

  // Ensure db is destroyed before dependent db_cache and filterpolicy
  db.reset();
}

void HmemDBStore::close()
{
  derr << __FILE__ << " " << __FUNCTION__ << " " << __LINE__ << dendl;
  // stop compaction thread
  compact_queue_lock.Lock();
  if (compact_thread.is_started()) {
    compact_queue_stop = true;
    compact_queue_cond.Signal();
    compact_queue_lock.Unlock();
    compact_thread.join();
  } else {
    compact_queue_lock.Unlock();
  }

  if (logger)
    cct->get_perfcounters_collection()->remove(logger);
}

int HmemDBStore::submit_transaction(KeyValueDB::Transaction t)
{
  derr << __FILE__ << " " << __FUNCTION__ << " " << __LINE__ << dendl;
  struct kvs_ops cmd = {0};
  static unsigned long total_len = 0;
  int status;
  HmemDBTransactionImpl * _t =
    static_cast<HmemDBTransactionImpl *>(t.get());
  leveldb::Status s = db->Write(leveldb::WriteOptions(), &(_t->bat));
  logger->inc(l_hmemdb_txns);

  for (vector<HmemDBOp>::iterator it=_t->ops.begin(); it != _t->ops.end(); ++it) {
    if (it->type == HMEMDB_WRITE) {
      derr << "HMEMDB_WRITE Key " << it->key << dendl;
      cmd.cmd = PUT;
      cmd.key = (unsigned char *)it->key.c_str();
      cmd.key_len = it->key.length();
      cmd.value = (unsigned char *)it->data.c_str();
      cmd.value_len = it->data.length();
      //total_len += cmd.value_len;
      status = kvs_cmd(kvs_handle, &cmd);
      if (status)
          derr << "Key Put failed " << cmd.key << dendl; 
    } else if (it->type == HMEMDB_DELETE) {
      cmd.cmd = DELETE;
      cmd.key = (unsigned char *)it->key.c_str();
      cmd.key_len = it->key.length();
      status = kvs_cmd(kvs_handle, &cmd);
      if (status)
          derr << "Key Delete failed " << cmd.key << dendl; 
    }
  } 
  return s.ok() ? 0 : -1;
}

int HmemDBStore::submit_transaction_sync(KeyValueDB::Transaction t)
{
  //derr << __FILE__ << " " << __FUNCTION__ << " " << __LINE__ << dendl;
  struct kvs_ops cmd = {0};
  static unsigned long total_len = 0;
  int status;
  HmemDBTransactionImpl * _t =
    static_cast<HmemDBTransactionImpl *>(t.get());
  leveldb::Status s;
  leveldb::WriteOptions options;
  options.sync = true;
  s = db->Write(options, &(_t->bat));
  logger->inc(l_hmemdb_txns);

  for (vector<HmemDBOp>::iterator it=_t->ops.begin(); it != _t->ops.end(); ++it) {
    if (it->type == HMEMDB_WRITE) {
      cmd.cmd = PUT;
      cmd.key = (unsigned char *)it->key.c_str();
      cmd.key_len = it->key.length();
      cmd.value = (unsigned char *)it->data.c_str();
      cmd.value_len = it->data.length();
      total_len += cmd.value_len;
      derr << "Key Put  " << cmd.key << dendl;
      status = kvs_cmd(kvs_handle, &cmd);
      if (status)
          derr << "Key Put Success " << dendl;
    } else if (it->type == HMEMDB_DELETE) {
      cmd.cmd = DELETE;
      cmd.key = (unsigned char *)it->key.c_str();
      cmd.key_len = it->key.length();
      status = kvs_cmd(kvs_handle, &cmd);
      if (status)
          derr << "Key Delete failed " << cmd.key << dendl; 
    }
  } 
  return 0;
  //return s.ok() ? 0 : -1;
}

void HmemDBStore::HmemDBTransactionImpl::set(
  const string &prefix,
  const string &k,
  const bufferlist &to_set_bl)
{
  //derr << __FILE__ << " " << __FUNCTION__ << " " << __LINE__ << dendl;
  string key = combine_strings(prefix, k);
  buffers.push_back(to_set_bl);
  bufferlist &bl = *(buffers.rbegin());
  keys.push_back(key);
  derr << "Set: Prefix " <<  prefix << "Key " << k << dendl;
  bat.Delete(leveldb::Slice(*(keys.rbegin())));
  bat.Put(leveldb::Slice(*(keys.rbegin())),
	  leveldb::Slice(bl.c_str(), bl.length()));

  ops.push_back(HmemDBOp(HMEMDB_WRITE, key, to_set_bl));
}

void HmemDBStore::HmemDBTransactionImpl::rmkey(const string &prefix,
					         const string &k)
{
  derr << __FILE__ << " " << __FUNCTION__ << " " << __LINE__ << dendl;
  string key = combine_strings(prefix, k);
  keys.push_back(key);
  bat.Delete(leveldb::Slice(*(keys.rbegin())));

  ops.push_back(HmemDBOp(HMEMDB_DELETE, key));
}

void HmemDBStore::HmemDBTransactionImpl::rmkeys_by_prefix(const string &prefix)
{
  derr << __FILE__ << " " << __FUNCTION__ << " " << __LINE__ << dendl;
  KeyValueDB::Iterator it = db->get_iterator(prefix);
  for (it->seek_to_first();
       it->valid();
       it->next()) {
    string key = combine_strings(prefix, it->key());
    keys.push_back(key);
    bat.Delete(*(keys.rbegin()));
  }
}

int HmemDBStore::get(
    const string &prefix,
    const std::set<string> &keys,
    std::map<string, bufferlist> *out)
{
  //derr << __FILE__ << " " << __FUNCTION__ << " " << __LINE__ << dendl;
  KeyValueDB::Iterator it = get_iterator(prefix);
  for (std::set<string>::const_iterator i = keys.begin();
       i != keys.end();
       ++i) {
    //derr << "get keys " << keys << dendl;
    //derr << "prefix " << prefix << dendl;
    it->lower_bound(*i);
    //derr << "it->key " << *i << dendl;
    if (it->valid() && it->key() == *i) {
      out->insert(make_pair(*i, it->value()));
    } else if (!it->valid())
      break;
  }
  logger->inc(l_hmemdb_gets);
  return 0;
}

string HmemDBStore::combine_strings(const string &prefix, const string &value)
{
  //derr << __FILE__ << " " << __FUNCTION__ << " " << __LINE__ << dendl;
  string out = prefix;
  out.push_back(0);
  out.append(value);
  return out;
}

bufferlist HmemDBStore::to_bufferlist(leveldb::Slice in)
{
  //derr << __FILE__ << " " << __FUNCTION__ << " " << __LINE__ << dendl;
  bufferlist bl;
  bl.append(bufferptr(in.data(), in.size()));
  return bl;
}

int HmemDBStore::split_key(leveldb::Slice in, string *prefix, string *key)
{
  //derr << __FILE__ << " " << __FUNCTION__ << " " << __LINE__ << dendl;
  string in_prefix = in.ToString();
  size_t prefix_len = in_prefix.find('\0');
  if (prefix_len >= in_prefix.size())
    return -EINVAL;

  if (prefix)
    *prefix = string(in_prefix, 0, prefix_len);
  if (key)
    *key= string(in_prefix, prefix_len + 1);
  return 0;
}

void HmemDBStore::compact()
{
  derr << __FILE__ << " " << __FUNCTION__ << " " << __LINE__ << dendl;
  logger->inc(l_hmemdb_compact);
  db->CompactRange(NULL, NULL);
}


void HmemDBStore::compact_thread_entry()
{
  derr << __FILE__ << " " << __FUNCTION__ << " " << __LINE__ << dendl;
  compact_queue_lock.Lock();
  while (!compact_queue_stop) {
    while (!compact_queue.empty()) {
      pair<string,string> range = compact_queue.front();
      compact_queue.pop_front();
      logger->set(l_hmemdb_compact_queue_len, compact_queue.size());
      compact_queue_lock.Unlock();
      logger->inc(l_hmemdb_compact_range);
      compact_range(range.first, range.second);
      compact_queue_lock.Lock();
      continue;
    }
    compact_queue_cond.Wait(compact_queue_lock);
  }
  compact_queue_lock.Unlock();
}

void HmemDBStore::compact_range_async(const string& start, const string& end)
{
  derr << __FILE__ << " " << __FUNCTION__ << " " << __LINE__ << dendl;
  Mutex::Locker l(compact_queue_lock);

  // try to merge adjacent ranges.  this is O(n), but the queue should
  // be short.  note that we do not cover all overlap cases and merge
  // opportunities here, but we capture the ones we currently need.
  list< pair<string,string> >::iterator p = compact_queue.begin();
  while (p != compact_queue.end()) {
    if (p->first == start && p->second == end) {
      // dup; no-op
      return;
    }
    if (p->first <= end && p->first > start) {
      // merge with existing range to the right
      compact_queue.push_back(make_pair(start, p->second));
      compact_queue.erase(p);
      logger->inc(l_hmemdb_compact_queue_merge);
      break;
    }
    if (p->second >= start && p->second < end) {
      // merge with existing range to the left
      compact_queue.push_back(make_pair(p->first, end));
      compact_queue.erase(p);
      logger->inc(l_hmemdb_compact_queue_merge);
      break;
    }
    ++p;
  }
  if (p == compact_queue.end()) {
    // no merge, new entry.
    compact_queue.push_back(make_pair(start, end));
    logger->set(l_hmemdb_compact_queue_len, compact_queue.size());
  }
  compact_queue_cond.Signal();
  if (!compact_thread.is_started()) {
    compact_thread.create();
  }
}
