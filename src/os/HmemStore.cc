// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "HmemStore.h"
#include "common/ceph_crypto.h"

#include <set>
#include <map>
#include <string>
#include "include/memory.h"
#include <errno.h>
using std::string;
#include "common/perf_counters.h"

#define dout_subsys ceph_subsys_keyvaluestore

int HmemStore::init()
{
  // init defaults.  caller can override these if they want
  // prior to calling open.
  devname = cct->_conf->devname;
  storename = cct->_conf->storename;
  return 0;
}

int HmemStore::_test_init(CephContext *c)
{
  return  0;
}

int HmemStore::do_open(ostream &out, bool create_if_missing)
{
    int status  = kvs_open(devname, storename, &devhandle)    
    if (!status) {
    derr << "Unable to connect to hmem store " << devname << ":" << storename
	 << " : " << status<< dendl;
    return -EINVAL;
  }

  PerfCountersBuilder plb(g_ceph_context, "hmem", l_hmem_first, l_hmem_last);
  plb.add_u64_counter(l_hmem_gets, "hmem_get");
  plb.add_u64_counter(l_hmem_txns, "hmem_transaction");
  logger = plb.create_perf_counters();
  cct->get_perfcounters_collection()->add(logger);
  return 0;
}

HmemStore::HmemStore(CephContext *c) :
  cct(c),
  logger(NULL)
{
    devname =  c->_conf->devname;
    storename = c->_conf->storename;
}

HmemStore::~HmemStore()
{
  close();
  delete logger;
}

void HmemStore::close()
{
  kvs_close(kvshandle);  
  if (logger)
    cct->get_perfcounters_collection()->remove(logger);
}

int HmemStore::submit_transaction(KeyValueDB::Transaction t)
{
  HmemTransactionImpl * _t =
    static_cast<HmemTransactionImpl *>(t.get());
  struct kvs_ops  cmd = {0};
  int status = 0;
  dout(20) << "hmem submit_transaction" << dendl;

  for (vector<HmemOp>::iterator it = _t->ops.begin();
       it != _t->ops.end(); ++it) {
    if (it->type == HMEM_OP_WRITE) {
      string data(it->data.c_str(), it->data.length());
      dout(30) << "hmem before put of " << it->key << " (" << data.length() << " bytes)" << dendl;
      
      cmd.cmd = PUT;
      cmd.key =  it->key.c_str();
      cmd.key_len =  it->key.length();
      cmd.value = it->data.c_str();
      cmd.value_len = it->data.length();
      status = kvs_cmd(kvshandle, &cmd);
      dout(30) << "hmem after put of " << it->key << dendl;
    } else {
      assert(it->type == HMEM_OP_DELETE);
      dout(30) << "hmem before delete" << dendl;
      cmd.cmd = DELETE;
      cmd.key = it->key.c_str();
      cmd.key_len = it->key.length();

      status = kvs_cmd(kvshandle, &cmd);
      dout(30) << "hmem after delete" << dendl;
    }
    if (status < 0) {
      derr << "hmem error submitting transaction: "
	   << status << dendl;
      return -1;
    }
  }

  logger->inc(l_hmem_txns);
  return 0;
}

int HmemStore::submit_transaction_sync(KeyValueDB::Transaction t)
{
  return submit_transaction(t);
}

void HmemStore::HmemTransactionImpl::set(
  const string &prefix,
  const string &k,
  const bufferlist &to_set_bl)
{
  string key = combine_strings(prefix, k);
  dout(30) << "hmem set key " << key << dendl;
  ops.push_back(HmemOp(HMEM_OP_WRITE, key, to_set_bl));
}

void HmemStore::HmemTransactionImpl::rmkey(const string &prefix,
					         const string &k)
{
  string key = combine_strings(prefix, k);
  dout(30) << "hmem rm key " << key << dendl;
  ops.push_back(HmemOp(HMEM_OP_DELETE, key));
}

void HmemStore::HmemTransactionImpl::rmkeys_by_prefix(const string &prefix)
{
  dout(20) << "hmem rmkeys_by_prefix " << prefix << dendl;
  KeyValueDB::Iterator it = db->get_iterator(prefix);
  for (it->seek_to_first();
       it->valid();
       it->next()) {
    string key = combine_strings(prefix, it->key());
    ops.push_back(HmemOp(HMEM_OP_DELETE, key));
    dout(30) << "hmem rm key by prefix: " << key << dendl;
  }
}

int HmemStore::get(
    const string &prefix,
    const std::set<string> &keys,
    std::map<string, bufferlist> *out)
{
  struct kvs_cmd cmd = {0};
  int status = 0;
  dout(30) << "hmem get prefix: " << prefix << " keys: " << keys << dendl;
  for (std::set<string>::const_iterator i = keys.begin();
       i != keys.end();
       ++i) {
    string key = combine_strings(prefix, *i);
    dout(30) << "before get key " << key << dendl;
    cmd.cmd = GET;
    cmd.key =  key.c_str();
    cmd.key_len = key.length();
   
    kvs_cmd(kvshandle, &cmd);

    if (status < 0)
      break;
    dout(30) << "hmem get got key: " << key << dendl;
    out->insert(make_pair(key, to_bufferlist(*cmd.value)));
  }
  logger->inc(l_hmem_gets);
  return 0;
}

string HmemStore::combine_strings(const string &prefix, const string &value)
{
  string out = prefix;
  out.push_back(1);
  out.append(value);
  return out;
}

bufferlist HmemStore::to_bufferlist(struct kvs_cmd  &cmd)
{
  bufferlist bl;
  bl.append(*(cmd.value));
  return bl;
}

int HmemStore::split_key(string in_prefix, string *prefix, string *key)
{
  size_t prefix_len = in_prefix.find('\1');
  if (prefix_len >= in_prefix.size())
    return -EINVAL;

  if (prefix)
    *prefix = string(in_prefix, 0, prefix_len);
  if (key)
    *key= string(in_prefix, prefix_len + 1);
  return 0;
}

HmemStore::HmemWholeSpaceIteratorImpl::HmemWholeSpaceIteratorImpl()
{
  dout(30) << "Hmem iterator constructor() not supported yet" << dendl;
}

int HmemStore::HmemWholeSpaceIteratorImpl::seek_to_first(const string &prefix)
{
  dout(30) << "hmem iterator seek_to_first(prefix): " << prefix << dendl;
  keys_iter = keys.lower_bound(prefix);
  return 0;
}

int HmemStore::HmemWholeSpaceIteratorImpl::seek_to_last()
{
  dout(30) << "hmem iterator seek_to_last()" << dendl;
  keys_iter = keys.end();
  if (keys.begin() != keys_iter)
    --keys_iter;
  return 0;
}

int HmemStore::HmemWholeSpaceIteratorImpl::seek_to_last(const string &prefix)
{
  dout(30) << "hmem iterator seek_to_last(prefix): " << prefix << dendl;
  keys_iter = keys.upper_bound(prefix + "\2");
  if (keys.begin() == keys_iter) {
    keys_iter = keys.end();
  } else {
    --keys_iter;
  }
  return 0;
}

int HmemStore::HmemWholeSpaceIteratorImpl::upper_bound(const string &prefix, const string &after) {
  dout(30) << "hmem iterator upper_bound()" << dendl;
  string bound = combine_strings(prefix, after);
  keys_iter = keys.upper_bound(bound);
  return 0;
}

int HmemStore::HmemWholeSpaceIteratorImpl::lower_bound(const string &prefix, const string &to) {
  dout(30) << "hmem iterator lower_bound()" << dendl;
  string bound = combine_strings(prefix, to);
  keys_iter = keys.lower_bound(bound);
  return 0;
}

bool HmemStore::HmemWholeSpaceIteratorImpl::valid() {
  dout(30) << "hmem iterator valid()" << dendl;
  return keys_iter != keys.end();
}

int HmemStore::HmemWholeSpaceIteratorImpl::next() {
  dout(30) << "hmem iterator next()" << dendl;
  if (keys_iter != keys.end()) {
      ++keys_iter;
      return 0;
  }
  return -1;
}

int HmemStore::HmemWholeSpaceIteratorImpl::prev() {
  dout(30) << "hmem iterator prev()" << dendl;
  if (keys_iter != keys.begin()) {
      --keys_iter;
      return 0;
  }
  keys_iter = keys.end();
  return -1;
}

string HmemStore::HmemWholeSpaceIteratorImpl::key() {
  dout(30) << "hmem iterator key()" << dendl;
  string out_key;
  split_key(*keys_iter, NULL, &out_key);
  return out_key;
}

pair<string,string> HmemStore::HmemWholeSpaceIteratorImpl::raw_key() {
  dout(30) << "hmem iterator raw_key()" << dendl;
  string prefix, key;
  split_key(*keys_iter, &prefix, &key);
  return make_pair(prefix, key);
}

bufferlist HmemStore::HmemWholeSpaceIteratorImpl::value() {
  dout(30) << "hmem iterator value()" << dendl;
  unique_ptr<kinetic::KineticRecord> record;
  kinetic_status = kinetic_conn->Get(*keys_iter, record);
  return to_bufferlist(*record.get());
}

int HmemStore::HmemWholeSpaceIteratorImpl::status() {
  dout(30) << "hmem iterator status()" << dendl;
  return 0 ;
}
