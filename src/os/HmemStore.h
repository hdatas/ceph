// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef KINETIC_STORE_H
#define KINETIC_STORE_H

#include "include/types.h"
#include "include/buffer.h"
#include "KeyValueDB.h"
#include <set>
#include <map>
#include <string>
#include "include/memory.h"
#include "include/kvs.h"

#include <errno.h>
#include "common/errno.h"
#include "common/dout.h"
#include "include/assert.h"
#include "common/Formatter.h"

#include "common/ceph_context.h"

class PerfCounters;

enum {
  l_hmem_first = 38400,
  l_hmem_gets,
  l_hmem_txns,
  l_hmem_last,
};

/**
 * Uses Hmemstore to implement the KeyValueDB interface
 */
class HmemStore : public KeyValueDB {
  CephContext *cct;
  PerfCounters *logger;
  string devname;
  string storename;
  unsigned long long kvshandle;

  int do_open(ostream &out, bool create_if_missing);

public:
  HmemStore(CephContext *c);
  ~HmemStore();

  static int _test_init(CephContext *c);
  int init();

  /// Opens underlying db
  int open(ostream &out) {
    return do_open(out, false);
  }
  /// Creates underlying db if missing and opens it
  int create_and_open(ostream &out) {
    return do_open(out, true);
  }

  void close();

  enum HmemOpType {
    HMEM_OP_WRITE,
    HMEM_OP_DELETE,
  };

  struct HmemOp {
    HmemOpType type;
    std::string key;
    bufferlist data;
    HmemOp(HmemOpType type, const string &key) : type(type), key(key) {}
    HmemOp(HmemOpType type, const string &key, const bufferlist &data)
      : type(type), key(key), data(data) {}
  };

  class HmemTransactionImpl : public KeyValueDB::TransactionImpl {
  public:
    vector<HmemOp> ops;
    HmemStore *db;

    HmemTransactionImpl(HmemStore *db) : db(db) {}
    void set(
      const string &prefix,
      const string &k,
      const bufferlist &bl);
    void rmkey(
      const string &prefix,
      const string &k);
    void rmkeys_by_prefix(
      const string &prefix
      );
  };

  KeyValueDB::Transaction get_transaction() {
    return ceph::shared_ptr< HmemTransactionImpl >(
      new HmemTransactionImpl(this));
  }

  int submit_transaction(KeyValueDB::Transaction t);
  int submit_transaction_sync(KeyValueDB::Transaction t);
  int get(
    const string &prefix,
    const std::set<string> &key,
    std::map<string, bufferlist> *out
    );

  class HmemWholeSpaceIteratorImpl :
    public KeyValueDB::WholeSpaceIteratorImpl {
    std::set<std::string> keys;
    std::set<std::string>::iterator keys_iter;
  public:
    HmemWholeSpaceIteratorImpl(kinetic::BlockingKineticConnection *conn);
    virtual ~HmemWholeSpaceIteratorImpl() { }

    int seek_to_first() {
      return seek_to_first("");
    }
    int seek_to_first(const string &prefix);
    int seek_to_last();
    int seek_to_last(const string &prefix);
    int upper_bound(const string &prefix, const string &after);
    int lower_bound(const string &prefix, const string &to);
    bool valid();
    int next();
    int prev();
    string key();
    pair<string,string> raw_key();
    bufferlist value();
    int status();
  };

  /// Utility
  static string combine_strings(const string &prefix, const string &value);
  static int split_key(string in_prefix, string *prefix, string *key);
  static bufferlist to_bufferlist(const kinetic::KineticRecord &record);
  virtual uint64_t get_estimated_size(map<string,uint64_t> &extra) {
    // not used by the osd
    return 0;
  }


protected:
  WholeSpaceIterator _get_iterator() {
    return ceph::shared_ptr<KeyValueDB::WholeSpaceIteratorImpl>(
								new HmemWholeSpaceIteratorImpl(kinetic_conn.get()));
  }

  // TODO: remove snapshots from interface
  WholeSpaceIterator _get_snapshot_iterator() {
    return _get_iterator();
  }

};

#endif
