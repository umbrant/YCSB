/**
 * Redis client binding for YCSB.
 *
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

package com.yahoo.ycsb.db;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.Future;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.MemcachedClient;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

public class MemcacheClient extends DB {

  private MemcachedClient client;

  public static final String HOSTS_PROPERTY = "memcached.host";

  public void init() throws DBException {
    Properties props = getProperties();
    // space delimited list of host:port pairs
    String hosts = props.getProperty(HOSTS_PROPERTY);
    
    try {
      client = new MemcachedClient(new BinaryConnectionFactory(),
          AddrUtil.getAddresses(hosts));
    } catch (IOException e) {
      throw new DBException("Got error when trying to init memcache client"
          + e.getMessage());
    }
  }

  public void cleanup() throws DBException {
    client.shutdown();
  }

  @Override
  public int read(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {
    client.get(key);
    //Future<Object> future = client.asyncGet(key);
    return 0;
  }

  /**
   * This currently just does a get of the startkey.
   * memcached doesn't support scan operations.
   */
  @Override
  public int scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    read(table, startkey, fields, null);
    return 0;
  }

  @Override
  public int multiget(String table, Collection<String> keys,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    client.getBulk(keys);
    return 0;
  }

  @Override
  public int update(String table, String key,
      HashMap<String, ByteIterator> values) {
    return insert(table, key, values);
  }

  @Override
  public int insert(String table, String key,
      HashMap<String, ByteIterator> values) {
    client.add(key, 0, StringByteIterator.getStringMap(values));
    return 0;
  }

  @Override
  public int delete(String table, String key) {
    client.delete(key);
    return 0;
  }
}
