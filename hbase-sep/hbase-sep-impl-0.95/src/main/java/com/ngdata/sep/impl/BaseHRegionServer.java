/*
 * Copyright 2012 NGDATA nv
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
 */
package com.ngdata.sep.impl;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

/**
 *
 */
public class BaseHRegionServer implements AdminProtos.AdminService.BlockingInterface, Server {

  @Override
  public AdminProtos.GetRegionInfoResponse getRegionInfo(RpcController rpcController, AdminProtos.GetRegionInfoRequest getRegionInfoRequest) throws ServiceException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public AdminProtos.GetStoreFileResponse getStoreFile(RpcController rpcController, AdminProtos.GetStoreFileRequest getStoreFileRequest) throws ServiceException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public AdminProtos.GetOnlineRegionResponse getOnlineRegion(RpcController rpcController, AdminProtos.GetOnlineRegionRequest getOnlineRegionRequest) throws ServiceException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public AdminProtos.OpenRegionResponse openRegion(RpcController rpcController, AdminProtos.OpenRegionRequest openRegionRequest) throws ServiceException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public AdminProtos.CloseRegionResponse closeRegion(RpcController rpcController, AdminProtos.CloseRegionRequest closeRegionRequest) throws ServiceException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public AdminProtos.FlushRegionResponse flushRegion(RpcController rpcController, AdminProtos.FlushRegionRequest flushRegionRequest) throws ServiceException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public AdminProtos.SplitRegionResponse splitRegion(RpcController rpcController, AdminProtos.SplitRegionRequest splitRegionRequest) throws ServiceException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public AdminProtos.CompactRegionResponse compactRegion(RpcController rpcController, AdminProtos.CompactRegionRequest compactRegionRequest) throws ServiceException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public AdminProtos.MergeRegionsResponse mergeRegions(RpcController rpcController, AdminProtos.MergeRegionsRequest mergeRegionsRequest) throws ServiceException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public AdminProtos.ReplicateWALEntryResponse replicateWALEntry(RpcController rpcController, AdminProtos.ReplicateWALEntryRequest replicateWALEntryRequest) throws ServiceException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public ClientProtos.MultiResponse replay(RpcController rpcController, ClientProtos.MultiRequest multiRequest) throws ServiceException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public AdminProtos.RollWALWriterResponse rollWALWriter(RpcController rpcController, AdminProtos.RollWALWriterRequest rollWALWriterRequest) throws ServiceException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public AdminProtos.GetServerInfoResponse getServerInfo(RpcController rpcController, AdminProtos.GetServerInfoRequest getServerInfoRequest) throws ServiceException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public AdminProtos.StopServerResponse stopServer(RpcController rpcController, AdminProtos.StopServerRequest stopServerRequest) throws ServiceException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Configuration getConfiguration() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public ZooKeeperWatcher getZooKeeper() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public CatalogTracker getCatalogTracker() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public ServerName getServerName() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void abort(String s, Throwable throwable) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean isAborted() {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void stop(String s) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean isStopped() {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
