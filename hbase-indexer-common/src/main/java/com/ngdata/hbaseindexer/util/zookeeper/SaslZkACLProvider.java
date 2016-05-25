/*
 * Copyright 2015 NGDATA nv
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

package com.ngdata.hbaseindexer.util.zookeeper;

import com.ngdata.hbaseindexer.ConfKeys;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.KerberosName;

import java.util.ArrayList;
import java.util.List;

/**
 * ACLProvider that sets all ACLs to be owned by a configurable user
 * (typically hbase) via sasl auth, and readable by world.
 */
public class SaslZkACLProvider implements ACLProvider {
    private final String saslUser;
    private final List<ACL> defaultACLs;
    private final List<ACL> indexerProcessACLs;
    private final String zkBaseNode;

    public SaslZkACLProvider(Configuration conf, String hostname) throws Exception {
        this.saslUser = getPrincipalName(conf, hostname);
        defaultACLs = new ArrayList<ACL>();
        defaultACLs.add(new ACL(ZooDefs.Perms.ALL, new Id("sasl", saslUser)));

        // the indexer process znodes have to be readable for service discovery
        indexerProcessACLs = new ArrayList<ACL>(defaultACLs);
        indexerProcessACLs.add(new ACL(ZooDefs.Perms.READ, ZooDefs.Ids.ANYONE_ID_UNSAFE));
        this.zkBaseNode = conf.get(ConfKeys.ZK_ROOT_NODE) + "/indexerprocess";
    }

    @Override
    public List<ACL> getAclForPath(String path) {
        if (isIndexerProcessZNode(path)) {
            return indexerProcessACLs;
        } else {
            return getDefaultAcl();
        }
    }

    @Override
    public List<ACL> getDefaultAcl() {
        return defaultACLs;
    }

    private String getPrincipalName(Configuration conf, String hostname) throws Exception {
        // essentially running as an HBase RegionServer
        String principalProp = conf.get("hbase.regionserver.kerberos.principal");
        if (principalProp != null) {
            String princ = SecurityUtil.getServerPrincipal(principalProp, hostname);
            KerberosName kerbName = new KerberosName(princ);
            return kerbName.getShortName();
        }
        return "hbase";
    }

    private boolean isIndexerProcessZNode(String path) {
        return (path != null && (path.equals(zkBaseNode) || path.startsWith(zkBaseNode + "/")));
    }
}
