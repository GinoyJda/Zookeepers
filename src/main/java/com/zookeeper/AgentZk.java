package com.zookeeper;

import org.apache.zookeeper.*;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by yanjd on 2017/12/27.
 */
public class AgentZk {
    public static final String ZK_IPS = "10.176.63.102:2181";
    public static int sessionTimeout = 5000;
    public static final String rootPath = "/soc";
    public static final String agentId ;
    private static Watcher connectionWatcher;
    private static ZooKeeper zk;
    public static String serverIp;

    static{
        Properties p = new Properties();
        try {
            p.load(AgentZk.class.getResourceAsStream("/node.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        agentId = String.valueOf(p.getProperty("node.id"));
        serverIp = String.valueOf(p.getProperty("server.addr"));
    }

    public static void main(String args[]){
        // 创建一个与服务器的连接
        try {
            //Define the connection watch ,which watches the connection event.
            connectionWatcher = new Watcher() {
                // 监控所有被触发的事件
                public void process(WatchedEvent event) {
                    System.out.println("已经触发了" + event.getType() + "事件！");
                }
            };

            zk = new ZooKeeper(ZK_IPS, sessionTimeout, connectionWatcher);

            if(zk.exists(rootPath,false) == null){
                // 根目录
                zk.create(rootPath, "soc".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
                //server节点
                zk.create(rootPath+"/"+serverIp, serverIp.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
                zk.setData(rootPath+"/"+serverIp,serverIp.getBytes(),-1);
                //agent节点
                zk.create(rootPath+"/"+serverIp+"/"+agentId, agentId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
                zk.setData(rootPath+"/"+serverIp+"/"+agentId,agentId.getBytes(),-1);

            }else {
                if(zk.exists(rootPath+"/"+serverIp,false) == null){
                    //server节点
                    zk.create(rootPath+"/"+serverIp, serverIp.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
                    zk.setData(rootPath+"/"+serverIp,serverIp.getBytes(),-1);
                    //agent节点
                    zk.create(rootPath+"/"+serverIp+"/"+agentId, agentId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
                    zk.setData(rootPath+"/"+serverIp+"/"+agentId,agentId.getBytes(),-1);
                }else {
                    if(zk.exists(rootPath+"/"+serverIp+"/"+agentId,false) == null){
                        //agent节点
                        zk.create(rootPath+"/"+serverIp+"/"+agentId, agentId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
                        zk.setData(rootPath+"/"+serverIp+"/"+agentId,agentId.getBytes(),-1);
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
