package com.zookeeper;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.Properties;

/**
 * Created by yanjd on 2017/12/27.
 */
public class ServerZk {

    public static String serverIp;
    private static Watcher connectionWatcher;
    private static ZooKeeper zk;
    public static final String ZK_IPS = "10.176.63.102:2181";
    public static int sessionTimeout = 5000;
    public static final String rootPath = "/soc";
    static{
        Properties p = new Properties();
        try {
            p.load(AgentZk.class.getResourceAsStream("/node.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        serverIp = String.valueOf(p.getProperty("server.id"));
    }

    public static void main(String args[]) throws IOException, KeeperException, InterruptedException {
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
        }else{
            if(zk.exists(rootPath+"/"+serverIp,false) == null){
                //server节点
                zk.create(rootPath+"/"+serverIp, serverIp.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
                zk.setData(rootPath+"/"+serverIp,serverIp.getBytes(),-1);
            }
        }
    }
}
