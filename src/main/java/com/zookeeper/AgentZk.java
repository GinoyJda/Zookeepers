package com.zookeeper;

import org.apache.zookeeper.*;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Created by yanjd on 2017/12/27.
 */
public class AgentZk  implements Runnable{
    public   String ZK_IPS = "10.176.63.102:2181";
    public  int sessionTimeout = 5000;
    public   String rootPath = "/soc";
    public   String agentId ;
    private  Watcher agentPathWatcher;
    private  ZooKeeper zk;
    public  String serverIp;
    public  String noticePoint = "notice";
    protected boolean alive = true;
    private Watcher connectionWatcher = null;


    public AgentZk(){
        Properties p = new Properties();
        try {
            p.load(AgentZk.class.getResourceAsStream("/node.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        agentId = String.valueOf(p.getProperty("node.id"));
        serverIp = String.valueOf(p.getProperty("server.addr"));

        registNoticeMonitor();

        connectionWatcher = new Watcher() {
            public void process(WatchedEvent event) {
                if(event.getType() == Event.EventType.None &&
                        event.getState() == Event.KeeperState.SyncConnected) {
                    System.out.printf("\n connection watch output event: %s", event.toString());
                }
            }
        };

        try {
            zk = new ZooKeeper(ZK_IPS, sessionTimeout, connectionWatcher);

        } catch (IOException e) {
            e.printStackTrace();
        }

        registZooKeeperTree();

        // Set a watch on the parent znode
        try {
            zk.getChildren(rootPath+"/"+serverIp+"/"+agentId, agentPathWatcher);
            System.out.print(rootPath+"/"+serverIp+"/"+agentId);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public void registNoticeMonitor(){
        agentPathWatcher = new Watcher() {
            public void process(WatchedEvent event) {
                System.out.println(event.getType());
                if(event.getType() == Event.EventType.NodeChildrenChanged){
                    try {
                        Thread.sleep(3000);
                        //step1;modify agentId
                        String newServerid = new String(zk.getData(rootPath+"/"+serverIp+"/"+agentId,true,null));
                        System.out.println("newServerid:"+newServerid);
                        //删除老分支
//                        zk.delete(rootPath+"/"+serverIp+"/"+agentId+"/"+noticePoint,-1);
                        zk.delete(rootPath+"/"+serverIp+"/"+agentId,-1);
                        serverIp = newServerid;
                        registZooKeeperTree();
                        //重新监听
                        zk.getChildren(rootPath+"/"+serverIp+"/"+agentId, agentPathWatcher);
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    //step2:modify config
                }
            }
        };
    }


    public  void registZooKeeperTree(){
        // 创建一个与服务器的连接
        try {
            //Define the connection watch ,which watches the connection event.
            if(zk.exists(rootPath,false) == null){
                // 根目录
                zk.create(rootPath, "soc".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
                //server节点
                zk.create(rootPath+"/"+serverIp, serverIp.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
                zk.setData(rootPath+"/"+serverIp,serverIp.getBytes(),-1);
                //agent节点
                zk.create(rootPath+"/"+serverIp+"/"+agentId, agentId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
                zk.setData(rootPath+"/"+serverIp+"/"+agentId,serverIp.getBytes(),-1);
                zk.create(rootPath+"/"+serverIp+"/"+agentId+"/"+noticePoint, noticePoint.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);

            }else {
                if(zk.exists(rootPath+"/"+serverIp,false) == null){
                    //server节点
                    zk.create(rootPath+"/"+serverIp, serverIp.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
                    zk.setData(rootPath+"/"+serverIp,serverIp.getBytes(),-1);
                    //agent节点
                    zk.create(rootPath+"/"+serverIp+"/"+agentId, agentId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
                    zk.setData(rootPath+"/"+serverIp+"/"+agentId,serverIp.getBytes(),-1);
                    zk.create(rootPath+"/"+serverIp+"/"+agentId+"/"+noticePoint, noticePoint.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
                }else {
                    if(zk.exists(rootPath+"/"+serverIp+"/"+agentId,false) == null){
                        //agent节点
                        zk.create(rootPath+"/"+serverIp+"/"+agentId, agentId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
                        zk.setData(rootPath+"/"+serverIp+"/"+agentId,serverIp.getBytes(),-1);
                        zk.create(rootPath+"/"+serverIp+"/"+agentId+"/"+noticePoint, noticePoint.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
                    }else {
                        if(zk.exists(rootPath+"/"+serverIp+"/"+agentId+"/"+noticePoint,false) == null){
                            zk.create(rootPath+"/"+serverIp+"/"+agentId+"/"+noticePoint, noticePoint.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
                        }
                    }
                }
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    public void run() {
        try {
            synchronized (this) {
                while (alive) {
                    if(zk != null && agentPathWatcher != null){
                        List<String> children = null;
                        try {
                            children = zk.getChildren(rootPath+"/"+serverIp+"/"+agentId, agentPathWatcher);
                            System.err.println("init members: " + children);
                        } catch (KeeperException e) {
                            e.printStackTrace();
                        }
                        //wait util the other threads call notify (or notifyAll) method
                        wait();
                    }else{
                        System.out.printf(" init error, try again !");
                        Thread.sleep(1000);
                    }
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        } finally {
            this.close();
        }
    }


    public synchronized void close() {
        try {
            zk.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String args[]){
        new AgentZk().run();
    }
}
