package com.zookeeper;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author yanjd
 */
public class MonServerBalance implements Runnable {

    public static final String SOC_PATH = "/soc";
    public static final String ZK_PORT = "10.176.63.102:2181";
    private Watcher connectionWatcher = null;
    private Watcher childrenWatcher = null;
    private List<String> historyServers;
    private ZooKeeper zk;
    protected boolean alive = true;
    public  String noticePoint = "notice";


    public MonServerBalance() throws InterruptedException, IOException, KeeperException {
        init();
    }

    public void init() throws IOException, KeeperException, InterruptedException{
        //Define the connection watch ,which watches the connection event.
        connectionWatcher = new Watcher() {
            public void process(WatchedEvent event) {
                if(event.getType() == Event.EventType.None &&
                        event.getState() == Event.KeeperState.SyncConnected) {
                    System.out.printf("\n connection watch output event: %s", event.toString());
                }
            }
        };

        //Children watcher listens the NodeChildrenChanged event.
        childrenWatcher = new Watcher() {
            public void process(WatchedEvent event) {
                System.out.printf("\n childrenWatcher Received: %s", event.toString());
                if (event.getType() == Event.EventType.NodeChildrenChanged) {
                    try {
                        //Get current list of child znode, reset the watch
                        List<String> children = zk.getChildren(SOC_PATH, this);
                        printLog("--------- Cluster Membership Change ---------");
                        //Update live service objects in really environment.
                        //before
                        System.err.println("before historyServers: " + historyServers);
                        //算法入口
                        rebalance();
                        //after
                        historyServers = children;
                        System.err.println("after historyServers: " + historyServers);

                        printLog("Members: " + children);
                    } catch (KeeperException e) {
                        throw new RuntimeException(e);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        alive = false;
                        throw new RuntimeException(e);
                    }
                }
            }
        };

        zk = new ZooKeeper(ZK_PORT, 10000, connectionWatcher);
        // Ensure the parent znode exists
        if(zk.exists(SOC_PATH, false) == null) {
            //Create the node.
            zk.create(SOC_PATH, "ClusterMonitorRoot".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        // Set a watch on the parent znode
        historyServers = zk.getChildren(SOC_PATH, childrenWatcher);
        System.err.println();
        System.err.println("historyServers: " + historyServers);
    }

    public synchronized void close() {
        try {
            zk.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    public void printLog (String message) {
        System.out.printf("\n group members: %s", message);
    }

    public void run() {
        try {
            synchronized (this) {
                while (alive) {
                    if(zk != null && childrenWatcher != null){
                        List<String> children = null;
                        try {
                            children = zk.getChildren(SOC_PATH, childrenWatcher);
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

    //重新负载分配
    public void rebalance() throws KeeperException, InterruptedException {

        List<String> servers = zk.getChildren(SOC_PATH, childrenWatcher);
        List<String> agents = new ArrayList<String>();
        for(int i = 0;i<servers.size();i++){
            List<String> agent = zk.getChildren(SOC_PATH+"/"+servers.get(i), childrenWatcher);
            for(int j = 0;j<agent.size();j++){
                agents.add(agent.get(j));
            }
        }
        Map<String,List<String>> mapping  = allotOfAverage(servers,agents);

        for(int i = 0;i<historyServers.size();i++){
            if(mapping.containsKey(historyServers.get(i))){
                List<String> newAgents = mapping.get(historyServers.get(i));
                List<String> oldAgents = zk.getChildren(SOC_PATH+"/"+historyServers.get(i), childrenWatcher);
                oldAgents.removeAll(newAgents);
                for(int j = 0;j<oldAgents.size();j++){
                    zk.delete(SOC_PATH+"/"+servers.get(i)+"/"+oldAgents.get(j)+"/"+noticePoint,-1);
                    System.out.println(SOC_PATH+"/"+servers.get(i)+"/"+oldAgents.get(j)+"/"+noticePoint);
                    String serverid = getServerId(mapping,oldAgents.get(j));
                    zk.setData(SOC_PATH+"/"+servers.get(i)+"/"+oldAgents.get(j),serverid.getBytes(),-1);

                }
            }
        }

    }

    /*
    * 获取serverId
    */
    public String getServerId(Map<String,List<String>> mapping,String agentId){
        for (String key : mapping.keySet()) {
            System.out.println("key= "+ key + " and value= " + mapping.get(key));
            for(int i = 0;i<mapping.get(key).size();i++){
                if(agentId.equals(mapping.get(key).get(i))){
                    System.out.println("key: "+key);
                    return key;
                }
            }

        }
        return null;
    }
    /*
     * 分配算法
    */
    public Map<String,List<String>> allotOfAverage(List<String> users, List<String> tasks){
        Map<String,List<String>> allot=new ConcurrentHashMap<String,List<String>>(); //保存分配的信息
        if(users!=null&&users.size()>0&&tasks!=null&&tasks.size()>0){
            for(int i=0;i<tasks.size();i++){
                int j=i%users.size();
                if(allot.containsKey(users.get(j))){
                    List<String> list=allot.get(users.get(j));
                    System.out.println("tasks.get(i) "+tasks.get(i));
                    list.add(tasks.get(i));
                    allot.put(users.get(j), list);
                }else{
                    List<String> list=new ArrayList<String>();
                    System.out.println("tasks.get(i) "+tasks.get(i));
                    list.add(tasks.get(i));
                    allot.put(users.get(j), list);
                }
            }
        }
        return allot;
    }

    public static void main(String[] args)
            throws IOException, InterruptedException, KeeperException {
        new MonServerBalance().run();
    }
}