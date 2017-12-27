package com.zookeeper;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.List;

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

        zk = new ZooKeeper(ZK_PORT, 2000, connectionWatcher);
        // Ensure the parent znode exists
        if(zk.exists(SOC_PATH, false) == null) {
            //Create the node.
            zk.create(SOC_PATH,
                    "ClusterMonitorRoot".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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


    public void rebalance(){

    }

    public static void main(String[] args)
            throws IOException, InterruptedException, KeeperException {
        new MonServerBalance().run();
    }
}