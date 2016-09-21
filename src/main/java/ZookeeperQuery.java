import com.jayway.jsonpath.JsonPath;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;

public class ZookeeperQuery {
    static {
        Logger.getRootLogger().setLevel(Level.OFF);
    }

    public static void main(String[] args) throws Exception {
        String command;
        if (args.length == 0) {
            command = "/";
        } else {
            command = args[0];
        }
        ZooKeeper zk = new ZooKeeper("192.168.99.100:2181", 10000, null);
        if ("leader".equals(command)) {
            String topicName = args[1];
            String query = "/brokers/topics/" + topicName + "/partitions/0/state";
            int leader = JsonPath.parse(new String(zk.getData(query, false, null))).read("$.leader");
            System.out.println(leader);
        } else if ("describeTopic".equals(command)) {
            String topicName = args[1];
            String query = "/brokers/topics/" + topicName + "/partitions/0/state";
            System.out.println("Executing zookeeper: get " + query);
            System.out.println(new String(zk.getData(query, false, null)));
        } else if ("brokers".equals(command)) {
            System.out.println("Executing zookeeper: get /brokers/ids");
            List<String> ids = zk.getChildren("/brokers/ids", false);
            for (String id : ids) {
                String brokerPath = "/brokers/ids/" + id;
                System.out.println("Executing zookeeper: get " + brokerPath);
                String brokerInfo = new String(zk.getData(brokerPath, false, null));
                System.out.println(id + ": " + brokerInfo);
            }
        } else {
            System.out.println("Executing zookeeper: get " + command);
            List<String> availableCommands = zk.getChildren(command, false);
            if (availableCommands.isEmpty()) {
                System.out.println(new String(zk.getData(command, false, null)));
            } else {
                System.out.println(availableCommands);
            }
        }
    }
}