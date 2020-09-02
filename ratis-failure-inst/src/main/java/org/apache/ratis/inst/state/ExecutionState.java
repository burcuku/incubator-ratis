package org.apache.ratis.inst.state;

import org.apache.ratis.inst.message.Message;
import org.apache.ratis.inst.message.MetaMessage;
import org.apache.ratis.inst.util.FileLogger;

import java.util.*;

public class ExecutionState {

    private HashMap<String, NodeState> nodes;

    public ExecutionState() {
        nodes = new HashMap<>();

        List<String> nodeIds = Arrays.asList("s0", "s1", "s2"); //todo
        nodeIds.forEach(p -> {
            nodes.put(p, new NodeState(p));
        });
    }

    synchronized public void addMessage(Message message, boolean isDropped) {
        nodes.get(message.from).addMessage(message, isDropped);
    }

    synchronized public void addMessage(String node, MetaMessage message, boolean isDropped) {
        nodes.get(node).addMessage(message, isDropped);
    }

    synchronized public void addMessageToAll(MetaMessage message, boolean isDropped) {
        nodes.keySet().forEach(n -> nodes.get(n).addMessage(message, isDropped));
    }

    synchronized public void logMessages(String folder, String traceFile) {
        String message = "";
        for(NodeState n: nodes.values()) {
            message = message.concat("\n\nNode: " + n.nodeId + "\n");
            message = message.concat(n.getMessagesAsStr());
        }
        message = message.concat("---");
        FileLogger.logToFile(folder, traceFile, message, true);
    }

    synchronized public void logFailures(String folder, String traceFile) {
        String message = "";
        for(NodeState n: nodes.values()) {
            message = message.concat("\n\nNode: " + n.nodeId + "\n");
            message = message.concat(n.getFailuresAsStr());
        }

    }

    public long lastHeardIndex(String nodeId) {
        if(!nodes.containsKey(nodeId)) return -1;
        return nodes.get(nodeId).getLastHeardIndex();
    }


}
