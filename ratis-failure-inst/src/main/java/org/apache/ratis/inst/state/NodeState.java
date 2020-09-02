package org.apache.ratis.inst.state;

import org.apache.ratis.inst.message.Loggable;
import org.apache.ratis.inst.message.Message;
import org.apache.ratis.inst.message.MetaMessage;

import java.util.*;

public class NodeState {

    final String nodeId;

    LinkedHashMap<Loggable, Boolean> messages; // list of messages sent from this node, together with isDropped
    long lastHeardIndex = -10; // max. index for SimpleRoundCounter, excluding RequestVotes

    public NodeState(String id) {
        this.nodeId = id;
        messages = new LinkedHashMap<>();
    }

    public synchronized void addMessage(Message message, boolean isDropped) {
        Message duplicate = null;
        for(Loggable ms: messages.keySet()){  // the duplicate message is most likely towards the end //todo revise with stream
            if(ms instanceof MetaMessage) continue;
            Message m = (Message) ms;
            if(m.hasSameContent(message)) {
                m.incNumExecuted();
                duplicate = m;
                messages.remove(m); // will be added back
                break;
            }
        }
        if(duplicate == null) { // the message does not have a duplicate
            messages.put(message, isDropped);
            message.incNumExecuted();
            if(!message.isLEMessage() && message.r.getIndex() > lastHeardIndex) lastHeardIndex = message.r.getIndex();
        } else { // the message has a duplicate
            messages.put(duplicate, isDropped); //update its location in list
        }
    }

    public synchronized void addMessage(MetaMessage message, boolean isDropped) {
        messages.put(message, isDropped);
    }

    public long getLastHeardIndex() {
        return lastHeardIndex;
    }

    public synchronized String getMessagesAsStr() {
        String s = "";
        boolean lastMessageDropped = false;

        for(Map.Entry<Loggable, Boolean> e: messages.entrySet()) {
            if (e.getValue()) s = s.concat("     -D   ");
            s = s.concat(e.getKey().logContent()).concat("\n");
            lastMessageDropped = e.getValue();
        }

        if(lastMessageDropped)
            s = s.concat("Note: LAST message is dropped from node: " + nodeId).concat("\n");

        return s;
    }

    public synchronized String getFailuresAsStr() {
        String s = "";
        for(Map.Entry<Loggable, Boolean> e: messages.entrySet()) {
            if (e.getValue()) s = s.concat(e.getKey().logContent()).concat("\n");
        }

        return s;
    }

}
