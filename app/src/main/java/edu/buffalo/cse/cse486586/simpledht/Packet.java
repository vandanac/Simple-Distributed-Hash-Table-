package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;
import java.util.HashMap;

public class Packet  implements Serializable {
    MessageType type;
    String key;
    String value;
    String successorId;
    String predecessorID;
    String queryPort;
    String senderNode;
    String remoteNode;
    String cursor;
    public enum MessageType{
        INSERT, QUERY, DELETE, ACKNOWLEDGEMENT, JOIN , NEIGHBORS, FORWARDED
    }

}
