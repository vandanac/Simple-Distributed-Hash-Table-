package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.Map;
import java.util.TreeMap;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    static final String TAG = "DhtContenProvider";
    private static final String[] columnNames = {"key", "value"};
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    static final int SERVER_PORT = 10000;
    static final String managerPort = "5554";
    static String successorId = "0";
    static String predecessorId = "0";
    static String hashedSuccID, hashedPredID, hashedMyID;
    static String myID;
    static ArrayList<Integer> nodes;
    static TreeMap<String,String> nodeMap;
    static Uri mUri;
    static StringBuilder sb;
    static boolean cursorReady = false;
    static boolean isForwarded = false;

    private String getMyNodeID() {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        return (portStr);
    }

    private void setAndHashNodes(Packet msg) {
        try {
            if (msg.predecessorID != null) {
                predecessorId = msg.predecessorID;
                hashedPredID = genHash(predecessorId);
                Log.e(TAG, "PRED updated to " + predecessorId);
            }
            if (msg.successorId != null) {
                successorId = msg.successorId;
                hashedSuccID = genHash(successorId);
                Log.e(TAG, "SUCC updated to " + successorId);
            }
        } catch (NoSuchAlgorithmException ex) {

        }
    }

    @Override
    public boolean onCreate() {

        myID = getMyNodeID();
        try {
            hashedMyID = genHash(myID);
        }
        catch(NoSuchAlgorithmException ex){

        }
        nodes = new ArrayList<Integer>();
        nodeMap = new TreeMap<String, String>();
        sb = new StringBuilder();

        Log.v(TAG, myID);
        //Join Request
        if (!myID.equals("5554")) {
            Packet joinRequest = new Packet();
            joinRequest.type = Packet.MessageType.JOIN;
            joinRequest.senderNode = getMyNodeID();
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, joinRequest);
        }
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT, 50);
            serverSocket.setSoTimeout(1000);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
        }
        return true;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        String hashedKey = null;
        try {
            hashedKey = genHash(selection);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        if (selection.equalsIgnoreCase("@") || selection.equalsIgnoreCase("*")) {
            //Delete all local files
            String[] filelist = getContext().fileList();
            for (String filename : filelist) {
              getContext().deleteFile(filename);
                //Log.v(TAG, Boolean.toString(status));
            }

            if (selection.equalsIgnoreCase("*") && !isForwarded && !successorId.equals("0")) {
            Log.e(TAG, "Forward Delete query");
            Packet p = new Packet();
            p.key = selection;
            p.type = Packet.MessageType.DELETE;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, p);
            }

        }
        else if ((successorId.equals("0") && predecessorId.equals("0"))
                || (hashedKey.compareTo(hashedPredID) >0 && hashedMyID.compareTo(hashedKey) >= 0)
                || (hashedKey.compareTo(hashedMyID) < 0 && hashedMyID.compareTo(hashedPredID) < 0)
                || (hashedKey.compareTo(hashedPredID) > 0 && hashedMyID.compareTo(hashedPredID) < 0)) {

            getContext().deleteFile(selection);
        }

            return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub


        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        String key = values.getAsString("key");
        String hashedKey = null;
        try {
            hashedKey = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        try {
            String string = values.getAsString("value");
            if ((successorId.equals("0") && predecessorId.equals("0"))
                    || (hashedKey.compareTo(hashedPredID) >0 && hashedMyID.compareTo(hashedKey) >= 0)
                    || (hashedKey.compareTo(hashedMyID) < 0 && hashedMyID.compareTo(hashedPredID) < 0)
                    || (hashedKey.compareTo(hashedPredID) > 0 && hashedMyID.compareTo(hashedPredID) < 0)) {

                Log.e(TAG, "Attempted to insert");
                FileOutputStream outputStream;
                outputStream = getContext().openFileOutput(key.toString(), Context.MODE_PRIVATE);
                outputStream.write(string.getBytes());
                outputStream.close();

            } else {
                Log.e(TAG, "Forward Insert query");
                Packet p = new Packet();
                p.key = key;
                p.value = string;
                p.type = Packet.MessageType.INSERT;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, p);
            }
        } catch (Exception e) {
            e.printStackTrace();
            Log.e(TAG, "Insert failed");
        }

        Log.v("Insert", values.toString());
        return uri;
    }


    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        String hashedKey = null;
        sb.setLength(0);
        try {
            hashedKey = !selection.equalsIgnoreCase("@") && !selection.equalsIgnoreCase("*") ? genHash(selection) : null;
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        if (selection.equalsIgnoreCase("@") || selection.equalsIgnoreCase("*")) {
            int c;
            FileInputStream inputStream;
            try {

                Log.e(TAG, "Search @|* query");
                MatrixCursor cursor = new MatrixCursor(columnNames);
                String[] filelist = getContext().fileList();
                for (String filename : filelist) {
                    StringBuilder message = new StringBuilder();
                    inputStream = getContext().openFileInput(filename);
                    while ((c = inputStream.read()) != -1) {
                        message.append((char) c);
                    }
                    if (message != null) {
                        String[] pair = {filename, message.toString()};
                        cursor.addRow(pair);
                        sb.append(pair[0].concat("-").concat(pair[1]));sb.append(",");
                        inputStream.close();
                    }
                }
                if( selection.equalsIgnoreCase("*") && !isForwarded && !successorId.equals("0"))
                {
                    Log.e(TAG, "Forward query");
                    cursorReady = false;
                    Packet p = new Packet();
                    p.key = selection;
                    p.senderNode = myID;
                    p.type = Packet.MessageType.QUERY;
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, p);

                    //Read from String Builder
                    return BuildCursor();
                }


                return cursor;
            } catch (FileNotFoundException ex) {
                Log.e(TAG, " File Not Found");
            } catch (Exception ex) {
                Log.e(TAG, "Content provider query failed");
            }
        }
        else
        {
            if ((successorId.equals("0") && predecessorId.equals("0"))
                || (hashedKey.compareTo(hashedPredID) > 0 && hashedMyID.compareTo(hashedKey) >= 0)
                || (hashedKey.compareTo(hashedMyID) < 0 && hashedMyID.compareTo(hashedPredID) < 0)
                || (hashedKey.compareTo(hashedPredID) > 0 && hashedMyID.compareTo(hashedPredID) < 0)) {
            try {

                    int c;
                    StringBuilder message = new StringBuilder();
                    FileInputStream inputStream;
                    inputStream = getContext().openFileInput(selection);
                    while ((c = inputStream.read()) != -1) {
                        message.append((char) c);
                    }
                    if (message != null) {
                        MatrixCursor cursor = new MatrixCursor(columnNames);
                        String[] pair = {selection, message.toString()};
                        cursor.addRow(pair);
                        inputStream.close();
                        return cursor;
                    }

            } catch (FileNotFoundException ex) {
                Log.e(TAG, selection + " File Not Found");
            } catch (Exception ex) {
                ex.printStackTrace();
                Log.e(TAG, "Content provider query failed");
            }

        } else{

                Log.e(TAG, "Forward query");
                cursorReady = false;
                Packet p = new Packet();
                p.key = selection;
                p.senderNode = myID;
                p.type = Packet.MessageType.QUERY;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, p);

                //Read from String Builder
              return BuildCursor();

            }
    }

        return null;
    }

    public Cursor BuildCursor(){

        while(!cursorReady)
        {

        }
        Log.v(TAG,"Building Cursor");
        sb.setLength(sb.length() - 1);
        String str  = sb.toString();
        String pairs[] = str.split(",");
        MatrixCursor cursor = new MatrixCursor(columnNames);
        for(String pair : pairs){
            String[] keypair = pair.split("-");
            cursor.addRow(keypair);
        }
        Log.v(TAG,"Cursor count " + Integer.toString(cursor.getCount()));
        return cursor;
    }
    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class ServerTask extends AsyncTask<ServerSocket, Packet, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Socket socket = new Socket();
            while (true) {
                try {

                    socket = serverSocket.accept();
                    ObjectInputStream obj = new ObjectInputStream(socket.getInputStream());
                    ObjectOutputStream objOutputStream = new ObjectOutputStream(socket.getOutputStream());
                    Packet msg;
                    if((msg = (Packet)obj.readObject()) != null) {
                        if (msg.type == Packet.MessageType.JOIN) {
                            String hashedID = genHash(msg.senderNode);

                            Log.v("TAG", msg.senderNode);
                            if(successorId == "0") {
                                Packet myUpdate = new Packet();
                                myUpdate.successorId = msg.senderNode;
                                myUpdate.predecessorID = msg.senderNode;
                                setAndHashNodes(myUpdate);

                                Packet update = new Packet();
                                update.remoteNode = msg.senderNode;
                                update.successorId = myID;
                                update.predecessorID = myID;
                                update.type = Packet.MessageType.NEIGHBORS;
                                publishProgress(update);

                            }
                            else if(hashedID.compareTo(hashedMyID) > 0 && hashedID.compareTo(hashedSuccID) <0 || ((hashedID.compareTo(hashedMyID) > 0 || hashedID.compareTo(hashedSuccID) < 0) && hashedSuccID.compareTo(hashedMyID) < 0))
                            {
                                msg.successorId = msg.senderNode;

                                Packet update = new Packet();
                                update.remoteNode = msg.senderNode;
                                update.type = Packet.MessageType.NEIGHBORS;
                                update.successorId = successorId;
                                update.predecessorID = myID;
                                publishProgress(update);


                                // Update pointer predecessor node
                                Packet updateSucc = new Packet();
                                updateSucc.type = Packet.MessageType.NEIGHBORS;
                                updateSucc.remoteNode = successorId;
                                updateSucc.predecessorID = msg.senderNode;
                                publishProgress(updateSucc);


                                setAndHashNodes(msg);
                            }
                            else{
                                msg.remoteNode = successorId;
                                publishProgress(msg);
                            }



                        } else if (msg.type == Packet.MessageType.QUERY) {
                            String hashedKey = null;

                            msg.remoteNode = successorId;
                            try {
                                hashedKey = !msg.key.equalsIgnoreCase("*") ? genHash(msg.key) : null;
                            } catch (NoSuchAlgorithmException e) {
                                e.printStackTrace();
                            }
                            if(!msg.key.equalsIgnoreCase("*") && hashedMyID.compareTo(hashedPredID) > 0 && hashedMyID.compareTo(hashedKey) < 0)
                            {
                                Log.v(TAG,"Sent successor id");
                                objOutputStream.writeObject(msg);
                                objOutputStream.flush();
                            }
                            else
                            {
                                StringBuilder result = new StringBuilder();
                                isForwarded = true;
                                Cursor cursor =  query(mUri, null, msg.key, null, null);
                                isForwarded = false;
                                cursor.moveToFirst();
                                while(!cursor.isAfterLast())
                                {
                                    String pair = cursor.getString(0).concat("-").concat(cursor.getString(1));
                                    Log.e(TAG,pair);
                                    result.append(pair);
                                    result.append(",");
                                    cursor.moveToNext();
                                }
                                msg.cursor = result.toString();

                                objOutputStream.writeObject(msg);
                                objOutputStream.flush();

                            }

                            Packet ack  = (Packet)obj.readObject();

                        }else if(msg.type == Packet.MessageType.DELETE){
                            String hashedKey = null;
                            msg.remoteNode = successorId;
                            try {
                                hashedKey = !msg.key.equalsIgnoreCase("*") ? genHash(msg.key) : null;
                            } catch (NoSuchAlgorithmException e) {
                                e.printStackTrace();
                            }
                            if(!msg.key.equalsIgnoreCase("*") && hashedMyID.compareTo(hashedPredID) > 0 && hashedMyID.compareTo(hashedKey) < 0)
                            {
                                Log.v(TAG,"Sent successor id");
                                objOutputStream.writeObject(msg);
                                objOutputStream.flush();
                                Packet ack  = (Packet)obj.readObject();
                            }
                            else
                            {
                                msg.type = Packet.MessageType.ACKNOWLEDGEMENT;
                                objOutputStream.writeObject(msg);
                                objOutputStream.flush();
                                publishProgress(msg);

                            }
                        }
                        else
                        {
                            if (msg.type == Packet.MessageType.NEIGHBORS)
                                setAndHashNodes(msg);
                            else if (msg.type == Packet.MessageType.INSERT)
                                   publishProgress(msg);

                            Packet reply = new Packet();
                            reply.type = Packet.MessageType.ACKNOWLEDGEMENT;
                            objOutputStream.writeObject(reply);
                            objOutputStream.flush();
                        }

                    }
                    obj.close();
                    objOutputStream.close();
                } catch (SocketTimeoutException ex) {

                } catch (IOException ex) {
                    ex.printStackTrace();
                    Log.e("ST", "Server socket IO Exception " + ex.toString());
                } catch (Exception c) {
                    Log.e("ST", c.toString());
                } finally {
                    try {
                        socket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

            }


        }

        protected void onProgressUpdate(Packet...p) {

            if (p[0].type == Packet.MessageType.NEIGHBORS ||p[0].type == Packet.MessageType.JOIN)
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, p[0]);
            else if (p[0].type == Packet.MessageType.INSERT) {
                try {
                    Log.e(TAG, "Received forward insert" + p[0].key);
                    ContentValues cv = new ContentValues();
                    cv.put(KEY_FIELD, p[0].key);
                    cv.put(VALUE_FIELD, p[0].value);
                    insert(mUri, cv);
                } catch (Exception e) {
                    Log.e(TAG, "onProgressUpdate Insert Failed");
                }
            }else if(p[0].type == Packet.MessageType.DELETE) {
                isForwarded = true;
                delete(mUri, p[0].key, null);
                isForwarded = false;
            }


            return;
        }
    }
        private class ClientTask extends AsyncTask<Packet, Void, Void> {

            @Override
            protected Void doInBackground(Packet... msg) {
                try {
                    Packet.MessageType type = msg[0].type;
                    String remotePort;
                    if (type == Packet.MessageType.JOIN) {
                        if(msg[0].remoteNode != null)
                            remotePort = msg[0].remoteNode;
                        else
                            remotePort = managerPort;
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(remotePort)*2);
                        Log.v("Join Forward",remotePort );
                        ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                        ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                        oos.writeObject(msg[0]);
                        oos.flush();

                        ois.close();
                        oos.close();

                        socket.close();
                    } else if (type == Packet.MessageType.NEIGHBORS) {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(msg[0].remoteNode) * 2);
                        ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                        oos.writeObject(msg[0]);

                        Log.v(TAG, "CT Sent " + msg[0].remoteNode + " to change succ/pred to" + msg[0].successorId + " "+msg[0].predecessorID) ;
                        oos.flush();
                        Packet response;

                        ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                        if((response= (Packet) ois.readObject())!= null && response.type == Packet.MessageType.ACKNOWLEDGEMENT) {
                            ois.close();
                            oos.close();
                            socket.close();
                        }
                    } else if (type == Packet.MessageType.INSERT) {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(successorId) * 2);
                        ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                        ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                        oos.writeObject(msg[0]);
                        oos.flush();
                        Packet response;
                        if((response= (Packet) ois.readObject())!= null && response.type == Packet.MessageType.ACKNOWLEDGEMENT) {
                            ois.close();
                            oos.close();
                            socket.close();
                        }
                    }
                    else if(type == Packet.MessageType.QUERY) {
                        remotePort = successorId;
                        while(true){

                            Log.e(TAG, "Forward query to " +remotePort);
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(remotePort)*2);
                            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                            ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                            oos.writeObject(msg[0]);
                            oos.flush();
                            Packet response;
                            if((response= (Packet) ois.readObject())!= null) {
                                Packet reply = new Packet();
                                reply.type = Packet.MessageType.ACKNOWLEDGEMENT;
                                oos.writeObject(reply);
                                oos.flush();
                                remotePort = response.remoteNode;
                                    if(response.cursor != null) {
                                        sb.append(response.cursor);
                                        if (!msg[0].key.equalsIgnoreCase("*")) {
                                            ois.close();
                                            oos.close();
                                            socket.close();
                                            break;
                                        }
                                    }
                                ois.close();
                                oos.close();
                                socket.close();
                                if(response.remoteNode.equals(response.senderNode))
                                    break;

                            }

                        }
                        synchronized (this) {
                            cursorReady = true;
                        }
                    }
                    else if(type == Packet.MessageType.DELETE){
                        remotePort = successorId;
                        while(true){

                            Log.e(TAG, "Forward query to " +remotePort);
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(remotePort)*2);
                            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                            ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                            oos.writeObject(msg[0]);
                            oos.flush();
                            Packet response;
                            if((response= (Packet) ois.readObject())!= null) {
                                if(response.remoteNode.equals(response.senderNode) || response.type == Packet.MessageType.ACKNOWLEDGEMENT)
                                    break;

                                remotePort = response.remoteNode;
                                Packet reply = new Packet();
                                reply.type = Packet.MessageType.ACKNOWLEDGEMENT;
                                oos.writeObject(reply);
                                oos.flush();


                                ois.close();
                                oos.close();
                                socket.close();


                            }

                        }


                    }
                    } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    e.printStackTrace();
                    Log.e(TAG, "ClientTask socket IOException" + msg[0].type.toString());
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }

                return null;
            }
        }
    }

