package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;


public class SimpleDhtProvider extends ContentProvider {

    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final String EMULATOR1 = "5554";
    static final String EMULATOR2 = "5556";
    static final String EMULATOR3 = "5558";
    static final String EMULATOR4 = "5560";
    static final String EMULATOR5 = "5562";
    private String myPort;
    private String nodeAttachPort = "5554";
    private String previousPort = "0";
    private String successorPort = "0";
    private String myHashValue =  "";
    private String previousPortHash = "";
    private String successorPortHash = "";
    static final int SERVER_PORT = 10000;
    private ServerSocket serverSocket;
    private SQLiteDatabase sqLiteDatabase;
    private Uri cpUri;
    String[] remotePorts = new String[]{REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
    static ArrayList<String> REMOTE_PORTS = new ArrayList<String>(Arrays.asList(REMOTE_PORT0,
            REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4));
    static ArrayList<String> EMULATORS = new ArrayList<String>(Arrays.asList(EMULATOR1, EMULATOR2, EMULATOR3, EMULATOR4, EMULATOR5));
    private List<String> chordMembers = new ArrayList<String>();
    private Map<String, String> hashPortMapping = new HashMap<String, String>();
    private KVStorageSqliteOpenHelper storageSqliteOpenHelper;
    private Map<String, String> hm = new HashMap<String, String>();
    //emulator to remote port mapping
    String TABLE_NAME = KVStorageSqliteOpenHelper.TABLE_KVSTORAGE;
    String COLUMN_KEY = KVStorageSqliteOpenHelper.COLUMN_KEY;
    String COLUMN_VALUE = KVStorageSqliteOpenHelper.COLUMN_VALUE;
    String totalResult = "";
    boolean fetchResult = false;
    int resultCount = 0;
    String queryValAvd = "";


    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        sqLiteDatabase = storageSqliteOpenHelper.getWritableDatabase();
        String keyHash = "";
        try {
            keyHash = genHash(selection);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        if (previousPort.equals("0") && successorPort.equals("0")) {
            if (selection.equals("*") || selection.equals("@")) {
                return sqLiteDatabase.delete(TABLE_NAME, null, null);
            } else {
                String[] args = {selection};
                return sqLiteDatabase.delete(TABLE_NAME, COLUMN_KEY + "=?", args);
            }
        } else if (selection.equals("*")) {
            //delete in all avds

            String message = "*";
            //tell all avd to delete, so in loop lunch client tasks
            Log.e("in delete 1", "sending to all");
            for (int i = 0; i < remotePorts.length; i++) {
                if (!remotePorts.equals(myPort)) {
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, remotePorts[i], message, "Delete");
                }
            }

            try {
                Thread.sleep(600);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            return sqLiteDatabase.delete(TABLE_NAME, null, null);

        } else if (selection.equals("@")) {
            return sqLiteDatabase.delete(TABLE_NAME, null, null);
        } else if (checkCondition(keyHash)) {
            String[] args = {selection};
            return sqLiteDatabase.delete(TABLE_NAME, COLUMN_KEY + "=?", args);
        } else {
            //key exists at some other avd.
            Log.e("in delete 2 sending to ", hm.get(successorPort));

            String message = selection;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, hm.get(successorPort), message, "Delete");
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
        String keyHash = "";
        try {
            keyHash = genHash(values.getAsString("key"));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        if (previousPort.equals("0") && successorPort.equals("0")) {
            storageSqliteOpenHelper.add(values);
        } else if ((keyHash.compareTo(previousPortHash) >= 0) && keyHash.compareTo(myHashValue) < 0) {
            storageSqliteOpenHelper.add(values);
        } else if (previousPortHash.compareTo(myHashValue) > 0 && successorPortHash.compareTo(myHashValue) > 0) {
            //as hash values of previous and successor are greater it is first node in the ring
            if (keyHash.compareTo(previousPortHash) >=0) {
                storageSqliteOpenHelper.add(values);
            } else if (keyHash.compareTo(myHashValue) < 0) {
                storageSqliteOpenHelper.add(values);
            } else {
                //pass to the next node to check
                Log.e("in insert 1 sending to ", hm.get(successorPort));
                String message = values.getAsString("key")+":"+values.getAsString("value");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, hm.get(successorPort), message, "Insert");
            }

        } else {
            //now this key needs to get inserted in some other avd
            Log.e("in insert 2 sending to ", hm.get(successorPort));
            String message = values.getAsString("key")+":"+values.getAsString("value");
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, hm.get(successorPort), message, "Insert");
        }
        return null;
    }

    @Override
    public boolean onCreate() {
        Log.e("in on create", "starting");
        // TODO Auto-generated method stub
        hm.put("5554", REMOTE_PORT0);
        hm.put("5556", REMOTE_PORT1);
        hm.put("5558", REMOTE_PORT2);
        hm.put("5560", REMOTE_PORT3);
        hm.put("5562", REMOTE_PORT4);

        storageSqliteOpenHelper = new KVStorageSqliteOpenHelper(getContext());
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        myPort = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        cpUri  = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");

        try {
            myHashValue = genHash(myPort);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        String hashval = "";

        for (int i=0; i<EMULATORS.size(); i++) {
            try {
                hashval = genHash(EMULATORS.get(i));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            hashPortMapping.put(hashval, EMULATORS.get(i));
        }

        try {
            serverSocket = new ServerSocket(SERVER_PORT);
        } catch (IOException e) {
            e.printStackTrace();
        }

        new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        if (!myPort.equals(nodeAttachPort)) {
            //send request for joining the chord
            String remote_port = hm.get(nodeAttachPort);
            Log.e("sending joinrequest oc "+remote_port, "from "+myPort);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, remote_port, myPort, "join");
        } else {
            try {
                chordMembers.add(genHash(myPort));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub
        sqLiteDatabase = storageSqliteOpenHelper.getWritableDatabase();
        String keyToHash = "";
        String[] dataCols = {COLUMN_KEY, COLUMN_VALUE};
        String[] query = {selection};

        try {
            keyToHash = genHash(selection);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        String searchParams = COLUMN_KEY + " = ?";

        if (previousPort.equals("0") && successorPort.equals("0")) {
            if (selection.equals("*") || selection.equals("@")) {
                return fetchOwnData();
            } else {
                Cursor cursor = sqLiteDatabase.query(TABLE_NAME, dataCols, searchParams, query, null, null, null);
                cursor.moveToFirst();
                Object[] values = {cursor.getString(0), cursor.getString(1)};
                MatrixCursor mC = new MatrixCursor(dataCols);
                mC.addRow(values);
                cursor.close();
                sqLiteDatabase.close();
                return mC;
            }
        } else if (selection.equals("@")) {
            return fetchOwnData();
        } else if (selection.equals("*")) {

            //send requests to all 5 avd and wait for their response

            MatrixCursor mC = new MatrixCursor(dataCols);
            String message = "*" + "#" + myPort;

            for (int i = 0; i < remotePorts.length; i++) {
                if (!remotePorts.equals(myPort)) {
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, remotePorts[i], message, "Query");
                }
            }

            while (true) {
                //here we check if we receive messages for all 5 avds
                try {
                    Thread.sleep(700);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                if (resultCount == 4) {
                    //here I have got the result from all 4 avds in my totalResult so just return cursor now
                    String[] allAvdResults = totalResult.split("\\$$");

                    for (int i=0;i<allAvdResults.length;i++) {
                        String[] keyVals = allAvdResults[i].split("\\|");
                        for (int j=0;j<keyVals.length;j++) {
                            String[] pair = keyVals[i].split("\\-");
                            Object[] values = {pair[0], pair[1]};
                            mC.addRow(values);
                        }
                    }
                    break;
                }
            }
            //now add own avd results
            Cursor cursor = sqLiteDatabase.rawQuery("SELECT * FROM "+TABLE_NAME, null);
            cursor.moveToFirst();

            while (!cursor.isAfterLast()) {
                Object[] avdData = {cursor.getString(0), cursor.getString(1)};
                mC.addRow(avdData);
                cursor.moveToNext();
            }

            cursor.close();
            sqLiteDatabase.close();
            return mC;

        } else if (checkCondition(keyToHash)) {
            Cursor cursor = sqLiteDatabase.query(TABLE_NAME, dataCols, searchParams, query, null, null, null);
            cursor.moveToFirst();
            Object[] values = {cursor.getString(0), cursor.getString(1)};
            MatrixCursor mR = new MatrixCursor(dataCols);
            mR.addRow(values);
            cursor.close();
            sqLiteDatabase.close();
            return mR;
        } else {
            //here the key is with some other avd we need to wait and pass request to next avd to send back
            Log.e("Query to another avd", hm.get(successorPort));
            String message = selection+"#"+myPort;
            MatrixCursor mR = new MatrixCursor(dataCols);

            try {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, hm.get(successorPort), message, "Query");
            } catch (Exception e) {
                e.printStackTrace();
            }

            while (true) {
                Log.e("in while loop", "waiting");
                try {
                    Thread.sleep(700);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                if (!queryValAvd.equals("")) {
                    Log.e("got the result now", queryValAvd);
                    String[] p = queryValAvd.split("\\&");
                    Object[] values = {p[1], p[2]};
                    mR.addRow(values);
                    break;
                }
            }

            Log.e("sending query", "result");
            return mR;
        }
        //return null;
    }


    //need to check this conditions
    private boolean checkCondition(String keyHash) {
        if (keyHash.compareTo(previousPortHash) >=0 && keyHash.compareTo(myHashValue) < 0) {
            return true;
        }
        return false;
    }

    public MatrixCursor fetchOwnData() {
        Log.e("fetching own data", myPort);
        String[] dataCols = {COLUMN_KEY, COLUMN_VALUE};
        MatrixCursor m= new MatrixCursor(dataCols);
        Cursor cursor = sqLiteDatabase.rawQuery("SELECT * FROM "+TABLE_NAME, null);
        cursor.moveToFirst();

        while (!cursor.isAfterLast()) {
            Object[] avdData = {cursor.getString(0), cursor.getString(1)};
            m.addRow(avdData);
            cursor.moveToNext();
        }

        cursor.close();
        sqLiteDatabase.close();
        return m;
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


    public void sendToClientTask(String sendMessage, String port) {
        Log.e("Send final result to", port);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, port, sendMessage, "Result");
    }

    public void querySelfAndAnotherAvd(String key, String requestingPort) {
        Log.e("quering key "+key, "request from "+requestingPort+" to "+myPort);


        sqLiteDatabase = storageSqliteOpenHelper.getWritableDatabase();
        String hashRes="";
        String[] dataCols = {COLUMN_KEY, COLUMN_VALUE};
        String[] query = {key};
        String searchParams = COLUMN_KEY + " = ?";

        try {
            hashRes = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        if (checkCondition(hashRes)) {
            //get the value from DB and send to the requesting avd
            Log.e("in check condition", "myport is"+myPort);
            Cursor cursor = sqLiteDatabase.query(TABLE_NAME, dataCols, searchParams, query, null, null, null);
            cursor.moveToFirst();
            String k = cursor.getString(0);
            String v = cursor.getString(1);
            //now send back to requesting port
            String message = k+"&"+v;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, requestingPort, message, "Queried");

        } else {
            //check for next succesor
            String message = key+"#"+requestingPort;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, hm.get(successorPort), message, "Query");
        }
    }

    public void informNodeAboutSP(String...values) {
        String predecessor_to_set = values[0];
        String successor_to_set = values[1];
        String decision = values[2];
        String originalPort = values[3];

        Log.e("new joined port is "+originalPort, "my port "+myPort);

        if (decision.equals("inform")) {
            //inform the node about its joining
            String msg = predecessor_to_set + ":" + successor_to_set;
            Log.e("sending inform req", hm.get(originalPort));
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, hm.get(originalPort), msg, "inform");

            Log.e("original port in onP", originalPort);
            String information = myPort + ":" + originalPort;

            String port = hashPortMapping.get(predecessor_to_set);

            if (!port.equals(nodeAttachPort)) {
                Log.e("in inform suc", information);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, hm.get(port), information, "SetSuccessor");
            } else {
                Log.e("in else", "setting pre"+originalPort);
                successorPort = originalPort;
                try {
                    successorPortHash = genHash(originalPort);
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }

            port = hashPortMapping.get(successor_to_set);

            if (!port.equals(nodeAttachPort)) {
                Log.e("in inform pre", information);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, hm.get(port), information, "SetPredecessor");
            } else {
                Log.e("in else", "setting suc"+originalPort);
                previousPort = originalPort;
                try {
                    previousPortHash = genHash(originalPort);
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            /** Here we are continuously creating socket coz it breaks after one communication **/
            while (true) {
                try {
                    Log.e("on server", "request received");
                    //serverSocket.setSoTimeout(4500);
                    Socket socket = null;
                    socket = serverSocket.accept();
                    BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    Log.e("reading line", "now");
                    String message = input.readLine();
                    String[] parts = message.split("\\:");

                    out.println("Ack");
                    out.close();

                    Log.e("request type is ",parts[0]);

                    //here requests are handled of Join,
                    if (parts[0].equals("join")) {
                        Log.e("join request for", parts[1]);
                        String response = joinChordDHT(genHash(parts[1]));
                        String[] breakVals = response.split("\\!");
                        String val1 = breakVals[0];
                        String val2 = breakVals[1];
                        //response = response + ":" + "inform" + ":" + parts[1];
                        informNodeAboutSP(val1, val2, "inform", parts[1]);
                    } else if (parts[0].equals("inform")) {
                        previousPortHash = parts[1];
                        successorPortHash = parts[2];
                        previousPort = hashPortMapping.get(previousPortHash);
                        successorPort = hashPortMapping.get(successorPortHash);
                    } else if (parts[0].equals("SetPredecessor")) {
                        Log.e("Setting predecessor",parts[2]);
                        previousPort = parts[2];
                        previousPortHash = genHash(parts[2]);
                    } else if (parts[0].equals("SetSuccessor")) {
                        Log.e("Setting Succesor",parts[2]);
                        successorPort = parts[2];
                        successorPortHash = genHash(parts[2]);
                    } else if (parts[0].equals("Insert")) {
                        ContentValues cv = new ContentValues();
                        String k = parts[1].split("\\:")[0];
                        String v = parts[1].split("\\:")[1];
                        cv.put("key", k);
                        cv.put("value", v);
                        insert(cpUri, cv);
                    } else if (parts[0].equals("Query")) {
                        String[] svl = parts[1].split("#");
                        String sel = svl[0];
                        String requesting_port = svl[1];

                        if (sel.equals("*")) {
                            //here we need to return all own data, some AVD requested the data
                            Cursor cursor = fetchOwnData();
                            cursor.moveToFirst();
                            String sendResult = myPort+":";
                            int local_count = 0;

                            while (!cursor.isAfterLast()) {
                                String intermediateResult = cursor.getString(cursor.getColumnIndex("key")) + "-" +
                                        cursor.getString(cursor.getColumnIndex("value"));
                                if (local_count == 0) {
                                    sendResult += intermediateResult;
                                } else {
                                    sendResult += "|"+intermediateResult;
                                }
                                local_count++;
                            }
                            cursor.close();
                            //return the result back to requesting avd
                            sendToClientTask(sendResult, requesting_port);
                        } else {
                         //here we need to process query to check if it is on this avd and return the result
                            querySelfAndAnotherAvd(sel, requesting_port);
                        }
                    } else if (parts[0].equals("Delete")) {
                        if (parts[1].equals("*")) {
                            sqLiteDatabase = storageSqliteOpenHelper.getWritableDatabase();
                            sqLiteDatabase.delete(TABLE_NAME, null, null);
                        } else {
                            delete(cpUri, parts[1], null);
                        }
                    } else if (parts[0].equals("Result")) {
                        resultCount++;
                        if (totalResult.equals("")) {
                            totalResult += parts[2];
                        } else {
                            totalResult += "$$"+parts[2];
                        }
                    } else if (parts[0].equals("Queried")) {
                        String res = parts[1];
                        queryValAvd = res;
                    }

                } catch (SocketTimeoutException e) {
                    e.printStackTrace();
                } catch (SocketException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
                /*
                 * TODO: Fill in your server code that receives messages and passes them
                 * to onProgressUpdate().
                 */
                return null;
                }
            }

        public String joinChordDHT(String node) {
            Log.e("node joining", node);
            Log.e("chordmember size ", String.valueOf(chordMembers.size()));
            chordMembers.add(node);
            //sort the nodes
            Collections.sort(chordMembers);
            int position = chordMembers.indexOf(node);
            Log.e("position is", String.valueOf(position));
            String current_successor = "";
            String current_predecesor = "";
            if (position == 0) {
                current_successor = chordMembers.get(1);
                current_predecesor = chordMembers.get(chordMembers.size() - 1);
            } else if (position == (chordMembers.size() - 1)) {
                current_successor = chordMembers.get(0);
                current_predecesor = chordMembers.get(chordMembers.size() - 2);
            } else {
                current_successor = chordMembers.get(position+1);
                current_predecesor = chordMembers.get(position-1);
            }

            Log.e("current pre "+current_predecesor, "current suc "+current_successor);
            return current_predecesor+"!"+current_successor;
        }

        @Override
        protected void onProgressUpdate(String... values) {
        }
    }


    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            String request_type = msgs[2];
            String remote_port =  msgs[0];

            try {
                Log.e("sending to port "+remote_port, request_type +" from "+myPort);

                String msg = request_type+":"+msgs[1]+":"+myPort;
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remote_port));
                //socket.setSoTimeout(2000);
                BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())), true);
                //sending mesage
                out.println(msg);
                input.readLine();
                out.close();
                socket.close();
                /*
                 * TODO: Fill in your client code that sends out a message.
                 */
            } catch (SocketTimeoutException e) {
                e.printStackTrace();
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }
    }
}
