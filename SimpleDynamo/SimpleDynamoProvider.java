package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	/*Initialization and misc*/
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final int SERVER_PORT = 10000;
	static final String[] sPort = new String[]{"5554","5556","5558","5560","5562"};
	/*Ring Content*/
	private ArrayList<String> nodeRing = new ArrayList<String>();
	private HashMap<String, String> portMap = new HashMap<String, String>();
	private String myNode;
	private String myPort;
	/*Storage*/
	ConcurrentHashMap<String, String> localStorage = new ConcurrentHashMap<String, String>();
	ConcurrentHashMap<String, String> queryStorage = new ConcurrentHashMap<String, String>();
	/*Concurrency*/
	private int queryAll_count = 0;

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		localStorage.remove(selection);
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		synchronized (this){
			String key = values.getAsString("key");
			String value = values.getAsString("value");
			String msg = portMap.get(myNode)+"@"+"INSERT"+"@"+key + ":" + value;
			String storeNode = nodeStore(key);
			String[] replicas = succReplicators(storeNode);
			String replicator1 = replicas[0];
			String replicator2 = replicas[1];
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,portMap.get(storeNode));
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,portMap.get(replicator1));
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,portMap.get(replicator2));
			try{
				Thread.sleep(500);
			}catch(InterruptedException e){

			}
		}
		return null;
	}

	@Override
	public boolean onCreate() {
		//clear local storage just to be safe but don't have to
		localStorage.clear();
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		try{
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		}
		catch(IOException e){
			Log.e(TAG, "Can't create a ServerSocket");
		}
		Log.d(TAG, "Setting up Ring...");
		setUp(portStr,myPort);
		Log.d(TAG, "Ring setup done");
		Log.d(TAG, "Checking if app crashed...");
		//Using file list to detect if the app was crashed or forced stopped.
		for(String s: getContext().fileList()){
			if(s.equals("APK_INSTALLED")){
				Log.d(TAG,"Crash detected, entering recovery mode...");
				recovery();
				return false;
			}
		}
		Log.d(TAG,"No crash detected");
		try{
			//When app is first install, save a file to help with "Recovery" - hacky way of doing it.
			String value = "Test";
			FileOutputStream file = getContext().openFileOutput("APK_INSTALLED",Context.MODE_PRIVATE);
			file.write(value.getBytes());
			file.close();
			Log.d(TAG, "Successfully saving APK_INSTALLED file");
		}catch(IOException e){
			Log.e(TAG, "Error saving APK_INSTALLED file");
		}
		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		synchronized (this){
			MatrixCursor mCursor = new MatrixCursor(new String[] {"key", "value"});
			//Query specific device.
			if(selection.equals("@")){
				for (Map.Entry<String, String> entry : localStorage.entrySet()) {
					mCursor.addRow(new String[]{entry.getKey(), entry.getValue()});
				}
			}
			//Query all devices in the ring
			else if(selection.equals("*")){
				String msg = portMap.get(myNode)+"@"+"QUERY_ALL"+"@"+"NONE";
				for (Map.Entry<String, String> entry : localStorage.entrySet()) {
					mCursor.addRow(new String[]{entry.getKey(), entry.getValue()});
				}
				for (String serial : nodeRing){
					if(serial != myNode){
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,portMap.get(serial));
					}
				}
				while(true){
					//Wait till query at least 3 devices before continuing. (Simulation of 5 devices)
					if(queryAll_count == 3){
						queryAll_count = 0;
						break;
					}
				}
				for (Map.Entry<String, String> entry : queryStorage.entrySet()) {
					mCursor.addRow(new String[]{entry.getKey(), entry.getValue()});
				}
				queryStorage.clear();
			}
			//Query specific key
			else{
				String keyDest = nodeStore(selection);
				String msg = portMap.get(myNode)+"@"+"QUERY_ONE"+"@"+selection;
				String[] succReplica = succReplicators(keyDest);
				String replica1 = succReplica[0];
				String replica2 = succReplica[1];
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,portMap.get(keyDest));
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,portMap.get(replica1));
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,portMap.get(replica2));
				while(!queryStorage.containsKey(selection)){
					//do nothing
				}
//				Log.d(TAG,"db: "+queryStorage.get(selection));
				mCursor.addRow(new String[]{selection, queryStorage.get(selection)});
				queryStorage.clear();
			}
			return mCursor;
		}
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
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

	private void setUp(String mySP, String myPort){
		//Ring setup
		for(String serial : sPort){
			try{
				String hashId = genHash(serial);
				if(serial.equals(mySP)){
					myNode = hashId;
					portMap.put(hashId,myPort);
				}
				else{
					String otherPort = String.valueOf((Integer.parseInt(serial) * 2));
					portMap.put(hashId,otherPort);
				}
				nodeRing.add(hashId);
			}catch(NoSuchAlgorithmException e){
				Log.e(TAG,"Creating Hash Node ID failed.");
			}
		}
		Collections.sort(nodeRing);
	}

	private void storeMessage(String key, String value){
		localStorage.put(key,value);
		Log.d(TAG,"Stored: "+key+" , "+value);
	}

	private String nodeStore(String key){
		//Return the node of the ring where the message should belong.
		String keyHash;
		String storeNode = "";
		try{
			keyHash = genHash(key);
			for(String hashNode : nodeRing){
				int prevIndex = nodeRing.indexOf(hashNode)-1;
				if(prevIndex <0){
					prevIndex += nodeRing.size();
				}
				if(keyHash.compareTo(nodeRing.get(prevIndex)) > 0 && keyHash.compareTo(hashNode) <= 0){
					storeNode = hashNode;
				}
			}
			if(storeNode.isEmpty()){
				storeNode = nodeRing.get(0);
			}
		}
		catch(NoSuchAlgorithmException e){
			Log.e(TAG, "Insert keyHash error.");
		}
		return storeNode;
	}

	private void recovery(){
		/* Recover all data for one device required 1 before and 1 after because of replicators*/
		String savior1;
		String savior2;
		String msg = myNode+"@"+"RECOVERY"+"@"+"NONE";
		if(nodeRing.indexOf(myNode) == nodeRing.size()-1){
			savior1 = portMap.get(nodeRing.get(nodeRing.size()-2));
			savior2 = portMap.get(nodeRing.get(0));
		}
		else if(nodeRing.indexOf(myNode) == 0){
			savior1 = portMap.get(nodeRing.get(nodeRing.size()-1));
			savior2 = portMap.get(nodeRing.get(1));
		}
		else{
			savior2 = portMap.get(nodeRing.get(nodeRing.indexOf(myNode)-1));
			savior1 = portMap.get(nodeRing.get(nodeRing.indexOf(myNode)+1));
		}
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,savior1);
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,savior2);
    	Log.d(TAG, "Recovering done.");
	}

	private boolean recoverReplica(String recoverNode, String checkNode){
		//Check if the node recovering from is the correct replica
		String node1;
		String node2;
		String node3 = recoverNode;
		int nodeIndex = nodeRing.indexOf(recoverNode);
		if(nodeIndex == 0){
			node1 = nodeRing.get(nodeRing.size()-1);
			node2 = nodeRing.get(nodeRing.size()-2);
		}
		else if(nodeIndex == 1){
			node1 = nodeRing.get(nodeRing.size()-1);
			node2 = nodeRing.get(0);
		}
		else{
			node1 = nodeRing.get(nodeIndex-1);
			node2 = nodeRing.get(nodeIndex-2);
		}
		if(checkNode.equals(node3) || checkNode.equals(node2) || checkNode.equals(node1)){
			return true;
		}
		return false;
	}

	private String[] succReplicators(String storeNode){
		//Return the 2 replica for the requested node.
		String replicator1;
		String replicator2;
		if(nodeRing.indexOf(storeNode) == nodeRing.size()-2){
			replicator1 = nodeRing.get(nodeRing.indexOf(storeNode)+1);
			replicator2 = nodeRing.get(0);
		}
		else if(nodeRing.indexOf(storeNode) == nodeRing.size()-1){
			replicator1 = nodeRing.get(0);
			replicator2 = nodeRing.get(1);
		}
		else{
			replicator1 = nodeRing.get(nodeRing.indexOf(storeNode)+1);
			replicator2 = nodeRing.get(nodeRing.indexOf(storeNode)+2);
		}
		String[] succReplica = {replicator1,replicator2};
		return succReplica;
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			try{
				while(true){
					Socket sock = serverSocket.accept();
					DataInputStream dInput = new DataInputStream(sock.getInputStream());
					String recv = dInput.readUTF();
					String[] recvSplit = recv.split("@");
					if(recvSplit[1].equals("INSERT")){
						String[] data = recvSplit[2].split(":");
						storeMessage(data[0],data[1]);
					}
					else if(recvSplit[1].equals("QUERY_ALL")){
						String msg = "";
						for (Map.Entry<String, String> entry : localStorage.entrySet()) {
							msg += entry.getKey()+":"+entry.getValue()+"/";
						}
						String msgSend = portMap.get(myNode)+"@"+"QUERY_ALL_BACK"+"@"+msg;
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgSend,recvSplit[0]);
					}
					else if(recvSplit[1].equals("QUERY_ALL_BACK")){
						String[] dataSplit = recvSplit[2].split("/");
						for(String pair : dataSplit){
							String[] keyValue = pair.split(":");
							queryStorage.put(keyValue[0],keyValue[1]);
						}
						queryAll_count++;
					}
					else if(recvSplit[1].equals("QUERY_ONE")){
						if(localStorage.get(recvSplit[2]) != null){
							String msg = recvSplit[2]+":"+localStorage.get(recvSplit[2]);
							String msgSend = portMap.get(myNode)+"@"+"QUERY_ONE_DONE"+"@"+msg;
							new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgSend,recvSplit[0]);
						}
					}
					else if(recvSplit[1].equals("QUERY_ONE_DONE")){
						String[] keyValue = recvSplit[2].split(":");
						queryStorage.put(keyValue[0],keyValue[1]);
					}
					else if(recvSplit[1].equals("RECOVERY")) {
						String msg = "";
						for (Map.Entry<String, String> entry : localStorage.entrySet()) {
							String storeNode = nodeStore(entry.getKey());
							if(recoverReplica(recvSplit[0],storeNode)){
								Log.d(TAG, portMap.get(storeNode)+"|"+entry.getKey()+" , "+entry.getValue());
								msg += entry.getKey()+":"+entry.getValue()+"/";
							}
						}
						String msgSend = portMap.get(myNode)+"@"+"RECOVERY_BACK"+"@"+msg;
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgSend,portMap.get(recvSplit[0]));
					}
					else if(recvSplit[1].equals("RECOVERY_BACK")){
						if(recvSplit.length == 3){
							String[] dataSplit = recvSplit[2].split("/");
							for(String pair : dataSplit){
								String[] keyValue = pair.split(":");
								localStorage.put(keyValue[0],keyValue[1]);
								Log.d(TAG,"Restore: "+keyValue[0]+" , "+keyValue[1]);
							}
						}
					}
				}
			}catch (IOException e){
				Log.e(TAG, "Server Socket Error");
			}
			return null;
		}
	}

	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {
			try{
				Socket sock = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[1]));
				DataOutputStream dOutput = new DataOutputStream(sock.getOutputStream());
				dOutput.writeUTF(msgs[0]);
				dOutput.flush();
			}catch(IOException e){
				Log.e(TAG, "Client Task Error");
			}catch(Exception e){
				Log.e(TAG,"Client Error");
			}
			return null;
		}
	}
}