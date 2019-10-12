package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.widget.ProgressBar;

import org.w3c.dom.Node;

public class SimpleDynamoProvider extends ContentProvider {

	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final int SERVER_PORT = 10000;
//    static int defaultTimeout = 100;
	static String thisPortStr = null;
	static String hashMyPort = "";  // to store hash value of this port
    // TreeMap to store keys along with their versions
    TreeMap<String, Integer> versionMap = new TreeMap<String, Integer>();
	// List of ports present in the chord
	ArrayList<String> portList = new ArrayList<String>(Arrays.asList("5562", "5556", "5554", "5558", "5560"));
    String flagFileName = "RecoveryIdentifier";
    boolean delayFlag = false;

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		Log.d(TAG, "In delete() function");
		int delCount = 0;
		try {
			File directory = getContext().getFilesDir();

			if (!selection.equals("*") && !selection.equals("@")){
                // Destination Port and ports needed for replication
                String portNum = null;
                String repliPort1 = null;
                String repliPort2 = null;
                String[] triplePorts = new String[3];
                Log.d(TAG, "Deleting key: "+selection);
				File file = new File(directory, selection);
				// Reference to delete a file: https://www.geeksforgeeks.org/delete-file-using-java/
				if (file.delete())
					delCount += 1;

                String key_hash = genHash(selection);
                int lastChordIndex = portList.size()-1;

                if (key_hash.compareTo(genHash(portList.get(0))) <= 0){
                    Log.d(TAG, "Smaller than 1st node's hash, so delete in the first node");
                    portNum = portList.get(0);
                    repliPort1 = portList.get(1);
                    repliPort2 = portList.get(2);
                }
                else if (key_hash.compareTo(genHash(portList.get(lastChordIndex))) > 0){
                    Log.d(TAG,"Key's hash is larger than last node's hash, so delete in the first node");
                    portNum = portList.get(0);
                    repliPort1 = portList.get(1);
                    repliPort2 = portList.get(2);
                }else {
                    for (int i=1; i<portList.size(); i++){
                        if (key_hash.compareTo(genHash(portList.get(i-1))) > 0 &&
                                key_hash.compareTo(genHash(portList.get(i))) <= 0){
                            portNum = portList.get(i);
                            if (i < 3){
                                repliPort1 = String.valueOf(Integer.parseInt(portList.get(i+1)));
                                repliPort2 = String.valueOf(Integer.parseInt(portList.get(i+2)));
                            }
                            else if (i == 3){
                                repliPort1 = String.valueOf(Integer.parseInt(portList.get(i+1)));
                                repliPort2 = String.valueOf(Integer.parseInt(portList.get(0)));
                            }
                            else if (i == 4){
                                repliPort1 = String.valueOf(Integer.parseInt(portList.get(0)));
                                repliPort2 = String.valueOf(Integer.parseInt(portList.get(1)));
                            }
                            Log.d(TAG, "Correct port number to delete is: "+portNum);
                            break;
                        }
                    }
                }
                // All ports to be connected for Deletion
                triplePorts[0] = portNum;
                triplePorts[1] = repliPort1;
                triplePorts[2] = repliPort2;

                for (int i = 0; i < 3; i++) {
                    try {
                        String port = triplePorts[i];
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(port) * 2);
                        socket.setSoTimeout(500);
                        Log.d(TAG, "Requesting to delete in port: "+port);

                        // Send query message to destPort
                        String msgToBeSent = "Delete Message|" + selection + "\n";
                        PrintWriter out = new PrintWriter(socket.getOutputStream());
                        out.write(msgToBeSent);
                        out.flush();

                        // Read message back after querying all
                        BufferedReader in = new BufferedReader(
                                new InputStreamReader(socket.getInputStream()));
                        String str = in.readLine();
                        if (!str.equals("null") && str != null && str.contains("Deleted a File")) {
                            socket.close();
                        }
                    }catch (SocketTimeoutException ste){
                        Log.e(TAG, "Socket Timeout Exception in query()");
                        ste.printStackTrace();
                    }catch (NullPointerException npe){
                        Log.e(TAG, "Null Pointer Exception in query()");
                        npe.printStackTrace();
                    }catch (IOException ioe){
                        Log.e(TAG, "IO Exception in query()");
                        ioe.printStackTrace();
                    }
                }
			}

			else if(selection.equals("@")){
				Log.d(TAG,"Deleting all files from this AVD");
				delCount = deleteAll();
			}else if (selection.equals("*")){
				Log.d(TAG, "Deleting all files from all AVDs");
				for (String port : portList){
					Log.d(TAG, "Deleting all messages from port: "+port);
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(port)*2);
						// Send delete message to destPort
						String msgToBeSent = "Delete All|"+"\n";
						PrintWriter out = new PrintWriter(socket.getOutputStream());
						out.write(msgToBeSent);
						out.flush();
						// Read message back after deleting all
						BufferedReader in = new BufferedReader(
								new InputStreamReader(socket.getInputStream()));
						String str = in.readLine();
						Log.d(TAG, "String received after deleting all files: "+str);
						if (str.contains("Number of files deleted")){
							String[] splitString = str.split("\\|");
							delCount += Integer.parseInt(splitString[1]);
							socket.close();
						}
					}catch (IOException ioe){
						Log.e(TAG, "IO Exception while deleting *");
						ioe.printStackTrace();
					}
				}
			}
		}catch (Exception e){
			Log.e(TAG, "Exception found in delete()");
			e.printStackTrace();
		}
		return delCount;
	}

	public int deleteAll(){
		/*
		 * Responsible for deleting all messages in the current AVD
		 * */
		int tempCount = 0;
		Log.d(TAG,"In deleteAll of port: "+thisPortStr);
		try{
			File directory = getContext().getFilesDir();
			File[] files = directory.listFiles();
			for (File file : files) {
			    if (!file.getName().equals(flagFileName)) {
                    if (file.delete())
                        tempCount += 1;
                }
			}
		}catch (Exception e){
			Log.e(TAG, "Exception found in deleteAll()");
			e.printStackTrace();
		}
		return tempCount;
	}

    public Void deleteAMessage(String selection){
		/*
		 * Responsible for deleting a single message in the current AVD
		 * */
	    Log.d(TAG, "Deleting a single message in port: "+thisPortStr);
        File directory = getContext().getFilesDir();
        File file = new File(directory, selection);
        // Reference to delete a file: https://www.geeksforgeeks.org/delete-file-using-java/
        file.delete();
        return null;
    }

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		/*
		 * References:
		 * https://stackoverflow.com/questions/2390244/how-to-get-the-keys-from-contentvalues
		 * https://developer.android.com/training/data-storage/files#WriteInternalStorage
		 * https://stackoverflow.com/questions/28242386/cannot-resolve-method-openfileoutputjava-lang-string-int
		 */
		while (delayFlag){
		    //delay during recovery phase
        }
		String filename = (String)values.get("key");
		String fileContents = (String)values.get("value");
		// Destination Port and ports needed for replication
		String destPort = null;
		String repliPort1 = null;
		String repliPort2 = null;
		// starting port is thisPortStr
		try {
			String key_hash = genHash(filename);
			int lastChordIndex = portList.size()-1;
			if (key_hash.compareTo(genHash(portList.get(0))) <= 0){
				Log.d(TAG, "Smaller than 1st node's hash, so insert in the first node");
                destPort = String.valueOf(Integer.parseInt(portList.get(0))*2);
                repliPort1 = String.valueOf(Integer.parseInt(portList.get(1))*2);
                repliPort2 = String.valueOf(Integer.parseInt(portList.get(2))*2);
			}
			else if (key_hash.compareTo(genHash(portList.get(lastChordIndex))) > 0){
				Log.d(TAG,"Key's hash is larger than last node's hash, so insert in the first node");
                destPort = String.valueOf(Integer.parseInt(portList.get(0))*2);
                repliPort1 = String.valueOf(Integer.parseInt(portList.get(1))*2);
                repliPort2 = String.valueOf(Integer.parseInt(portList.get(2))*2);
			}
			else {
				for (int i=1; i<portList.size(); i++){
					if (key_hash.compareTo(genHash(portList.get(i-1))) > 0 &&
							key_hash.compareTo(genHash(portList.get(i))) <= 0){
						destPort = String.valueOf(Integer.parseInt(portList.get(i))*2);
						if (i < 3){
                            repliPort1 = String.valueOf(Integer.parseInt(portList.get(i+1))*2);
                            repliPort2 = String.valueOf(Integer.parseInt(portList.get(i+2))*2);
                        }
                        else if (i == 3){
                            repliPort1 = String.valueOf(Integer.parseInt(portList.get(i+1))*2);
                            repliPort2 = String.valueOf(Integer.parseInt(portList.get(0))*2);
                        }
                        else if (i == 4){
                            repliPort1 = String.valueOf(Integer.parseInt(portList.get(0))*2);
                            repliPort2 = String.valueOf(Integer.parseInt(portList.get(1))*2);
                        }
						break;
					}
				}
			}

            ArrayList<String> insertPorts = new ArrayList<String>();
			insertPorts.add(destPort);
			insertPorts.add(repliPort1);
			insertPorts.add(repliPort2);

			int i = 0;
			for (String port: insertPorts){
			    try{
			        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(port));
				    socket.setSoTimeout(500);

				    if (i == 0) {
				        i += 1;
                        // Send insert message to destPort
                        String msgToBeSent = "Insert|" + filename + "|" + fileContents + "|" + destPort + "\n";
                        PrintWriter out = new PrintWriter(socket.getOutputStream());
                        out.write(msgToBeSent);
                        out.flush();

                        // Read message back after insert
                        BufferedReader in = new BufferedReader(
                                new InputStreamReader(socket.getInputStream()));
                        String str = in.readLine();
                        if (str != null && str.equals("Insert Done")){
                            socket.close();
                        }
                    }
                    else {
                        i += 1;
                        // Send insert message to replicas
                        String msgToBeSent1 = "Replicate|" + filename + "|" + fileContents + "|" + destPort + "\n";
                        PrintWriter out1 = new PrintWriter(socket.getOutputStream());
                        out1.write(msgToBeSent1);
                        out1.flush();

                        // Read message back after replication
                        BufferedReader in1 = new BufferedReader(
                                new InputStreamReader(socket.getInputStream()));
                        String str1 = in1.readLine();
                        if (str1 != null && str1.equals("Replicate Done")) {
                            socket.close();
                        }
                    }
                }catch (SocketTimeoutException ste){
			        Log.e(TAG, "Socket Timeout Exception in insert() while inserting");
			        ste.printStackTrace();
                }catch (IOException ioe){
			        Log.e(TAG, "IO Exception in insert() while inserting");
			        ioe.printStackTrace();
                }catch (NullPointerException npe){
			        Log.e(TAG, "NullPointer Exception in insert() while inserting");
			        npe.printStackTrace();
                }catch (Exception e){
			        Log.e(TAG, "Exception in insert() while inserting");
			        e.printStackTrace();
                }
            }
		}
		 catch (Exception e) {
			e.printStackTrace();
		}
		Log.v("insert", values.toString());
		return uri;
	}

	// Common method to insert into file
	public void insertIntoFile(String filename, String fileContents, String coordinator){
		/*
		 * This module contains the logic to insert key-value into a file
		 * */
		Log.d(TAG, "Inserting into file");
		try {
		    String versionNum;
		    File file = new File(getContext().getFilesDir(), filename);
		    if (file.exists()){
                FileInputStream f = new FileInputStream(file);
                BufferedReader br = new BufferedReader(new InputStreamReader(f));
                String ln = br.readLine();
                Log.v(TAG, "Line Read in insertIntoFile(): " + ln);
                br.close();
                String[] splitString = ln.split("-");
                versionNum = String.valueOf(Integer.parseInt(splitString[1])+1);
            }
            else {
                versionNum = "1";
            }
			FileOutputStream outputStream;
			outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
			String msgToWrite = coordinator+"-"+versionNum+"-"+fileContents;
			outputStream.write(msgToWrite.getBytes());
			outputStream.close();
		}catch (FileNotFoundException f){
			Log.e(TAG,"File not found in insertIntoFile for filename: "+filename);
			f.printStackTrace();
		}catch (IOException ioe){
			Log.e(TAG,"IO Exception in insertIntoFile for filename: "+filename);
			ioe.printStackTrace();
		}
	}

	// Method to insert after recovery
    public void insertIntoFileAfterRecovery(String filename, String fileContents, String coordinator,
                                            String currVersionNum){
        /*
         * This module contains the logic to insert key-value into a file after crash recovery
         * Only difference is here it checks the version before inserting file
         * */
        Log.d(TAG, "Inserting into file after crash recovery");
        try {
            String versionNum;
            int versionNumTemp;
            File file = new File(getContext().getFilesDir(), filename);
            boolean flag = true;
            if (file.exists()){
                FileInputStream f = new FileInputStream(file);
                BufferedReader br = new BufferedReader(new InputStreamReader(f));
                String ln = br.readLine();
                Log.v(TAG, "Line Read in insertIntoFile(): " + ln);
                br.close();
                String[] splitString = ln.split("-");
                versionNumTemp = Integer.parseInt(splitString[1]);
                versionNum = String.valueOf(Math.max(versionNumTemp, Integer.parseInt(currVersionNum)));
                if (String.valueOf(versionNumTemp).equals(versionNum)){
                    flag = false;
                }
            }
            else {
                versionNum = currVersionNum;
            }
            if (flag) {
                FileOutputStream outputStream;
                outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                String msgToWrite = coordinator + "-" + versionNum + "-" + fileContents;
                outputStream.write(msgToWrite.getBytes());
                outputStream.close();
            }
        }catch (FileNotFoundException f){
            Log.e(TAG,"File not found in insertIntoFileAfterRecovery for filename: "+filename);
            f.printStackTrace();
        }catch (IOException ioe){
            Log.e(TAG,"IO Exception in insertIntoFileAfterRecovery for filename: "+filename);
            ioe.printStackTrace();
        }
    }

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
        Log.d(TAG, "Chord defined is: ");
        System.out.println(Arrays.toString(portList.toArray()));

		// Reference: From SimpleMessenger code
		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		Log.d(TAG, "In Port: "+myPort);
		thisPortStr = portStr;
		try {
			hashMyPort = genHash(portStr);
		}catch (NoSuchAlgorithmException nsae){
			Log.e(TAG, "No such algorithm exception in onCreate()");
			nsae.printStackTrace();
		}

		// Create a Flag file to differentiate first time launch and recovery phase
        try{
            // Reference to check if file exists: https://stackoverflow.com/a/16238204/10316954
            File directory = getContext().getFilesDir();
            File file = new File(directory, flagFileName);
            if (file.exists()){
                // Float a new AsyncTask thread to recover data
                Log.d(TAG,"Floating a new AsyncTask ");
                new CrashRecovery().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
            }
            else {
                // Writing a flag file for the first time
                Log.d(TAG, "Writing file into Content Provider");
                FileOutputStream outputStream;
                outputStream = getContext().openFileOutput(flagFileName, Context.MODE_PRIVATE);
                outputStream.write("1".getBytes());
                outputStream.close();
            }
        }catch (FileNotFoundException fnfe){
            Log.e(TAG, "File not found exception");
            fnfe.printStackTrace();
            return false;
        }catch (IOException ioe){
            Log.e(TAG, "IO Exception");
            ioe.printStackTrace();
            return false;
        }

		// Server Socket
		try {
			Log.d(TAG, "Trying to create ServerSocket object");
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			serverSocket.setReuseAddress(true);
			Log.d(TAG, "Hola in onCreate() of this AVD");
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "Printing stack trace");
			e.printStackTrace();
			Log.e(TAG, "Can't create a ServerSocket");
			return false;
		}
		return true;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
						String sortOrder) {
		/*
		 *  Sid notes:
		 *  The ContentResolver object communicates with the provider object,
		 *  an instance of a class that implements ContentProvider.
		 *  https://developer.android.com/training/data-storage/files#WriteInternalStorage
		 *  https://stackoverflow.com/questions/18290864/create-a-cursor-from-hardcoded-array-instead-of-db
		 */
        while (delayFlag){
            //delay during recovery phase
        }
		try {
//			File directory = getContext().getFilesDir();
			// Matrix Cursor
			String[] columns = new String[]{"key", "value"};
			MatrixCursor cursor = new MatrixCursor(columns);
			Log.d(TAG, "Selection parameter is: "+selection);
			String portNum = "";
            String repliPort1 = null;
            String repliPort2 = null;
            String[] triplePorts = new String[3];

			if (selection.equals("@")){
				String allFilesList = queryAllFilesInAVD();
				String[] allFiles = allFilesList.split("-");
				for (String file: allFiles){
					String[] keyValPair = file.split("\\|");
					String key = keyValPair[0];
					String val = keyValPair[1];
					if (!key.equals(flagFileName)) {
                        String[] input = new String[]{key, val};
                        cursor.addRow(input);
                    }
				}
			}
			else if (!selection.equals("*")){
				// These conditions are similar to insert
				String key_hash = genHash(selection);
				int lastChordIndex = portList.size()-1;

				if (key_hash.compareTo(genHash(portList.get(0))) <= 0){
					Log.d(TAG, "Smaller than 1st node's hash, so query in the first node");
					portNum = portList.get(0);
					repliPort1 = portList.get(1);
					repliPort2 = portList.get(2);
				}
				else if (key_hash.compareTo(genHash(portList.get(lastChordIndex))) > 0){
					Log.d(TAG,"Key's hash is larger than last node's hash, so query in the first node");
					portNum = portList.get(0);
                    repliPort1 = portList.get(1);
                    repliPort2 = portList.get(2);
				}else {
					for (int i=1; i<portList.size(); i++){
						if (key_hash.compareTo(genHash(portList.get(i-1))) > 0 &&
								key_hash.compareTo(genHash(portList.get(i))) <= 0){
							portNum = portList.get(i);
                            if (i < 3){
                                repliPort1 = String.valueOf(Integer.parseInt(portList.get(i+1)));
                                repliPort2 = String.valueOf(Integer.parseInt(portList.get(i+2)));
                            }
                            else if (i == 3){
                                repliPort1 = String.valueOf(Integer.parseInt(portList.get(i+1)));
                                repliPort2 = String.valueOf(Integer.parseInt(portList.get(0)));
                            }
                            else if (i == 4){
                                repliPort1 = String.valueOf(Integer.parseInt(portList.get(0)));
                                repliPort2 = String.valueOf(Integer.parseInt(portList.get(1)));
                            }
							Log.d(TAG, "Correct port number to query is: "+portNum);
							break;
						}
					}
				}
				// All ports to be connected for Querying
                triplePorts[0] = portNum;
                triplePorts[1] = repliPort1;
                triplePorts[2] = repliPort2;
                String maxVersion = "0";
                String maxVal = "";

                // Get value with the latest version
                for (int i = 0; i < 3; i++) {
                    try {
                        String port = triplePorts[i];
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(port) * 2);
                        socket.setSoTimeout(500);
                        Log.d(TAG, "Requesting to query in port: "+port);

                        // Send query message to destPort
                        String msgToBeSent = "Query Message|" + selection + "\n";
                        PrintWriter out = new PrintWriter(socket.getOutputStream());
                        out.write(msgToBeSent);
                        out.flush();

                        // Read message back after querying all
                        BufferedReader in = new BufferedReader(
                                new InputStreamReader(socket.getInputStream()));
                        String str = in.readLine();
                        if (!str.equals("null") && str != null && str.contains("One File")) {
                            socket.close();
                            String valNVersion = str.split("::")[1];
                            String[] splitString = valNVersion.split("-");
                            System.out.println(Arrays.toString(splitString));
                            if (splitString[0] == null || splitString[0].equals("null") ||
                            splitString[0].isEmpty()){
                                continue;
                            }
                            String version = splitString[0];
                            String val = splitString[1];
                            if (version.compareTo(maxVersion) > 0) {
                                Log.d(TAG, "Version greater than max");
                                Log.d(TAG, maxVersion);
                                Log.d(TAG, version);
                                maxVersion = version;
                                maxVal = val;
                            }
                        }
                    }catch (SocketTimeoutException ste){
                        Log.e(TAG, "Socket Timeout Exception in query()");
                        ste.printStackTrace();
                    }catch (NullPointerException npe){
                        Log.e(TAG, "Null Pointer Exception in query()");
                        npe.printStackTrace();
                    }
                    catch (IOException ioe){
                        Log.e(TAG, "IO Exception in query()");
                        ioe.printStackTrace();
                    }
                }
                Log.d(TAG, "maxVal to be inserted is: "+maxVal);
                String[] input = new String[]{selection, maxVal};
                cursor.addRow(input);
				}
			else if(selection.equals("*")){
				for (String port : portList){
					Log.d(TAG, "Querying all messages from port: "+port);
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(port)*2);
						socket.setSoTimeout(500);
						// Send query message to destPort
						String msgToBeSent = "Query All|"+"\n";
						PrintWriter out = new PrintWriter(socket.getOutputStream());
						out.write(msgToBeSent);
						out.flush();
						// Read message back after querying all
						BufferedReader in = new BufferedReader(
								new InputStreamReader(socket.getInputStream()));
						String str = in.readLine();
						Log.d(TAG, "String received after querying all files: "+str);
						if (!str.equals("null") && str!=null && str.contains("No files in this AVD")){
							Log.d(TAG, "No files in this AVD so continue");
							socket.close();
						}
						else if (!str.equals("null") && str != null && str.contains("All files")){
							socket.close();
							String allFilesList = str.split("::")[1];
							String[] allFiles = allFilesList.split("-");
							for (String file: allFiles){
								String[] keyValPair = file.split("\\|");
								if (keyValPair[0] == null || keyValPair[0].equals("null") ||
                                keyValPair[0].isEmpty()){
								    continue;
                                }
								String key = keyValPair[0];
								String val = keyValPair[1];
								if (!key.equals(flagFileName)) {
                                    String[] input = new String[]{key, val};
                                    cursor.addRow(input);
                                }
							}
						}
					}catch (SocketTimeoutException ste){
					    Log.e(TAG, "SocketTimeOut Exception while querying *");
					    ste.printStackTrace();
                    }catch (NullPointerException npe){
					    Log.e(TAG, "Null Pointer Exception while querying *");
					    npe.printStackTrace();
                    }
					catch (IOException ioe){
						Log.e(TAG, "IO Exception while querying *");
						ioe.printStackTrace();
					}
				}
			}
			return cursor;
		}
		catch (Exception e){
			Log.e(TAG, "File not Found or IOException while reading File in entire Query()");
			e.printStackTrace();
		}

		Log.v("query", selection);
		return null;
	}

	// Common method to return one key, value pair as a string
	public String queryOneFile(String selection){
        /*
        Returns a string after querying a key
         */
		Log.d(TAG, "In queryOneFile");
		String keyValPair = "";
		try{
			File directory = getContext().getFilesDir();
			File file = new File(directory, selection);
			FileInputStream f = new FileInputStream(file);
			BufferedReader br = new BufferedReader(new InputStreamReader(f));
			String ln = br.readLine();
			Log.v(TAG, "Line Read: " + ln);
			br.close();
			String[] splitStr = ln.split("-");
			// String to be returned
			keyValPair += splitStr[1]+"-"+splitStr[2];
		}catch (FileNotFoundException fnfe){
			Log.e(TAG, "File not found exception");
			fnfe.printStackTrace();
			return null;
		}catch (IOException ioe){
			Log.e(TAG, "IO Exception");
			ioe.printStackTrace();
			return null;
		}
		return keyValPair;
	}

	// Common method to return all key, value pairs in an AVD as a string
	public String queryAllFilesInAVD() {
	    /*
	    Returns a string containing all key value pairs present in the AVD
	     */
		Log.d(TAG, "In queryAllFilesInAVD");
		String keyValPairs = "";
		try {
			File directory = getContext().getFilesDir();
			File[] files = directory.listFiles();
			for (File file : files) {
				if (!file.getName().equals(flagFileName)) {
                    FileInputStream f = new FileInputStream(file);
                    BufferedReader br = new BufferedReader(new InputStreamReader(f));
                    String ln = br.readLine();
                    br.close();

                    String key_name = file.getName();
                    String[] ln1 = ln.split("-");
                    keyValPairs += key_name + "|" + ln1[2] + "-";
                }
			}
		}catch (FileNotFoundException fnfe){
			Log.e(TAG, "File not found exception");
			fnfe.printStackTrace();
			return null;
		}catch (IOException ioe){
			Log.e(TAG, "IO Exception");
			ioe.printStackTrace();
			return null;
		}
		Log.d(TAG, "Files being returned is: "+keyValPairs);
		return keyValPairs;
	}

    public String queryMessagesOfPort(String portNum) {
	    /*
	    Method to fetch all messages of a specific port number only
	     */
	    Log.d(TAG, "In queryMessagesOfPort()");
        String keyValPairs = "";
        try {
            File directory = getContext().getFilesDir();
            File[] files = directory.listFiles();
            for (File file : files) {
                if (!file.getName().equals(flagFileName)) {
                    FileInputStream f = new FileInputStream(file);
                    BufferedReader br = new BufferedReader(new InputStreamReader(f));
                    String ln = br.readLine();
                    br.close();

                    String key_name = file.getName();
                    String[] ln1 = ln.split("-");
                    // Only if the key belongs to the port number, then append it
                    if (ln1[0].equals(portNum)) {
                        String ver = ln1[1];
                        keyValPairs += ver+"|"+portNum+"|"+key_name + "|" + ln1[2] + "-";
                    }
                }
            }
        }catch (FileNotFoundException fnfe){
            Log.e(TAG, "File not found exception");
            fnfe.printStackTrace();
            return null;
        }catch (IOException ioe){
            Log.e(TAG, "IO Exception");
            ioe.printStackTrace();
            return null;
        }
        Log.d(TAG, "Files being returned is: "+keyValPairs);
        return keyValPairs;
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

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			while (true){
                while (delayFlag){
                    //delay during recovery phase
                }
				try {
					Log.d(TAG,"In Server Socket");
					// BufferedReader to receive messages
					Socket socket = serverSocket.accept();
					socket.setSoTimeout(500);
					BufferedReader in = new BufferedReader(
							new InputStreamReader(socket.getInputStream()));
					String str = in.readLine();
					Log.d(TAG, "String received in Server Socket is: "+str);
					if (str.contains("Insert")){
						Log.d(TAG,"Insert request");
						String[] splitString = str.split("\\|");
						if (versionMap.containsKey(splitString[1])){
						    versionMap.put(splitString[1], versionMap.get(splitString[1])+1);
                        }
                        else {
                            versionMap.put(splitString[1], 1);
                        }
						insertIntoFile(splitString[1], splitString[2], splitString[3]);
						// Send acknowledgement that insert is done
						PrintWriter out = new PrintWriter(socket.getOutputStream());
						out.write("Insert Done\n");
						out.flush();
					}
                    else if (str.contains("Replicate")){
                        Log.d(TAG,"Replicate request");
                        String[] splitString = str.split("\\|");
                        if (versionMap.containsKey(splitString[1])){
                            versionMap.put(splitString[1], versionMap.get(splitString[1])+1);
                        }
                        else {
                            versionMap.put(splitString[1], 1);
                        }
                        insertIntoFile(splitString[1], splitString[2], splitString[3]);
                        // Send acknowledgement that replication is done
                        PrintWriter out = new PrintWriter(socket.getOutputStream());
                        out.write("Replicate Done\n");
                        out.flush();
                    }
					else if (str.contains("Query All")){
						Log.d(TAG, "Query all request in port: "+thisPortStr);
						String msg = queryAllFilesInAVD();
						Log.d(TAG, "String of messages returned is: "+msg);
						PrintWriter out = new PrintWriter(socket.getOutputStream());
						if (msg.equals("")){
							out.write("No files in this AVD"+"\n");
						}
						else {
							out.write("All files::" + msg + "\n");
						}
						out.flush();
					}
					else if(str.contains("Query Message")){
						String key = str.split("\\|")[1];
						Log.d(TAG, "Query request for the message: "+key+" in port: "+thisPortStr);
						String valReturned = queryOneFile(key);
						Log.d(TAG, "Value returned in Server is: "+valReturned);
						PrintWriter out = new PrintWriter(socket.getOutputStream());
						out.write("One File::"+valReturned+"\n");
						out.flush();
					}
					else if (str.contains("Delete All")){
						Log.d(TAG, "In Delete All of Server Socket");
						int ret = deleteAll();
						String retVal = String.valueOf(ret);
						Log.d(TAG, "Value returned from deleteAll(): "+retVal);
						// Send it to client
						PrintWriter out = new PrintWriter(socket.getOutputStream());
						out.write("Number of files deleted|"+retVal+"\n");
						out.flush();
					}
					else if (str.contains("Lost Messages")){
                        String coordinator = str.split("\\|")[1];
                        String ret = queryMessagesOfPort(coordinator);
                        Log.d(TAG, "Value returned in server for lost messages is: "+ret);
                        PrintWriter out = new PrintWriter(socket.getOutputStream());
                        out.write("Replica Files::"+ret+"\n");
                        out.flush();
                    }
                    else if (str.contains("Delete Message")){
                        String key = str.split("\\|")[1];
                        Log.d(TAG, "Delete request for the message: "+key+" in port: "+thisPortStr);
                        deleteAMessage(key);
                        PrintWriter out = new PrintWriter(socket.getOutputStream());
                        out.write("Deleted a File"+"\n");
                        out.flush();
                    }
				}catch (SocketTimeoutException ste){
				    Log.e(TAG, "Socket Timeout Exception in ServerTask");
				    ste.printStackTrace();
                }
				catch (IOException ioe){
					Log.e(TAG, "Receive message failed in ServerTask");
					ioe.printStackTrace();
				}
			}
		}
	}

	private class CrashRecovery extends AsyncTask<String, Void, Void>{
	    /*
	    AsyncTask to ensure this node gets latest info from its replicas as well as replicate
	    the previous two node's data after recovering from crash.
	     */
        @Override
        protected Void doInBackground(String... msgs){
            delayFlag = true;
            // Delete contents of this AVD
            deleteAll();

            int index = portList.indexOf(thisPortStr);
            String[] replicaPorts = new String[2];
            String[] prevPorts = new String[2];
            if (index == 0){
                replicaPorts[0] = portList.get(1);
                replicaPorts[1] = portList.get(2);
                prevPorts[0] = portList.get(4);
                prevPorts[1] = portList.get(3);
            }
            else if (index == 1){
                replicaPorts[0] = portList.get(2);
                replicaPorts[1] = portList.get(3);
                prevPorts[0] = portList.get(0);
                prevPorts[1] = portList.get(4);
            }
            else if (index == 2){
                replicaPorts[0] = portList.get(3);
                replicaPorts[1] = portList.get(4);
                prevPorts[0] = portList.get(0);
                prevPorts[1] = portList.get(1);
            }
            else if (index == 3){
                replicaPorts[0] = portList.get(4);
                replicaPorts[1] = portList.get(0);
                prevPorts[0] = portList.get(2);
                prevPorts[1] = portList.get(1);
            }
            else if (index == 4){
                replicaPorts[0] = portList.get(0);
                replicaPorts[1] = portList.get(1);
                prevPorts[0] = portList.get(2);
                prevPorts[1] = portList.get(3);
            }

            // Get data from replicas
            for (int i=0; i < 2; i++){
                try {
                    Log.d(TAG, "Getting data from replicas");
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(replicaPorts[i])*2);
                    socket.setSoTimeout(500);
                    String msgToSend = "Lost Messages|"+String.valueOf(Integer.parseInt(thisPortStr)*2)+"\n";

                    // Ask for messages from replica
                    PrintWriter out = new PrintWriter(socket.getOutputStream());
                    out.write(msgToSend);
                    out.flush();

                    // Get messages from replica
                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(socket.getInputStream()));
                    String str = in.readLine();
                    Log.d(TAG, "Message from replicas is: "+str);

                    if (str.contains("Replica Files")){
                        if (str.split("::").length != 1) {
                            String allFilesList = str.split("::")[1];
                            String[] allFiles = allFilesList.split("-");
                            for (String file : allFiles) {
                                String[] keyValPair = file.split("\\|");
                                String ver = keyValPair[0];
                                String port = keyValPair[1];
                                String key = keyValPair[2];
                                String val = keyValPair[3];
                                insertIntoFileAfterRecovery(key, val, port, ver);
                            }
                            socket.close();
                        }
                    }

                }catch (SocketTimeoutException ste){
                    Log.e(TAG, "Socket Timeout Exception in CrashRecovery");
                    ste.printStackTrace();
                }
                catch (UnknownHostException uhe){
                    Log.e(TAG, "Unknown host exception in CrashRecovery");
                    uhe.printStackTrace();
                }catch (IOException ioe){
                    Log.e(TAG, "IOException in CrashRecovery");
                    ioe.printStackTrace();
                }
            }

            // Replicate data
            for (int i=0; i < 2; i++){
                try {
                    Log.d(TAG, "Replicate data from previous two ports");
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(prevPorts[i])*2);
                    socket.setSoTimeout(500);
                    String msgToSend = "Lost Messages|"+String.valueOf(Integer.parseInt(prevPorts[i])*2)+"\n";

                    // Ask for messages from replica
                    PrintWriter out = new PrintWriter(socket.getOutputStream());
                    out.write(msgToSend);
                    out.flush();

                    // Get messages
                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(socket.getInputStream()));
                    String str = in.readLine();
                    Log.d(TAG, "Message from coordinator node is: "+str);

                    if (str.contains("Replica Files")){
                        if (str.split("::").length != 1) {
                            String allFilesList = str.split("::")[1];
                            String[] allFiles = allFilesList.split("-");
                            for (String file : allFiles) {
                                String[] keyValPair = file.split("\\|");
                                String ver = keyValPair[0];
                                String port = keyValPair[1];
                                String key = keyValPair[2];
                                String val = keyValPair[3];
                                insertIntoFileAfterRecovery(key, val, port, ver);
                            }
                            socket.close();
                        }
                    }

                }catch (SocketTimeoutException ste){
                    Log.e(TAG, "Socket Timeout Exception in CrashRecovery Replication");
                    ste.printStackTrace();
                }
                catch (UnknownHostException uhe){
                    Log.e(TAG, "Unknown host exception in CrashRecovery Replication");
                    uhe.printStackTrace();
                }catch (IOException ioe){
                    Log.e(TAG, "IOException in CrashRecovery Replication");
                    ioe.printStackTrace();
                }
            }
            delayFlag = false;
            return null;
        }
    }

}
