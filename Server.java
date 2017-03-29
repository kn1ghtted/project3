/* Sample code for basic Server */

import java.net.MalformedURLException;
import java.rmi.*;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;

public class Server implements IServer{
	private static final String MASTER_STRING = "Master";
	private static final int FRONT = 1;
	private static final int MIDDLE = 2;
	private static final int MASTER = 0;
	private static final String FRONTTIER_STRING = "FrontTier";
	private static final String MIDDLETIER_STRING = "MiddleTier";
	private static final float LOG_PERIOD_LENGTH = 2000;
	private static final long ADJUST_COOLDOWN = 1000;
	private static final int FRONTTIER_TRHESHOLD = 1;
	private static final int MIDLETIER_SHUT_THRESHOLD = 2;
	private static final int QUEUELENGTH_MID_RATIO = 1;
	// a concurrent the map each VM ID to its tier
	private static ConcurrentMap<Integer, Integer> frontTierMap;
	private static ConcurrentMap<Integer, Integer> middleTierMap;

	public static ConcurrentLinkedDeque<Cloud.FrontEndOps.Request> requestQueue;
	private static VMInfo vmInfo;
	private static int MID_FRONT_RATIO = 4;
	private static List<Integer> logArray;
	private static long initTimeStamp;
	private static long lastAdjustTime;
	private int vmID;
	private ServerLib SL;

	public Server(ServerLib SL, int vmID) {
		this.vmID = vmID;
		this.SL = SL;
	}




	public static void main ( String args[] ) throws Exception {
		if (args.length != 3) throw new Exception("Need 3 args: <cloud_ip> <cloud_port> <VM id>");
		String ip = args[0];
		int port = Integer.parseInt(args[1]);
		ServerLib SL = new ServerLib(ip, port);
		int vmID = Integer.parseInt(args[2]);
		IServer master = null;
		System.err.println("VM started, ip = " + ip + " port = " + port + " vmID = " + vmID);
		if (vmID == 1){
//      int VMNum = getVMNum(currentTime);
//      for (int i = 0; i < VMNum; i ++){
//        SL.startVM();
//      }
			requestQueue = new ConcurrentLinkedDeque<>();
			frontTierMap = new ConcurrentHashMap<>();
			middleTierMap = new ConcurrentHashMap<>();
			logArray = new ArrayList<>();
//      vmMap = new ConcurrentHashMap<>();
			registerMaster(SL, ip, port, vmID);
			myStartVM(SL, 2, MIDDLE);
			initTimeStamp = System.currentTimeMillis();
			lastAdjustTime = initTimeStamp;
		}
		else{
			master = getMasterInstance(ip, port);
			// we design first VM started by server to be a mid-tier
			if (vmID == 2){
				registerMidTier(SL, ip, port, vmID);
			}
			else{
				if (master.isFrontTier(vmID)){
					System.err.println("In new FrontTier!");
					registerFrontTier(SL, ip, port, vmID);
				}
				else{
					System.err.println("In new Mid tier!");
					registerMidTier(SL, ip, port, vmID);
				}
			}
		}

		// main loop
		while (true) {
			Cloud.FrontEndOps.Request r;
			if (vmInfo.getType() == MASTER){
				r = SL.getNextRequest();
				if (SL.getStatusVM(2) == Cloud.CloudOps.VMStatus.Running){
					// what happens if RMI is called inside its own class TODO??
					requestQueue.push(r);
					logPush();
				}else{
					System.err.println("servering processing request!");
					SL.processRequest(r);
				}
				if (adjustVMs(SL, ip, port)){
					lastAdjustTime = System.currentTimeMillis();
					System.err.println("FrontTierSize = " + frontTierMap.size());
					System.err.println("MiddleTierSize = " + middleTierMap.size());
				}
				updateLogArray(getLogPeriod());
			}
			else{
				if(SL.getStatusVM(1) == Cloud.CloudOps.VMStatus.Running){
					if (vmInfo.getType() == FRONT){
						r = SL.getNextRequest();
						master.pushRequest(r);
					}
					else{
						int masterQueueLength = master.getRequestQueueLength();
						if(masterQueueLength > 0) {
							r = master.popRequest();
							if (r != null){
								SL.processRequest(r);
							}
						}
					}
				}
			}
		}
	}

	private static boolean adjustVMs(ServerLib SL, String ip, int port) {
		if (logArray.size() < 2){
			return false;
		}
		int queueLength = logArray.get(logArray.size() - 1);
		int deltaRequest = queueLength - logArray.get(logArray.size() - 2);
		System.err.println(logArray);
		boolean adjusted = false;
		if ((System.currentTimeMillis() - lastAdjustTime) > ADJUST_COOLDOWN && (deltaRequest != 0)){
			if ((deltaRequest > 0)){
				adjusted = true;
				System.err.println("Adding VM!!!! detlaRequest = " + deltaRequest);
				// add middle tier
				int targetSize = deltaRequest + middleTierMap.size();
//        int targetSize = Math.min(deltaRequest + middleTierMap.size(), queueLength / QUEUELENGTH_MID_RATIO);
				for (int i = middleTierMap.size(); i < targetSize; i++)
				{
					int targetID = frontTierMap.size() + middleTierMap.size() + 1;
					myStartVM(SL, targetID, MIDDLE);
				}
				// add front tier, tar
				// give frontier a threshold
				if (frontTierMap.size() < FRONTTIER_TRHESHOLD){
					for (int i = 0; i < FRONTTIER_TRHESHOLD; i++){
						int targetID = frontTierMap.size() + middleTierMap.size() + 1;
						myStartVM(SL, targetID, FRONT);
					}
				}
//        else{
//          int targetFrontTierSize = (middleTierMap.size() / MID_FRONT_RATIO) - frontTierMap.size();
//          System.err.println("target frontier size = " + targetFrontTierSize);
//          for (int i = 0; i < targetFrontTierSize; i++)
//          {
//            int targetID = frontTierMap.size() + middleTierMap.size() + 1;
//            myStartVM(SL, targetID, FRONT);
//          }
//        }

			}
		}
//    if ((logArray.get(logArray.size() - 1) == logArray.get(logArray.size() - 2)) && (logArray.get(logArray.size() - 2) == 1)){
//      // shutdown mid tier
//      adjusted = true;
//      if (middleTierMap.size() > MIDLETIER_SHUT_THRESHOLD){
//        for (Integer i : middleTierMap.keySet()){
//          String url = String.format("//%s:%d/%s", ip, port, MIDDLETIER_STRING + Integer.toString(i));
//          System.err.println("Closing url = " + url);
//          try {
//            IServer instance = (IServer) Naming.lookup(url);
//            instance.shutDown();
//          } catch (NotBoundException e) {
//            e.printStackTrace();
//          } catch (MalformedURLException e) {
//            e.printStackTrace();
//          } catch (RemoteException e) {
//            e.printStackTrace();
//          }
//          break;
//        }
//      }
//
//    }

		return adjusted;
	}

	private static void myStartVM(ServerLib SL, int vmID, int type) {
		if (type == FRONT){
			frontTierMap.put(vmID, vmID);
		}
		else{
			middleTierMap.put(vmID, vmID);
		}
		SL.startVM();
	}

	private static void registerMaster(ServerLib SL, String ip, int port, int vmID) {
		SL.register_frontend();
		vmInfo = new VMInfo(MASTER, SL, new Date(), vmID);
		String url = String.format("//%s:%d/%s", ip, port, MASTER_STRING);
		System.err.println("Binding master with url = " + url);
		bind(url);
	}


	private static void registerFrontTier(ServerLib SL, String ip, int port, int vmID) {
		SL.register_frontend();
		vmInfo = new VMInfo(FRONT, SL, new Date(), vmID);
		String url = String.format("//%s:%d/%s", ip, port, FRONTTIER_STRING + vmID);
		System.err.println("Binding FrontTier " + vmID + " with url = " + url);
		bind(url);
	}

	// TODO
	private static void registerMidTier(ServerLib SL, String ip, int port, int vmID) {
		vmInfo = new VMInfo(MIDDLE, SL, new Date(), vmID);
		String url = String.format("//%s:%d/%s", ip, port, MIDDLETIER_STRING + vmID);
		System.err.println("Binding MiddleTier " + vmID + " with url = " + url);
		bind(url);
	}

	private static void bind(String url) {
		try {
			Remote exportServer = UnicastRemoteObject.exportObject(new Server(vmInfo.getSL(), vmInfo.vmID), 0);
			Naming.bind(url, exportServer);
		} catch (AlreadyBoundException e) {
			e.printStackTrace();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}

	public static long getLogPeriod() {
		long time = System.currentTimeMillis();
		long logPeriod = (long)((double)(time - initTimeStamp) / (double) LOG_PERIOD_LENGTH);
		return logPeriod;
	}


	// a function only called by the master to determine if a VM is FrontTier or not
	public boolean isFrontTier(int vmID) throws RemoteException {
		assert (vmInfo.getType() == MASTER);
		return (frontTierMap.containsKey(vmID));
	}

	public synchronized void pushRequest(Cloud.FrontEndOps.Request r) throws RemoteException {
		requestQueue.push(r);
		Server.logPush();
	}

	private static void logPush() {
		long logPeriod = getLogPeriod();
		updateLogArray(logPeriod);
		logArray.set(logArray.size()-1, requestQueue.size());
//    logArray.set(logArray.size()-1, (logArray.get(logArray.size()-1) + 1));
//    System.err.println("Logged push, logArray = " + logArray);

	}


	private static void updateLogArray(long logPeriod) {
		while(logArray.size() < logPeriod){
			logArray.add(requestQueue.size());
		}
	}


	public synchronized Cloud.FrontEndOps.Request popRequest() throws RemoteException {
		if (requestQueue.size() > 0){
			return requestQueue.pop();
		}
		else return null;
	}

	public synchronized int getRequestQueueLength() throws RemoteException {
		return requestQueue.size();
	}

	public void shutDown() throws RemoteException {
		System.err.println("Shutting down instance!! vmID = " + vmID);
		SL.shutDown();
	}

//  private static int getVMNum(float currentTime){
//    int VMNum;
//    if ((currentTime >= 0) && (currentTime <= 7)){
//      VMNum = 1;
//    }else if(currentTime <= 12){
//      VMNum = 2;
//    }else if (currentTime < 14){
//      VMNum = 3;
//    }else if (currentTime <= 18){
//      VMNum = 2;
//    }else if (currentTime <= 22){
//      VMNum = 3;
//    }
//    else{
//      VMNum = 1;
//    }
//    return VMNum;
//  }

	public static IServer getMasterInstance(String ip, int port) {
		return getVMInstance(ip, port, MASTER_STRING);
	}


	public static IServer getVMInstance(String ip, int port, String name) {
		String url = String.format("//%s:%d/%s", ip, port, name);
		try {
			System.err.println("Looking up url = " + url);
			IServer instance = (IServer)Naming.lookup(url);
			return instance;
		} catch (NotBoundException e) {
			e.printStackTrace();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		System.exit(1);
		return null;
	}


}

