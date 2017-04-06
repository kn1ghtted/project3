
import org.omg.PortableServer.SERVANT_RETENTION_POLICY_ID;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.PasswordAuthentication;
import java.rmi.*;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;

public class Server implements IServer{
	private static final int FRONT = 1;
	private static final int MIDDLE = 2;
	private static final int MASTER = 0;
	private static final String FRONTTIER_STRING = "FrontTier";
	private static final String MIDDLETIER_STRING = "MiddleTier";
	private static final float LOG_PERIOD_LENGTH = 2000;
	private static final long ADJUST_COOLDOWN = 0;
	private static final int FRONTTIER_THRESHOLD = 0;
	private static final double QUEUELENGTH_MIDDLETIER_RATIO = 2.5;
	private static final int MIDLETIER_SHUT_THRESHOLD = 2;
	private static final double DROP_PROB = 0.3;
	private static final int DROP_THRESHOLD = 5;
	private static final long IDLE_THRESHOLD = 4000;
	private static final int SLLENGTH_FRONTTIER_RATIO = 5;
	private static final int MIDTIER_UPPERBOUND = 10;
	private static final int FRONTTIER_UPPERBOUND = 2;
	private static final long SAMPLING_PERIOD = 3;
	// ratio between initial sampling arrival rate and midttiers started after sampling
	private static final double ARRIVAL_RATE_MIDTIER_RATIO = 2;
	private static int MID_FRONT_RATIO = 6;
	// a concurrent the map each VM ID to its tier
	private static ConcurrentMap<Integer, Integer> frontTierMap;
	private static ConcurrentMap<Integer, Integer> middleTierMap;
	public static ConcurrentLinkedDeque<Cloud.FrontEndOps.Request> requestQueue;
	private static List<Integer> logArray;
	private static long initTimeStamp;
	private static int nextVMID;

	private static ServerLib SL;
	private static VMInfo vmInfo; // TODO one per main class?
	private static long lastAdjustTime;
	private static long lastProcessTIme;
	private static Cloud.DatabaseOps myCache;
	private static long slaveInitTime; // TODO just for debug
	private static long masterInitTime; // TODO just for debug
	private static boolean samplingEnded; // used by master to indicate if
	private static int clientCnt;
	// the initial sampling period has ended

	private int vmID;
	private static boolean isShutDown;

	public Server(ServerLib SL, int vmID) {
		this.vmID = vmID;
		this.SL = SL; // static or non static? if initialize Server within main, does this
		isShutDown = false;
	}


	public static void main ( String args[] ) throws Exception {
		if (args.length != 3) throw new Exception("Need 3 args: <cloud_ip> <cloud_port> <VM id>");
		String ip = args[0];
		int port = Integer.parseInt(args[1]);
		ServerLib SL = new ServerLib(ip, port);
		int vmID = Integer.parseInt(args[2]);
		IServer master = null;
		if (vmID == 1){
//			int VMNum = getVMNum(currentTime);
//			for (int i = 0; i < VMNum; i ++){
//				SL.startVM();
//			}
			requestQueue = new ConcurrentLinkedDeque<>();
			frontTierMap = new ConcurrentHashMap<>();
			middleTierMap = new ConcurrentHashMap<>();
			logArray = new ArrayList<>();
//			vmMap = new ConcurrentHashMap<>();
			registerMaster(SL, ip, port, vmID);
			myStartVM(2, MIDDLE);
			myStartVM(3, MIDDLE);
			nextVMID = 4;
			initTimeStamp = System.currentTimeMillis();
			lastAdjustTime = initTimeStamp;
			myCache = new MyCache(SL.getDB(), ip, port);
			clientCnt = 0;

		}
		else{
			if(SL.getStatusVM(1) == Cloud.CloudOps.VMStatus.Running){
				master = MyLib.getMasterInstance(ip, port);
				myCache = (Cloud.DatabaseOps) MyLib.getVMInstance(ip, port, MyLib.CACHE_STRING);
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
				masterInitTime = master.getInitTime();
				slaveInitTime = System.currentTimeMillis();
				lastProcessTIme = slaveInitTime;
				System.err.println("VM " + vmID + " started, time = " + (slaveInitTime - masterInitTime)/1000);


			}
		}

		// main loop
		while (true) {
			Cloud.FrontEndOps.Request r;
			if (vmInfo.getType() == MASTER){
				r = SL.getNextRequest();
				if (!samplingEnded){
					initialSampling();
				}
				if (!tryDrop(r)){
					if ((SL.getStatusVM(2) == Cloud.CloudOps.VMStatus.Running) ||
							(System.currentTimeMillis() - initTimeStamp >= 6000)){
						// >= 6000 to prevent the shut down of vm 2
						requestQueue.push(r);
						logPush();
					}else{
						SL.drop(r);
//						SL.processRequest(r, myCache);
					}
					if (adjustVMs(SL, ip, port)){
						System.err.println("Adjusted VM!");
						lastAdjustTime = System.currentTimeMillis();
						System.err.println("FrontTierSize = " + frontTierMap.size());
						System.err.println("MiddleTierSize = " + middleTierMap.size());
					}
					updateLogArray(getLogPeriod());
				}
			}
			else{
				if(SL.getStatusVM(1) == Cloud.CloudOps.VMStatus.Running){
					//fronttier, whileloop
					tryShutDown(master, vmID, FRONT);
					if (vmInfo.getType() == FRONT){
						int SLQueueLength = SL.getQueueLength();
						if (SLQueueLength == 0){
							System.err.println("SL.getQueueLength() is empty ");
						}
						else{
							System.err.println("SL.getQueueLength() = " + SL.getQueueLength());
						}
						r = SL.getNextRequest();
						lastProcessTIme = System.currentTimeMillis();
						if (!tryDrop(r)){
							master.pushRequest(r);
						}
					}
					// midtier, while loo
					else{
						tryShutDown(master, vmID, MIDDLE);
						r = master.popRequest();
						if (r != null){
							SL.processRequest(r, myCache);
							lastProcessTIme = System.currentTimeMillis();
						}
					}
				}
			}
		}
	}

	// sample first couple of seconds, and use that to decide how many more VMs to start at time = 5
	private static void initialSampling() {
		clientCnt += 1;
		long time = System.currentTimeMillis();
		if ((time - initTimeStamp) >= SAMPLING_PERIOD){
			samplingEnded = true;
			double actualSamplingTime = (double)(time - initTimeStamp) / 1000;
			double arrivalRate = (double)clientCnt/actualSamplingTime;
			int targetMidTiers = (int)Math.floor(arrivalRate / ARRIVAL_RATE_MIDTIER_RATIO);
			System.err.println("Sampling ended! arrival rate = " + arrivalRate + ", targetMidtiers = " + targetMidTiers);
			int size = middleTierMap.size();
			for (int i = size; i < Math.min(targetMidTiers, MIDTIER_UPPERBOUND); i++){
				myStartVM(nextVMID, MIDDLE);
			}
		}

	}

	private static void tryShutDown(IServer master, int vmID, int type) throws RemoteException {
		if (System.currentTimeMillis() - lastProcessTIme > IDLE_THRESHOLD){
			if (!isShutDown){
				if (master.removeVM(vmID, type)){
					System.err.println("Shutting down instance!! vmID = " + vmID);
					SL.shutDown();
					isShutDown = true;
					long time = System.currentTimeMillis();
					System.err.println("VM " + vmID + " ended at " + (time - masterInitTime)/1000 + " started at "
							+ (slaveInitTime - masterInitTime)/1000);
					System.exit(0);
				}
			}
		}
	}

	private static boolean tryDrop(Cloud.FrontEndOps.Request r) {
		if ((SL.getQueueLength() > DROP_THRESHOLD)){
			return dropWithProb(DROP_PROB, r);
		}
		return false;
	}

	private static boolean dropWithProb(double drop_prob, Cloud.FrontEndOps.Request r) {
		double prob = Math.random();
		if (prob < drop_prob){
			SL.drop(r);
			return true;
		}
		return false;
	}

	private static boolean adjustVMs(ServerLib SL, String ip, int port) {
//		if (logArray.size() < 2){
//			return false;
//		}
		int deltaRequest = getDeltaRequest();
		boolean adjusted = false;
		if ((System.currentTimeMillis() - lastAdjustTime) > ADJUST_COOLDOWN){
			if ((deltaRequest > 0)){
				// add middle tier
				int previousSize = middleTierMap.size();
				int targetSize = previousSize + deltaRequest;
				System.err.println("requestQueue.size() = " + requestQueue.size());
				int surpressedNum = (int)Math.floor((double)requestQueue.size() / QUEUELENGTH_MIDDLETIER_RATIO);
				System.err.println("requestQueue.size() /ratio = " + surpressedNum);
				targetSize = Math.min(Math.min(surpressedNum, targetSize), MIDTIER_UPPERBOUND);
				System.err.println("previousSize, targetSize = " + previousSize + " , " + targetSize);
				for (int i = previousSize; i < targetSize; i++)
				{
					adjusted = true;
					myStartVM(nextVMID, MIDDLE);
				}
//
			}
			//				// add front tier, tar
			int targetFrontTierSize = Math.min((int)Math.floor(SL.getQueueLength()
					/ SLLENGTH_FRONTTIER_RATIO), FRONTTIER_UPPERBOUND);
			int SLQueueLength = SL.getQueueLength();
			if (SLQueueLength == 0){
				System.err.println("SL.getQueueLength() is empty ");
			}
			else{
				System.err.println("SL.getQueueLength() = " + SL.getQueueLength());
			}
			System.err.println("target frontier size = " + targetFrontTierSize);
			System.err.println("fronttiermap.size = " + frontTierMap.size());
			int size = frontTierMap.size();
			for (int i = size; i < targetFrontTierSize; i++)
			{
				adjusted = true;
				myStartVM(nextVMID, FRONT);
			}
			//else{
//				if (logArray.size() >= 2){
//					System.err.println("Checking consecutive 1s: " + logArray);
//					if ((logArray.get(logArray.size() - 1) == logArray.get(logArray.size() - 2)) && (logArray.get(logArray.size() - 2) == 1)) {
//						System.err.println("Consecutive ones!");
//						// shutdown mid tier
//						adjusted = true;
//						System.err.println("middleTierMap = " + middleTierMap);
//						while (middleTierMap.size() > MIDLETIER_SHUT_THRESHOLD) {
//							shutDownVM(ip, port, middleTierMap, MIDDLETIER_STRING);
//						}
//					}
//				}
		}

		//}

		return adjusted;
	}

//	private static void shutDownVM(String ip, int port, ConcurrentMap<Integer, Integer> map,
//										String urlString) {
//		System.err.println("In shut down");
//		Integer idShutDown = 0;
//		boolean success = true;
//		for (Integer i : map.keySet()) {
//			String url = String.format("//%s:%d/%s", ip, port, urlString + Integer.toString(i));
//			System.err.println("Closing url = " + url);
//			System.err.println("This macineg is : " + SL.getStatusVM(i));
//			try {
//				idShutDown = i;
//				if (SL.getStatusVM(i) == Cloud.CloudOps.VMStatus.Running){
//					IServer instance = (IServer) Naming.lookup(url);
//					instance.shutDown();
//				}
//			} catch (NotBoundException e) {
//				success = false;
//				e.printStackTrace();
//			} catch (MalformedURLException e) {
//				e.printStackTrace();
//			} catch (RemoteException e) {
//				e.printStackTrace();
//			}
//			break;
//		}
//		if (success){
//			map.remove(idShutDown);
//		}
//	}

	private static void myStartVM(int vmID, int type) {
		if (type == FRONT){
			System.err.println("Starting fronttier, frontiermap = " + frontTierMap);
			frontTierMap.put(vmID, vmID);
			System.err.println("Finished starting fronttier, frontiermap = " + frontTierMap);

		}
		else{
			System.err.println("Starting middletier, middletiermap = " + middleTierMap);
			middleTierMap.put(vmID, vmID);
			System.err.println("Finished staring middletier, middletiermap = " + middleTierMap);
		}
		nextVMID += 1;
		SL.startVM();
	}

	private static void registerMaster(ServerLib SL, String ip, int port, int vmID) {
		SL.register_frontend();
		vmInfo = new VMInfo(MASTER, SL, new Date(), vmID);
		String url = String.format("//%s:%d/%s", ip, port, MyLib.MASTER_STRING);
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

	public static int getDeltaRequest() {
//		if (logArray.size() >= 4) {
//			return ((logArray.get(logArray.size() - 1) + logArray.get(logArray.size() - 2)) -
//					(logArray.get(logArray.size() - 3) + logArray.get(logArray.size() - 4)))/2;
//		}
//		else{
//			return logArray.get(logArray.size() - 1) - logArray.get(logArray.size() - 2);
//		}
		return requestQueue.size() - middleTierMap.size();
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
//		logArray.set(logArray.size()-1, (logArray.get(logArray.size()-1) + 1));
		System.err.println("Logged push, logArray = " + logArray);

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
		if (!isShutDown){
			System.err.println("Shutting down instance!! vmID = " + vmID);
			SL.shutDown();
			isShutDown = true;
		}
	}

	// return true if can remove, and remove vmid
	public boolean removeVM(int vmID, int type) throws RemoteException {
		ConcurrentMap<Integer, Integer> map;
		int threshold;
		if (type == FRONT){
			map = frontTierMap;
			threshold = FRONTTIER_THRESHOLD;
		}
		else{
			map = middleTierMap;
			threshold = MIDLETIER_SHUT_THRESHOLD;
		}
		if (map.size() > threshold){
			map.remove(vmID);
			System.err.println("Removed  " + type +  " middletiermap = " + map);
			return true;
		}
		return false;
	}

	public long getInitTime() throws RemoteException {
		return initTimeStamp;
	}


//	private static int getVMNum(float currentTime){
//		int VMNum;
//		if ((currentTime >= 0) && (currentTime <= 7)){
//			VMNum = 1;
//		}else if(currentTime <= 12){
//			VMNum = 2;
//		}else if (currentTime < 14){
//			VMNum = 3;
//		}else if (currentTime <= 18){
//			VMNum = 2;
//		}else if (currentTime <= 22){
//			VMNum = 3;
//		}
//		else{
//			VMNum = 1;
//		}
//		return VMNum;
//	}


}