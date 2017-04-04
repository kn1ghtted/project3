import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by teddyding on 4/3/17.
 */
public class MyCache extends UnicastRemoteObject implements Cloud.DatabaseOps{
    private ConcurrentMap<String, String> cacheMap;
    private final Cloud.DatabaseOps origDB;

    protected MyCache(Cloud.DatabaseOps origDB) throws RemoteException {
        this.origDB = origDB;
    }

    public String get(String s) throws RemoteException {
        if (cacheMap.containsKey(s)){
            return cacheMap.get(s);
        }
        else{
            String returnVal = origDB.get(s);
            cacheMap.put(s, returnVal);
            return returnVal;
        }
    }

    public boolean set(String s, String s1, String s2) throws RemoteException {
        boolean ret = origDB.set(s, s1, s2);
        cacheMap.put(s, s1);
        return ret;
    }

    public boolean transaction(String s, float v, int i) throws RemoteException {
        return false;
    }

//    public boolean initialize(){
//
//    }
}