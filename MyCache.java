import java.net.MalformedURLException;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by teddyding on 4/3/17.
 */
public class MyCache extends UnicastRemoteObject implements Cloud.DatabaseOps{
    private ConcurrentMap<String, String> cacheMap;
    private final Cloud.DatabaseOps origDB;

    protected MyCache(Cloud.DatabaseOps origDB, String ip, int port) throws RemoteException {
        this.origDB = origDB;
        initialize(ip, port);
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
        return origDB.transaction(s, v, i);
    }

    public void initialize(String ip, int port){
        cacheMap = new ConcurrentHashMap<>();
//        IServer master = MyLib.getMasterInstance(ip, port);
//        try {
//            origDB = master.getOrigDB();
//        } catch (RemoteException e) {
//            e.printStackTrace();
//        }
        registerCache(ip, port);
    }


    private void registerCache(String ip, int port) {
        String url = String.format("//%s:%d/%s", ip, port, MyLib.CACHE_STRING);
        System.err.println("Binding cache with url = " + url);
        try {
            Naming.bind(url, this);
        } catch (AlreadyBoundException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }
}