import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Created by teddyding on 4/4/17.
 */
public class MyLib {

    public static final String MASTER_STRING = "Master";
    public static final String CACHE_STRING = "Cache";


    public static IServer getMasterInstance(String ip, int port) {
            return (IServer)getVMInstance(ip, port, MASTER_STRING);
    }

    @SuppressWarnings("unchecked")
    public static <T> T getVMInstance(String ip, int port, String name) {
        String url = String.format("//%s:%d/%s", ip, port, name);
        try {
            System.err.println("Looking up url = " + url);
            Remote instance = Naming.lookup(url);
            return (T)instance;
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
