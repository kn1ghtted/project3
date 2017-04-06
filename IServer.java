import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Created by teddyding on 3/27/17.
 */
public interface IServer extends Remote{

    boolean isFrontTier(int vmID) throws RemoteException;

    void pushRequest(Cloud.FrontEndOps.Request requestQueue) throws RemoteException;

    Cloud.FrontEndOps.Request popRequest() throws RemoteException;

    int getRequestQueueLength() throws RemoteException;

    void shutDown() throws RemoteException;

    boolean removeVM(int vmID, int type) throws RemoteException;

    long getInitTime() throws RemoteException; // TODO just for debug
}
