import java.util.Date;

/**
 * Created by teddyding on 3/27/17.
 */
public class VMInfo {

    private ServerLib SL;
    private final int type;
    public Date date;
    public int vmID;

    public VMInfo(int master, ServerLib SL, Date date, int vmID) {
        type = master;
        this.SL = SL;
        this.date = date;
        this.vmID = vmID;
    }

    public int getType() {
        return type;
    }

    public ServerLib getSL() {
        return SL;
    }
}
