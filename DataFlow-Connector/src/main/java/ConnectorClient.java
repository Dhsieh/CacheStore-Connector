import com.sm.localstore.impl.HessianSerializer;
import com.sm.message.Invoker;
import com.sm.storage.Serializer;
import com.sm.store.client.RemoteClientImpl;
import com.sm.store.client.grizzly.GZRemoteClientImpl;
import com.sm.store.cluster.ClusterNodes;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import voldemort.store.cachestore.Key;
import voldemort.store.cachestore.Value;
import voldemort.utils.Pair;

import java.util.List;

import static com.sm.store.cluster.Utils.ADMIN_STORE;
import static com.sm.store.cluster.Utils.CLUSTER_KEY;

/**
 * Created by derekhsieh on 7/10/16.
 */
public class ConnectorClient implements Client{
    protected static final Log logger = LogFactory.getLog(ConnectorClient.class);

    private String url;
    private final String store;
    private RemoteClientImpl remoteClient;
    private long timeOut = 120*1000L;
    private Serializer embeddedSerializer = new HessianSerializer();

    public ConnectorClient(String url, String store) {
        this.url = url;
        this.store = store;
        init();
    }

    //use 30 seconds as timeout
    protected void init() {
        remoteClient = new GZRemoteClientImpl(url, embeddedSerializer, store, true, timeOut);
    }

    @Override
    public List<Pair<byte[], byte[]>> nextBlock(int begin, int size) {
        Invoker invoker = new Invoker(this.getClass().getName(), NextBlock, new Object[] { begin, size});
        List<Pair<byte[], byte[]>> list = null;
        for ( int i = 0 ; i < 3 ; i++) {
            try {
                list = (List<Pair<byte[], byte[]>>) remoteClient.invoke(invoker);
                if (list != null) break;
            } catch (Exception ex) {
                logger.error( "i= "+i+" "+ex.getMessage(), ex);
            }
        }
        return list;
    }

    @Override
    public List<Integer> findTotal2BatchSize() {
        Invoker invoker = new Invoker(this.getClass().getName(), FindTotal2BatchSize, new Object[] {});
        List<Integer> list = (List<Integer>) remoteClient.invoke(invoker);
        return list;
    }

    public List<Long> findTotalBlock() {
        Invoker invoker = new Invoker(this.getClass().getName(), "findTotalBlock", new Object[] { });
        List<Long> list = (List<Long>) remoteClient.invoke(invoker);
        return list;
    }

    public List<ClusterNodes> findClusterList(){
        RemoteClientImpl client = new GZRemoteClientImpl(url, embeddedSerializer, ADMIN_STORE);
        Value value = client.get(Key.createKey(CLUSTER_KEY));
        client.close();
        return  (List<ClusterNodes>) value.getData();
    }

    @Override
    public void close() {
        remoteClient.close();
    }

    public long getTimeOut() {
        return timeOut;
    }

    public void setTimeOut(long timeOut) {
        this.timeOut = timeOut;
    }
}
