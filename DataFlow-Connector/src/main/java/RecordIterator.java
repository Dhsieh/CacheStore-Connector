import com.sm.storage.Serializer;
import voldemort.utils.Pair;

import java.util.ArrayList;
import java.util.List;

import static voldemort.store.cachestore.BlockUtil.toKey;

/**
 * Created by derekhsieh on 7/10/16.
 */
public class RecordIterator<K, V> {
    private Client client;
    private Serializer serializer;
    private List<Pair<K, V>> list;
    private int record = 0;

    public RecordIterator(Client client, Serializer serializer) {
        this.client = client;
        this.serializer = serializer;
    }

    /**
     * call client which can be remote or in process and convert into target KV
     * retrieve the batch of record into list as iterator
     *
     * @param begin - start record no
     * @param size  - batch size of records
     */
    public RecordIterator<K, V> build(int begin, int size) {
        List<Pair<byte[], byte[]>> pairList = client.nextBlock(begin, size);
        list = new ArrayList<Pair<K, V>>(pairList.size());
        //convert Key -> K , serialize byte[] to V
        for (Pair<byte[], byte[]> each : pairList) {
            list.add(new Pair<K, V>((K) toKey(each.getFirst()), (V) serializer.toObject(each.getSecond())));
        }
        //reset record counter
        record = 0;
        return this;
    }

    /**
     * @return list of two integer
     * First - total record
     * second - batch record size
     */
    public List<Integer> findTotal2BatchSize() {
        return client.findTotal2BatchSize();
    }

    public List<Long> findTotalBlock() {
        return client.findTotalBlock();
    }

    /**
     * @return next K V pair
     */
    public Pair<K, V> next() {
        if (hasNext())
            return list.get(record++);
        else
            return null;
    }

    /**
     * @return true more data
     * false, end of iterator
     */
    public boolean hasNext() {
        if (record < list.size())
            return true;
        else
            return false;
    }

    public void close() {
        client.close();
    }

    public int getRecord() {
        return record;
    }

    public String toString() {
        return "client " + client.toString() + " record " + record + " list size " + (list == null ? 0 : list.size());
    }
}
