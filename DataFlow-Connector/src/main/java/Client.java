import voldemort.utils.Pair;

import java.util.List;

/**
 * Created by derekhsieh on 7/10/16.
 */
public interface Client {
    public final static String FindTotal2BatchSize = "findTotal2BatchSize";
    public final static String NextBlock = "nextBlock";
    public final static String FindTotalBlock = "findTotalBlock";
    /**
     *
     * @param begin of record no
     * @param size  number of record in the block
     * @return List of Pair<Key, byte[]>
     */
    List<Pair<byte[],byte[]>> nextBlock(int begin, int size);

    /**
     *  find the total record for node and batch size (number of record) for 2MB payload
     * @return  List<Integer>,  first element = totalRecord , second = number of record for each block
     *
     */
    public List<Integer> findTotal2BatchSize();

    public List<Long> findTotalBlock();

    public void close();
}
