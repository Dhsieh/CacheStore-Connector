import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.base.Preconditions;
import com.sm.localstore.impl.HessianSerializer;
import com.sm.storage.Serializer;
import com.sm.store.client.RemoteClientImpl;
import com.sm.store.client.grizzly.GZRemoteClientImpl;
import com.sm.store.cluster.ClusterNodes;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import voldemort.store.cachestore.Key;
import voldemort.store.cachestore.Value;
import voldemort.utils.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

import static com.sm.store.cluster.Utils.ADMIN_STORE;
import static com.sm.store.cluster.Utils.CLUSTER_KEY;

/**
 * Created by derekhsieh on 7/10/16.
 */
public class CacheStoreIO {
    private static final Logger LOG = LoggerFactory.getLogger(CacheStoreIO.class);
    public static final String DEFAULT_URL = "localhost:6172";

    public static Source read() {
        return new Source(DEFAULT_URL, null, null, null);
    }


    public static Read.Bounded<KV> readFrom(String store, Serializer serializer) {
        return Read.from(new Source(DEFAULT_URL, store, null, serializer));
    }

    /**
     * Returns a {@code PTransform} that reads
     */
    public static Read.Bounded<KV> readFrom(String url, String store, Serializer serializer) {
        return Read.from(new Source(url, store, null, serializer));
    }

    public static class Source extends BoundedSource<KV> {

        private static final Logger LOG = LoggerFactory.getLogger(Source.class);
        private static final long serialVersionUID = 0;
        String url;
        String store;
        Serializer serializer;
        Pair<Integer, Integer> recordRange;

        private Source(String url, String store, Pair<Integer, Integer> recordRange, Serializer serializer) {
            this.url = url;
            this.store = store;
            this.recordRange = recordRange;
            if (serializer == null)
                this.serializer = new HessianSerializer();
            else
                this.serializer = serializer;
        }

        public Source withUrl(String url) {
            return new Source(url, store, recordRange, serializer);
        }

        public Source withStore(String store) {
            return new Source(url, store, recordRange, serializer);
        }

        public Source withSerializer(Serializer serializer) {
            return new Source(url, store, recordRange, serializer);
        }

        public Pair<Integer, Integer> getRecordRange() {
            return recordRange;
        }

        @Override
        public List<Source> splitIntoBundles(long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
            return findSplit(desiredBundleSizeBytes);
        }

        @Override
        public void validate() {
            Preconditions.checkNotNull(url);
            Preconditions.checkNotNull(store);
            Preconditions.checkNotNull(serializer);
            if ( recordRange == null ) {
                recordRange = findRecordRange();
            }
        }

        @Override
        public Coder<KV> getDefaultOutputCoder() {
            return new CacheStoreCoder();
        }

        @Override
        public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
            List<ClusterNodes> list = findClusterList();
            long total = 0 ;
            for (ClusterNodes each : list) {
                ConnectorClient client = new ConnectorClient(each.getServerArray()[0], store);
                //first is total record, second is file size
                List<Long> rec = client.findTotalBlock();
                total += rec.get(1);
                client.close();
            }
            return total;
        }

        @Override
        public BoundedReader<KV> createReader(
                PipelineOptions pipelineOptions) throws IOException {
            return new CacheStoreReader(this);
        }

        @Override
        public boolean producesSortedKeys(PipelineOptions options) throws Exception {
            return false;
        }

        private List<Source> findSplit(long size) {
            List<ClusterNodes> list = findClusterList();
            LOG.info("clusterNodeList "+list.size()+" "+list.toString());
            List<String> urls = new ArrayList<String>();
            List<Pair<Long, Long>> nodeList = new ArrayList<Pair<Long, Long>>();
            for (ClusterNodes each : list) {
                int i = new Random().nextInt(each.getServerArray().length);
                urls.add(each.getServerArray()[i]);
                ConnectorClient client = new ConnectorClient(each.getServerArray()[i], store);
                //first is total record, second is file size
                List<Long> rec = client.findTotalBlock();
                client.close();
                nodeList.add(new Pair<Long, Long>(rec.get(0), rec.get(1)));
            }
            List<Source> toReturn = new ArrayList<Source>();
            for (int k = 0; k < nodeList.size(); k++) {
                Pair<Long, Long> each = nodeList.get(k);
                String hostUrl = urls.get(k);
                int offset = 1;
                //find out how many blocks per url
                int block = (int) (each.getSecond() % size == 0 ? (each.getSecond() / size) :
                        (each.getSecond() / size + 1));
                //compute how many records per batch
                int blockSize = (int) (each.getFirst() * size / each.getSecond());
                for (int i = 0; i < block; i++) {
                    //check last block
                    if (i  == block - 1) {
                        toReturn.add(new Source(hostUrl, store, new Pair(offset, each.getFirst() - offset), serializer));
                    } else {
                        toReturn.add(new Source(hostUrl, store, new Pair(offset, blockSize), serializer));
                    }
                    offset += blockSize;
                }
            }
            return toReturn;
        }

        private Pair<Integer, Integer> findRecordRange() {
            ConnectorClient client = new ConnectorClient(url, store);
            //first is total record, second is file size
            List<Long> rec = client.findTotalBlock();
            client.close();
            return new Pair<Integer, Integer>(  0, (int) rec.get(0).longValue() );
        }

        /**
         * read cluster configuration from url
         *
         * @return List<ClusterNodes>
         */
        private List<ClusterNodes> findClusterList() {
            RemoteClientImpl client = new GZRemoteClientImpl(url, null, ADMIN_STORE);
            Value value = client.get(Key.createKey(CLUSTER_KEY));
            client.close();
            return (List<ClusterNodes>) value.getData();
        }
    }

    public static class CacheStoreReader<K, V> extends BoundedSource.BoundedReader<KV<K,V>> {
        Source source;
        Client client;
        RecordIterator recordIterator;

        public CacheStoreReader(Source source) {
            this.source = source;
            init();
        }

        private void init() {
            client = new ConnectorClient(source.url, source.store);
            recordIterator = new RecordIterator(client, source.serializer);
        }

        @Override
        public boolean start() throws IOException {
            LOG.info("start with range " + source.getRecordRange().toString());
            recordIterator.build((Integer) source.getRecordRange().getFirst(), (Integer) source.getRecordRange().getSecond());
            return true;
        }

        @Override
        public boolean advance() throws IOException {
            return recordIterator.hasNext();
        }

        @Override
        public KV<K, V> getCurrent() throws NoSuchElementException {
            Pair<K, V> pair = recordIterator.next();
            return KV.of(pair.getFirst(), pair.getSecond());
        }

        @Override
        public Instant getCurrentTimestamp() throws NoSuchElementException {
            return BoundedWindow.TIMESTAMP_MIN_VALUE;
        }

        @Override
        public void close() throws IOException {
            client.close();
            recordIterator.close();
        }

        @Override
        public BoundedSource getCurrentSource() {
            return source;
        }
    }
}
