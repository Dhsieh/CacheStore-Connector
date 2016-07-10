import com.caucho.hessian.io.Hessian2Input;
import com.caucho.hessian.io.Hessian2Output;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.StandardCoder;
import com.google.cloud.dataflow.sdk.values.KV;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * Created by derekhsieh on 7/10/16.
 */
public class CacheStoreCoder<K,V> extends StandardCoder<KV<K, V>> {

    @Override
    public void encode(KV<K, V> value, OutputStream outStream, Context context) throws CoderException, IOException {
        Hessian2Output hessian2Output = null;
        try {
            hessian2Output = new Hessian2Output( outStream);
            hessian2Output.writeObject( value.getKey());
            hessian2Output.writeObject( value.getValue());
            hessian2Output.flush();
        } catch (java.io.IOException ioe) {
            throw new RuntimeException(ioe.getMessage(), ioe);
        }
    }

    @Override
    public KV<K,V> decode(InputStream inStream, Context context) throws CoderException, IOException {
        Hessian2Input hessian2Input = null;
        try {
            hessian2Input = new Hessian2Input(inStream);
            K key = (K) hessian2Input.readObject() ;
            V value = (V) hessian2Input.readObject();
            return KV.of(key, value);
        } catch (java.io.IOException ioe) {
            throw new RuntimeException(ioe.getMessage(), ioe);
        }
    }

    public List<? extends Coder<?>> getCoderArguments() {
        return null;
    }

    public void verifyDeterministic() throws NonDeterministicException {
    }
}
