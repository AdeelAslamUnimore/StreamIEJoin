package com.stormiequality.join;

import com.esotericsoftware.kryo.Kryo;
import org.apache.storm.serialization.DefaultKryoFactory;
import org.apache.storm.serialization.IKryoDecorator;

import java.util.Map;

public class KryoSerliazation extends DefaultKryoFactory {
    @Override
    public Kryo getKryo(Map<String, Object> conf) {
       Kryo kryo = super.getKryo(conf);
       // Kryo kryo= new Kryo();
        kryo.register(java.util.BitSet.class);
        kryo.register(com.stormiequality.BTree.Node.class);
        kryo.register(com.stormiequality.BTree.Key.class);
        kryo.register(long[].class);
        return kryo;
    }
}
