package org.scut.ccnl.genomics.io;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.util.Collections;
import java.util.List;

/**
 * Special serializer for Collections.nCopies
 *
 * @param <T> Element type
 */
public class CollectionsNCopiesSerializer<T> extends Serializer<List<T>> {
    @Override
    public void write(Kryo kryo, Output output, List<T> object) {
        output.writeInt(object.size(), true);
        if (object.size() > 0) {
            kryo.writeClassAndObject(output, object.get(0));
        }
    }

    @Override
    public List<T> read(Kryo kryo, Input input, Class<List<T>> type) {
        int size = input.readInt(true);
        if (size > 0) {
            T object = (T) kryo.readClassAndObject(input);
            return Collections.nCopies(size, object);
        } else {
            return Collections.emptyList();
        }
    }
}