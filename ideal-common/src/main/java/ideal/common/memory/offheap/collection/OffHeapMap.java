/*
 * Copyright (C) 2018 The Sylph Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ideal.common.memory.offheap.collection;

import ideal.common.memory.offheap.MemoryBlock;
import ideal.common.memory.offheap.Utils;

import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class OffHeapMap<K, V>
        implements Map<K, V>
{
    private final Utils.Serialization<V> serialization;
    private final Utils.Deserialization<V> deserialization;
    private final Map<K, MemoryBlock> blockMap;

    public OffHeapMap(
            Utils.Serialization<V> serialization,
            Utils.Deserialization<V> deserialization)
    {
        this.serialization = requireNonNull(serialization, "serialization is null");
        this.deserialization = requireNonNull(deserialization, "serialization is null");
        this.blockMap = new ConcurrentHashMap<>();
    }

    public OffHeapMap(
            Utils.Serialization<V> serialization,
            Utils.Deserialization<V> deserialization,
            Class<? extends Map> blockMapClass)
    {
        this.serialization = requireNonNull(serialization, "serialization is null");
        this.deserialization = requireNonNull(deserialization, "serialization is null");
        requireNonNull(blockMapClass, "blockMapClass is null");
        checkArgument(!blockMapClass.isInterface(), "blockMapClass is Interface");
        checkArgument(!Modifier.isAbstract(blockMapClass.getModifiers()), "blockMapClass is Abstract");
        try {
            this.blockMap = (Map<K, MemoryBlock>) blockMapClass.newInstance();
        }
        catch (InstantiationException | IllegalAccessException e) {
            throw new IllegalArgumentException("blockMapClass init failed", e);
        }
    }

    @Override
    public int size()
    {
        return blockMap.size();
    }

    @Override
    public boolean isEmpty()
    {
        return blockMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object key)
    {
        return blockMap.containsKey(key);
    }

    /**
     * Very expensive
     */
    @Override
    public boolean containsValue(Object value)
    {
        throw new UnsupportedOperationException("this method have't support!");
    }

    @Override
    public V get(Object inKey)
    {
        K key = (K) inKey;
        MemoryBlock memoryAddress = blockMap.get(key);
        if (memoryAddress == null) {
            return null;
        }
        byte[] bytes = memoryAddress.getByteValue();
        return deserialization.apply(bytes);
    }

    @Override
    public V put(K key, V value)
    {
        byte[] bytes = serialization.apply(value);
        try (MemoryBlock old = blockMap.put(key, new MemoryBlock(bytes))) {
            if (old != null) {
                return deserialization.apply(old.getByteValue());
            }
        }
        return null;
    }

    @Override
    public V remove(Object key)
    {
        try (MemoryBlock memoryBlock = blockMap.remove(key)) {
            if (memoryBlock != null) {
                return deserialization.apply(memoryBlock.getByteValue());
            }
        }
        return null;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> inMap)
    {
        requireNonNull(inMap, "inMap is null");
        for (Entry<? extends K, ? extends V> it : inMap.entrySet()) {
            this.put(it.getKey(), it.getValue());
        }
    }

    @Override
    public void clear()
    {
        synchronized (blockMap) {
            for (K k : blockMap.keySet()) {
                this.remove(k);
            }
        }
    }

    @Override
    public Set<K> keySet()
    {
        return blockMap.keySet();
    }

    /**
     * Very expensive
     */
    @Override
    public Collection<V> values()
    {
        return this.keySet()
                .stream()
                .map(this::get)
                .collect(Collectors.toList());
    }

    @Override
    public Set<Entry<K, V>> entrySet()
    {
        return blockMap.entrySet().stream().map(it -> new Entry<K, V>()
        {
            @Override
            public K getKey()
            {
                return it.getKey();
            }

            @Override
            public V getValue()
            {
                return deserialization.apply(it.getValue().getByteValue());
            }

            @Override
            public V setValue(V value)
            {
                throw new UnsupportedOperationException("this method have't support!");
            }
        }).collect(Collectors.toSet());
    }
}
