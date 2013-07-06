package se.kth.ik223x;

public interface HiveDataProvider<K, D> {

    /**
     * Obtains a data based on a specific key. Key
     * maybe unique identifier, types, random characters, etc.
     *
     * @param key
     * @param data
     */
    void obtain(K key, D data);
}
