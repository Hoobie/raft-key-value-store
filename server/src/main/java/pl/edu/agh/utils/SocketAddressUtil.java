package pl.edu.agh.utils;

import org.apache.commons.lang3.tuple.Pair;

public class SocketAddressUtil {
    public static Pair<String, Integer> splitHostAndPort(String address) {
        String[] hostAndPort = address.split(":");
        return Pair.of(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
    }
}
