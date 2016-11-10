package pl.edu.agh.utils;

import pl.edu.agh.messages.RaftMessage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public class MessageUtils {
    private static final int MAX_PARTS = 4;
    private static String partOfResult = "";
    private static int parts = 0;

    // Sometimes Netty sends messages in couple of separate packages...
    // Here I have to sometimes try to patch them up (packages of four at max)
    public static Object toObject(String s) {
        try {
            byte[] data = partOfResult.equals("") ? Base64.getDecoder().decode(s) : Base64.getDecoder().decode(partOfResult + s);
            ObjectInputStream ois;
            ois = new ObjectInputStream(
                    new ByteArrayInputStream(data));
            Object o = ois.readObject();
            ois.close();

            partOfResult = "";
            parts = 0;
            RaftMessage message = ((RaftMessage) o); // Let's try to cast it. That way we won't return 'magic' objects
            return o;
        } catch (Exception e) {
            partOfResult += s;
            parts++;
            if (parts > MAX_PARTS) {
                partOfResult = "";
                parts = 0;
            }
            return new Object();
        }
    }

    public static String toString(Object object) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(object);
            oos.close();
            return Base64.getEncoder().encodeToString(baos.toByteArray());
        } catch (Exception e) {
            return "";
        }
    }
}
