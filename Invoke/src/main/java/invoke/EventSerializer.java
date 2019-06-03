package invoke;

import org.apache.beam.sdk.nexmark.model.Event;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public final class EventSerializer {

    public static Event deserialize(InputStream in) {
       final Event result;
       try {
           result = Event.CODER.decode(in);
           return result;
       } catch(Exception e) {
           System.out.println("Decode failed");
       } finally {
           return null;
       }
    }

    public static byte[] serialize(Event data) {
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            Event.CODER.encode(data, bos);
            bos.close();
            return bos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public void close() {

    }
}
