package handler;

import org.apache.beam.sdk.nexmark.model.Event;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class EventCoder {
    public EventCoder() {

    }
    public static void encode(Event data, OutputStream o){
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            Event.CODER.encode(data, bos);
            System.out.println(bos.toByteArray());
            o.write(bos.toByteArray());
            bos.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
    public static Event decode(InputStream in){
        final Event result;
        try {
            result = Event.CODER.decode(in);
            System.out.println("INFO: "+result.toString());
            return result;
        } catch(Exception e) {
            e.printStackTrace();
            System.out.println("Decode failed");
            throw new RuntimeException(e);
        }
    }
}
