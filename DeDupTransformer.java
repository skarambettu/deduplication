import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class DeDupTransformer implements ValueTransformerWithKey<String, String, String>
{

    private final String storeName;
    private ProcessorContext context;

    private KeyValueStore<String, String> stateStore;


    @Override
    public void init(ProcessorContext context)
    {

        System.out.println("<<init");
        this.context = context;

        try
        {
            stateStore = (KeyValueStore<String, String>)this.context.getStateStore(this.storeName);
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        System.out.println(">>init");

    }

    public DeDupTransformer(final String storeName)
    {
        this.storeName = storeName;
    }


    public String transform(String key, String value)
    {
        String output = null;

        if (isDuplicate(key))
        {
            System.out.println("Its a duplicate :"+key);
            output = null;
        }
        else
        {
            System.out.println("Not a duplicate :"+key);
            output = value;
            rememberNewEvent(key);
        }

        return output;
    }

    private boolean isDuplicate(final String key)
    {
        System.out.println("<<" + key);
        String ifExists = stateStore.get(key);
        System.out.println("Value exists in the store :"+ifExists);
        return(null != ifExists);
    }


    private void rememberNewEvent(final String key)
    {
        System.out.println(".." + key);
        stateStore.putIfAbsent(key, key);
    }

    @Override
    public void close()
    {
        // Note: The store should NOT be closed manually here via `eventIdStore.close()`!
        // The Kafka Streams API will automatically close stores when necessary.
    }

}
