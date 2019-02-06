package tlc.tracking;

import com.google.cloud.datastore.*;
import com.google.cloud.datastore.StructuredQuery.*;
import org.restlet.data.Form;
import org.restlet.data.Parameter;
import org.restlet.resource.Delete;
import org.restlet.resource.Get;
import org.restlet.resource.Post;
import org.restlet.resource.ServerResource;

import java.lang.reflect.Array;
import java.util.List;

/*
 * Get a token to test your application locally with datastore:
 * https://cloud.google.com/docs/authentication/production
 *
 *   1. Create a new service account with role owner https://console.cloud.google.com/apis/credentials/serviceaccountkey
 *   2. Download the key, we will refer to the path to the key as [PATH]
 *   3. Run: export GOOGLE_APPLICATION_CREDENTIALS="[PATH]"
 *   4. Run in the same terminal (not in your IDE): mvn appengine:run
 *
 *   Note: The id property is not the key, it represents the Run identifier. So, many entities can have the same id
 *   It just means that they are part of the same run.
 */


public class RunResource extends ServerResource {

    private Datastore datastore;
    private KeyFactory recordsKey;

    public RunResource() {
        datastore = DatastoreOptions.getDefaultInstance().getService();
        recordsKey = datastore.newKeyFactory().setKind("record");
    }

    /*
     * Enable you to convert a List object in an Array
     * It will help you pass lists to variadic functions
     *
     * In an alternative world where datastore.put() accepts Strings instead of Entities, we could write:
     * List<String> places = new ArrayList<>();
     * places.add("Buenos Aires");
     * places.add("Córdoba");
     * places.add("La Plata");
     * String[] placesArr = batch(String.class, places);
     * datastore.put(placesArr);
     *
     * You might need this function to do:
     *   1. batch operations with datastore.put()
     *   2. batch operations with datastore.delete()
     *   3. dynamically build CompositeFilter.and() (you must add some logic however,
     *      as "and" takes a fixed parameters before its vararg parameter - be clever :D)
     */
    private static <T> T[] batch(Class<T> c,  List<T> g) {
        @SuppressWarnings("unchecked")
        T[] res = (T[]) Array.newInstance(c, g.size());
        g.toArray(res);
        return res;
    }

    @Post("json")
    public void bulkAdd(RecordList toAdd) {
        /*
         * Doc that might help you:
         * https://cloud.google.com/datastore/docs/concepts/entities#creating_an_entity
         */

        for(Record r : toAdd) System.out.println(r);
        Key recKey = datastore.allocateId(recordsKey.newKey());
        Entity record = Entity
          .newBuilder(recKey)
          .set("hello","world")
          .set("foo","bar")
          .build();
        datastore.put(record);
        //@FIXME You must add these Records in Google Datastore
    }

    @Get("json")
    public RecordList search() {
        /*
         * Doc that might help you:
         * https://cloud.google.com/datastore/docs/concepts/queries#composite_filters
         * https://cloud.google.com/datastore/docs/concepts/indexes#index_configuration
         * Check also src/main/webapp/WEB-INF/datastore-indexes.xml
         */

        // Read and print URL parameters
        Form form = getRequest().getResourceRef().getQueryAsForm();
        for (Parameter parameter : form) {
            System.out.print("parameter " + parameter.getName());
            System.out.println(" -> " + parameter.getValue());
        }

        // Build a dummy result
        RecordList res = new RecordList();
        res.add(new Record(5, 43.8, 12.6, "lea", 154789));
        res.add(new Record(5, 43.8, 12.6, "john", 154789));

        //@FIXME You must query Google Datastore to retrieve the records instead of sending dummy results
        //@FIXME Don't forget to apply potential filters got from the URL parameters

        return res;
    }

    @Delete("json")
    public void bulkDelete() {
        /*
         * Doc that might help you:
         * https://cloud.google.com/datastore/docs/concepts/entities#deleting_an_entity
         * You might to do one or more query before to get some keys...
         */

        String[] run_ids = getRequest().getAttributes().get("list").toString().split(",");
        for (String r : run_ids)
          System.out.println("To delete: "+r);
        //@FIXME You must delete every records that contain one of the run_id in run_ids
    }
}
