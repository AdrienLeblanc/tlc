package tlc.tracking;

import com.google.cloud.datastore.*;
import com.google.cloud.datastore.StructuredQuery.CompositeFilter;
import com.google.cloud.datastore.StructuredQuery.PropertyFilter;
import org.restlet.data.Form;
import org.restlet.data.Parameter;
import org.restlet.resource.Delete;
import org.restlet.resource.Get;
import org.restlet.resource.Post;
import org.restlet.resource.ServerResource;

import java.lang.reflect.Array;
import java.util.LinkedList;
import java.util.List;

import static java.lang.Double.parseDouble;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;

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
    @SuppressWarnings("all")
    private static <T> T[] batch(Class<T> c, List<T> g) {
        @SuppressWarnings("unchecked")
        T[] res = (T[]) Array.newInstance(c, g.size());
        g.toArray(res);
        return res;
    }

    @Post("json")
    @SuppressWarnings("all")
    public void bulkAdd(RecordList toAdd) {
        /*
         * Doc that might help you:
         * https://cloud.google.com/datastore/docs/concepts/entities#creating_an_entity
         */

        for (Record r : toAdd) {
            Key recKey = datastore.allocateId(recordsKey.newKey());
            Entity record = Entity
                    .newBuilder(recKey)
                    .set("id", r.id)
                    .set("user", r.user)
                    .set("lat", r.lat)
                    .set("lon", r.lon)
                    .set("timestamp", r.timestamp)
                    .build();
            datastore.put(record);
            System.out.println(r.toString() + " ajouté.");
        }
    }

    @Get("json")
    @SuppressWarnings("all")
    public RecordList search() {
        /*
         * Doc that might help you:
         * https://cloud.google.com/datastore/docs/concepts/queries#composite_filters
         * https://cloud.google.com/datastore/docs/concepts/indexes#index_configuration
         * Check also src/main/webapp/WEB-INF/datastore-indexes.xml
         */

        // URL parameters
        Form form = getRequest().getResourceRef().getQueryAsForm();

        List<PropertyFilter> filters = new LinkedList<>();
        for (Parameter parameter : form) {
            switch (parameter.getName()) {
                case "id":
                    if (parameter.getValue().contains(",")) {
                        String[] bounds = parameter.getValue().split(",");
                        filters.add(PropertyFilter.ge(parameter.getName(), parseLong(bounds[0])));
                        filters.add(PropertyFilter.le(parameter.getName(), parseLong(bounds[1])));
                    } else {
                        filters.add(PropertyFilter.eq(parameter.getName(), parseLong(parameter.getValue())));
                    }
                    break;
                case "user":
                    filters.add(PropertyFilter.eq(parameter.getName(), parameter.getValue()));
                case "lat":
                    if (parameter.getValue().contains(",")) {
                        String[] bounds = parameter.getValue().split(",");
                        filters.add(PropertyFilter.ge(parameter.getName(), parseDouble(bounds[0])));
                        filters.add(PropertyFilter.le(parameter.getName(), parseDouble(bounds[1])));
                    } else {
                        filters.add(PropertyFilter.eq(parameter.getName(), parseDouble(parameter.getValue())));
                    }
                    break;
                case "lon":
                    if (parameter.getValue().contains(",")) {
                        String[] bounds = parameter.getValue().split(",");
                        filters.add(PropertyFilter.ge(parameter.getName(), parseDouble(bounds[0])));
                        filters.add(PropertyFilter.le(parameter.getName(), parseDouble(bounds[1])));
                    } else {
                        filters.add(PropertyFilter.eq(parameter.getName(), parseDouble(parameter.getValue())));
                    }
                    break;
                case "timestamp":
                    if (parameter.getValue().contains(",")) {
                        String[] bounds = parameter.getValue().split(",");
                        filters.add(PropertyFilter.ge(parameter.getName(), parseLong(bounds[0])));
                        filters.add(PropertyFilter.le(parameter.getName(), parseLong(bounds[1])));
                    } else {
                        filters.add(PropertyFilter.eq(parameter.getName(), parseLong(parameter.getValue())));
                    }
                    break;
            }
        }

        Query<Entity> query;
        if (!filters.isEmpty()) {
            if (filters.size() == 1) {
                query = Query.newEntityQueryBuilder()
                        .setKind("record")
                        .setFilter(filters.get(0))
                        .build();
            } else {
                query = Query.newEntityQueryBuilder()
                        .setKind("record")
                        .setFilter(CompositeFilter.and(
                                filters.get(0),
                                batch(PropertyFilter.class, filters.subList(1, filters.size() - 1))))
                        .build();
            }
        } else {
            query = Query.newEntityQueryBuilder()
                    .setKind("record")
                    .build();
        }

        RecordList res = new RecordList();
        QueryResults<Entity> results = datastore.run(query);
        while (results.hasNext()) {
            Entity entity = results.next();
            Record record = new Record();
            record.id = (int) entity.getLong("id");
            record.user = entity.getString("user");
            record.lat = entity.getDouble("lat");
            record.lon = entity.getDouble("lon");
            record.timestamp = entity.getLong("timestamp");
            res.add(record);
            System.out.println(record.toString() + " renvoyé.");
        }

        return res;
    }

    @Delete("json")
    @SuppressWarnings("all")
    public void bulkDelete() {
        /*
         * Doc that might help you:
         * https://cloud.google.com/datastore/docs/concepts/entities#deleting_an_entity
         * You might to do one or more query before to get some keys...
         */

        Query<Entity> query;

        String[] run_ids = getRequest().getAttributes().get("list").toString().split(",");
        for (String r : run_ids) {
            query = Query.newEntityQueryBuilder()
                    .setKind("record")
                    .setFilter(CompositeFilter.and(PropertyFilter.eq("id", parseInt(r))))
                    .build();
            QueryResults<Entity> results = datastore.run(query);
            while (results.hasNext()) {
                Entity entity = results.next();
                datastore.delete(entity.getKey());

                Record record = new Record();
                record.id = (int) entity.getLong("id");
                record.user = entity.getString("user");
                record.lat = entity.getDouble("lat");
                record.lon = entity.getDouble("lon");
                record.timestamp = entity.getLong("timestamp");
                System.out.println(record.toString() + " supprimé.");
            }
        }
    }
}
