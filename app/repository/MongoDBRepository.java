package repository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import models.Hero;
import models.ItemCount;
import models.YearAndUniverseStat;
import org.bson.Document;
import play.libs.Json;
import utils.ReactiveStreamsUtils;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Singleton
public class MongoDBRepository {

    private final MongoCollection<Document> heroesCollection;

    @Inject
    public MongoDBRepository(MongoDatabase mongoDatabase) {
        this.heroesCollection = mongoDatabase.getCollection("heroes");
    }


    public CompletionStage<Optional<Hero>> heroById(String heroId) {
         String query = "{'id': '"+ heroId + "'}";
         Document document = Document.parse(query);
         return ReactiveStreamsUtils.fromSinglePublisher(heroesCollection.find(document).first())
                 .thenApply(result -> Optional.ofNullable(result).map(Document::toJson).map(Hero::fromJson));
    }

    public CompletionStage<List<YearAndUniverseStat>> countByYearAndUniverse() {
        String group1 =  "{ $group:"+
           "{ _id:"+
              "{ yearAppearance: \"$identity.yearAppearance\", universe: \"$identity.universe\" },"+
             "count: { \"$sum\":1 }"+
            "}"+
        "}";
        String group2 = "{ $group:"+
          "{ _id:"+
              "\"$_id.yearAppearance\","+
              "byUniverse:"+
              "{"+
                  "$push: { universe: \"$_id.universe\", count: \"$count\" }"+
              "}"+
          "}"+
        "}";
        String sort = "{ $sort: { \"_id\" : 1 } }";
        String noEmptyValues = "{ $match : { \"identity.yearAppearance\" : { $ne : \"\" } } }";

        List<Document> pipeline = new ArrayList<Document>();
        pipeline.add(Document.parse(noEmptyValues));
        pipeline.add(Document.parse(group1));
        pipeline.add(Document.parse(group2));
        pipeline.add(Document.parse(sort));
        return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(pipeline))
                .thenApply(documents -> {
                    return documents.stream()
                                    .map(Document::toJson)
                                    .map(Json::parse)
                                    .map(jsonNode -> {
                                        int year = jsonNode.findPath("_id").asInt();
                                        ArrayNode byUniverseNode = (ArrayNode) jsonNode.findPath("byUniverse");
                                        Iterator<JsonNode> elements = byUniverseNode.elements();
                                        Iterable<JsonNode> iterable = () -> elements;
                                        List<ItemCount> byUniverse = StreamSupport.stream(iterable.spliterator(), false)
                                                .map(node -> new ItemCount(node.findPath("universe").asText(), node.findPath("count").asInt()))
                                                .collect(Collectors.toList());
                                        return new YearAndUniverseStat(year, byUniverse);

                                    })
                                    .collect(Collectors.toList());
                });
    }


    public CompletionStage<List<ItemCount>> topPowers(int top) {
         return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(Arrays.asList(
                 Aggregates.unwind("$powers"), Aggregates.sortByCount("$powers"), Aggregates.limit(top))))
                 .thenApply(documents -> {
                     return documents.stream()
                             .map(Document::toJson)
                             .map(Json::parse)
                             .map(jsonNode -> {
                                 return new ItemCount(jsonNode.findPath("_id").asText(), jsonNode.findPath("count").asInt());
                             })
                             .collect(Collectors.toList());
                 });
    }

    public CompletionStage<List<ItemCount>> byUniverse() {
         return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(Arrays.asList(
         Aggregates.group("$identity.universe", Accumulators.sum("count", 1)))))
                 .thenApply(documents -> {
                     return documents.stream()
                             .map(Document::toJson)
                             .map(Json::parse)
                             .map(jsonNode -> {
                                 System.out.println(jsonNode.findPath("_id").asText());
                                 System.out.println(jsonNode.findPath("count").asInt());
                                 return new ItemCount(jsonNode.findPath("_id").asText(), jsonNode.findPath("count").asInt());
                             })
                             .collect(Collectors.toList());
                 });
    }

}
