var mongodb = require("mongodb");
var csv = require("csv-parser");
var fs = require("fs");

var MongoClient = mongodb.MongoClient;
var mongoUrl = "mongodb://localhost:27017";
const dbName = "marvel";
const collectionName = "heroes";


const insertHeroes = (db, callback) => {
    const collection = db.collection('heroes');

    const heroes = [];
    fs.createReadStream('./all-heroes.csv')
        .pipe(csv())
        // Pour chaque ligne on créé un document JSON pour l'acteur correspondant
        .on('data', data => {
            heroes.push({
                name: data["name"],
                id: data["id"],
                aliases: stringToList(data["aliases"]),
                secret_identities: stringToList(data["secretIdentities"]),
                description: data["description"],
                partners:  stringToList(data["partners"]),
                universe: data["universe"],
                first_appearance: data["yearAppearance"],
                powers:  stringToList(data["powers"])

            });
        })
        // A la fin on créé l'ensemble des heroes dans MongoDB
        .on('end', () => {
            collection.insertMany(heroes, (err, result) => {
                callback(result);
            });
        });
}

MongoClient.connect(mongoUrl, (err, client) => {
    if (err) {
        console.error(err);
        throw err;
    }
    const db = client.db(dbName);
    insertHeroes(db, result => {
        console.log(`${result.insertedCount} heroes inserted`);
        client.close();
    });
});

function stringToList(st){
    if (st === "") return [];
    else {
        return st.split(",")
    }
}