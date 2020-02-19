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
                id: data["id"],
                name: data["name"],
                imageUrl: data["imageUrl"],
                backgroundImageUrl: data["backgroundImageUrl"],
                externalLink: data["externalLink"],
                description: data["description"],
                identity: {
                    secretIdentities: stringToList(data["secretIdentities"]),
                    birthPlace: data["birthPlace"],
                    occupation: data["occupation"],
                    aliases: stringToList(data["aliases"]),
                    alignment: data["alignment"],
                    firstAppearance: data["firstAppearance"],
                    yearAppearance: data["yearAppearance"],
                    universe: data["universe"],
                },
                appearance: {
                    gender: data["gender"],
                    type: data["type"],
                    race: data["race"],
                    height: data["height"],
                    weight: data["weight"],
                    eyeColor: data["eyeColor"],
                    hairColor: data["hairColor"],
                },

                teams: stringToList(data["teams"]),
                powers:  stringToList(data["powers"]),
                partners:  stringToList(data["partners"]),
                skills : {
                    intelligence: optinalStringToInt(data["intelligence"]),
                    strength: optinalStringToInt(data["strength"]),
                    speed: optinalStringToInt(data["speed"]),
                    durability: optinalStringToInt(data["durability"]),
                    combat: optinalStringToInt(data["combat"]),
                    power: optinalStringToInt(data["power"]),
                },
                creators: stringToList(data["creators"])
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

function optinalStringToInt(st) {
    if (st && (st !== "")){
        return parseInt(st)
    } else {
        return ""
    }
}