const csv = require('csv-parser');
const fs = require('fs');

const { Client } = require('@elastic/elasticsearch')
const esClient = new Client({ node: 'http://localhost:9200' })
const heroesIndexName = 'heroes'


async function run () {

    // Création de l'indice
    esClient.indices.create({ index: heroesIndexName,
        body: {
            mappings: {
                properties: {
                    suggest: {"type": "completion"}
                }
            }
        }

    }, (err, resp) => {
        if (err) console.trace(err.message);
    });



    let heroes = [];
    // Read CSV file
    const BULK_SIZE = 20000;
    fs.createReadStream('all-heroes.csv')
        .pipe(csv({
            separator: ','
        }))
        .on('data', (data) => {
            heroes.push({
                name: data["name"],
                id: data["id"],
                gender: data["gender"],
                imageUrl: data["imageUrl"],
                aliases: stringToList(data["aliases"]),
                secret_identities: stringToList(data["secretIdentities"]),
                description: data["description"],
                partners:  stringToList(data["partners"]),
                universe: data["universe"],
                first_appearance: data["yearAppearance"],
                powers:  stringToList(data["powers"]),
                suggest: [
                    {
                        input: data.name,
                        weight: 10
                    },
                    {
                        input: data.aliases,
                        weight: 5
                    },
                    {
                        input: data.secret_identities,
                        weight: 5
                    }
                ]

            });
            if (heroes.length >= BULK_SIZE) {
                let heroes_full = heroes;
                heroes = [];
                esClient.bulk(createBulkInsertQuery(heroes_full), (err, resp) => {
                    if (err) console.error(err);
                    else {
                        // console.log(resp)
                        console.log(`Inserted ${resp.body.items.length} events`);
                    }
                });
            }
            // console.log(data);
        })
        .on('end', () => {

            esClient.bulk(createBulkInsertQuery(heroes), (err, resp) => {
                if (err) console.error(err);
                else console.log(`Inserted ${resp.body.items.length} events`);
                esClient.close();
                console.log('Terminated!');
            });
        });
}

// Fonction utilitaire permettant de formatter les données pour l'insertion "bulk" dans elastic
function createBulkInsertQuery(heroes) {
    const body = heroes.reduce((acc, hero) => {
        const { name, id, aliases, secret_identities, description,
            partners, universe, first_appearance, powers, gender, imageUrl } = hero;
        acc.push({ index: { _index: heroesIndexName, _type: '_doc', _id: hero.object_id } })
        acc.push({ name, id, aliases, secret_identities, description,
            partners, universe, first_appearance, powers, gender, imageUrl })
        return acc
    }, []);

    return { body };
}

function stringToList(st){
    if (st === "") return [];
    else {
        return st.split(",")
    }
}

run().catch(console.error);
