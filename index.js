const { DATABASE_SCHEMA, DATABASE_URL, SHOW_PG_MONITOR } = require('./config');
const massive = require('massive');
const monitor = require('pg-monitor');
const axios = require('axios').default;

// Call start
(async () => {
    console.log('main.js: before start');

    const db = await massive({
        connectionString: DATABASE_URL,
        ssl: false,
    }, {
        // Massive Configuration
        scripts: process.cwd() + '/migration',
        allowedSchemas: [DATABASE_SCHEMA],
        whitelist: [`${DATABASE_SCHEMA}.%`],
        excludeFunctions: true,
    }, {
        // Driver Configuration
        noWarnings: true,
        error: function (err, client) {
            console.log(err);
            //process.emit('uncaughtException', err);
            //throw err;
        }
    });

    if (!monitor.isAttached() && SHOW_PG_MONITOR === 'true') {
        monitor.attach(db.driverConfig);
    }

    const execFileSql = async (schema, type) => {
        return new Promise(async resolve => {
            const objects = db['user'][type];

            if (objects) {
                for (const [key, func] of Object.entries(objects)) {
                    console.log(`executing ${schema} ${type} ${key}...`);
                    await func({
                        schema: DATABASE_SCHEMA,
                    });
                }
            }

            resolve();
        });
    };

    //public
    const migrationUp = async () => {
        return new Promise(async resolve => {
            await execFileSql(DATABASE_SCHEMA, 'schema');

            //cria as estruturas necessarias no db (schema)
            await execFileSql(DATABASE_SCHEMA, 'table');
            await execFileSql(DATABASE_SCHEMA, 'view');

            console.log(`reload schemas ...`)
            await db.reload();

            resolve();
        });
    };

    try {
        await migrationUp();
        //1. Consumir a API (https://datausa.io/api/data?drilldowns=Nation&measures=Population) e gravar o resultado na tabela "api_data" no na coluna "doc_record".
        // Saida da API:
        // {"data":[{"ID Nation":"01000US","Nation":"United States","ID Year":2020,"Year":"2020","Population":326569308,"Slug Nation":"united-states"},...
        const URL_API = "https://datausa.io/api/data?drilldowns=Nation&measures=Population";

        const { data } = await axios.get(URL_API);

        await db[DATABASE_SCHEMA].api_data.insert({
            doc_record: JSON.stringify(data),
        });

        const result = await db[DATABASE_SCHEMA].api_data.find({
            is_active: true
        });

        // a. em memoria no nodejs usando map, filter, for etc
        const sumPopulation = result[0].doc_record.data.reduce((acc, curr) => {
            if (curr.Year === "2020" || curr.Year === "2019" || curr.Year === "2018") {
            acc += curr.Population;
            }
            return acc;
        }, 0);



    } catch (e) {
        console.log(e.message)
    } finally {
        console.log('finally');
    }
    console.log('main.js: after start');
})();