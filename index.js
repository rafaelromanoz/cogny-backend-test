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

        const { data: { data } } = await axios.get(URL_API);

        await db[DATABASE_SCHEMA].api_data.insert({
            doc_record: JSON.stringify(data)
        });

        const result = await db[DATABASE_SCHEMA].api_data.find({},{fields: ['doc_record']});

        // a. em memoria no nodejs usando map, filter, for etc
        const sumPopulation = result.map(({doc_record}) => doc_record).reduce((acc, curr) => {
            if (curr[0].Year === "2020" || curr[0].Year === "2019" || curr[0].Year === "2018") {
            acc += curr[0].Population;
            }
            return acc;
        }, 0);

        console.log('SOMA DA POPULAÇÃO', sumPopulation);
        //  b. usando SELECT no postgres, pode fazer um SELECT inline no nodejs.
        const query = `SELECT SUM((doc_record[0]->>'Population')::numeric) as total_population
                       FROM rafaelromanoz.api_data WHERE doc_record[0]->>'Year' IN ('2020', '2019', '2018')` ;
        const populationSumFromPostgres = await db.query(query);

        console.log('USANDO POSTGRES INLINE QUERY', parseInt(populationSumFromPostgres[0].total_population));


        //c. usando SELECT no postgres, pode fazer uma VIEW no banco de dados.

        const createView = `CREATE OR REPLACE VIEW viewTotalPopulation AS SELECT SUM((doc_record[0]->>'Population')::numeric) as total_population
                       FROM rafaelromanoz.api_data WHERE doc_record[0]->>'Year' IN ('2020', '2019', '2018')` ;

        await db.query(createView);

        const selectedFromView = await db.query('SELECT * FROM viewTotalPopulation');

        console.log('USANDO VIEW DO POSTGRES', parseInt(selectedFromView[0].total_population));


    } catch (e) {
        console.log(e.message)
    } finally {
        console.log('finally');
    }
    console.log('main.js: after start');
})();