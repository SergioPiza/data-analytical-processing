const path = require("path");
const csv = require("fast-csv");
const fs = require("fs");
const bd = require("./connect");
const prompt = require("prompt");
const moment = require("moment"); // require
const { rawListeners } = require("process");
moment().format();

let rowCount = 0;

let tableExists = false;
(async () => {
	await bd
		.query(
			`SELECT EXISTS (
					SELECT FROM information_schema.tables 
					WHERE  table_schema = 'public'
					AND    table_name   = 'stock_indexes'
				)`
		)
		.then((r) => {
			tableExists = r.rows[0].exists;
		});

	let answer = false;
	if (tableExists) {
		prompt.start();

		console.log(
			"There is already a stock indexes table in your database. This script will delete the existing table and create and populate a new one."
		);
		// console.log(
		// 	" Y(es) or type anything else to cancel."
		// );

		const query = require("cli-interact").getYesNo;
		answer = query("Do you wish to continue?");
	} else {
		console.log("Creating table stock_indexes");
	}
	//answer = result.answer.toString().toUpperCase();

	if (answer || !tableExists) {
		const sql = fs.readFileSync("sql/stock_indexes.sql").toString();
		await bd
			.query(sql)
			.then(() => {
				tableExists = false; // a partir daqui essa variável perde seu sentido original e passa a ser uma flag para continuar o programa ou não
			})
			.catch((e) => {
				console.log(e);
			});
	}

	if (tableExists) return;

	console.log("Beginning parsing of dataset rows");
	fs.createReadStream("datasets/stock_indexes.csv")
		.pipe(csv.parse({ headers: true }))
		.on("error", (error) => console.error(error))
		.on("data", async (row) => {
			await bd
				.query(
					`INSERT INTO stock_indexes (codigo_acao_empresa, nome_empresa, setor_codigo_empresa, setor_empresa, segmento_empresa) VALUES ($1,                  $2,           $3,                   $4,            $5)`,
					[
						row.code.toUpperCase(),
						row.stock.toUpperCase(),
						row.segment_code.toUpperCase(),
						row.segment_area.toUpperCase(),
						row.segment_name.toUpperCase(),
					]
				)
				.catch((e) => console.log(e));

			rowCount++;
			if (rowCount % 100 == 0)
				console.log(`Inserted ${rowCount} rows...`);
		})
		.on("end", async (rowCount) => {
			console.log(`Added all stock indexes from file! (${rowCount})`);
			console.log("Beginning to export table to csv folder");

			await bd
				.query("SELECT * FROM stock_indexes ORDER BY id")
				.then((res) => {
					const jsonData = JSON.parse(JSON.stringify(res.rows));
					const ws = fs.createWriteStream("csv/stock_indexes.csv");
					csv.write(jsonData, { headers: true })
						.on("finish", function () {
							console.log(
								"Exported date description table to csv/stock_indexes.csv!"
							);
						})
						.pipe(ws);
				})
				.catch((e) => console.log(e));
		});
})();
