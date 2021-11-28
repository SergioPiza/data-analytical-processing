const path = require("path");
const csv = require("fast-csv");
const fs = require("fs");
const bd = require("./connect");
const prompt = require("prompt");
const moment = require("moment"); // require
const { rawListeners } = require("process");
moment().format();

let rowCount = 0;
let rowCountInserted = 0;

let tableExists = false;
(async () => {
	await bd
		.query(
			`SELECT EXISTS (
					SELECT FROM information_schema.tables 
					WHERE  table_schema = 'public'
					AND    table_name   = 'acao'
				)`
		)
		.then((r) => {
			tableExists = r.rows[0].exists;
		});

	let answer = false;
	if (tableExists) {
		prompt.start();

		console.log(
			"There is already a acao table in your database. This script will delete the existing table and create and populate a new one."
		);
		// console.log(
		// 	" Y(es) or type anything else to cancel."
		// );

		const query = require("cli-interact").getYesNo;
		answer = query("Do you wish to continue?");
	} else {
		console.log("Creating table acao");
	}
	//answer = result.answer.toString().toUpperCase();

	if (answer || !tableExists) {
		const sql = fs.readFileSync("sql/acao.sql").toString();
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

	console.log("Reading stock_indexes.csv");
	let stockIndexes = [];
	fs.createReadStream("csv/stock_indexes.csv")
		.pipe(csv.parse({ headers: true }))
		.on("error", (error) => console.error(error))
		.on("data", (row) => {
			stockIndexes[row.codigo_acao_empresa] = row.id;
		});

	console.log("Reading date_desc.csv");
	let dateDesc = [];
	fs.createReadStream("csv/date_desc.csv")
		.pipe(csv.parse({ headers: true }))
		.on("error", (error) => console.error(error))
		.on("data", (row) => {
			dateDesc[row.data] = row.id;
		});

	console.log("Beginning parsing of dataset rows");
	fs.createReadStream("datasets/b3_stocks_1994_2020.csv")
		.pipe(csv.parse({ headers: true }))
		.on("error", (error) => console.error(error))
		.on("data", async (row) => {
			// console.log(row);
			const chaveTempo = dateDesc[row.datetime];
			const chaveEmpresa = stockIndexes[row.ticker];

			// console.log(chaveEmpresa);
			if (chaveTempo && chaveEmpresa) {
				await bd
					.query(
						`INSERT INTO acao (chave_tempo, chave_empresa, abertura_acao, fechamento_acao, pico_acao, baixa_acao, volume_acao) 
							   VALUES ($1,			$2,			   $3,	  	      $4,			   $5, 		  $6, 		  $7)`,
						[
							chaveTempo,
							chaveEmpresa,
							row.open,
							row.close,
							row.high,
							row.low,
							row.volume,
						]
					)
					.catch((e) => console.log(e));
				rowCountInserted++;
			}

			rowCount++;

			if (rowCount % 25000 == 0)
				console.log(`Parsed ${rowCount} rows...`);
			if (rowCountInserted % 25000 == 0 && rowCountInserted > 0)
				console.log(`Inserted ${rowCountInserted} rows...`);

			// delete chaveTempo
			// delete chaveEmpresa
			// delete rowDate
			// delete row
		})
		.on("end", async (rowCount) => {
			console.log(`Added all acoes from file! (${rowCount})`);
			console.log(
				`(${rowCount - rowCountInserted}) rows were not inserted`
			);
			console.log("Beginning to export table to csv folder");

			await bd
				.query("SELECT * FROM acao ORDER BY id")
				.then((res) => {
					const jsonData = JSON.parse(JSON.stringify(res.rows));
					const ws = fs.createWriteStream("csv/acao.csv");
					csv.write(jsonData, { headers: true })
						.on("finish", function () {
							console.log(
								"Exported date description table to csv/acao.csv!"
							);
						})
						.pipe(ws);
				})
				.catch((e) => console.log(e));
		});
})();
