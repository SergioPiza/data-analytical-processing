const path = require("path");
const csv = require("fast-csv");
const fs = require("fs");
const bd = require("./connect");
const prompt = require("prompt");
const moment = require("moment"); // require
moment().format();

let minDate = new Date("01/01/2030");
let maxDate = new Date("01/01/1900");
let rowCount = 0;

const months = [
	"JANEIRO",
	"FEVEREIRO",
	"MARCO",
	"ABRIL",
	"MAIO",
	"JUNHO",
	"JULHO",
	"AGOSTO",
	"SETEMBRO",
	"OUTUBRO",
	"NOVEMBRO",
	"DEZEMBRO",
];
const weekdays = [
	"SEGUNDA",
	"TERCA",
	"QUARTA",
	"QUINTA",
	"SEXTA",
	"SABADO",
	"DOMINGO",
];

let tableExists = false;
(async () => {
	await bd
		.query(
			`SELECT EXISTS (
					SELECT FROM information_schema.tables 
					WHERE  table_schema = 'public'
					AND    table_name   = 'date_desc'
				)`
		)
		.then((r) => {
			tableExists = r.rows[0].exists;
		});

	if (tableExists) {
		prompt.start();

		console.log(
			"There is already a date description table in your database. This script will delete the existing table and create and populate a new one."
		);
		// console.log(
		// 	" Y(es) or type anything else to cancel."
		// );

		const query = require("cli-interact").getYesNo;
		let answer = query("Do you wish to continue?");
		//answer = result.answer.toString().toUpperCase();
		if (answer) {
			const sql = fs.readFileSync("sql/date_desc.sql").toString();
			await bd
				.query(sql)
				.then(() => {
					tableExists = false; // a partir daqui essa variável perde seu sentido original e passa a ser uma flag para continuar o programa ou não
				})
				.catch((e) => {
					console.log(e);
				});
		}
	}

	if (tableExists) return;

	console.log("Beginning parsing of dataset rows");
	fs.createReadStream("datasets/b3_stocks_1994_2020.csv")
		.pipe(csv.parse({ headers: true }))
		.on("error", (error) => console.error(error))
		.on("data", (row) => {
			rowCount++;
			if (rowCount % 250000 == 0)
				console.log(`Parsed ${rowCount} rows...`);
			const rowDate = new Date(row.datetime + " ");
			if (rowDate < minDate) minDate.setTime(rowDate.getTime());
			if (rowDate > maxDate) maxDate.setTime(rowDate.getTime());
		})
		.on("end", async (rowCount) => {
			console.log("Min Date: " + minDate.toString());
			console.log("Max Date: " + maxDate.toString());
			console.log(`Parsed ${rowCount} rows`);

			console.log(
				"Setting mindate to beginning of its semester and maxdate to end of its semester"
			);
			minDate.setMonth(minDate.getMonth() > 5 ? 6 : 0);
			maxDate.setMonth(maxDate.getMonth() > 5 ? 11 : 5);
			minDate.setDate(1);
			maxDate = new Date(
				maxDate.getFullYear(),
				maxDate.getMonth() + 1,
				0
			);

			console.log("Min Date: " + minDate.toString());
			console.log("Max Date: " + maxDate.toString());
			let totalDays = moment(maxDate);
			totalDays = totalDays.diff(moment(minDate), "days");
			let lastPercentage = 0;
			let daysAdded = 0;
			for (
				let actualDate = new Date(minDate);
				actualDate <= maxDate;
				actualDate.setDate(actualDate.getDate() + 1)
			) {
				daysAdded++;
				if (lastPercentage % 10 == 0) {
					console.log(`Added ${lastPercentage}% of all dates...`);
					lastPercentage++;
				}

				let actualPercentage = Math.floor(
					(daysAdded * 100) / totalDays
				);
				if (
					actualPercentage % 10 == 0 &&
					actualPercentage > lastPercentage
				)
					lastPercentage = actualPercentage;

				await bd
					.query(
						`INSERT INTO date_desc (data, dia, mes, ano, mes_desc, mes_desc_full,
                bimestre, trimestre, semestre, dia_semana, dia_semana_desc, dia_semana_desc_full) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
						[
							actualDate,
							actualDate.getDate(),
							actualDate.getMonth() + 1,
							actualDate.getFullYear(),
							months[actualDate.getMonth()].substring(0, 3),
							months[actualDate.getMonth()],
							Math.ceil((actualDate.getMonth() + 1) / 2),
							Math.ceil((actualDate.getMonth() + 1) / 3),
							Math.ceil((actualDate.getMonth() + 1) / 6),
							actualDate.getDay() + 1,
							weekdays[actualDate.getDay()].substring(0, 3),
							weekdays[actualDate.getDay()],
						]
					)
					.catch((e) => console.log(e));
			}
			console.log("Added all days from mindate to maxdate to database!");
			console.log("Beginning to export table to csv folder");

			await bd
				.query("SELECT * FROM date_desc ORDER BY id")
				.then((res) => {
					const jsonData = JSON.parse(JSON.stringify(res.rows));
					const ws = fs.createWriteStream("csv/date_desc.csv");
					csv.write(jsonData, { headers: true })
						.on("finish", function () {
							console.log(
								"Exported date description table to csv/date_desc.csv!"
							);
						})
						.pipe(ws);
				})
				.catch((e) => console.log(e));
		});
})();
