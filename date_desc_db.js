const path = require("path");
const csv = require("fast-csv");
const fs = require("fs");
const bd = require("./connect");

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

fs.createReadStream("datasets/b3_stocks_1994_2020.csv")
	.pipe(csv.parse({ headers: true }))
	.on("error", (error) => console.error(error))
	.on("data", (row) => {
		rowCount++;
		if (rowCount % 250000 == 0) console.log(`Parsed ${rowCount} rows...`);
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
		maxDate = new Date(maxDate.getFullYear(), maxDate.getMonth() + 1, 0);

		console.log("Min Date: " + minDate.toString());
		console.log("Max Date: " + maxDate.toString());
		console.log(maxDate.getDate());

		for (
			let actualDate = new Date(minDate);
			actualDate <= maxDate;
			actualDate.setDate(actualDate.getDate() + 1)
		) {
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
		console.log("added all days from mindate to maxdate to database!");
	});
