const { Pool } = require("pg");
require("dotenv").config();

const access = {
	user: process.env.BD_USER,
	host: process.env.BD_HOST,
	database: process.env.BD_DATABASE,
	password: process.env.BD_PASSWORD,
	port: process.env.BD_PORT,
	//ssl: { rejectUnauthorized: false },
	ssl: process.env.BD_SSL || false,
};
const connection = new Pool(access);
module.exports = connection;
