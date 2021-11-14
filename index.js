const fs = require('fs')
const csv = require('csv-parser')
const randomWords = require('random-words')
const units = [];
function generateMarket(market) {
    return `${market}`.toLowerCase();
}
fs.createReadStream('code_challenge.csv')
  .pipe(csv())
  .on('data', function (row) {
    const Market = generateMarket(row.Market);
    const password = randomWords(3).join("-");
    
    const unit = {
        Market,
        Date: row.Date,
        AdUnitName: row.AdUnitName,
        AdUnitID: row.AdUnitID,
        Typetag: row.Typetag,
        RevenueSource: row.RevenueSource,
        Market: row.Market,
        Queries: row.Queries,
        Clicks: row.Clicks,
        Impressions: row.Impressions,
        PageRpm: row.AdUnitID,
        ImpressionRpm: row.ImpressionRpm,
        TrueRevenue: row.TrueRevenue,
        Coverage: row.Coverage,
        Ctr: row.Ctr,
        password
    }
    units.push(unit)
  })
  .on('end', function () {
      console.table(units)
      
// Importing mysql and csvtojson packages
// Requiring module
const csvtojson = require('csvtojson');
const mysql = require("mysql2");

// Database credentials
const hostname = "localhost",
	username = "root",
	password = "root",
	databsename = "csvtomysql"


// Establish connection to the database
let con = mysql.createConnection({
	host: hostname,
	user: username,
	password: password,
	database: databsename,
});

con.connect((err) => {
	if (err) return console.error(
			'error: ' + err.message);

	con.query("DROP TABLE units",
		(err, drop) => {

		// Query to create table "units"
		var createStatament =
		"CREATE TABLE units(Creation Date, " +
		"AdUnitName char(50), AdUnitID int,Typetag int, RevenueSource char(30),Market char(30),Queries int,Clicks int,Impressions int,PageRpm int,ImpressionRpm int,TrueRevenue int, Coverage int,Ctr int)"

		// Creating table "units"
		con.query(createStatament, (err, drop) => {
			if (err)
				console.log("ERROR: ", err);
		});
	});
});

// CSV file name
const fileName = "code_challenge.csv";

csvtojson().fromFile(fileName).then(source => {

	// Fetching the data from each row
	// and inserting to the table "units"
	for (var i = 0; i < source.length; i++) {
		var Date = source[i]["Date"],
			AdUnitName = source[i]["AdUnitName"],
			AdUnitID = source[i]["AdUnitID"],
			Typetag = source[i]["Typetag"],
      RevenueSource = source[i]["RevenueSource"],
      Market = source[i]["Market"],
      Queries = source[i]["Queries"],
      Clicks = source[i]["Clicks"],
      Impressions = source[i]["Impressions"],
      PageRpm = source[i]["PageRpm"],
      ImpressionRpm = source[i]["ImpressionRpm"],
      TrueRevenue = source[i]["TrueRevenue"],
      Coverage = source[i]["Coverage"],
      Ctr = source[i]["Ctr"],

		 insertStatement =`INSERT INTO units values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
 
    var items = [Date, AdUnitName, AdUnitID, Typetag,RevenueSource,Market,Queries,Clicks,Impressions,PageRpm,ImpressionRpm,TrueRevenue,Coverage,Ctr];

		// Inserting data of current row
		// into database
		con.query(insertStatement, items,
			(err, results, fields) => {
			if (err) {
				console.log(
	"Unable to insert item at row ", i + 1);
				return console.log(err);
			}
		});
	}
	console.log(
"All items stored into database successfully");
});

      
    })