var express = require("express");
var bodyParser = require("body-parser");
var routes = require("./routes/routes.js");
var cors = require('cors');

var app = express();
app.use(cors())

app.use(bodyParser.json({limit: '50mb', type:'application/json'}));
app.use(bodyParser.urlencoded({ extended: true }));

app.use('/api',routes);

const port = process.env.PORT || 3001;
var server = app.listen(port, function () {
    server.setTimeout(600000);
    console.log("app running on port.", server.address().port);
});
