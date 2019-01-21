var express = require("express");
var router = express.Router();
var services=require("../services/services.js");

router.post('/savefilestos3',services.saveFilesToS3);
router.get('/readjsonfroms3/:keyname',services.readJsonFromS3);
router.get('/readcsvasstream/:keyname',services.readCsvAsStream);


module.exports = router