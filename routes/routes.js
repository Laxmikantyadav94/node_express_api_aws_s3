var express = require("express");
var router = express.Router();
var services=require("../services/services.js");


router.get('/lists3buckets',services.listS3Buckets);
router.get('/creates3bucket/:bucketname',services.createS3Bucket);
router.get('/creates3subfolder',services.createS3SubFolder);
router.get('/checks3subfolderexist',services.checkS3SubFolderExist);
router.get('/gets3bucketcors/:bucketname',services.getS3BucketCors);
router.post('/sets3bucketcors',services.setS3BucketCors);
router.get('/gets3bucketacl/:bucketname',services.getS3BucketAcl);
router.post('/savefilestos3',services.saveFilesToS3);
router.post('/createfilewithpublicreadacl',services.createFileWithPublicReadAcl);
router.get('/getbucketpolicy',services.getBucketPolicy);
router.get('/setreadonlyanonuserbucketpolicy',services.setReadOnlyAnonUserBucketPolicy);
router.get('/deletebucketpolicy',services.deleteBucketPolicy);
router.post('/savefilestos3subfolder',services.saveFilesToS3SubFolder);
router.get('/readjsonfroms3/:keyname',services.readJsonFromS3);
router.get('/readcsvasstream/:keyname',services.readCsvAsStream);
router.get('/getobjectsfrombucket',services.getObjectsFromBucket);
router.delete('/deletes3bucket/:bucketname',services.deleteS3Bucket);

module.exports = router