var aws = require('aws-sdk'); 
var appSetting = require('../staticfile/app.setting.json');

var s3 = new aws.S3({ accessKeyId: appSetting.S3AccessKeyId, secretAccessKey: appSetting.S3SecretAccessKey}); //create a s3 Object with s3 User ID and Key

module.exports=s3;