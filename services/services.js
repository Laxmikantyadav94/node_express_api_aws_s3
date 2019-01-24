var controller = require('../controllers/controller.js');
var appSetting = require('../staticfile/app.setting.json');


const services ={
    listS3Buckets :async function(req,res,next){
        try{
            let respObj =await controller.listS3Buckets();
            res.status(200).json(respObj);
        }catch(err){
            res.status(500).json(err);
        }
    },
    createS3Bucket:async function(req,res,next){
        try{
            let respObj =await controller.createS3Bucket(req.params.bucketname);
            res.status(200).json(respObj);
        }catch(err){
            res.status(500).json(err);
        }
    },
    createS3SubFolder:async function(req,res,next){
        try{
            let respObj =await controller.createS3SubFolder();
            res.status(200).json(respObj);
        }catch(err){
            res.status(500).json(err);
        }
    },
    checkS3SubFolderExist:async function(req,res,next){
        try{
            let respObj =await controller.checkS3SubFolderExist();
            res.status(200).json("Bucket Found.");
        }catch(err){
            if(err.statusCode==404){
                res.status(404).json("Bucket not Found.");
            }else {
                res.status(500).json(err);
            }            
        }
    },
    getS3BucketCors:async function(req,res,next){
        try{
            let respObj =await controller.getS3BucketCors(req.params.bucketname);
            res.status(200).json(respObj);
        }catch(err){
            res.status(500).json(err);
        }
    },
    setS3BucketCors:async function(req,res,next){
        try{
            // Create initial parameters JSON for putBucketCors
            var thisConfig = {
                AllowedHeaders:["Authorization"],
                AllowedMethods:[],
                AllowedOrigins:["*"],
                ExposeHeaders:[],
                MaxAgeSeconds:3000
            };
            // Create array of allowed methods parameter based on parameters
            var allowedMethods = [];
            req.body.allowedMethods.forEach(val => {
                if (val.toUpperCase() === "POST") {allowedMethods.push("POST")};
                if (val.toUpperCase() === "GET") {allowedMethods.push("GET")};
                if (val.toUpperCase() === "PUT") {allowedMethods.push("PUT")};
                if (val.toUpperCase() === "PATCH") {allowedMethods.push("PATCH")};
                if (val.toUpperCase() === "DELETE") {allowedMethods.push("DELETE")};
                if (val.toUpperCase() === "HEAD") {allowedMethods.push("HEAD")};
            });

            // create CORS params
            thisConfig.AllowedMethods = allowedMethods;
            var corsRules = new Array(thisConfig);

            let respObj =await controller.setS3BucketCors(req.body.bucketname,corsRules);
            res.status(200).json(respObj);
        }catch(err){
            res.status(500).json(err);
        }
    },
    getS3BucketAcl:async function(req,res,next){
        try{
            let respObj =await controller.getS3BucketAcl(req.params.bucketname);
            res.status(200).json(respObj);
        }catch(err){
            res.status(500).json(err);
        }
    },
    saveFilesToS3 : async function(req,res,next){
        try{
            let respObj = await controller.saveFilesToS3(req.body.data,req.body.fileName);
            res.status(200).json(respObj);
        }catch(err){
            res.status(500).json(err);
        }
    },
    saveFilesToS3SubFolder:async function(req,res,next){
        try{
            let respObj = await controller.saveFilesToS3SubFolder(req.body.data,req.body.fileName);
            res.status(200).json(respObj);
        }catch(err){
            res.status(500).json(err);
        }
    },
    readJsonFromS3 :async function(req,res,next){
        try{
            let data = await controller.readJsonFromS3(req.params.keyname);
            res.status(200).json(data);
        }catch(err){
            res.status(500).json(err);
        }
    },
    readCsvAsStream :async function(req,res,next){
        try{
            let data = await controller.readCsvAsStream(req.params.keyname);
            res.status(200).json(data);
        }catch(err){
            res.status(500).json(err);
        }
    },
    getObjectsFromBucket:async function(req,res,next){
        try{
            let data = await controller.getObjectsFromBucket();
            res.status(200).json(data);
        }catch(err){
            res.status(500).json(err);
        }
    },
    deleteS3Bucket:async function(req,res,next){
        try{
            let respObj =await controller.deleteS3Bucket(req.params.bucketname);
            res.status(200).json(respObj);
        }catch(err){
            res.status(500).json(err);
        }
    }
}

module.exports= services;