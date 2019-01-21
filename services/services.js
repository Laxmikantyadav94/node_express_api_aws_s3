var controller = require('../controllers/controller.js');
var appSetting = require('../staticfile/app.setting.json');


const services ={
    saveFilesToS3 : async function(req,res,next){
        try{
            let respObj = await controller.saveFilesToS3(req.body.data,req.body.fileName);
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
    }
}

module.exports= services;