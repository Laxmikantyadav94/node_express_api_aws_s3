
var parse = require('csv-parse');
var request= require('request');
var s3= require('../Database/awsS3.js');
var appSetting = require('../staticfile/app.setting.json');

const controller ={
    listS3Buckets:function(){
        return new Promise(function(resolve,reject){
            // Call S3 to list current buckets
            s3.listBuckets(function(err, data) {
                if (err) {
                    reject(err);
                } else {
                    resolve(data.Buckets);
                }
            });
        })
    },
    createS3Bucket:function(bucketName){
        return new Promise(function(resolve,reject){           
            // Create the parameters for calling createBucket
            var bucketParams = {
                Bucket : appSetting.S3Bucket
            }; 
            // Call S3 to create the bucket
            s3.createBucket(bucketParams, function(err, data) {
                if (err) {
                    reject(err);
                } else {
                    resolve(data.Location);
                }
            });
        })
    },
    getS3BucketCors:function(bucketName){
        return new Promise(function(resolve,reject){           
            // Create the parameters 
            var bucketParams = {
                Bucket : bucketName
            }; 
            // call S3 to retrieve CORS configuration for selected bucket
            s3.getBucketCors(bucketParams, function(err, data) {
                if (err) {
                    reject(err)
                } else if (data) {
                    resolve(data.CORSRules);
                }
            });
           
        })
    },
    setS3BucketCors:function(bucketName,corsRules){
        return new Promise(function(resolve,reject){           
            // Create the parameters 
            var params = {
                Bucket : bucketName,
                CORSConfiguration: {CORSRules: corsRules}
            }; 
            // call S3 to retrieve CORS configuration for selected bucket
            s3.putBucketCors(params, function(err, data) {
                if (err) {
                    reject(err)
                } else if (data) {
                    resolve(data);
                }
            });
           
        })
    },
    getS3BucketAcl:function(bucketName){
        return new Promise(function(resolve,reject){           
            // Create the parameters 
            var bucketParams = {
                Bucket : bucketName
            }; 
            // call S3 to retrieve CORS configuration for selected bucket
            s3.getBucketAcl(bucketParams, function(err, data) {
                if (err) {
                    reject(err)
                } else if (data) {
                    resolve(data.Grants);
                }
            });
           
        })
    },
    createS3SubFolder:function(){
        return new Promise(function(resolve,reject){           
            let params = {
                Bucket: appSetting.S3Bucket+"/folder", 
                Key: "subfolder/", 
                Body:''
            };
            //upload can also be used in place of putObject
            s3.putObject(params,function(err,data){
                if(err) return reject(err);
                resolve(data);
            });
        })
    },
    checkS3SubFolderExist:function(){
        return new Promise(function(resolve,reject){           
            let params = {
                Bucket: appSetting.S3Bucket+"/folder", 
                Key: "subfolderr/"
            };
            s3.headObject(params,function(err,data){
                if(err) return reject(err);
                resolve(data);
            });
        })
    },
    readCsvAsStream: async function(keyName)   {

      return new Promise(function(resolve,reject){  
        var csvData =[];
        const parser = parse({
          delimiter: ','
        }) 
         var getParams = {
            Bucket: appSetting.S3Bucket,
            Key: keyName
        }
        
        //Fetch or read data from aws s3
        s3.getObject(getParams).createReadStream().pipe(parser)
            .on('data', function(csvrow) {
                //do something with csvrow
                let obj ={};
                obj.id=csvrow[0],
                obj.prop1=csvrow[1],
                obj.prop2=csvrow[2],
                obj.prop3=csvrow[3],
                obj.prop4=csvrow[4],
                obj.prop5=csvrow[5],
                obj.prop6=csvrow[6],
                obj.prop7=csvrow[7],
                obj.prop8=csvrow[8],
                obj.prop9=csvrow[9],

                csvData.push(obj);
            })
            .on('end',function() {
              //do something wiht csvData
              resolve(csvData);
            })
            .on('error', function(error) {
              reject(error);
            });
        })  

    },
    saveFilesToS3: function(elasticData,logName){
      return new Promise(function(resolve,reject){  
        var currentDate= new Date();
        var fileName=logName+"_"+currentDate.toJSON()+".json"
        const params = {
          Bucket: appSetting.S3Bucket, // pass your bucket name
          Key: appSetting.S3FolderName+fileName, // file will be saved as testBucket/contacts.csv
          Body: JSON.stringify(elasticData)
          };
          s3.upload(params, function(s3Err, data) {
              if (s3Err) reject(s3Err);
              var resultObj={
                "bucket":appSetting.S3Bucket,
                "key":appSetting.S3FolderName+fileName,
                "location":data.Location
              }
              resolve(resultObj);
          });
        });  
    },
    readFileFromS3:function(keyName){
        return new Promise(function(resolve,reject){
            let params={
                Bucket:appSetting.S3Bucket,
                Key:keyName
            }
    
            s3.getObject(params,function(err,data){
                if(err) reject(err);
                let objectData = data.Body.toString('utf-8'); // Use the encoding necessary
                resolve(objectData);
            })       

        })
    },
    getsignedUrlForObject: function(keyName){
        return new Promise(function(resolve,reject){
            let params={
                Bucket:appSetting.S3Bucket,
                Key:keyName
            }
            s3.getSignedUrl('getObject',params,function(err,data){
                resolve(data)
            })
        })
    },
    uploadMultipart:function(req){
        return new Promise(function(resolve,reject){           
            
            var startTime = new Date();
            
            var partSize = 1024 * 1024 * 5; // 5mb chunks except last part
            var numPartsLeft = Math.ceil(buffer.length / partSize);
            var maxUploadTries = 3;         
                        
            var multipartMap = {
                Parts: []
            };
            controller.initiateMuntipart();
        })       

    },

    initiateMuntipart:function(req){  

            var file = req.files.file;
            var buffer = fs.readFileSync(file.path);
            var partNum = 0;
            var multipartParams = {
                Bucket: appSetting.S3Bucket,
                Key: file.name,
                ContentType: file.type
            };
            console.log('Creating multipart upload for:', file.name);
            s3.createMultipartUpload(multipartParams, function(mpErr, multipart) {
              if (mpErr) return console.error('Error!', mpErr);
              console.log('Got upload ID', multipart.UploadId);
          
              for (var start = 0; start < buffer.length; start += partSize) {
                partNum++;
                var end = Math.min(start + partSize, buffer.length);
                var partParams = {
                  Body: buffer.slice(start, end),
                  Bucket: multipartParams.Bucket,
                  Key: multipartParams.Key,
                  PartNumber: String(partNum),
                  UploadId: multipart.UploadId
                };
          
                console.log('Uploading part: #', partParams.PartNumber, ', Start:', start);
                controller.uploadPart(multipart, partParams);
              }
        })
    },
    completeMultipartUpload:function (s3, doneParams) {
        s3.completeMultipartUpload(doneParams, function(err, data) {
          if (err) return console.error('An error occurred while completing multipart upload');
          var delta = (new Date() - startTime) / 1000;
          console.log('Completed upload in', delta, 'seconds');
          console.log('Final upload data:', data);
        });
      },
    uploadPart:function (multipart, partParams, tryNum) {
        var tryNum = tryNum || 1;
        s3.uploadPart(partParams, function(multiErr, mData) {
          console.log('started');
          if (multiErr) {
            console.log('Upload part error:', multiErr);
    
            if (tryNum < maxUploadTries) {
              console.log('Retrying upload of part: #', partParams.PartNumber);
              uploadPart(s3, multipart, partParams, tryNum + 1);
            } else {
              console.log('Failed uploading part: #', partParams.PartNumber);
            }
            // return;
          }
    
          multipartMap.Parts[this.request.params.PartNumber - 1] = {
            ETag: mData.ETag,
            PartNumber: Number(this.request.params.PartNumber)
          };
          console.log('Completed part', this.request.params.PartNumber);
          console.log('mData', mData);
          if (--numPartsLeft > 0) return; // complete only when all parts uploaded
    
          var doneParams = {
            Bucket: multipartParams.Bucket,
            Key: multipartParams.Key,
            MultipartUpload: multipartMap,
            UploadId: multipart.UploadId
          };
    
          console.log('Completing upload...');
          completeMultipartUpload(s3, doneParams);
        })
    },
    createFileWithPublicReadAcl :function(data,fileName){
        return new Promise(function(resolve,reject){  
            let params={
                Bucket:appSetting.S3Bucket,
                Key:fileName,
                Body:JSON.stringify(data),
                ACL:'public-read'
            }
            //to set public-read your bucket's public access setting should allow this

            //look -https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#canned-acl for more canned Acl options
            s3.upload(params,function(err,data){
                if(err) reject(err);
                resolve(data);
            })
        })
    },
    getBucketPolicy : function(){
        return new Promise(function(resolve,reject){
            let params={
                Bucket:appSetting.S3Bucket
            }
            s3.getBucketPolicy(params,function(err,data){
                if(err) reject(err);
                resolve(data);
            })
        })
    },
    setReadOnlyAnonUserBucketPolicy:function(){
        return new Promise(function(resolve,reject){
            let bucket=appSetting.S3Bucket;
            var readOnlyAnonUserPolicy = {
                Version: "2012-10-17",
                Statement: [
                  {
                    Sid: "AddPerm",
                    Effect: "Allow",
                    Principal: "*",
                    Action: [
                      "s3:GetObject"
                    ],
                    Resource: [
                      ""
                    ]
                  }
                ]
              };
              
              // create selected bucket resource string for bucket policy
              var bucketResource = "arn:aws:s3:::" + bucket + "/*";
              readOnlyAnonUserPolicy.Statement[0].Resource[0] = bucketResource;
              
              // convert policy JSON into string and assign into params
              var bucketPolicyParams = {Bucket: bucket,
                 Policy: JSON.stringify(readOnlyAnonUserPolicy)
                };
                
                //to set buckey policy your bucket's public access setting should allow this
            s3.putBucketPolicy(bucketPolicyParams,function(err,data){
                if(err) reject(err)
                resolve(data)
            })
        });
    },
    deleteBucketPolicy:function(){
        return new Promise(function(resolve,reject){ 
            var bucketPolicyParams = {
                Bucket: appSetting.S3Bucket
               };
               
           s3.deleteBucketPolicy(bucketPolicyParams,function(err,data){
               if(err) reject(err)
               resolve(data)
           })
        })
    },
    saveFilesToS3SubFolder:function(data,logName){
        return new Promise(function(resolve,reject){  
          var currentDate= new Date();
          var fileName=logName+"_"+currentDate.toJSON()+".json"
          const params = {
            Bucket: appSetting.S3Bucket+"/folder/subfolder", // pass your bucket name
            Key: fileName, 
            Body: JSON.stringify(data)
            };
            s3.upload(params, function(s3Err, data) {
                if (s3Err) reject(s3Err);
                var resultObj={
                  "bucket":appSetting.S3Bucket,
                  "key":appSetting.S3FolderName+fileName,
                  "location":data.Location
                }
                resolve(resultObj);
            });
          });  
      },
      downloadFileFromS3 : function(keyName){
        return new Promise(function(resolve,reject){  
                var getParams = {
                    Bucket: appSetting.S3Bucket,
                    Key: keyName
                }

               resolve(s3.getObject(getParams).createReadStream());
        })  
    },
    getObjectsFromBucket:function(){
        return new Promise(function(resolve,reject){
            // Create the parameters for calling createBucket
            var bucketParams = {
                Bucket : appSetting.S3Bucket
            };                    
                                                
            // Call S3 to create the bucket
            s3.listObjects(bucketParams, function(err, data) {
                if (err) {
                    reject(err);
                } else {
                    resolve(data);
                }
            });
        })  
    },
    deleteS3Bucket:function(bucketName){
        return new Promise(function(resolve,reject){           
            // Create the parameters for calling createBucket
            var bucketParams = {
                Bucket : bucketName
            }; 
            // Call S3 to create the bucket
            s3.deleteBucket(bucketParams, function(err, data) {
                if (err) {
                    reject(err);
                } else {
                    resolve(data.Location);
                }
            });
        })
    } 
}

module.exports=controller;