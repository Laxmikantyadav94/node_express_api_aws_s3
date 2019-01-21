var mySql= require('../Database/mySql.js');

const dataAccess ={
    internalSuppressionCount:function(){
        return new Promise(function(resolve,reject){
            /* 
                DNC
                common -30
                end client -90
                active compaign
                hard bounce
            */
            var sql =`SELECT COUNT(*) AS 'count' FROM suppression_data  WHERE supp_name ='DNC';
            SELECT COUNT(*) AS 'count'  FROM suppression_data WHERE updated_on >= ( SELECT DATE_SUB(CURDATE(), INTERVAL 30 DAY));
            SELECT COUNT(*) AS 'count' FROM suppression_data WHERE updated_on >= ( SELECT DATE_SUB(CURDATE(), INTERVAL 90 DAY));
            SELECT COUNT(*) AS 'count' FROM active_campaign ;
            SELECT COUNT(*) AS 'count' FROM email_varification WHERE MW_Reply='Internal'`;
            mySql.fsdb.query(sql,function(err, results, fields){
                if (err) reject(err);
                resolve(results);
            });
        }) 
    },
    getParentindustry :function(){

        return new Promise(function(resolve,reject){             
            let query ="SELECT DISTINCT `parent_industry` AS 'parentIndustry' FROM `mstr_industry` WHERE (`parent_industry` <> ''  AND `parent_industry` IS NOT NULL ) ORDER BY `parent_industry` ";
            mySql.fsdb.query(query, function(err, result) {
                if (err) {
                    reject(err);
                }
                resolve(result);
            });
        })  
       
    },
    getSubindustry :function(parentIndustries){

        return new Promise(function(resolve,reject){ 
            let query ="SELECT DISTINCT `sub_industry` AS 'subIndustry' FROM `mstr_industry` WHERE `parent_industry`  IN ( ? ) AND (`sub_industry` <> ''  AND `sub_industry` IS NOT NULL )  ORDER BY `sub_industry`";
            mySql.fsdb.query(query,[parentIndustries], function(err, result) {
                if (err) {
                    reject(err);
                }
                resolve(result);
            });
        })  
       
    },
    getDepartment :function(){

        return new Promise(function(resolve,reject){             
            let query ="SELECT DISTINCT `parent_department` As 'parentDepartment' FROM `ref_department_hierarchy` WHERE (`parent_department` <> ''  AND `parent_department` IS NOT NULL ) ORDER BY `parent_department` ASC";
            mySql.fsdb.query(query, function(err, result) {
                if (err) {
                    reject(err);
                }
                resolve(result);
            });
        })  
       
    },
    getSubDepartment :function(parentDepartments){

        return new Promise(function(resolve,reject){ 
            let query ="SELECT DISTINCT `sub_department` As 'subDepartment' FROM `ref_department_hierarchy` WHERE `parent_department` IN ( ? ) AND (`sub_department` <> ''  AND `sub_department` IS NOT NULL ) ORDER BY `sub_department` ASC";
            mySql.fsdb.query(query,[parentDepartments], function(err, result) {
                if (err) {
                    reject(err);
                }
                resolve(result);
            });
        })  
       
    },
    getFunction :function(parentDepartments,subDepartments){

        return new Promise(function(resolve,reject){ 
            let query ="SELECT DISTINCT `function` AS 'function' FROM `ref_department_hierarchy` WHERE `parent_department` IN ( ? ) AND  `sub_department` IN ( ? ) AND (`function` <> ''  AND `function` IS NOT NULL )  ORDER BY `function` ASC";
            mySql.fsdb.query(query,[parentDepartments,subDepartments], function(err, result) {
                if (err) {
                    reject(err);
                }
                resolve(result);
            });
        })  
       
    },
    getEmployeeRange :function(){

        return new Promise(function(resolve,reject){ 
            let query ="SELECT DISTINCT `emp_range`  AS 'empRange' FROM `mstr_emprange` WHERE (`emp_range` <> ''  AND `emp_range` IS NOT NULL ) ";
            mySql.fsdb.query(query, function(err, result) {
                if (err) {
                    reject(err);
                }
                resolve(result);
            });
        })  
       
    },
    getRevenueRange :function(){

        return new Promise(function(resolve,reject){ 
            let query ="SELECT DISTINCT `rev_range`  AS 'revRange' FROM `mstr_revrange`  WHERE (`rev_range` <> ''  AND `rev_range` IS NOT NULL )";
            mySql.fsdb.query(query, function(err, result) {
                if (err) {
                    reject(err);
                }
                resolve(result);
            });
        })  
       
    },
    getCountry :function(){

        return new Promise(function(resolve,reject){             
            let query ="SELECT DISTINCT  id, `name` AS 'country'  FROM  `mstr_countries` WHERE (`name` <> ''  AND `name` IS NOT NULL ) ORDER BY `name`";
            mySql.fsdb.query(query, function(err, result) {
                if (err) {
                    reject(err);
                }
                resolve(result);
            });
        })  
       
    },
    getState :function(countries){

        return new Promise(function(resolve,reject){ 
            let query ="SELECT DISTINCT id ,`name` AS 'state'  FROM  `mstr_states` WHERE `country_id` IN ( ? ) AND (`name` <> ''  AND `name` IS NOT NULL )  ORDER BY `name`";
            mySql.fsdb.query(query,[countries], function(err, result) {
                if (err) {
                    reject(err);
                }
                resolve(result);
            });
        })  
       
    },
    getCity :function(countries,states){

        return new Promise(function(resolve,reject){ 
            let query ="SELECT DISTINCT id ,`name` AS 'city'  FROM  `mstr_cities` WHERE `country_id` IN ( ? ) AND `state_id` IN ( ? )  AND (`name` <> ''  AND `name` IS NOT NULL )  ORDER BY `name`";
            mySql.fsdb.query(query,[countries, states ], function(err, result) {
                if (err) {
                    reject(err);
                }
                resolve(result);
            });
        })  
       
    },
    seniority:function(){

        return new Promise(function(resolve,reject){ 
            let query ="SELECT DISTINCT `seniority` FROM `mstr_seniority` WHERE (`seniority` <> ''  AND `seniority` IS NOT NULL )  ORDER BY seniority";
            mySql.fsdb.query(query, function(err, result) {
                if (err) {
                    reject(err);
                }
                resolve(result);
            });
        })  

    },
    getsuppressionClientCodes :function(){
        return new Promise(function(resolve,reject){
            var sql = "SELECT DISTINCT `client_supp` AS 'clientSupp' FROM `suppression_data`  WHERE  (`client_supp` <> ''  AND `client_supp` IS NOT NULL )  ORDER BY `client_supp`";
            mySql.fsdb.query(sql,function(error, results, fields){
                resolve(results);
            });
        })       
    },
    getActiveCampaignClientEmails : function(isActiveCompaign){
        if(isActiveCompaign)
        {
            return new Promise(function(resolve,reject){
                var sql = "SELECT GROUP_CONCAT(email_address) AS email_address FROM `active_campaign`";
                mySql.fsdb.query('SET SESSION group_concat_max_len = 10000000');
                mySql.fsdb.query(sql,function(err, results, fields){
                    if (err) reject(err);
                    if(results !=undefined &&  results[0]!=undefined && results[0] !=null)
                    {
                        let clients =results[0].email_address || "";
                        let activeClientArray =clients.split(",");  
                        resolve(activeClientArray);
                    }else{
                        return [];
                    }                    
                });
            })
        }else{
            return [];
        }
               
    },
    getEndClientEmails : function(endClient){
        if(endClient.clientCodes !=undefined && endClient.clientCodes  !=null && endClient.clientCodes.length > 0)
        {
            return new Promise(function(resolve,reject){
                var sql ="SELECT GROUP_CONCAT(DISTINCT email_address) AS email_address FROM `suppression_data` WHERE `client_code` IN ( ? ) AND `updated_on` >= ( SELECT DATE_SUB(CURDATE(), INTERVAL ? DAY))";
                mySql.fsdb.query('SET SESSION group_concat_max_len = 10000000');
                mySql.fsdb.query(sql,[endClient.clientCodes ,endClient.interval],function(err, results, fields){
                    if (err) reject(err);
                    if(results !=undefined &&  results[0]!=null && results[0] !=null)
                    {
                        let clients =results[0].email_address || "";
                        let endClientArray =clients.split(",");  
                        resolve(endClientArray);
                    }else{
                        resolve([]);
                    }
                });
            })
        }else{
            return [];
        }               
    },
    getDncClientEmails : function(isDncFilter){
        if(isDncFilter)
        {
            return new Promise(function(resolve,reject){
                var sql ="SELECT GROUP_CONCAT(DISTINCT email_address) AS email_address FROM `suppression_data`  WHERE `supp_name`='DNC'";
                mySql.fsdb.query('SET SESSION group_concat_max_len = 10000000');
                mySql.fsdb.query(sql,function(err, results, fields){
                    if (err) reject(err);
                    if(results !=undefined &&  results[0]!=undefined && results[0] !=null)
                    {
                        let clients =results[0].email_address || "";
                        let dncClientArray =clients.split(",");  
                        resolve(dncClientArray);
                    }else{
                        resolve([]);
                    }
                });
            }) 
        }else{
            return [];
        }               
    },
    getCommonClientEmails : function(commonFilter){
        if(commonFilter != undefined && commonFilter !=null && typeof(commonFilter.interval) =='number') 
        {
            return new Promise(function(resolve,reject){
                var sql ="SELECT GROUP_CONCAT(DISTINCT email_address) AS email_address FROM `suppression_data` WHERE `updated_on` >= ( SELECT DATE_SUB(CURDATE(), INTERVAL ? DAY))";
                mySql.fsdb.query('SET SESSION group_concat_max_len = 10000000');
                mySql.fsdb.query(sql,commonFilter.interval,function(err, results, fields){
                    if (err) reject(err);
                    if(results !=undefined &&  results[0]!=undefined && results[0] !=null)
                    {
                        let clients =results[0].email_address || "";
                        let commonClientArray =clients.split(",");  
                        resolve(commonClientArray);
                    }else{
                        resolve([]);
                    }                    
                });
            })
        }else{
            return [];
        }               
    },
    getWpClientEmails : function(wpFilter){
        if(wpFilter) 
        {
            return new Promise(function(resolve,reject){
                var sql ="SELECT  GROUP_CONCAT(DISTINCT `Email_Address`) AS email_address FROM `email_varification` WHERE `MW_Reply`='Internal'";
                mySql.fsdb.query('SET SESSION group_concat_max_len = 10000000');
                mySql.fsdb.query(sql,function(err, results, fields){
                    if (err) reject(err);
                    if(results !=undefined &&  results[0]!=undefined && results[0] !=null)
                    {
                        let clients =results[0].email_address || "";
                        let wpClientArray =clients.split(",");  
                        resolve(wpClientArray);
                    }else{
                        resolve([]);
                    }   
                    
                });
            })
        }else{
            return [];
        }               
    }
    
}

module.exports=dataAccess;