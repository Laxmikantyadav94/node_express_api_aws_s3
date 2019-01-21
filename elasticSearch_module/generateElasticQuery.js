
const esb = require('elastic-builder');
var mySqlDataAccess = require('./dataAccess');
var controller = require('./controller');
var searchBody;
var boolQuery;

const generateElasticQuery ={
        
    generateQuery: async function(reqBody){
        searchBody =esb.requestBodySearch()
        boolQuery=esb.boolQuery();
        let arrayShouldQuery=[];
        var suppressionFilterEmails=[];

        /*file filter -start*/
        let externalFilters=reqBody.externalFilters ;
        if(externalFilters !=undefined && externalFilters !=null) 
        {
           await Promise.all([
            this.getExternalEmailSuppressionQuery(externalFilters.emailSuppressionUrl),
            this.getExternalCompanySuppressionQuery(externalFilters.companySuppressionUrl),
            this.getExternalDomainSuppressionQuery(externalFilters.domainSuppressionUrl),
            this.getExternalEmailAbmQuery(externalFilters.emailAbmUrl),
            this.getExternalCompanyAbmQuery(externalFilters.companyAbmUrl),
            this.getExternalDomainAbmQuery(externalFilters.domainAbmUrl)
            ]
        ).then(function(result) {
            console.log("success");
        }).catch(function(err){
            console.log("faliure");
        });
        }
        /*file filter -end*/
        
        /*common filters -start */
        if(reqBody.searchable !=undefined && reqBody.searchable !=null) 
        {
            reqBody.searchable.forEach(element => {
                if(typeof(element.value)=="boolean"){
                    boolQuery.must(esb.existsQuery(element.key));
                }

                if(typeof(element.value)=="string"){
                    boolQuery.must(esb.matchQuery(element.key,element.value));
                }

                if(typeof(element.value)=="number"){
                    boolQuery.must(esb.termQuery(element.key,element.value));
                }

                if(element.value instanceof Array){
                let localBoolQuery=esb.boolQuery();

                    element.value.forEach(item => {
                        if(typeof item != "object"){
                            localBoolQuery.should(esb.matchQuery(element.key,item));
                        }else{
                            if(item.key =="range"){
                                let array =item.value.split("-");
                                if(!(isNaN(array[0]) || isNaN(array[1]))){
                                    localBoolQuery.should(esb.rangeQuery(element.key).gte(array[0]).lte(array[1]));
                                }else if(isNaN(array[1])){
                                    localBoolQuery.should(esb.rangeQuery(element.key).gte(array[0]));
                                }else if(isNaN(array[0])){
                                    localBoolQuery.should(esb.rangeQuery(element.key).lte(array[0]));
                                }
                            }
                        }
                    });
                    arrayShouldQuery.push(localBoolQuery);
                    boolQuery.must(arrayShouldQuery);
                }
            });
            
            
        }
               
        /*common filters -end */


        /* suppression filters - start*/

        let suppressionFilters= reqBody.suppressionFilters;
        if(suppressionFilters !=undefined && suppressionFilters !=null) 
        {

            await Promise.all([
                mySqlDataAccess.getActiveCampaignClientEmails(suppressionFilters.activeCampaign),
                mySqlDataAccess.getEndClientEmails(suppressionFilters.endClient),
                mySqlDataAccess.getDncClientEmails(suppressionFilters.dnc),
                mySqlDataAccess.getCommonClientEmails(suppressionFilters.common),
                mySqlDataAccess.getWpClientEmails(suppressionFilters.whitepapers)
            ]
            ).then(function(result) {
                result.forEach(element => {
                    suppressionFilterEmails=suppressionFilterEmails.concat(element);
                });
                if(suppressionFilterEmails.length >0)
                {
                    boolQuery.mustNot(esb.termsQuery("email_address.keyword",suppressionFilterEmails))
                }
            }).catch(function(){

            });
            
            
            let dncVendorSuppression=suppressionFilters.dncVendor
            if(dncVendorSuppression !=undefined && dncVendorSuppression !=null &&  dncVendorSuppression.filePath !=undefined){
                var  result = await controller.getDncVendorSuppressiondata(dncVendorSuppression.filePath);
                let localBoolQuery=esb.boolQuery();
                let localMultiMatchArray=[];

                let dncVendornameCompany =result[0];
                let dncVendorEmails =result[1];
                dncVendornameCompany.forEach(element => {
                    localMultiMatchArray.push(esb.multiMatchQuery([ "firstname", "lastname","company_name" ],element).type("cross_fields").operator("and"))
                });

                localBoolQuery.should(localMultiMatchArray);
                boolQuery.mustNot(localBoolQuery);

                boolQuery.mustNot(esb.termsQuery("email_address.keyword",dncVendorEmails)) 
            }

            let cpcSuppression=suppressionFilters.cpc;
            if(cpcSuppression !=undefined && cpcSuppression !=null &&  typeof(cpcSuppression.docCount) =='number'){
                let cpcAggsQuery= esb.termsAggregation("companyAggs","company_name.keyword").size(10000);        
                let subAggs =esb.topHitsAggregation("topHits").size(cpcSuppression.docCount);
                cpcAggsQuery.agg(subAggs); 
                searchBody.agg(cpcAggsQuery);
            }
        }
        /* suppression filters - end*/
        

        searchBody.query(boolQuery);
        return searchBody.toJSON();
    },
    dataScoreQuery:function(){
        searchBody =esb.requestBodySearch();
        let query= esb.termsQuery("datascore.keyword",["P1","P2","P3","P4","P5","P6"]) ;
        let aggs =esb.termsAggregation("dataScoreAggs","datascore.keyword").size(10000);
        searchBody.query(query);
        searchBody.agg(aggs);
        return searchBody.toJSON();
    },
    getExternalEmailSuppressionQuery: function(url){
        return new Promise( async function(resolve,reject){

            if(url !=undefined && url !=null){
                try{
                    
                    let fileContent = await controller.getFileContent(url);                   
                    boolQuery.mustNot(esb.termsQuery("email_address.keyword",fileContent))
                    resolve(true);

                }catch(err){
                    reject(reject);
                }
            }
            resolve(true)
        });    

    },
    getExternalCompanySuppressionQuery:  function(url){
       return new Promise( async function(resolve,reject){

            if(url !=undefined && url !=null){
                try{
                    let fileContent = await controller.getFileContent(url);
                    boolQuery.mustNot(esb.termsQuery("company_name.keyword",fileContent))
                    resolve(true);

                }catch(err){
                    reject(reject);
                }
            }
            resolve(true)
        });   
    },
    getExternalDomainSuppressionQuery:  function(url){
        return new Promise( async function(resolve,reject){

            if(url !=undefined && url !=null){
                try{
                    let fileContent =await controller.getFileContent(url);
                    boolQuery.mustNot(esb.termsQuery("website.keyword",fileContent))
                    resolve(true);

                }catch(err){
                    reject(reject);
                }
            }
            resolve(true)
        });   
    },
    getExternalEmailAbmQuery:  function(url){
        return new Promise( async function(resolve,reject){

            if(url !=undefined && url !=null){
                try{
                    let fileContent =await controller.getFileContent(url);
                    boolQuery.must(esb.termsQuery("email_address.keyword",fileContent))
                    resolve(true);

                }catch(err){
                    reject(reject);
                }
            }
            resolve(true)
        });   
    },
    getExternalCompanyAbmQuery: function(url){
        return new Promise(async function(resolve,reject){

            if(url !=undefined && url !=null){
                try{
                    let fileContent =await controller.getFileContent(url);
                    boolQuery.must(esb.termsQuery("company_name.keyword",fileContent))
                    resolve(true);

                }catch(err){
                    reject(reject);
                }
            }
            resolve(true)
        });   
    },
    getExternalDomainAbmQuery:  function(url){
        return new Promise(async function(resolve,reject){

            if(url !=undefined && url !=null){
                try{
                    let fileContent =await controller.getFileContent(url);
                    boolQuery.must(esb.termsQuery("website.keyword",fileContent))
                    resolve(true);

                }catch(err){
                    reject(reject);
                }
            }
            resolve(true)
        });   
    }

}

module.exports=generateElasticQuery;