var AWS = require('aws-sdk');
var dynamodb = new AWS.DynamoDB();
var fs = require('fs');

var retireTablesInHours = 24
var archiveTableInDays = 4


function describeTable(tableName){
    return new Promise(function(resolve,reject) {
        dynamodb.describeTable({ TableName: tableName } , function(err, data) {
            if (err) reject({err:err, stack:err.stack}); // an error occurred
            else return resolve(data)        // successful response
        });
    })
}

function retireTables(tables){
        console.log("--retiretables--")
        var tablesToRetire = tables.filter(function(table){
            return table.toRetire
            
        });
        
        console.log("there are " + tablesToRetire.length +  " tables to retire")
        
        if (tablesToRetire){
            return Promise.all(tablesToRetire.map(function(table){
                return retire(table.TableName)
            }))
        } else {
            return Promise.resolve()
        }


}

function retire(tableName) {
    console.log("retiring table " + tableName)
    return new Promise(function(resolve,reject) {
        var params = {      
            TableName:tableName,
            ProvisionedThroughput: {
                ReadCapacityUnits: 1, 
                WriteCapacityUnits: 1
            }
        }
        
        dynamodb.updateTable(params, function(err, data) {
            if (err) reject({err:err, stack:err.stack})
            else resolve (data);
        });
    })
}

function archiveTables(tables){
        console.log("--archivetables--")
        var tablesToArchive = tables.filter(function(table){
            return table.toArchive
            
        });
        
        console.log("there are " + tablesToArchive.length +  " tables to archive")
        
        return Promise.all(tablesToArchive.map(function(table){
            return archive(table.TableName)
        }))

}

function archive(tableName) {
    console.log("archiving table " + tableName)
    return new Promise(function(resolve,reject) {

        var params = {      
            TableName:tableName,
        }
        console.log("attempting to delete table " + tableName)
        dynamodb.deleteTable(params, function(err, data) {
            if (err) reject({err:err, stack:err.stack})
            else resolve (data);
        });
    })
}

function say(t) {
    return (d) => {
        console.log(t, d)
        return d
    }
}

function pretty(data){
    return JSON.stringify(data,null,2)
}

exports.handler = function(event, context) {

    var scriptStartDate = new Date()

    console.log("--start--")

    dynamodb.listTables({}, function(err, data) {
        if (err) console.log(err, err.stack); // an error occurred
        else
        {
            data.TableNames = data.TableNames.filter((t) => {
                return (t.substr(0,10) == 'prod-pool-')
            })
            console.log(data)
            console.log("there are " +data.TableNames.length+ " tables online.")
            var tableDetailedInfo = data.TableNames.map(function(tableName){
                return describeTable(tableName)
                .then(function(table){
                    var tableCreateDate = table.Table.CreationDateTime
                    
                    var retireDate = new Date(tableCreateDate);
                    retireDate.setHours(retireDate.getHours()+retireTablesInHours);
                    
                    var archiveDate = new Date(tableCreateDate);
                    archiveDate.setHours(archiveDate.getHours()+ (24*archiveTableInDays) );
            
                    
                    var toRetire = (
                                        (scriptStartDate > retireDate) &&
                                        (
                                            (table.Table.ProvisionedThroughput.ReadCapacityUnits !== 1)  ||
                                            (table.Table.ProvisionedThroughput.WriteCapacityUnits !== 1)
                                        )
                                   )
                                   
                    var toArchive = scriptStartDate > archiveDate

                    if (toRetire) console.log("table " + table.Table.TableName + " is being set up to be retired.")
                    if (toArchive) console.log("table " + table.Table.TableName + " is being set up to be archived.")


                    var out = {
                        TableName:table.Table.TableName,
                        read:table.Table.ProvisionedThroughput.ReadCapacityUnits,
                        write:table.Table.ProvisionedThroughput.WriteCapacityUnits,
                        created:tableCreateDate,
                        retireDate:retireDate,
                        archiveDate:archiveDate,
                        toRetire: toRetire,
                        toArchive: toArchive,
                    } 
                    
                    return Promise.resolve(out) 
                })
            })

            Promise.all(tableDetailedInfo)
            .then(say("table data:\n"))
            .then((tableData)=>{
                return Promise.all([
                    retireTables(tableData),
                    archiveTables(tableData)
                ])
            })
            .then(JSON.stringify)
            .then(pretty)
            .then(console.log)
            .then(function(){
                context.done()
            })
            .catch(function(error){
                console.error(error)
                context.fail()
            })  

        }
    });
};



if (require.main === module) {
    exports.handler()
}

