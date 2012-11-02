var neo4j = require('neo4j');
var async = require('async');

var db = new neo4j.GraphDatabase('http://localhost:7474');


var count = 0;
var linkCount = 0;
var NUM_NODES = 50;
var NUM_LINKS = 10;
var nodes = [];

// Create a bunch of Nodes
async.whilst(
    function(){return count < NUM_NODES},
    function createNode(cb) {
        var node = db.createNode({nodeNum:count});
        
        node.save(function(err, savedNode) {
            nodes[count++] = savedNode;
            cb(err);
        })       
    },
    function doneCreateNodes(err) {
        if (!err) {
            console.log("Done creating %d nodes...", count);
            count = 0;
            // For each node, create relationships to the next node in the array
            async.whilst(
                function(){return count < nodes.length-2},
                function createLinksForEachNode(cb) {
                    linkCount = 0;
                    async.whilst(
                        function(){return linkCount < NUM_LINKS},
                        function createLink(cb) {
                            nodes[count].createRelationshipTo(nodes[count+1], 'TestLink', {linkNumber:linkCount}, function(err, theLink) {
                                linkCount++;
                                
                                if (err) {
                                    console.error("Error creating link from node #%d", count);
                                }
                                cb(err);
                            });
                        },
                        function doneCreateLink(err) {
                            cb(err);
                            count++;
                        }
                    );
                },
                function doneCreateLinksForNodes(err){
                    if (!err) {
                        console.log("Done creating nodes and links. First node id was %d, last node id was %d", nodes[0].id, nodes[nodes.length-1].id);
                        console.log("Issuing concurrent delete queries for each node.");
                        var numDeleted = 0;
                        async.forEach(
                            nodes,
                            function forEachNode(node, cb) {                           
                                deleteNodeWithRetry(node, function(err, result) {
                                    if (err) {
                                        console.error("Error deleting node. Err = %j, Result = %j", err, result);
                                    }
                                    else {
                                        console.log("Successfully deleted Node. Result = %j", result);
                                        numDeleted++;
                                    }
                                    cb();
                                });
                            },
                            function doneForEachNode(err) {
                                console.log("\n\n");
                                console.log("Successfully deleted %d nodes. Test complete with err = %j", numDeleted, err);                         
                                console.log("Issuing relationship queries");
                                console.log("If everything worked, the count of relationships should be 0 and the");
                                console.log('"return all links" query should return an empty result and no error.')
                                
                                var q = 'start r = rel(*) return count(*)';
                                db.query(q, function(err, result) {
                                    console.log("Result of query '%s' was: err = %s, result = %j", q, err, result);
                                    
                                    q = 'start r = rel(*) return r';
                                    
                                    db.query(q, function(err, result) {
                                        console.log("Result of query '%s' was: err = %s, result =  %j", q, err, result);
                                    })
                                });
                            }
                        );
                    }
                    else {
                        console.error("Error creating links for nodes %s", err);
                    }
                }
            );           
        }
        else {
            console.error("Error while creating nodes: %s", err);
        }
    }
);

/**
 * Delete a node retrying if a deadlock error is returned.
 * 
 * @param node
 * @param callback  function(err, result)
 * 
 */
function deleteNodeWithRetry(node, callback) {
    var retry = true;
    var numRetries = 0;
    var start = new Date().getTime();
    console.time(node.id);
    async.whilst(
        function needsRetry(){return retry},
        function issueDeleteQuery(cb){
            //console.log("deleteNodeWithRetry: Issuing delete query for node with id %d. Retry #%d", node.id, numRetries);
            db.query("START n = node({neoId}) MATCH n-[r?]-() DELETE r, n", {neoId:node.id}, function (err, result) {
                if (err) {
                    if (err.message && err.message.indexOf("deadlock") > -1) {
                        numRetries++;
                        console.warn("deleteNodeWithRetry: 'Deadlock' detected while deleting node %d. Retrying...", node.id);
                        cb();
                    }
                    else if (err.message && err.message.indexOf("not in use") > -1){
                        numRetries++;
                        console.warn("deleteNodeWithRetry: %s error detected while deleting node %d. Retrying...", err.message, node.id);
                        cb();
                    }
                    else if (err.message && err.message.indexOf("not found") > -1){
                        numRetries++;
                        console.warn("deleteNodeWithRetry: %s detected while deleting node %d. Retrying...", err.message, node.id);
                        cb();
                    }
                    else {
                        console.error("deleteNodeWithRetry: Error while deleting Node with id %d: %j", node.id, err);
                        cb(err);
                    }
                }
                else {
                    retry = false;
                    cb();
                }
            });
        },
        function queryDone(err) {
            if (err) {
                console.error("deleteNodeWithRetry (queryDone): Error while deleting Node with id %d: %j", node.id, err);
                callback(err, {nodeId:node.id, numRetries:numRetries});
            }
            else {
                //console.log("deleteNodeWithRetry: Successfully deleted Node with id %d with no err. Retries = %d", node.id, numRetries);
                console.timeEnd(node.id);
                var execTime = new Date().getTime() - start;
                callback(null, {nodeId:node.id, numRetries:numRetries, execTime:execTime});
            }
        }
    );
}
