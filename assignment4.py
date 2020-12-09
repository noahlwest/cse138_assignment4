#imports
import os, requests, sys, json, datetime, time
from flask import Flask, jsonify, request, Response

#create app with flask
app = Flask(__name__)

#decideShard()
#decides which shard a new key should belong to
#requests the key-count from every shard, assigns key to min key-count shard
#returns the shard that the new key should belong to
def decideShard():
    minKeys = getLocalKeyCount() #min defaults to local
    whichShard = selfShardID #defaults to local shardID
    #for each shard, see who has the least amount of keys
    for shard, addresses in shardAddressesDict.items():
        haveKeyCount = False
        for address in addresses:
            if (haveKeyCount == True):
                break
            baseUrl = ('http://' + address + '/kvs/key-count')
            try:
                timeoutVal = 4 / (len(shardAddressesDict) * replFactor) #4 seconds to have a nicer cushion
                r = requests.get(baseUrl, timeout=timeoutVal)
                requestedKeyCount = r.json().get('key-count')
                if(requestedKeyCount is not None):
                    haveKeyCount == True

                if(requestedKeyCount < minKeys):
                    minKeys = requestedKeyCount
                    whichShard = shard
            except:
                pass #even if we fail it doesn't matter, but we want to fail in a timely manner
            
    return whichShard

#decideNodeToShard()
#decides which nodes go to which shards
#takes the current list of nodes, and distributes them in <shard:[addresses]> dictionary according to replication factor
#updates the locally held shardAddresses dictionary, which is distributed by other functions
def decideNodeToShard():
    shardCounter = 1 #used for going through named shards
    replFactorCounter = 0 #used to count the amount of replicas we dedicate to a node
    #for each node, designate its address to a shard
    for node, address in nodeAddressDict.items():
        if replFactorCounter == replFactor: #if we have enough replicas of one shard:
            replFactorCounter = 0 #reset number of replicas to 0
            shardCounter += 1 #move to next shard
        #shardID used to navigate shardAddressesDict
        shardID = ("shard" + str(shardCounter))
        #get the current list of addresses assigned to a node
        #this dictionary should be clear()'d during reshard
        nodesList = shardAddressesDict.get(shardID)
        #if list does not exist, make a new one
        if nodesList is None:
            nodesList = []
        #append address to list of addresses that belong to a shard
        nodesList.append(address)
        #update the shardAddressesDict with the array of addresses
        shardAddressesDict.update({ shardID : nodesList })
        #increment counter to say how many replicas on this shard
        replFactorCounter += 1



#getLocalKeyCount()
#gets the local keycount by getting the length of localKvsDict
#returns the amount of keys in localKvsDict
def getLocalKeyCount():
    keyCount = len(localKvsDict)
    return keyCount

#isRequestValidToShard
#checks if a request is valid, to the shard it is assigned
@app.route('/kvs/isRequestValidToShard/<string:key>', methods = ['PUT'])
def isRequestValidToShard(key):
    #if there's no value in the json
    value = request.get_json().get('value')
    if(value is None):
        return jsonify(
            error="Value is missing",
            message="Error in PUT",
            isRequestGood=False
        ), 400

    #if the key is too long, arbitrarily, temporarily assign it to the local node
    #and send a failure, since it won't belong anywhere ever.
    #if the length is > 50
    if(len(key) > 50):
        return jsonify(
            error="Key is too long",
            message="Error in PUT",
            isRequestGood=False
        ), 400

    return jsonify(
        isRequestGood=True
    ), 200

#getKeyWithContext
#used to get a key/value pair from a node, with a timestamp for context
@app.route('/kvs/getKeyWithContext/<string:key>', methods = ['PUT'])
def getKeyWithContext(key):
    ourTime = keyTimeDict.get(key)
    if(ourTime is None):
        return "No context", 204
    else:
        return jsonify(
            value=localKvsDict.get(key),
            time=ourTime
        ), 200


#behavior for /kvs/keys
@app.route('/kvs/keys/<string:key>', methods = ['GET', 'PUT'])
def kvs(key):
    #----------------------------------
    #decide whether we're working locally or remotely.
    #find out who the key belongs to
    whichShard = keyShardDict.get(key)
    #if not on this shard
    if(whichShard != selfShardID):

        #non-local GET
        if(request.method == 'GET'):
            #if key/value pair does not exist: give 404 without address.
            if(whichShard is None):
                causalContextString = request.get_json().get("causal-context")
                jsonDict = {
                    "doesExist" : False,
                    "error" : "Key does not exist",
                    "message" : "Error in GET",
                    "causal-context" : causalContextString
                }
                jsonObject = json.dumps(jsonDict)
                return jsonObject, 404
            #key/value pair DOES exist somewhere else:
            #get the list of addresses of the shard with the key-value pair
            correctKeyAddresses = shardAddressesDict.get(whichShard)
            #until we get a response, try getting the value from each node on the shard
            for address in correctKeyAddresses:
                #get the url of the address and endpoint
                baseUrl = ('http://' + address + '/kvs/keys/' + key)
                #send GET to correct URL
                timeoutVal = 4 / len(correctKeyAddresses)
                r = None
                try:
                    r = requests.get(baseUrl, timeout=timeoutVal) #timeout is generous because we want a response
                except:
                    #except means node is down, but there's nothing we can do
                    #besides try another node, so we pass
                    pass
                #retrieve value, if r exists
                if r is not None:
                    value = r.json().get('value')
                #if r is not None and we got back a value
                if value is not None:
                    #no error-- must return valid response
                    causalContextString = request.get_json().get("causal-context")
                    jsonDict = {
                        "doesExist" : True,
                        "message" : "Retrieved successfully",
                        "value" : value,
                        "address" : address,
                        "causal-context" : causalContextString
                    }
                    jsonObject = json.dumps(jsonDict)
                    return jsonObject, 200
                    #should end execution
            #if no node is reachable, send a fail message
            causalContextString = request.get_json().get("causal-context")
            jsonDict = {
                "error" : "Unable to satisfy request",
                "message" : "Error in GET",
                "causal-context" : causalContextString
            }
            jsonObject = json.dumps(jsonDict)
            return jsonObject, 503

        #non-local PUT
        if(request.method == 'PUT'):

            #if it doesn't belong to a shard yet
            if(whichShard is None):
                #decide which shard to put this new key
                whichShard = decideShard()
                #get the list of addresses for the shard we will put it in
                correctKeyAddresses = shardAddressesDict.get(whichShard)
                #print("correctKeyAddresses", file=sys.stderr)
                #print(correctKeyAddresses, file=sys.stderr)

                isRequestGood = None
                #forward the request, and ask nodes on shard to check if its valid or not before modifying keyShard
                for address in correctKeyAddresses:
                    if isRequestGood == True:
                        break
                    else:
                        timeoutVal = 4 / len(correctKeyAddresses)
                        #print(timeoutVal, file=sys.stderr)
                        baseUrl = ('http://' + address + '/kvs/isRequestValidToShard/' + key)
                        try:
                            r = None
                            try:
                                r = requests.put(baseUrl, json={'value' : request.get_json().get('value')}, timeout=timeoutVal)
                                #timeout is generous because we want a response
                                isRequestGood = r.json().get('isRequestGood')
                            except:
                                print("Error:" + str(sys.exc_info()[0]), file=sys.stderr)
                                pass

                            if isRequestGood == False:
                                #return error
                                print("isRequestGood == False", file=sys.stderr)
                                causalContextString = request.get_json().get("causal-context")
                                jsonDict ={
                                    "message": r.json().get("message"),
                                    "error": r.json().get("error"),
                                    "address": address,
                                    "causal-context": causalContextString
                                }
                                jsonObject = json.dumps(jsonDict)
                                return jsonObject, 400
                        except:
                            pass
                            #error, node is down. Nothing we can do, try next node
                #no nodes reachable, return error
                if isRequestGood == None:
                    print("isRequestGood == None", file=sys.stderr)
                    causalContextString = request.get_json().get("causal-context")
                    jsonDict = {
                        "error" : "Unable to satisfy request",
                        "message" : "Error in PUT",
                        "causal-context" : causalContextString
                    }
                    jsonObject = json.dumps(jsonDict)
                    return jsonObject, 503

                #if it hits here, the request is confirmed valid and we can continue as normal
                #tell ourselves where this key belongs
                keyShardDict.update({key:whichShard})
                #broadcast that the chosen node now contains this key
                for shard, addresses in shardAddressesDict.items():
                    for address in addresses:
                        #build URL, send updateKey PUT
                        baseUrl = ('http://' + address + '/kvs/updateKey')
                        #tell everyone <shard> contains <key>
                        try:
                            r = requests.put(baseUrl, json={'shard' : whichShard, 'key' : key}, timeout=0.000001)
                            #set timeout to effective 0, because we don't care about response
                        except:
                            pass
                            #error means node is down

                #we should be okay to send a normal PUT request now, since the shards
                #all know where the key belongs, and it should miss this (whichShard is None) logic block.
                #we can fall through into the next "block" since it just sends a normal PUT


            #else: valid request and key is allocated to a shard
            #get the addresses of nodes on the shard with the key-value pair
            correctKeyAddresses = shardAddressesDict.get(whichShard)
            #print("correctKeyAddresses: " + str(correctKeyAddresses), file=sys.stderr)
            #send to all the nodes on the shard
            statusCode = None
            successAddress = None
            causalContextString = None
            for address in correctKeyAddresses:
                baseUrl = ('http://' + address + '/kvs/keys/' + key)
                timeoutVal = 4 / len(correctKeyAddresses)
                try:
                    myjsonDict = request.json
                    causalContextString = request.get_json().get("causal-context")
                    if(causalContextString is None):
                        #print("causalContextString: " + str(causalContextString), file=sys.stderr)
                        causalContextDict = {}
                        #now = datetime.datetime.now()
                        now = time.time_ns()
                        #print("now:" + str(now), file=sys.stderr)
                        keyInfo = [now, whichShard]
                        causalContextDict.update({key : keyInfo})
                        #print("made it past .update() line", file=sys.stderr)
                        causalContextString = json.dumps(causalContextDict)
                        #print("made it past json.loads line", file=sys.stderr)
                    myjsonDict.update({"causal-context": causalContextString})
                    #myjsonObject = json.dumps(myjsonDict)
                    #print("made it to request", file=sys.stderr)
                    #print("myjsonObject: " + str(myjsonObject), file=sys.stderr)
                    r = requests.put(baseUrl, json=myjsonDict, timeout=timeoutVal)
                    print("passed the request", file=sys.stderr)
                    causalContextString = r.json().get("causal-context")
                    print("causalContextString value: " + str(causalContextString), file=sys.stderr)
                    #timeout is generous because we want a response
                    if statusCode is None:
                        statusCode = r.status_code
                        successAddress = address
                except:
                    print("Error:", file=sys.stderr)
                    print(str(sys.exc_info()[0]), file=sys.stderr)
                    pass
            
            #update our local time for the variable, which is the only one that will get missed
            #and will only get missed if key didn't exist, and we decide it belongs to local shard.
            #if(whichShard == selfShardID):

                
                

            if causalContextString is None:
                print("causalContextString == None", file=sys.stderr)
                jsonDict = {
                    "error" : "Unable to satisfy request",
                    "message" : "Error in PUT",
                    "causal-context" : request.get_json().get("causal-context")
                }
                jsonObject = json.dumps(jsonDict)
                return jsonObject, 503
                #return error, because we couldn't communicate with anyone
            #this block should only get hit if key didn't exist before
            #and we decided to place it locally
            if(whichShard == selfShardID): #don't send address in response
                if(statusCode == 201):
                    #from the previous block of code, our causal context should be updated
                    #and contained in causalContextString
                    jsonDict = {
                        "message" : "Added successfully",
                        "replaced" : False,
                        "causal-context" : causalContextString
                    }
                    jsonObject = json.dumps(jsonDict)
                    return jsonObject, 201
                if(statusCode == 200):
                    jsonDict = {
                        "message" : "Updated successfully",
                        "replaced" : True,
                        "causal-context" : causalContextString
                    }
                    jsonObject = json.dumps(jsonDict)
                    return jsonObject, 200
            #if PUT is non-local
            #if created
            if(r.status_code == 201):
                #return response with added address
                jsonDict = {
                        "message" : "Added successfully",
                        "replaced" : False,
                        "address" : successAddress,
                        "causal-context" : causalContextString
                    }
                jsonObject = json.dumps(jsonDict)
                return jsonObject, 201
            #if updated
            if(r.status_code == 200):
                #Even though it's not on spec, IP is added because Aleck told us to here:
                # https://cse138-fall20.slack.com/archives/C01C01HF58S/p1605068093044400?thread_ts=1605067981.043200&cid=C01C01HF58S
                jsonDict = {
                        "message" : "Updated successfully",
                        "replaced" : True,
                        "address" : successAddress,
                        "causal-context" : causalContextString
                    }
                jsonObject = json.dumps(jsonDict)
                return jsonObject, 200

        #if(request.method == 'DELETE'):

    #else: work locally

    #local handling of GET
    if(request.method == 'GET'):
        #check if value exists
        if(localKvsDict.get(key) is None):
            #causal context does not need updated
            causalContextString = request.get_json().get("causal-context")
            jsonDict = {
                "doesExist" : False,
                "error" : "Key does not exist",
                "message" : "Error in GET",
                "causal-context" : causalContextString
            }
            jsonObject = json.dumps(jsonDict)
            return jsonObject, 404

        #value exists
        #check if our version is outdated, by comparing against the causal-context from client
        #get local timestamp
        ourTime = keyTimeDict.get(key)
        #get causal context's timestamp
        data = request.json
        keyInfo = None
        if(data is not None):
            causalContextString = data.get("causal-context")
            if(causalContextString is not None):
                causalContextDict = json.loads(causalContextString)
                keyInfo = causalContextDict.get(key)
        else:
            #no causal context
            pass
            

        theirTime = None
        if(keyInfo is not None):
            theirTime = keyInfo.get(timestampSlot)
        else:
            pass
        #if they have context, and ours is the same or better
        if((theirTime is not None and ourTime is not None) and (ourTime >= theirTime)):
            #give the client our value
            value = localKvsDict.get(key)
            #update the causal context to have our time
            keyInfo = [ourTime, selfShardID]
            causalContextDict.update({key: keyInfo})
            causalContextString = json.dumps(causalContextDict)
            jsonDict = {
                "doesExist" : True,
                "message" : "Retrieved successfully",
                "value" : value,
                "causal-context" : causalContextString
            }
            jsonObject = json.dumps(jsonDict)
            return jsonObject, 200
        #else if client context is None
        elif(theirTime is None):
            #give the client our local value
            value = localKvsDict.get(key)
            keyInfo = [ourTime, selfShardID]
            causalContextDict = {}
            causalContextDict.update({key: keyInfo})
            causalContextString = json.dumps(causalContextDict)
            jsonDict = {
                "doesExist" : True,
                "message" : "Retrieved successfully",
                "value" : value,
                "causal-context" : causalContextString
            }
            jsonObject = json.dumps(jsonDict)
            return jsonObject, 200
        #else: client has a context and it's more up-to-date than ours, or we are None and they are not
        else:
            #try to retrieve the updated value from the other members of our shard
            addresses = shardAddressesDict.get(selfShardID)
            updatedVal = None
            for address in addresses:
                baseUrl = ('http://' + address + '/kvs/getKeyWithContext/' + key)
                timeoutVal = 4 / len(addresses)
                try:
                    r = requests.put(baseUrl, timeout=timeoutVal)
                    theirTime = r.get_json().get("time")
                    if(theirTime > ourTime):
                        ourTime = theirTime
                        updatedVal = r.get_json().get("value")
                except:
                    pass
            #update our local values
            #overwrite the local time with the correct time (even if it's the same time)
            keyTimeDict.update({key, ourTime})
            #update our value, if their value is newer
            if(updatedVal is not None):
                localKvsDict.update({key, updatedVal})
            #no updatedVal, we couldn't contact anyone; NACK
            else:
                causalContextString = request.get_json().get("causal-context")
                jsonDict = {
                    "error" : "Unable to satisfy request",
                    "message" : "Error in PUT",
                    "causal-context" : request.get_json().get("causal-context")
                }
                jsonObject = json.dumps(jsonDict)
                return jsonObject, 503
            #return the correct value, with an updated causal-context
            keyInfo = [ourTime, selfShardID]
            #update our causal-context obj
            causalContextDict.update({key: keyInfo})
            #serialize our causal-context obj
            causalContextString = json.dumps(causalContextDict)
            jsonDict = {
                "message" : "Retrieved successfully",
                "doesExist" : True,
                "value" : localKvsDict.get(key),
                "causal-context" : causalContextString
            }
            jsonObject = json.dumps(jsonDict)
            return jsonObject, 200 
    #----------------------------------

    #----------------------------------
    #handling PUT
    #local handling of PUT
    if(request.method == 'PUT'):
        #if request has no valid value
        value = request.get_json().get('value')
        if(value is None):
            causalContextString = request.get_json().get('causal-context')
            jsonDict = {
                "error" : "Value is missing",
                "message" : "Error in PUT",
                "causal-context" : causalContextString
            }
            jsonObject = json.dumps(jsonDict)
            return jsonObject, 400

        #if value is valid but key length >50
        if(len(key) > 50):
            causalContextString = request.get_json().get('causal-context')
            jsonDict = {
                "error" : "Key is too long",
                "message" : "Error in PUT",
                "causal-context" : causalContextString
            }
            jsonObject = json.dumps(jsonDict)
            return jsonObject, 400

        #request key and value are valid
        updated = False
        created = False

        #does value already exist?
        if(localKvsDict.get(key) is not None):
            updated = True
        else:
            created = True

        value = request.get_json().get('value')
        localKvsDict.update({key : value})
        #update our local time for that variable
        #now = datetime.datetime.now()
        now = time.time_ns()
        #update the context
        causalContextString = request.get_json().get('causal-context')
        causalContectDict = None
        if(causalContextString is not None):
            causalContextDict = json.loads(causalContextString)
        else:
            causalContextDict = {}

        keyInfo = [now, selfShardID]
        causalContextDict.update({key : keyInfo})
        keyTimeDict.update({key : now})
        causalContextString = json.dumps(causalContextDict)
        if(created == True):
            jsonDict = {
                "message" : "Added successfully",
                "replaced" : False,
                "causal-context" : causalContextString
            }
            jsonObject = json.dumps(jsonDict)
            return jsonObject, 201
        else:
            jsonDict = {
                "message" : "Updated successfully",
                "replaced" : True,
                "causal-context" : causalContextString
            }
            jsonObject = json.dumps(jsonDict)
            return jsonObject, 200

    #----------------------------------

    #----------------------------------
    #handling DELETE
    #local handling of DELETE
    #if(request.method == 'DELETE'):
    #----------------------------------


#behavior for /kvs/updateKey
@app.route('/kvs/updateKey', methods = ['PUT'])
def updateKey():
    #update/add key
    if(request.method == 'PUT'):
        key = request.get_json().get('key')
        shard = request.get_json().get('shard')
        keyShardDict.update({ key : shard })
        return jsonify(
            message="OK"
        ), 200

    #delete key
    #if(request.method == 'DELETE'):


#behavior for /kvs/key-count
@app.route('/kvs/key-count', methods = ['GET'])
def getKeyCount():
    if(request.method == 'GET'):
        #standard response
        keyCount = getLocalKeyCount()
        jsonDict = {"message": "Key count retrieved successfully",
                    "key-count": keyCount,
                    "shard-id": selfShardID}
        jsonObject = json.dumps(jsonDict)
        return jsonObject, 200
        #have to do it this way because the nice jsonify way doesn't work


#behavior for /kvs/shards
@app.route('/kvs/shards', methods = ['GET'])
def getShards():
    if(request.method == 'GET'):
        shardList = []
        for shard, address in shardAddressesDict.items():
           shardList.append(shard)

        return jsonify(message="Shard membership retrieved successfully",
                        shards=shardList), 200


#behavior for /kvs/shards/<id>
@app.route('/kvs/shards/<string:id>', methods = ['GET'])
def getShardInfo(id):
    if(request.method == 'GET'):
        replicas = shardAddressesDict[id]
        count = None
        for address in replicas:
            if count is None:
                timeoutVal = 4 / len(replicas) #something
                baseUrl = ('http://' + address + '/kvs/key-count')
                try:
                    r = requests.get(baseUrl, timeout=timeoutVal)
                    count = r.json().get('key-count')
                except:
                    pass
            else:
                break

        jsonDict = {"message": "Shard information retrieved successfully",
                    "shard-id": id,
                    "key-count": count,
                    "replicas": replicas}
        jsonObject = json.dumps(jsonDict)
        return jsonObject, 200


#behavior for /kvs/updateView
#expects to receive a view string
@app.route('/kvs/updateView', methods=['PUT'])
def updateView():
    if(request.method == 'PUT'):
        #read in and parse viewString
        viewString = request.get_json().get('view')
        viewArray = str(viewString).split(',')
        shardAddressesDict.clear()

        i = 1
        #update nodeAddressDict
        for address in viewArray:
            nodeAddressDict.update({"node" + str(i) : address})
            i += 1

        global replFactor
        replFactor = request.get_json().get('repl-factor')

        #reset shardAddressesDict
        numShards = len(nodeAddressDict) // replFactor
        for i in range(numShards):
            shardID = "shard" + str(i + 1)
            emptyTempList = []
            shardAddressesDict.update({ shardID : emptyTempList})

        #deterministically allocate nodes to shards in shardAddressesDict
        decideNodeToShard()

        global selfShardID #global keyword so we know this isn't a local variable
        #update selfShardID
        for shard, addresses in shardAddressesDict.items():
            for address in addresses:
                if(selfAddress == address):
                    selfShardID = shard

        return "OK", 200


#behavior for /kvs/updateKeyShard
#expects to receive a json dump of a dictionary
@app.route('/kvs/updateKeyShard', methods=['PUT'])
def updateKeyShard():
    if(request.method == 'PUT'):
        keyShardDictString = request.get_json().get('keyShardDictString')
        loadedKeyShardDict = json.loads(keyShardDictString)
        global keyShardDict
        keyShardDict.clear()
        keyShardDict = loadedKeyShardDict.copy()
        return "OK", 200

#rearrangeKeys: causal context is not necessary for rearranging keys.
#rearrangeKeys should only be called during a view-change, where causal context
#   is free to be cleared, according to https://cse138-fall20.slack.com/archives/C01FKJLRZKN/p1606622040051200?thread_ts=1606621491.049500&cid=C01FKJLRZKN
@app.route('/kvs/rearrangeKeys', methods=['PUT'])
def rearrangeKeys():
    if(request.method == 'PUT'):
        localKvsDictCopy = localKvsDict.copy()
        #loop through copy of localKvsDict
        for key, value in localKvsDictCopy.items():
            #get the shard the key:value is supposed to be on
            correctShardID = keyShardDict.get(key)
            #get all the addresses of that shard
            addressList = shardAddressesDict.get(correctShardID)
            #if not the local shard: send to correct places then delete
            if (selfShardID != correctShardID):
                for address in addressList:
                    baseUrl = ('http://' + address + '/kvs/keys/' + key)
                    try:
                        r = requests.put(baseUrl, json={'key' : key, 'value' : value}, timeout=0.000001)
                        #timeout is set to effective 0 because we don't care about the response
                    except:
                        pass
                    
                del localKvsDict[key]
            #else, if it is the local shard, send to everyone else on this shard
            else:
                for address in addressList:
                    baseUrl = ('http://' + address + '/kvs/keys/' + key)
                    try:
                        r = requests.put(baseUrl, json={'key' : key, 'value' : value}, timeout=0.000001)
                        #timeout is set to effective 0 because we don't care about the response
                    except:
                        pass
                #don't delete from local

        return "OK", 200

#behavior for /kvs/view-change
@app.route('/kvs/view-change', methods = ['PUT'])
def putViewChange():

    if(request.method == 'PUT'):
        #get the new replFactor from the PUT request
        global replFactor
        replFactor = request.get_json().get('repl-factor')
        #get the new view from the PUT request
        viewString = request.get_json().get('view')
        #break up the view into an array of addresses
        viewArray = str(viewString).split(',')
        #copy the nodeAddressDict for sending requests to deactivated nodes
        oldNodeAddressDict = nodeAddressDict.copy()
        #clear the current nodeAddressDict
        nodeAddressDict.clear()
        #clear the current shardAddresses dict, too, since nodes can be assigned new shards
        shardAddressesDict.clear()

        #broadcast the new view to members of the old view
        for node, address in oldNodeAddressDict.items():
            #build URL, send updateView PUT
            baseUrl = ('http://' + address + '/kvs/updateView')
            #send the put request with the viewString
            try:
                r = requests.put(baseUrl, json={'view' : viewString, 'repl-factor' : replFactor}, timeout=0.000001)
                #should effective 0 timeout, because we don't know who is up or down and don't care about responses
            except:
                pass

        #send the new view to all members of the new view, which may have repeats
        #but will certainly include the nodes that were excluded by only sending the message
        #to the old group of nodes.
        for address in viewArray:
            baseUrl = ('http://' + address + '/kvs/updateView')
            try:
                r = requests.put(baseUrl, json={'view' : viewString, 'repl-factor' : replFactor}, timeout=0.000001)
                #should use effective 0 timeout, because we don't know who is up and don't care about responses
                #everyone SHOULD be up according to Aleck here: https://cse138-fall20.slack.com/archives/C01FKJLRZKN/p1606788502063700?thread_ts=1606788354.060500&cid=C01FKJLRZKN
            except:
                pass

        #update the nodeAddressDict with the current list of addresses
        i = 1
        for address in viewArray:
            nodeAddressDict.update({"node" + str(i) : address})
            i += 1
        #if we're the last one updated, that probably helps guard against sending stuff to unupdated nodes

        #copy the current keyShardDict
        #might want to send a request to pull all keys in case we're missing some
        tempKeyShardDict = keyShardDict.copy()
        #clear the keyShardDict to be updated with new {key : value} pairs
        keyShardDict.clear()

        nodeList = []
        #get list of addresses to put on shards
        for node, address in nodeAddressDict.items():
            nodeList.append(node)

        #decide number of shards to distribute keys to
        numShards = len(nodeAddressDict) // replFactor
        shardCounter = 1
        shardList = []
        for i in range(numShards):
            emptyTempList = []
            shardID = "shard" + str(shardCounter)
            shardAddressesDict.update({shardID, emptyTempList})
            shardList.append(shardID)
            shardCounter += 1

        #assign replFactor nodes to each shard
        nodeCount = 0
        shardCounter = 1
        for node, address in nodeAddressDict.items():
            if nodeCount == replFactor:
                nodeCount = 0
                shardCounter += 1
            shardID = "shard" + str(shardCounter)
            tempList = shardAddressesDict.get(shardID)
            tempList.append(address)
            shardAddressesDict.update({shardID : tempList})
            nodeCount += 1


        #round-robin redistribute keys to the local keyShardDict
        a = 0
        for key, shard in tempKeyShardDict.items():
            #with 5 shards will mod 5 e.g. 4 mod 5 = 4, 5 mod 5 = 5, so we will never out-of-bounds error
            keyShardDict.update ({ key : shardList[ a % (len(shardList))] })
            a += 1

        #send the new keyShardDict to members of the old view
        for node, address in oldNodeAddressDict.items():
            #build URL, send updateKeyShard PUT
            baseUrl = ('http://' + address + '/kvs/updateKeyShard')
            #serialize the dictionary
            keyShardDictString = json.dumps(keyShardDict)
            #send the dictionary to everyone
            try:
                r = requests.put(baseUrl, json={'keyShardDictString' : keyShardDictString}, timeout=0.000001)
                #timeout set to effective 0 because we try to send, but we don't care about the response
            except:
                pass

        #send the new keyShardDict to members of the new view (will have repeats)
        for address in viewArray:
            baseUrl = ('http://' + address + '/kvs/updateKeyShard')
            keyShardDictString = json.dumps(keyShardDict)
            try:
                r = requests.put(baseUrl, json={'keyShardDictString' : keyShardDictString}, timeout=0.000001)
                #timeout set to effective 0 because we try to send, but we don't care about the response
            except:
                pass
        
        #send a rearrangeKeys() type of broadcast
        #tell everyone in the old view to send their keys to the correct place
        #then delete them from local
        for node, address in oldNodeAddressDict.items():
            #build URL, send rearrangeKeys PUT
            baseUrl = ('http://' + address + '/kvs/rearrangeKeys')
            #send the PUT request, doesn't need additional data
            try:
                r = requests.put(baseUrl, timeout=0.000001)
                #timeout set to effective 0 because we try to send, but don't care about the response
            except:
                pass

        #send a rearrangeKeys() broadcast to everyone in the new view
        #tell everyone in the new view to send their keys to the right place and delete them
        for address in viewArray:
            baseUrl = ('http://' + address + '/kvs/rearrangeKeys')
            try:
                r = requests.put(baseUrl, timeout=0.000001)
                #timeout set to effective 0 because we try to send, but don't care about the response
            except:
                pass

        #all dicts should be up-to-date, all nodes should have the correct {key : value} pairs
        #get the address and keyCount of every node, then return to client

        #create and reply with {message="View change successful", shards=[{shard-id, key-count, replicas}]}
        shardList.clear()
        #list of dictionaries to be returned in json
        dictList = []
        for shard, addresses in shardAddressesDict.items():
            keyCount = None
            retDict = {}
            addressList = []
            retDict.update({'shard-id' : shard})
            retDict.update({'replicas' : addresses})
            for address in addresses:
                addressList.append(address)
                if keyCount is not None:
                    pass
                else:
                    baseUrl = ('http://' + address + '/kvs/key-count')
                    timeoutVal = 4 / len(addresses)
                    try:
                        r = requests.get(baseUrl, timeout=timeoutVal) #timeout is generous because we care about the response
                        keyCount = r.json().get('key-count')
                    except:
                        pass
            retDict.update({'key-count' : keyCount})
            dictList.append(retDict)

        return jsonify(
            message="View change successful",
            shards=dictList
        ), 200



#main driver
if __name__ == '__main__':
    #string of this node's IP:port
    selfAddress = os.getenv('ADDRESS')

    #string of the view
    viewString = os.getenv('VIEW')

    #replication factor
    replFactor = 1 #default of 1, because we can
    if os.getenv('REPL_FACTOR') is not None:
        replFactor = int(os.getenv('REPL_FACTOR'))

    #dictionary that holds {key : shard} to identify which shard a key belongs to
    keyShardDict = {}

    #need a local dictionary to hold this shard's key/value pairs
    localKvsDict = {}

    #dictionary that holds {node : address} to list all nodes and addresses we have
    nodeAddressDict = {}

    #dictionary that holds {shard : [addresses]} to identify the addresses that belong to a shard
    shardAddressesDict = {}

    viewArray = None
    if(viewString is not None):
        #turn viewString into array of addresses
        viewArray = str(viewString).split(',')

        i = 1
        #add all node : address pairs to nodeAddressDict
        for address in viewArray:
            nodeAddressDict.update({"node" + str(i) : address})
            i += 1

    #set up shardAddressesDict
    #need to know how many shards we need (num addresses / how many replicas)
    numShards = len(nodeAddressDict) // replFactor
    for i in range(numShards):
        #create shardID to later reference shards
        shardID = "shard" + str(i + 1)
        #empty list for creating shardAddressesDict
        emptyTempList = []
        #put shardID + empty list into dict
        shardAddressesDict.update({ shardID : emptyTempList})

    #allocate nodes to shards in shardAddressesDict
    decideNodeToShard()

    #verify things worked
    #print(shardAddressesDict.items(), file=sys.stderr)

    #value to decide which shard the local node is in respect to the view
    selfShardID = "default" #default value of "default" to indicate error

    #causal context object should be <key: [timestamp, shard]>
    #<key : timestamp> held locally for checking for outdated timestamp
    #timestamp is overwritten when value is overwritten
    keyTimeDict = {}
    timestampSlot = 0
    shardSlot = 1

    #decide which shardID belongs to local node
    for shard, addresses in shardAddressesDict.items():
        for address in addresses:
            if(selfAddress == address):
                selfShardID = shard


    app.run(host="0.0.0.0", port=13800, debug=True)
