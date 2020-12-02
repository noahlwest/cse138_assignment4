#imports
import os, requests, sys, json
from flask import Flask, jsonify, request, Response

#create app with flask
app = Flask(__name__)

#basic structure:
#receive request
#   determine whether to work locally or request remote work
#   verify request
#   if necessary, send requests
#   return to client

#decideShard()
#decides which shard a new key should belong to
#requests the key-count from every shard, assigns key to min key-count shard
#returns the shard that the new key should belong to
def decideShard():
    minKeys = getLocalKeyCount() #min defaults to local
    whichShard = selfShardID #defaults to local shardID
    #for each shard:address pair, see who has the least amount of keys
    for shard, address in shardAddressDict.items():
        baseUrl = ('http://' + address + '/kvs/key-count')
        r = requests.get(baseUrl)
        requestedKeyCount = r.json().get('key-count')
        if(requestedKeyCount < minKeys):
            minKeys = requestedKeyCount
            whichShard = shard
    return whichShard


#getLocalKeyCount()
#gets the local keycount by getting the length of localKvsDict
#returns the amount of keys in localKvsDict
def getLocalKeyCount():
    keyCount = len(localKvsDict)
    return keyCount


#behavior for /kvs/keys
@app.route('/kvs/keys/<string:key>', methods = ['GET', 'PUT'])
def kvs(key):
    #----------------------------------
    #decide whether we're working locally or remotely.
    #find out who the key belongs to
    whichShard = keyShardDict.get(key)
    #if not on this shard
    if(whichShard != selfShardID):

        if(request.method == 'GET'):
            #if key/value pair does not exist: give 404 without address.
            if(whichShard is None):
                return jsonify(
                    doesExist=False,
                    error="Key does not exist",
                    message="Error in GET"
                ), 404
            #key/value pair DOES exist somewhere else:
            #get the address of the shard with the key-value pair
            correctKeyAddress = shardAddressDict.get(whichShard)
            #get the url of the address and endpoint
            baseUrl = ('http://' + correctKeyAddress + '/kvs/keys/' + key)
            #send GET to correct URL
            r = requests.get(baseUrl)
            #retrieve value
            value = r.json().get('value')
            #no error-- must return valid response
            #even though it's not on the spec, sends an IP response because Aleck told us to
            # at https://cse138-fall20.slack.com/archives/C01C01HF58S/p1605068093044400?thread_ts=1605067981.043200&cid=C01C01HF58S
            return jsonify(
                doesExist=True,
                message="Retrieved successfully",
                value=value,
                address=correctKeyAddress
            ), 200

        if(request.method == 'PUT'):
            #if there's no value in the json
            value = request.get_json().get('value')
            if(value is None):
                return jsonify(
                    error="Value is missing",
                    message="Error in PUT"
                ), 400
            #if value is valid but key length >50
            if(len(key) > 50):
                return jsonify(
                    error="Key is too long",
                    message="Error in PUT"
                ), 400
            #valid request
            if(whichShard is None):
                #decide which shard to put this new key
                whichShard = decideShard()
                #get address of shard we will put it in
                correctKeyAddress = shardAddressDict.get(whichShard)
                #tell ourselves where this key belongs
                keyShardDict.update({key:whichShard})
                #tell all nodes (including self, but that's okay) that the chosen node now contains this key
                for shard, address in shardAddressDict.items():
                    #build URL, send updateKey PUT
                    baseUrl = ('http://' + address + '/kvs/updateKey')
                    #tell everyone <shard> contains <key>
                    r = requests.put(baseUrl, json={'shard' : whichShard, 'key' : key})
                    #no error checking, just assuming things work
                
                #we should be okay to send a normal PUT request now, since the shards
                #all know where the key belongs, and it should miss this (whichShard is None) logic block.
                #we can fall through into the next "block" since it just sends a normal PUT
                

            #else: valid request and key is allocated to a shard
            #get the address of the shard with the key-value pair
            correctKeyAddress = shardAddressDict.get(whichShard)
            #get the url of the address and endpoint
            baseUrl = ('http://' + correctKeyAddress + '/kvs/keys/' + key)
            #send PUT to correct URL
            r = requests.put(baseUrl, json=request.get_json())
            #this block should only get hit if key didn't exist before
            #and we decide to place it locally
            if(whichShard == selfShardID): #don't send address in response
                if(r.status_code == 201):
                    return jsonify(
                        message="Added successfully",
                        replaced=False
                    ), 201
                if(r.status_code == 200):
                    return jsonify(
                        message="Updated successfully",
                        replaced=True
                    ), 200
            #if PUT is non-local
            #if created
            if(r.status_code == 201):
                #return response with added address
                return jsonify(
                    message="Added successfully",
                    replaced=False,
                    address=correctKeyAddress
                ), 201
            #if updated
            if(r.status_code == 200):
                #Even though it's not on spec, IP is added because Aleck told us to here:
                # https://cse138-fall20.slack.com/archives/C01C01HF58S/p1605068093044400?thread_ts=1605067981.043200&cid=C01C01HF58S
                return jsonify(
                    message="Updated successfully",
                    replaced=True,
                    address=correctKeyAddress
                ), 200
        
        #if(request.method == 'DELETE'):

    #else: work locally
    
    #local handling of GET
    if(request.method == 'GET'):
        #check if value exists
        if(localKvsDict.get(key) is None):
            return jsonify(
                doesExist=False,
                error="Key does not exist",
                message="Error in GET"
            ), 404

        #value exists
        value = localKvsDict.get(key)
        return jsonify(
            doesExist=True,
            message="Retrieved successfully",
            value=value
        ), 200
    #----------------------------------

    #----------------------------------
    #handling PUT
    #local handling of PUT
    if(request.method == 'PUT'):
        #if request has no valid value
        value = request.get_json().get('value')
        if(value is None):
            return jsonify(
                error="Value is missing",
                message="Error in PUT"
            ), 400

        #if value is valid but key length >50
        if(len(key) > 50):
            return jsonify(
                error="Key is too long",
                message="Error in PUT"
            ), 400

        #request key and value are valid
        updated = False
        created = False

        #does value already exist?
        if(localKvsDict.get(key) is not None):
            updated = True
        else:
            created = True
        
        localKvsDict.update({key:value})

        if(created == True):
            return jsonify(
                message="Added successfully",
                replaced=False
            ), 201
        else:
            return jsonify(
                message="Updated successfully",
                replaced=True
            ), 200
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
        jsonDict = {"message": "Key count retrieved successfully", "key-count": keyCount}
        jsonObject = json.dumps(jsonDict)
        return jsonObject, 200
        #have to do it this way because the nice jsonify way doesn't work


#behavior for /kvs/updateView
#expects to receive a view string
@app.route('/kvs/updateView', methods=['PUT'])
def updateView():
    if(request.method == 'PUT'):
        #read in and parse viewString
        viewString = request.get_json().get('view')
        viewArray = str(viewString).split(',')
        shardAddressDict.clear()

        i = 1
        #update shardAddressDict
        for address in viewArray:
            shardAddressDict.update({"node" + str(i) : address})
            i += 1

        #update selfShardID
        for shard, address in shardAddressDict.items():
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

@app.route('/kvs/rearrangeKeys', methods=['PUT'])
def rearrangeKeys():
    if(request.method == 'PUT'):
        localKvsDictCopy = localKvsDict.copy()
        #loop through copy of localKvsDict
        for key, value in localKvsDictCopy.items():
            correctKeyShard = keyShardDict[key]
            correctKeyAddress = shardAddressDict[correctKeyShard]
            if(selfAddress != correctKeyAddress):
                #send key : value to correct place
                baseUrl = ('http://' + correctKeyAddress + '/kvs/keys/' + key)
                r = requests.put(baseUrl, json={'key' : key, 'value' : value})
                #delete key : value from local dict
                del localKvsDict[key]

        return "OK", 200

#behavior for /kvs/view-change
@app.route('/kvs/view-change', methods = ['PUT'])
def putViewChange():

    if(request.method == 'PUT'):
        #get the new view from the PUT request
        viewString = request.get_json().get('view')
        #break up the view into an array of addresses
        viewArray = str(viewString).split(',')
        #copy the shardAddressDict for sending requests to deactivated nodes
        oldShardAddressDict = shardAddressDict.copy()
        #clear the current shardAddressDict
        shardAddressDict.clear()

        #send the new view to members of the old view
        for shard, address in oldShardAddressDict.items():
            #build URL, send updateView PUT
            baseUrl = ('http://' + address + '/kvs/updateView')
            #send the put request with the viewString
            r = requests.put(baseUrl, json={'view' : viewString})

        #send the new view to all members of the new view, which may have repeats
        #but will certainly include the nodes that were excluded by only sending the message
        #to the old group of nodes.
        for address in viewArray:
            baseUrl = ('http://' + address + '/kvs/updateView')
            r = requests.put(baseUrl, json={'view' : viewString})
        

        #update the shardAddressDict with the current view
        i = 1
        for address in viewArray:
            shardAddressDict.update({"node" + str(i) : address})
            i += 1
        #if we're the last one updated, that probably helps guard against sending stuff to unupdated nodes
        #next assignment maybe we need ACKs?

        #copy the current keyShardDict
        tempKeyShardDict = keyShardDict.copy()
        #clear the keyShardDict to be updated with new {key : value} pairs
        keyShardDict.clear()

        shardList = []
        #get list of shards to distribute keys to
        for shard, address in shardAddressDict.items():
            shardList.append(shard)

        #round-robin redistribute keys to the local keyShardDict
        a = 0
        for key, shard in tempKeyShardDict.items():
            #with 5 shards will mod 5 e.g. 4 mod 5 = 4, 5 mod 5 = 5, so we will never out-of-bounds error
            keyShardDict.update ({ key : shardList[ a % (len(shardList))] })
            a += 1

        #send the new keyShardDict to members of the old view
        for shard, address in oldShardAddressDict.items():
            #build URL, send updateKeyShard PUT
            baseUrl = ('http://' + address + '/kvs/updateKeyShard')
            #serialize the dictionary
            keyShardDictString = json.dumps(keyShardDict)
            #send the dictionary to everyone
            r = requests.put(baseUrl, json={'keyShardDictString' : keyShardDictString})

        #send the new keyShardDict to members of the new view (will have repeats)
        for address in viewArray:
            baseUrl = ('http://' + address + '/kvs/updateKeyShard')
            keyShardDictString = json.dumps(keyShardDict)
            r = requests.put(baseUrl, json={'keyShardDictString' : keyShardDictString})
        
        #send a rearrangeKeys() type of broadcast
        #tell everyone in the old view to send their keys to the correct place
        #then delete them from local
        for shard, address in oldShardAddressDict.items():
            #build URL, send rearrangeKeys PUT
            baseUrl = ('http://' + address + '/kvs/rearrangeKeys')
            #send the PUT request, doesn't need additional data
            r = requests.put(baseUrl)

        #send a rearrangeKeys() broadcast to everyone in the new view
        #tell everyone in the new view to send their keys to the right place and delete them
        for address in viewArray:
            baseUrl = ('http://' + address + '/kvs/rearrangeKeys')
            r = requests.put(baseUrl)

        #all dicts should be up-to-date, all nodes should have the correct {key : value} pairs
        #get the address and keyCount of every node, then return to client

        #create and reply with couples of {address : keyCount}
        #list of dictionaries to be returned in json
        dictList = []
        for shard, address in shardAddressDict.items():
            #{"address" : address, "key-count" : keyCount} dict
            keyCounts = {}
            baseUrl = ('http://' + address + '/kvs/key-count')
            r = requests.get(baseUrl)
            currentKeyCount = r.json().get('key-count')
            keyCounts.update({'address' : address})
            keyCounts.update({'key-count' : currentKeyCount})
            dictList.append(keyCounts)


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

    #dictionary that holds {key : shard} to identify which shard a key belongs to
    keyShardDict = {}

    #need a local dictionary to hold this shard's key/value pairs
    localKvsDict = {}

    #dictionary that holds {shard : address} to identify the address of any shard
    shardAddressDict = {}
    
    viewArray = None
    if(viewString is not None):
        #turn viewString into array of addresses
        viewArray = str(viewString).split(',')

        i = 1
        #add all shard : address pairs to shardAddressDict
        for address in viewArray:
            shardAddressDict.update({"node" + str(i) : address})
            i += 1

    #value to decide which shard the local node is in respect to the view
    selfShardID = "default" #default value of "default" to indicate error
    #no error checking right now, since we're assuming whoever operates this
    #knows how things work.

    #decide which shardID belongs to local node
    for shard, address in shardAddressDict.items():
        if(selfAddress == address):
            selfShardID = shard
        

    app.run(host="0.0.0.0", port=13800, debug=True)
