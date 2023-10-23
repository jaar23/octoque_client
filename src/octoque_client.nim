import sugar, asyncnet, asyncdispatch, strformat, strutils
from net import Port
import results

const 
  NL        = "\r\L"
  SUCCESS   = "SUCCESS"
  FAIL      = "FAIL"
  PROCEED   = "PROCEED"
  EOR       = "ENDOFRESP"
  CONNECTED = "CONNECTED"
  DECLINE   = "DECLINE"
  PONG      = "PONG"

type
  Error = enum
    SERVER_ERROR,
    CLIENT_ERROR

  Command* = enum
    GET = "GET"
    PUT = "PUT"
    PUTACK = "PUTACK"
    PUBLISH = "PUBLISH"
    SUBSCRIBE = "SUBSCRIBE"
    UNSUBSCRIBE = "UNSUBSCRIBE"
    PING = "PING"
    CLEAR = "CLEAR"
    NEW = "NEW"
    #DISPLAY = "DISPLAY" ## display option not available in client
    CONNECT = "CONNECT"
    DISCONNECT = "DISCONNECT"
    #ACKNOWLEDGE = "ACKNOWLEDGE" ## ack not available in client

  Protocol* = enum
    OTQ,
    CUSTOM

  TransferMethod* = enum
    STREAM,
    BATCH

  ConnectionType* = enum
    BROKER,
    PUBSUB

  OtqResult* = Result[string, Error]

  OtqClient* = object
    hostname*: string
    port*: Port
    protocol: Protocol
    connection: AsyncSocket
    connected = false

  OtqRequest* = object
    protocol*: Protocol
    length*: uint32
    transferMethod*: TransferMethod
    payloadRows*: uint8
    numberOfMsg*: uint8
    command*: Command
    topic*: string
    data*: string


proc newOtqClient(hostname: string, port: Port, protocol: Protocol): ref OtqClient =
  var otqclient = (ref OtqClient)(hostname: hostname, port: port, protocol: protocol)
  return otqclient


proc readEOR(client: ref OtqClient):Future[void] {.async.} =
  while true:
    let resp = await client.connection.recvLine()
    if resp == EOR: break


proc exception(msg: string): string = 
  let declineMsg = msg.split(":")
  if declineMsg[0] == DECLINE: return declineMsg[1]
  else: return "unknown error: " & msg


proc successOrFail(client: ref OtqClient, printException = false): Future[bool] {.async.} =
  var resp = await client.connection.recvLine()
  if resp.strip() == SUCCESS: 
    await client.readEOR()
    result = true
  elif resp.strip() == PONG:
    result = true
  elif resp.strip() == FAIL:
    result = false
  else:
    if printException: echo exception(resp)
    result = false
  await client.readEOR()


proc connected*(client: ref OtqClient): bool = client.connected


proc connect*(client: ref OtqClient, username, password: string): Future[bool] {.async.} =
  var conn = await asyncnet.dial(client.hostname, client.port)
  let otqcommand = &"{client.protocol} {Command.CONNECT} {username} {password}{NL}"
  await conn.send(otqcommand)
  var resp = await conn.recvLine()
  if resp == CONNECTED:
    client.connection = conn
    ## read endofresp
    await client.readEOR()
    client.connected = true
    return true
  else:
    echo exception(resp)
    discard await conn.recvLine()
    client.connected = false
    return false


proc disconnect*(client: ref OtqClient): Future[void] {.async.} =
  let otqcommand = &"{client.protocol} {DISCONNECT}{NL}"
  await client.connection.send(otqcommand)
  await client.readEOR()
  client.connection = nil


proc put*(client: ref OtqClient, topic: string, payloadRows: uint8, 
          data: seq[string], transferMethod: TransferMethod = BATCH, 
          sentAck = false): Future[void] {.async.} =
  if payloadRows != data.len.uint8():
    echo "error: payload rows is not match with payload data size"
    return
  let command = if not sentAck: Command.PUT else: Command.PUTACK
  let otqcommand = &"{client.protocol} {command} {topic} {payloadRows} {transferMethod}{NL}"
  #echo otqcommand
  await client.connection.send(otqcommand)
  var resp = await client.connection.recvLine()
  #echo "resp: " & resp
  if resp.strip() == PROCEED:
    for d in data: await client.connection.send(d & NL)
    await client.readEOR()
  else: echo exception(resp)


proc get*(client: ref OtqClient, topic: string, numberOfMsgs: uint8 = 1,
    transferMethod: TransferMethod = BATCH): Future[seq[string]] {.async.} =
  result = newSeq[string]()
  let otqcommand = &"{client.protocol} {Command.GET} {topic} {numberOfMsgs} {transferMethod}{NL}"
  await client.connection.send(otqcommand)
  var resp = await client.connection.recvLine()
  if resp.strip() == PROCEED:
    while true:
      var data = await client.connection.recvLine()
      if data == EOR: break
      else: result.add(data)


## going to change in future
## when server change publish to publish to multiple topics
proc publish*(client: ref OtqClient, topics: seq[string], payloadRows: uint8,
              data: seq[string], transferMethod: TransferMethod = BATCH): Future[void] {.async.} =
  for topic in topics:
    await client.put(topic, payloadRows, data, transferMethod)
    echo "sent to {topic}"


proc subscribe*(client: ref OtqClient, topic: string, 
                cb: (data: string, unsubscribe: bool) -> void): Future[void] {.async.} =
  let otqcommand = &"{client.protocol} {SUBSCRIBE} {topic}{NL}"
  var unsubscribe = false
  await client.connection.send(otqcommand)
  while not unsubscribe:
    let resp = await client.connection.recvLine()
    cb(resp, unsubscribe)
    if unsubscribe:
      await client.readEOR()
      break


proc unsubscribe*(client: ref OtqClient, topic: string): Future[void] {.async.} =
  let otqcommand = &"{client.protocol} {UNSUBSCRIBE} {topic}{NL}"
  await client.connection.send(otqcommand)
  await client.readEOR()


proc newtopic*(client: ref OtqClient, topicName: string, connectionType: ConnectionType,
               capacity: int = 0, numberOfThread: uint = 1): Future[bool] {.async.} =
  let otqcommand = &"{client.protocol} {NEW} {topicName} {connectionType}{NL}"
  await client.connection.send(otqcommand)
  return await client.successOrFail() 


proc cleartopic*(client: ref OtqClient, topic: string): Future[bool] {.async.} =
  let otqcommand = &"{client.protocol} {CLEAR} {topic}{NL}"
  await client.connection.send(otqcommand)
  return await client.successOrFail() 


proc ping*(client: ref OtqClient, topic: string): Future[bool] {.async.} =
  let otqcommand = &"{client.protocol} {PING} {topic}{NL}"
  await client.connection.send(otqcommand)
  return await client.successOrFail()


