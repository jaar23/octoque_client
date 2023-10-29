import sugar, asyncnet, asyncdispatch, strformat, strutils, sequtils
from net import Port
import results

const
  NL = "\r\L"
  SUCCESS = "SUCCESS"
  FAIL = "FAIL"
  PROCEED = "PROCEED"
  EOR = "ENDOFRESP"
  CONNECTED = "CONNECTED"
  DECLINE = "DECLINE"
  PONG = "PONG"

type
  Error = enum
    SERVER_ERROR,
    CLIENT_ERROR

  ConnTypeError = object of CatchableError
  ConnectionError = object of CatchableError

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

  OtqConnection = object
    connection*: AsyncSocket
    connected = false
    id: int
    connectionType: ConnectionType
    occupied = false
    subscribeTo* = ""

  OtqClient* = object of RootObj
    hostname*: string
    port*: Port
    username: string
    password: string
    protocol: Protocol
    connections: seq[ref OtqConnection]

  PubsubClient* = object of OtqClient
    unsubscribed = false

  OtqRequest* = object
    protocol*: Protocol
    length*: uint32
    transferMethod*: TransferMethod
    payloadRows*: uint8
    numberOfMsg*: uint8
    command*: Command
    topic*: string
    data*: string


proc newOtqClient*(hostname: string, port: Port, username,
    password: string = ""): ref OtqClient =
  var connections = newSeq[ref OtqConnection]()
  return (ref OtqClient)(
    hostname: hostname,
    port: port,
    username: username,
    password: password,
    protocol: OTQ,
    connections: connections
  )


proc readEOR(otqconn: ref OtqConnection): Future[void] {.async.} =
  while true:
    #echo "reading util EOR"
    let resp = await otqconn.connection.recvLine()
    if resp == EOR: break


proc exception(msg: string): string =
  if msg == "": return ""
  let declineMsg = msg.split(":")
  if declineMsg[0] == DECLINE: return declineMsg[1]
  else: return "unknown error: " & msg


proc successOrFail(otqconn: ref OtqConnection, printException = false): Future[
    bool] {.async.} =
  var resp = await otqconn.connection.recvLine()
  if resp.strip() == SUCCESS:
    await otqconn.readEOR()
    result = true
  elif resp.strip() == PONG:
    result = true
  elif resp.strip() == FAIL:
    result = false
  else:
    if printException: echo exception(resp)
    result = false
  await otqconn.readEOR()


proc connect*(client: ref OtqClient, connectionType: ConnectionType): Future[(
    bool, ref OtqConnection)] {.async.} =
  var conn = await asyncnet.dial(client.hostname, client.port)
  let otqcommand = &"{client.protocol} {CONNECT} {client.username} {client.password}{NL}"
  await conn.send(otqcommand)
  var resp = await conn.recvLine()
  if resp == CONNECTED:
    let id = client.connections.len + 1
    var otqconn = (ref OtqConnection)(
      connection: conn,
      connected: true,
      connectionType: connectionType,
      id: id
    )
    client.connections.add(otqconn)
    #echo "connections: " & $client.connections.len
    await otqconn.readEOR()
    return (true, otqconn)
  else:
    echo exception(resp)
    return (false, nil)


proc acquireConnection(client: ref OtqClient,
    connectionType: ConnectionType): Future[ref OtqConnection] {.async.} =
  let availableConn = client.connections.filter(c => c.connectionType ==
      connectionType and c.connected)
  if availableConn.len == 0:
    #echo "have no available connection" & $connectionType
    var (connected, otqconn) = await client.connect(connectionType)
    if connected:
      #echo "acquired new connection " & $connectionType
      #echo "otqconn id: " & $otqconn.id
      otqconn.occupied = true
      return otqconn
    else:
      raise newException(ConnectionError, "unable to acquire connection from server")
  else:
    #echo "reusing connection" & $connectionType
    #echo "avalable connections: " & $availableConn.len
    var conn: ref OtqConnection
    for c in availableConn:
      if not c.occupied:
        conn = c
        c.occupied = true
        #echo "found existing connection...."
        break
    return conn


proc releaseConnection(client: ref OtqClient,
    otqconn: ref OtqConnection): void =
  otqconn.occupied = false
  #echo &"otqconn {otqconn.id} is released"


proc disconnect*(client: ref OtqClient, otqconn: ref OtqConnection): Future[
    void] {.async.} =
  let otqcommand = &"{client.protocol} {DISCONNECT}{NL}"
  await otqconn.connection.send(otqcommand)
  #echo "disconnect"
  let pos = client.connections.find(otqconn)
  client.connections.delete(pos)


proc disconnectFrom*(client: ref OtqClient, topic: string): Future[
    void] {.async.} =
  var otqconn: ref OtqConnection
  for conn in client.connections:
    if conn.subscribeTo == topic:
      otqconn = conn
      break
  let otqcommand = &"{client.protocol} {DISCONNECT}{NL}"
  if otqconn != nil:
    await otqconn.connection.send(otqcommand)
    #echo "disconnect"
    let pos = client.connections.find(otqconn)
    client.connections.delete(pos)


proc put*(client: ref OtqClient, topic: string, payloadRows: uint8,
          data: seq[string], transferMethod: TransferMethod = BATCH,
          sentAck = false): Future[void] {.async.} =
  if payloadRows != data.len.uint8():
    echo "error: payload rows is not match with payload data size"
    return
  var otqconn = await client.acquireConnection(BROKER)
  #echo "put otqconn id:" & $otqconn.id
  let command = if not sentAck: Command.PUT else: Command.PUTACK
  let otqcommand = &"{client.protocol} {command} {topic} {payloadRows} {transferMethod}{NL}"
  await otqconn.connection.send(otqcommand)
  var resp = await otqconn.connection.recvLine()
  if resp.strip() != "" and resp.strip() == PROCEED:
    for d in data:
      await otqconn.connection.send(d & NL)
    await otqconn.readEOR()
  else: echo exception(resp)
  client.releaseConnection(otqconn)


proc put*(client: ref OtqClient, topic: string, payloadRows: uint8,
          data: seq[string], otqconn: ref OtqConnection,
              transferMethod: TransferMethod = BATCH,
          sentAck = false): Future[void] {.async.} =
  if payloadRows != data.len.uint8():
    echo "error: payload rows is not match with payload data size"
    return
  let command = if not sentAck: Command.PUT else: Command.PUTACK
  let otqcommand = &"{client.protocol} {command} {topic} {payloadRows} {transferMethod}{NL}"
  await otqconn.connection.send(otqcommand)
  var resp = await otqconn.connection.recvLine()
  if resp.strip() != "" and resp.strip() == PROCEED:
    for d in data:
      await otqconn.connection.send(d & NL)
    await otqconn.readEOR()
  else: echo exception(resp)


proc get*(client: ref OtqClient, topic: string, numberOfMsgs: uint8 = 1,
    transferMethod: TransferMethod = BATCH): Future[seq[string]] {.async.} =
  result = newSeq[string]()
  var otqconn = await client.acquireConnection(BROKER)
  let otqcommand = &"{client.protocol} {GET} {topic} {numberOfMsgs} {transferMethod}{NL}"
  await otqconn.connection.send(otqcommand)
  var resp = await otqconn.connection.recvLine()
  if resp.strip() == PROCEED:
    while true:
      var data = await otqconn.connection.recvLine()
      if data == EOR:
        #echo "get is read-ed EOR"
        break
      else: result.add(data)
  client.releaseConnection(otqconn)


## going to change in future
## when server change publish to publish to multiple topics
proc publish*(client: ref OtqClient, topics: seq[string], payloadRows: uint8,
              data: seq[string], transferMethod: TransferMethod = BATCH): Future[
                  void] {.async.} =
  var otqconn = await client.acquireConnection(BROKER)
  for topic in topics:
    await client.put(topic, payloadRows, data, otqconn, transferMethod)
    echo &"sent to {topic}"
  client.releaseConnection(otqconn)


proc subscribe*(client: ref OtqClient, topic: string,
                cb: (topic: string, data: string) -> void): Future[
                    void] {.async.} =
  var otqconn = await client.acquireConnection(PUBSUB)
  #echo "subscribe otqconn id:" & $otqconn.id
  try:
    if otqconn.subscribeTo == topic:
      raise newException(ConnectionError, "topic is already subscribed")
    # if client.connectionType != PUBSUB:
    #   raise newException(ConnTypeError, "client connection type is invalid, only PUBSUB client can subscribe to topic")
    let otqcommand = &"{client.protocol} {SUBSCRIBE} {topic}{NL}"
    await otqconn.connection.send(otqcommand)
    otqconn.subscribeTo = topic
    while otqconn.subscribeTo != "" and otqconn.occupied:
      let resp = await otqconn.connection.recvLine()
      if resp.strip().len > 0:
        if resp == PROCEED:
          continue
        else:
          cb(topic, resp)
      else:
        await otqconn.connection.send("ACKNOWLEDGE\n")
      if otqconn.subscribeTo == "":
        echo "unsubscribe"
        echo "exit subscribe"
        break
  except ConnTypeError as cerr:
    raise newException(ConnTypeError, cerr.msg)
  except:
    raise newException(CatchableError, getCurrentExceptionMsg())
  finally:
    await client.disconnect(otqconn)


proc unsubscribe*(client: ref OtqClient, topic: string): Future[
    void] {.async.} =
  var otqconns = client.connections.filter(c => c.subscribeTo == topic)
  let otqcommand = &"{client.protocol} {UNSUBSCRIBE} {topic}{NL}"
  for conn in otqconns:
    await conn.connection.send(otqcommand)
    await client.disconnect(conn)


proc newtopic*(client: ref OtqClient, topicName: string, connectionType: ConnectionType,
               capacity: int = 0, numberOfThread: uint = 1): Future[
                   bool] {.async.} =
  var otqconn = await client.acquireConnection(BROKER)
  let otqcommand = &"{client.protocol} {NEW} {topicName} {connectionType}{NL}"
  await otqconn.connection.send(otqcommand)
  return await otqconn.successOrFail()


proc cleartopic*(client: ref OtqClient, topic: string): Future[bool] {.async.} =
  var otqconn = await client.acquireConnection(BROKER)
  let otqcommand = &"{client.protocol} {CLEAR} {topic}{NL}"
  await otqconn.connection.send(otqcommand)
  return await otqconn.successOrFail()


proc ping*(client: ref OtqClient, topic: string): Future[bool] {.async.} =
  var otqconn = await client.acquireConnection(BROKER)
  let otqcommand = &"{client.protocol} {PING} {topic}{NL}"
  await otqconn.connection.send(otqcommand)
  return await otqconn.successOrFail()


