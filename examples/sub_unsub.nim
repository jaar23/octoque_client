import octoque_client, asyncdispatch
import random, os

var otqclient = newOtqClient("localhost", Port(6789), "yj", "password")

proc rndStr: string =
  for _ in 0..10:
    add(result, char(rand(int('A') .. int('z'))))

proc listen() {.async.} =
  proc echoMsg(topic, msg: string) =
    echo "received >>> " & msg

  await otqclient.subscribe("pubsub", echoMsg)
 
proc main() {.async.} =
  await otqclient.put("pubsub", 5, @[rndStr(), rndStr(), rndStr(), rndStr(), rndStr()])
  asyncCheck listen()
  #sleep(10000)
  await otqclient.put("pubsub", 5, @[rndStr(), rndStr(), rndStr(), rndStr(), rndStr()])
  echo "unsubscribe..."
  await otqclient.unsubscribe("pubsub")


when isMainModule:
  asyncCheck main()
  runForever()
