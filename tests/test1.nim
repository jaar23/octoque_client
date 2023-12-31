# This is just an example to get you started. You may wish to put all of your
# tests into a single file, or separate them into multiple `test1`, `test2`
# etc. files (better names are recommended, just make sure the name starts with
# the letter 't').
#
# To run these tests, simply execute `nimble test`.

import unittest
from net import Port
import octoque_client
import asyncdispatch, os

var client = newOtqClient("localhost", Port(6789), "admin", "password")

test "put 1 message":
  var data = ""
  for i in 0..<100:
    data &= $i
  #echo "data size: " & $data.len
  client.put("default", 1, @[data]).waitFor()
  #sleep(1000)

test "get 1 message":
  let resp = client.get("default", 2).waitFor()
  #echo  $resp
  #sleep(1000)



test "publish message":
  var msgs = newSeq[string]()
  for m in 0..<5:
    var data = ""
    for i in 0..<100:
      data &= $i
    msgs.add(data)
  client.publish(@["pubsub"], msgs.len.uint8(), msgs).waitFor()
  #sleep(1000)

proc testSubscribeAsync(): Future[void] {.async.} =
  await client.put("default", 1, @["hello world"])
  proc sub(topic: string, data: string): void =
    echo ">>>>>" & data
  client.subscribe("pubsub", sub).await
  echo "unsubsribed"
  echo "called unsub"
  let iscall = await withTimeout(client.unsubscribe("pubsub"), 2000)
  return


test "subscribe message":
  asyncCheck testSubscribeAsync()
  client.disconnectFrom("pubsub").waitFor()
