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

var client = (ref OtqClient)(hostname: "localhost", port: Port(6789))
let isConnected = client.connect("yj", "password").waitFor()
echo "is connected? " & $isConnected

test "put 1 message":
  var data = ""
  for i in 0..<100_000:
    data &= $i
  echo "data size: " & $data.len
  sleep(2000)
  client.put("default", 1, @[data]).waitFor()

test "get 1 message":
  let resp = client.get("default", 2).waitFor()
  echo  $resp
