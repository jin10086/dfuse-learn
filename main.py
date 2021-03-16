from http.client import HTTPSConnection

import grpc
import json
import os
import ssl
import sys

from graphql import graphql_pb2_grpc
from graphql.graphql_pb2 import Request

#
# Code from getting started will go here ...
#
def get_token(api_key):
    connection = HTTPSConnection("auth.dfuse.io")
    connection.request(
        "POST",
        "/v1/auth/issue",
        json.dumps({"api_key": api_key}),
        {"Content-type": "application/json"},
    )
    response = connection.getresponse()

    if response.status != 200:
        raise Exception(" Status: %s reason: %s" % (response.status, response.reason))

    token = json.loads(response.read().decode())["token"]
    connection.close()

    return token


def create_client(token, endpoint):
    channel = grpc.secure_channel(
        endpoint,
        credentials=grpc.composite_channel_credentials(
            grpc.ssl_channel_credentials(), grpc.access_token_call_credentials(token)
        ),
    )

    return graphql_pb2_grpc.GraphQLStub(channel)


def stream_ethereum(client):
    # The client can be re-used for all requests, cache it at the appropriate level
    stream = client.Execute(
        Request(
            query=OPERATION_ETH,
            variables=json.dumps({
                "addresses": ["0x7a250d5630b4cf539739df2c5dacb4c659f2488d"],
                "fields": ["FROM", "ERC20_FROM"],
            })
        )
        
    )

    for rawResult in stream:
        if rawResult.errors:
            print("An error occurred")
            print(rawResult.errors)
        else:
            result = json.loads(rawResult.data)
            print(result)
            # for call in result["searchTransactions"]["node"]["matchingCalls"]:
            #     undo = result["searchTransactions"]["undo"]
            #     print(
            #         "Transfer %s -> %s [%s Ether]%s"
            #         % (
            #             call["from"],
            #             call["to"],
            #             call["value"],
            #             " REVERTED" if undo else "",
            #         )
            #     )


dfuse_api_key = os.environ.get("DFUSE_API_KEY")
if dfuse_api_key == None or dfuse_api_key == "your dfuse api key here":
    raise Exception("you must specify a DFUSE_API_KEY environment variable")

token = get_token(dfuse_api_key)
# The client can be re-used for all requests, cache it at the appropriate level

client = create_client(token, "mainnet.eth.dfuse.io:443")


OPERATION_ETH = """subscription ($addresses: [String!]!, $fields: [TRANSACTION_MATCH_FIELD!]!) {
  transactions(addresses: $addresses, matchAnyOf: $fields) {
    hash
    currentState
    transitionName
    transition {
      ... on TrxTransitionPooled {
        transaction {
          ... on Transaction {
            hash
            to
            from
            input {
              data
              json {
                type
                name
                value
              }
            }
          }
        }
      }
      ... on TrxTransitionSpeculativelyExecuted {
        trace {
          ... on TransactionTrace {
            hash
            to
            from
            flatCalls {
              ... on Call {
                index
                parentIndex
                depth
                callType
                from
                to
                value(encoding: HEX)
                gasConsumed
                gasLimit
                status
                failureCause
                inputData
                method {
                  hexSignature
                  textSignature
                }
                returnData
                storageChanges {
                  key
                  address
                  oldValue
                  newValue
                }
                balanceChanges {
                  address
                  oldValue
                  newValue
                  reason
                }
                logs {
                  address
                  topics
                  data
                }
              }
            }
            allLogs {
              ... on EventLog {
                address
                topics
                data
                blockIndex
                transactionIndex
              }
            }
          }
        }
      }
      ... on TrxTransitionMined {
        trace {
          ... on TransactionTrace {
            hash
            to
            from
            flatCalls {
              ... on Call {
                index
                parentIndex
                depth
                callType
                from
                to
                value(encoding: HEX)
                gasConsumed
                gasLimit
                status
                failureCause
                inputData
                method {
                  hexSignature
                  textSignature
                }
                returnData
                storageChanges {
                  key
                  address
                  oldValue
                  newValue
                }
                balanceChanges {
                  address
                  oldValue
                  newValue
                  reason
                }
                logs {
                  address
                  topics
                  data
                }
              }
            }
            allLogs {
              ... on EventLog {
                address
                topics
                data
                blockIndex
                transactionIndex
              }
            }
          }
        }
      }
      ... on TrxTransitionConfirmed {
        confirmations
      }
    }
  }
}
"""

stream_ethereum(client)
# print(OPERATION_ETH)
