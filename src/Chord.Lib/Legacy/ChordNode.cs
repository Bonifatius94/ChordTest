﻿// using Chord.Lib.Message;
// using Chord.Lib.Protocol;
// using Microsoft.Extensions.Logging;
// using System;
// using System.Collections.Generic;
// using System.Linq;
// using System.Net;
// using System.Net.Sockets;
// using System.Numerics;
// using System.Threading;
// using System.Threading.Tasks;

// namespace Chord.Lib
// {
//     /// <summary>
//     /// An implementation of a P2P node running the chord protocol.
//     /// </summary>
//     public class ChordNode
//     {
//         #region Constructor

//         /// <summary>
//         /// Initialize a new chord node with the given IP settings.
//         /// </summary>
//         /// <param name="localEndpoint">The local IP endpoint configuration of the node.</param>
//         /// <param name="logger">The local IP endpoint configuration of the node.</param>
//         public ChordNode(IPEndPoint localEndpoint, ILogger logger)
//         {
//             // apply logger
//             _logger = logger;

//             // apply IP settings
//             Local = new ChordEndpoint(localEndpoint);
//         }

//         #endregion Constructor

//         #region Members

//         /// <summary>
//         /// The logger handle for writing to the console output.
//         /// </summary>
//         private readonly ILogger _logger;

//         // TODO: think of merging the client / server code into this class
//         private readonly ChordClient _client = new ChordClient();
//         private readonly ChordServer _server = new ChordServer();

//         /// <summary>
//         /// The cancellation handle for exiting the message listener gracefully.
//         /// </summary>
//         private CancellationTokenSource _serverListenerCancel;

//         /// <summary>
//         /// The chord node's local IP endpoint configuration.
//         /// </summary>
//         public ChordEndpoint Local { get; private set; }

//         /// <summary>
//         /// The chord node's successors.
//         /// </summary>
//         public IList<ChordEndpoint> Successors { get; private set; } = new List<ChordEndpoint>();

//         /// <summary>
//         /// The chord node's predecessor.
//         /// </summary>
//         public ChordEndpoint Predecessor { get; private set; }

//         /// <summary>
//         /// The chord node's finger table SHA-1 hash identificators.
//         /// </summary>
//         public IList<ChordEndpoint> FingerList { get; private set; } = new List<ChordEndpoint>();

//         /// <summary>
//         /// The chord node's direct successor.
//         /// </summary>
//         public ChordEndpoint Successor => Successors.FirstOrDefault();

//         /// <summary>
//         /// Indicates the health state of the chord node.
//         /// </summary>
//         public ChordHealthState HealthState { get; set; } = ChordHealthState.Unjoined;
//         // TODO: assign the health state properly 

//         /// <summary>
//         /// An enumeration indicating the health state of a chord node.
//         /// </summary>
//         public enum ChordHealthState
//         {
//             /// <summary>
//             /// Chord node is not part of a network and therefore unusable.
//             /// </summary>
//             Unjoined,

//             /// <summary>
//             /// Chord node is currently sending join requests to successor / predecessor.
//             /// </summary>
//             Joining,

//             /// <summary>
//             /// Chord node is waiting for live-checks of successor / predecessor after a join operation.
//             /// </summary>
//             Stabilize,

//             /// <summary>
//             /// Chord node is ready for work.
//             /// </summary>
//             Idle
//         }

//         #endregion Members

//         #region Methods

//         /// <summary>
//         /// Perform a lookup request to retrieve the node managing the given hash key.
//         /// </summary>
//         /// <param name="key">The hash identifier to be looked up.</param>
//         /// <returns>the node id of the managing node</returns>
//         public async Task<ChordEndpoint> LookupKey(BigInteger key)
//         {
//             ChordEndpoint managingRemote;

//             // make sure that at least one successor is defined (for simple token-ring routing)
//             if (Successor == null) { throw new InvalidOperationException($"Node is not initialized properly! Please run { nameof(JoinNetwork) } function first!"); }

//             // check if the direct successor manages the hash
//             if (Successor.NodeId > key)
//             {
//                 managingRemote = Successor;
//             }
//             // foreward the request
//             else
//             {
//                 // send a lookup request via network
//                 var bestFinger = findBestFinger(key);
//                 var request = new ChordMessage(Local, key);
//                 var response = await _client.ExecuteWithResponse(Local.Endpoint, bestFinger.Endpoint, request);
//                 managingRemote = new ChordEndpoint(response.ManagingNodeEndpoint);
//             }

//             return managingRemote;
//         }

//         /// <summary>
//         /// Find an entrance into the P2P network by bootstrapping (brute-force).
//         /// </summary>
//         /// <param name="networkId">The network id.</param>
//         /// <param name="broadcast">The network broadcast.</param>
//         /// <returns>an endpoint that can be used for bootstrapping</returns>
//         public async Task<ChordEndpoint> FindBootstrapNode(IPAddress networkId, IPAddress broadcast)
//         {
//             // init min / max address for brute-force search range
//             int min = BitConverter.ToInt32(networkId.GetAddressBytes(), 0) + 1;
//             int max = BitConverter.ToInt32(broadcast.GetAddressBytes(), 0);

//             // loop through all possible peers
//             for (int address = min; address < max; address++)
//             {
//                 // put potential endpoint together
//                 var node = new ChordEndpoint(new IPEndPoint(new IPAddress(address), Local.Endpoint.Port));

//                 // skip own IP address
//                 if (new BigInteger(Local.Endpoint.Address.GetAddressBytes()) == new BigInteger(node.Endpoint.Address.GetAddressBytes())) { continue; }

//                 // run lookup and 3 sec timeout task
//                 var lookupRequest = new ChordMessage(Local, Local.NodeId);
//                 var lookupTask = _client.ExecuteWithResponse(Local.Endpoint, node.Endpoint, lookupRequest);
//                 var timeoutTask = Task.Delay(3000);

//                 // only terminate if the lookup task finishes before the timeout task
//                 if (await Task.WhenAny(lookupTask, timeoutTask) == lookupTask) { return lookupTask.Result.ManagingRemote; }
//                 else { lookupTask.Dispose(); }

//                 // TODO: implement greedy lookup trying multiple IP addresses at once
//             }

//             // default return value when all IP addresses 
//             return null;
//         }

//         /// <summary>
//         /// Join the P2P network using the given bootstrap node.
//         /// </summary>
//         /// <param name="bootstrapNode">The bootstrap node used for joining the P2P network.</param>
//         public async Task JoinNetwork(ChordEndpoint bootstrapNode)
//         {
//             // TODO: synchronize join / leave operations with mutex
//             HealthState = ChordHealthState.Joining;

//             // start listening to incoming messages, so incoming join requests can be answered (background task)
//             // listening needs to start first because otherwise the P2P network would run into a deadlock
//             _serverListenerCancel = new CancellationTokenSource();
//             _server.ListenMessages(Local, handleIncomingMessage, _serverListenerCancel.Token);

//             // 1) find successor
//             var lookupRequest = new ChordMessage(Local, Local.NodeId);
//             var successor = (await _client.ExecuteWithResponse(Local.Endpoint, bootstrapNode.Endpoint, lookupRequest)).ManagingRemote;

//             // 2) send join request to successor and get info on predecessor
//             var joinSuccessorRequest = new ChordMessage(Local, JoinType.JoinSuccessor);
//             var joinMetadata = await _client.ExecuteWithResponse(Local.Endpoint, successor.Endpoint, joinSuccessorRequest);

//             // 3) send join request to predecessor and finalize join procedure

//             // check if the node is the first to join the network
//             if (successor.NodeId.Equals(joinMetadata.PredecessorRemote.NodeId))
//             {
//                 // assign successor and predecessor (network with 2 nodes)
//                 Successors.Add(successor);
//                 Predecessor = successor;
//             }
//             // join the network normally (more than just 2 nodes)
//             else
//             {
//                 var joinPredecessorRequest = new ChordMessage(Local, JoinType.JoinPredecessor);
//                 _client.ExecuteNoResponse(Local.Endpoint, joinMetadata.PredecessorEndpoint, joinPredecessorRequest);
//             }

//             HealthState = ChordHealthState.Stabilize;

//             // 4) wait for live-checks of successor / predecessor

//             // try to find more than just one successor, e.g. at least 3 successors (background task)

//             // start sending live-check messages to successors / predecessors, e.g. each 30 seconds (background task)

//         }

//         /// <summary>
//         /// Leave the P2P network gracefully.
//         /// </summary>
//         public async Task LeaveNetwork()
//         {
//             // reject all new incoming messages (graceful server endpoint shutdown)
//             await Task.Run(() => _serverListenerCancel.Cancel());

//             // copy managed data to successor
//             // TODO: implement logic
//             throw new NotImplementedException();
//         }

//         private async Task handleIncomingMessage(ChordMessage message)
//         {
//             switch (message.Type)
//             {
//                 case ChordMessageType.KeyLookupRequest: await handleLookupRequest(message); break;
//                 case ChordMessageType.JoinRequest: await handleJoinRequest(message); break;
//                 case ChordMessageType.LiveCheck: await handleLiveCheck(message); break;
//                 // TODO: add leave request
//             }
//         }

//         private async Task handleLookupRequest(ChordMessage message)
//         {
//             // check if the successor is the managing node (or if there is an initiation lookup)
//             if (Successor.NodeId > message.LookupKeyNumeric)
//             {
//                 // send response to the requesting node
//                 // on initiation (successor = null): return the local endpoint as fallback
//                 var response = new ChordMessage(message, Successor ?? Local);
//                 _client.ExecuteNoResponse(Local.Endpoint, message.RequesterEndpoint, response);
//             }
//             // foreward the lookup request
//             else
//             {
//                 // foreward the request to the closest predeceding finger (that is alive)
//                 var bestFinger = findBestFinger(message.LookupKeyNumeric);
//                 _client.ExecuteNoResponse(Local.Endpoint, bestFinger.Endpoint, message);
//             }
//         }

//         private async Task handleJoinRequest(ChordMessage message)
//         {
//             // TODO: synchronize join / leave operations with mutex

//             // handle successor join
//             if (message.JoinType == JoinType.JoinSuccessor)
//             {
//                 // create response message
//                 // on initiation: return the local endpoint as fallback (initial 2-node network join)
//                 var response = new ChordMessage(message, Predecessor ?? message.RequesterRemote, FingerList);
//                 _client.ExecuteNoResponse(Local.Endpoint, message.RequesterEndpoint, response);

//                 // update predecessor
//                 Predecessor = message.RequesterRemote;

//                 // update successor (if joining node is the first one joining)
//                 if (Successor == null) { Successors.Add(message.RequesterRemote); }
//             }
//             // handle predecessor join
//             else
//             {
//                 // append the new successor to successor list
//                 Successors.Insert(0, message.RequesterRemote);

//                 // TODO: Think of the process. Are there other things to be done additionally?
//             }

//             // node needs to stabilize
//             HealthState = ChordHealthState.Stabilize;

//             // TODO: wait for live-checks of successor / predecessor (async)
//         }

//         private async Task handleLiveCheck(ChordMessage message)
//         {
//             // handle network join finalization
//             if (message.RequesterRemote.NodeId.Equals(Predecessor.NodeId) && message.FinalizeJoin) { HealthState = ChordHealthState.Idle; }

//             // handle requested stabilization procedure
//             if (message.Stabilize)
//             {
//                 // node needs to stabilize
//                 HealthState = ChordHealthState.Stabilize;

//                 // TODO: send live-checks with piggy-back infos and await responses
//             }
//         }

//         /// <summary>
//         /// Find the closest reachable predecessor of the requested key.
//         /// </summary>
//         /// <param name="key">The key to be looked up.</param>
//         /// <returns>the best endpoint to foreward the key lookup request to.</returns>
//         private ChordEndpoint findBestFinger(BigInteger key)
//         {
//             // TODO: take live-checks in consideration to minimize the risk of running into a timeout

//             // select best finger (closest predecessor of the lookup key)
//             var bestFinger = Local.NodeId < key
//                 ? FingerList.Where(x => x.NodeId < key).OrderByDescending(x => x.NodeId).FirstOrDefault()
//                 : FingerList.Where(x => x.NodeId > key).OrderBy(x => x.NodeId).FirstOrDefault();

//             return bestFinger;
//         }

//         #endregion Methods
//     }
// }
