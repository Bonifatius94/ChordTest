using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;

namespace Chord.Lib.Core
{
    // shortcut for message callback function: (message, receiver) -> response
    using MessageCallback = System.Func<IChordRequestMessage, IChordEndpoint, Task<IChordResponseMessage>>;

    /// <summary>
    /// Representing all functions and attributes of a logical chord node.
    /// </summary>
    public interface IChordNode
    {
        /// <summary>
        /// The chord node's id.
        /// </summary>
        long NodeId { get; }

        /// <summary>
        /// The chord node's local endpoint.
        /// </summary>
        IChordEndpoint Local { get; }

        /// <summary>
        /// The chord node's successor node endpoint.
        /// </summary>
        IChordEndpoint Successor { get; }

        /// <summary>
        /// The chord node's predecessor node endpoint.
        /// </summary>
        IChordEndpoint Predecessor { get; }

        /// <summary>
        /// The chord node's finger table used for routing.
        /// </summary>
        IDictionary<long, IChordEndpoint> FingerTable { get; }

        /// <summary>
        /// Create a new chord endpoint and join it to the network.
        /// </summary>
        /// <param name="findBootstrapNode">A function searching the network for a bootstrap node.</param>
        /// <returns>a task handle to be awaited asynchronously</returns>
        Task JoinNetwork(Func<Task<IChordEndpoint>> findBootstrapNode);

        /// <summary>
        /// Shut down this chord endpoint by leaving the network gracefully.
        /// </summary>
        /// <returns>a task handle to be awaited asynchronously</returns>
        Task LeaveNetwork();

        /// <summary>
        /// Look up the chord node responsible for the given key.
        /// </summary>
        /// <param name="key">The key to be looked up.</param>
        /// <param name="explicitReceiver">An explicit receiver to send the request to (optional).</param>
        /// <returns>a task handle to be awaited asynchronously</returns>
        Task<IChordEndpoint> LookupKey(long key, IChordEndpoint explicitReceiver=null);

        /// <summary>
        /// Check the health status of the given target chord endpoint.
        /// </summary>
        /// <param name="target">The endpoint to check the health of.</param>
        /// <param name="timeoutInSecs">The timeout seconds to be waited for a response (default: 10s).</param>
        /// <param name="failStatus">The default status when the check times out (default: questionable).</param>
        /// <returns>a task handle to be awaited asynchronously</returns>
        Task<ChordHealthStatus> CheckHealth(
            IChordEndpoint target, int timeoutInSecs=10,
            ChordHealthStatus failStatus=ChordHealthStatus.Questionable);

        /// <summary>
        /// Process the given chord request the local endpoint just received.
        /// </summary>
        /// <param name="request">The chord request to be processed.</param>
        /// <returns>a task handle to be awaited asynchronously</returns>
        Task<IChordResponseMessage> ProcessRequest(IChordRequestMessage request);
    }

    /// <summary>
    /// This class provides all core functionality of the chord protocol.
    /// 
    /// Note that this is supposed to be a logical chord endpoint abstracting
    /// the actual network traffic. If you want to use this code properly, 
    /// you need to provide a callback function for exchanging messages
    /// between chord endpoints, etc. (see constructor).
    /// </summary>
    public class ChordNode : IChordNode
    {
        /// <summary>
        /// Create a new chord node with the given request callback, upper resource id bound and timeout configuration.
        /// </summary>
        /// <param name="sendRequest">The callback function for sending requests to other chord nodes.</param>
        /// <param name="ipAddress">The local endpoint's IP address.</param>
        /// <param name="chordPort">The local endpoint's chord port.</param>
        /// <param name="maxId">The max. resource id to be addressed (default: 2^63-1).</param>
        /// <param name="monitorHealthSchedule">The delay in seconds between health monitoring tasks (default: 5 minutes).</param>
        /// <param name="updateTableSchedule">The delay in seconds between finger table update tasks (default: 5 minutes).</param>
        public ChordNode(MessageCallback sendRequest, string ipAddress, string chordPort,
            long maxId=long.MaxValue, int monitorHealthSchedule=600, int updateTableSchedule=600)
        {
            this.sendRequest = sendRequest;
            this.maxId = maxId;
            this.monitorHealthSchedule = monitorHealthSchedule;
            this.updateTableSchedule = updateTableSchedule;

            this.local = new ChordEndpoint() {
                NodeId = getRandId(),
                IpAddress = ipAddress,
                Port = chordPort,
                State = ChordHealthStatus.Starting
            };
            this.successor = this.local;
        }

        public long NodeId => local.NodeId;
        public IChordEndpoint Local => local;
        public IChordEndpoint Successor => successor;
        public IChordEndpoint Predecessor => predecessor;
        public IDictionary<long, IChordEndpoint> FingerTable => fingerTable;

        private long maxId;
        private MessageCallback sendRequest;
        private int monitorHealthSchedule;
        private int updateTableSchedule;

        private IChordEndpoint local = null;
        private IChordEndpoint successor = null;
        private IChordEndpoint predecessor = null;

        private IDictionary<long, IChordEndpoint> fingerTable
            = new Dictionary<long, IChordEndpoint>();

        private CancellationTokenSource backgroundTaskCallback;
        bool isJoinActive = false;
        //private Mutex updateFingerTableMutex = new Mutex();

        #region Chord Client

        public async Task JoinNetwork(Func<Task<IChordEndpoint>> findBootstrapNode)
        {
            // phase 0: find an entrypoint into the chord network

            var bootstrap = await findBootstrapNode();
            if (bootstrap == null) { throw new InvalidOperationException(
                "Cannot find a bootstrap node! Please try to join again!"); }

            // check for join abortion conditions
            if (bootstrap.Equals(local) || local.State == ChordHealthStatus.Idle) { return; }
            if (local.State != ChordHealthStatus.Starting) { throw new InvalidOperationException(
                "Cannot join cluster! Make sure the node is in 'Starting' state!"); }

            // phase 1: determine the successor by a key lookup

            do {

                // look up the successor's node id
                var lookupResponse = await sendRequest(
                    new ChordRequestMessage()
                    {
                        Type = ChordRequestType.KeyLookup,
                        RequesterId = local.NodeId,
                        RequestedResourceId = local.NodeId
                    },
                    bootstrap
                );
                successor = lookupResponse.Responder;

                // make sure that the node's id is unique
                // if not draw a new id and repeat the lookup
                if (successor.NodeId != local.NodeId) { break; }
                local.NodeId = getRandId();

            // continue until the generated node id is unique
            } while (true);

            // phase 2: initiate the join process

            // send a join initiation request to the successor
            var response = await sendRequest(
                new ChordRequestMessage() {
                    Type = ChordRequestType.InitNodeJoin,
                    RequesterId = local.NodeId
                },
                successor
            );

            if (!response.ReadyForDataCopy) {
                throw new InvalidOperationException("Network join failed! Cannot copy payload data!"); }

            // phase 3: copy all existing payload data this node is now responsible for

            // TODO: For production use, make sure to copy payload data, too.
            //       Keep in mind that the old successor is no more responsible for the ids
            //       being assigned to this newly created node. So there needs to be a kind of
            //       mechanism to copy the data first before enabling this node to avoid
            //       temporary data unavailability.

            // phase 4: finalize the join process

            // send a join initiation request to the successor
            // -> the successor sends an 'update successor' request to his predecessor
            // -> the predecessor's successor then points to this node
            // -> from then on this node is available to other Chord nodes on the network
            response = await sendRequest(
                new ChordRequestMessage() {
                    Type = ChordRequestType.CommitNodeJoin,
                    RequesterId = local.NodeId,
                    NewPredecessor = local
                },
                successor
            );

            // make sure the successor did successfully commit the join
            if (!response.CommitSuccessful) { throw new InvalidOperationException(
                "Joining the network unexpectedly failed! Please try again!"); }

            // apply the node settings
            predecessor = response.Predecessor;
            fingerTable = response.FingerTable.ToDictionary(x => x.NodeId);
            fingerTable.Add(successor.NodeId, successor);
            local.State = ChordHealthStatus.Idle;

            // -> the node is now ready for use

            // start the health monitoring / finger table update
            // procedures as scheduled background tasks
            //backgroundTaskCallback = new CancellationTokenSource();
            //createBackgroundTasks(backgroundTaskCallback.Token);
            // TODO: enable background tasks again
        }

        private void createBackgroundTasks(CancellationToken token)
        {
            // define a task running the background tasks
            Task.Run(() => {

                // run the 'monitor health' task on a regular time schedule
                var monitorTask = Task.Run(() => {

                    while (!token.IsCancellationRequested)
                    {
                        monitorFingerHealth(token);
                        Task.Delay(monitorHealthSchedule * 1000).Wait();
                    }
                });

                // run the 'update table' task on a regular time schedule
                var updateTableTask = Task.Run(() => {

                    while (!token.IsCancellationRequested)
                    {
                        updateFingerTable(token);
                        Task.Delay(updateTableSchedule * 1000).Wait();
                    }
                });

                // wait until both tasks exited gracefully by cancellation
                Task.WaitAll(new Task[] { monitorTask, updateTableTask });
            });
        }

        public async Task LeaveNetwork()
        {
            // phase 1: initiate the leave process

            // send a leave initiation request to the successor
            var response = await sendRequest(
                new ChordRequestMessage() {
                    Type = ChordRequestType.InitNodeLeave,
                    RequesterId = local.NodeId
                },
                successor
            );

            if (!response.ReadyForDataCopy) {
                throw new InvalidOperationException("Network leave failed! Cannot copy payload data!"); }

            // phase 2: copy all existing payload data from this node to the successor

            // TODO: For production use, make sure to copy payload data, too.
            //       Keep in mind that this node is no more responsible for the ids
            //       being assigned to the successor. So there needs to be a kind of
            //       mechanism to copy the data first before safely leaving without
            //       temporary data unavailability.

            // phase 3: finalize the leave process

            // send a join initiation request to the successor
            // -> the successor sends an 'update successor' request to this node's predecessor
            // -> the predecessor's successor then points to this node's successor
            // -> from then on this node is no more available by other Chord nodes and can leave
            response = await sendRequest(
                new ChordRequestMessage() {
                    Type = ChordRequestType.CommitNodeLeave,
                    RequesterId = local.NodeId,
                    NewPredecessor = predecessor
                },
                successor
            );

            // make sure the successor did successfully commit the join
            if (!response.CommitSuccessful) { throw new InvalidOperationException(
                "Leaving the network unexpectedly failed! Please try again!"); }

            // shut down all background tasks (health monitoring and finger table updates)
            backgroundTaskCallback.Cancel();
        }

        public async Task<IChordEndpoint> LookupKey(
            long key, IChordEndpoint explicitReceiver=null)
        {
            // handle bootstrap case for network initialization
            if (fingerTable.Count == 0) { return local; }

            // determine the receiver to be forwarded the lookup request to
            var receiver = explicitReceiver ?? fingerTable[getBestFingerId(key)];

            // send a key lookup request
            var response = await sendRequest(
                new ChordRequestMessage() {
                    Type = ChordRequestType.KeyLookup,
                    RequesterId = local.NodeId,
                    RequestedResourceId = key
                },
                receiver
            );

            // return the node responsible for the key
            return response.Responder;
        }

        public async Task<ChordHealthStatus> CheckHealth(
            IChordEndpoint target, int timeoutInSecs=10,
            ChordHealthStatus failStatus=ChordHealthStatus.Questionable)
        {
            // send a health check request
            var cancelCallback = new CancellationTokenSource();
            var timeoutTask = Task.Delay(timeoutInSecs * 1000);
            var healthCheckTask = Task.Run(() => sendRequest(
                new ChordRequestMessage() {
                    Type = ChordRequestType.HealthCheck,
                    RequesterId = local.NodeId,
                },
                target
            ), cancelCallback.Token);

            // return the reported health state or the fail status (timeout)
            bool timeout = await Task.WhenAny(timeoutTask, healthCheckTask) == timeoutTask;
            if (timeout) { cancelCallback.Cancel(); }
            return timeout ? failStatus : healthCheckTask.Result.Responder.State;
        }

        private void monitorFingerHealth(CancellationToken token)
        {
            // TODO: enter critical section (mutex)

            const int healthCheckTimeout = 10; // TODO: parameterize the delay by configuration
            var cachedFingers = fingerTable.Values.ToList();

            // perform a first health check for all finger nodes
            updateHealth(cachedFingers, healthCheckTimeout, ChordHealthStatus.Questionable);
            if (token.IsCancellationRequested) { return; } // TODO: release mutex

            // perform a second health check for all questionable finger nodes
            var questionableFingers = cachedFingers
                .Where(x => x.State == ChordHealthStatus.Questionable).ToList();
            updateHealth(questionableFingers, healthCheckTimeout / 2, ChordHealthStatus.Dead);
            if (token.IsCancellationRequested) { return; } // TODO: release mutex

            // TODO: exit critical section (mutex)
        }

        private void updateHealth(List<IChordEndpoint> fingers, int timeoutSecs, 
            ChordHealthStatus failStatus=ChordHealthStatus.Questionable)
        {
            // run health checks for each finger in parallel
            var healthCheckTasks = fingerTable.Values
                .Select(x => Task.Run(() => new { 
                    NodeId = x.NodeId,
                    Health=CheckHealth(x, timeoutSecs, failStatus).Result
                })).ToArray();
            Task.WaitAll(healthCheckTasks);

            // collect health check results
            var healthStates = healthCheckTasks.Select(x => x.Result)
                .ToDictionary(x => x.NodeId, x => x.Health);

            // update finger states
            fingers.ForEach(finger => finger.State = healthStates[finger.NodeId]);
        }

        private void updateFingerTable(CancellationToken token)
        {
            // TODO: enter critical section (mutex)

            // get the ids 2^i for i in { 0, ..., log2(maxId) - 1 } to be looked up
            var keys = Enumerable.Range(0, (int)Math.Log2(maxId) - 1)
                .Select(i => restMod((long)Math.Pow(i, 2) + local.NodeId, maxId))
                .Select(normKey => (long)restMod(normKey - local.NodeId, maxId));

            // run all lookup tasks in parallel
            var lookupTasks = keys.Select(x => LookupKey(x)).ToArray();
            Task.WaitAll(lookupTasks);
            // TODO: add timeout

            // create a new finger table by assigning the nodes that
            // responded to the lookup requests (including the successor)
            var fingers = lookupTasks.Select(x => x.Result);
            var newTable = fingers.ToDictionary(x => x.NodeId);

            // switch out the currently active finger table
            fingerTable = newTable;

            // TODO: add a procedure scanning for the entire network to fix
            //       cluster states with multiple unconnected sub-clusters
            //       implementing this cost-efficient could be done by scanning
            //       a few physically neighboured IPs addresses with each node
            //       to initiate chord ring fusions

            // TODO: leave critical section (mutex)
        }

        #endregion Chord Client

        #region Chord Server

        public async Task<IChordResponseMessage> ProcessRequest(
            IChordRequestMessage request)
        {
            switch (request.Type)
            {
                // key lookup, health check and update successor
                case ChordRequestType.KeyLookup:
                    return await processKeyLookup(request);
                case ChordRequestType.HealthCheck:
                    return await processHealthCheck(request);
                case ChordRequestType.UpdateSuccessor:
                    return await processUpdateSuccessor(request);

                // node join procedure (init/commit)
                case ChordRequestType.InitNodeJoin:
                    return await processInitNodeJoin(request);
                case ChordRequestType.CommitNodeJoin:
                    return await processCommitNodeJoin(request);

                // node leave procedure (init/commit)
                case ChordRequestType.InitNodeLeave:
                    return await processInitNodeLeave(request);
                case ChordRequestType.CommitNodeLeave:
                    return await processCommitNodeLeave(request);

                // handle unknown request types by throwing an error
                default: throw new ArgumentException(
                    $"Invalid argument! Unknown request type { request.Type }!");
            }
        }

        private async Task<IChordResponseMessage> processKeyLookup(
            IChordRequestMessage request)
        {
            // handle the special case for being the initial node of the cluster
            // serving the first lookup request of a node join
            if (request.RequestedResourceId.Equals(local.NodeId)) {
                return new ChordResponseMessage() { Responder = local }; }

            // perform key lookup and return the endpoint responsible for the key
            var responder = await LookupKey(request.RequestedResourceId);
            return new ChordResponseMessage() { Responder = responder };
        }

        private async Task<IChordResponseMessage> processHealthCheck(
            IChordRequestMessage request)
        {
            // just send back the local endpoint containing the health state
            return new ChordResponseMessage() { Responder = local };
        }

        private async Task<IChordResponseMessage> processInitNodeJoin(
            IChordRequestMessage request)
        {
            // prepare for a joining node as new predecessor
            // inform the payload component that it has to send the payload
            // data chunk to the joining node that it is now responsible for

            // wait until the concurrent join/leave operation is complete
            if (isJoinActive)
            {
                while (true)
                {
                    Task.Delay(50).Wait();
                    if (!isJoinActive) {  } // TODO: finish implementation
                }
            }

            // TODO: trigger copy process for payload data transmission

            return new ChordResponseMessage() {
                Responder = local,
                ReadyForDataCopy = true
            };
        }

        private async Task<IChordResponseMessage> processCommitNodeJoin(
            IChordRequestMessage request)
        {
            // request the prodecessor's successor to be updated to the joining node
            // -> predecessor.successor = joining node
            // -> this.predecessor = joining node

            Console.WriteLine($"joining { request.NewPredecessor } to { local }");

            var oldPredecessor = predecessor ?? local;
            var joiningNode = request.NewPredecessor;

            // send an 'update successor' request to the former predecessor
            var updatePredResponse = await sendRequest(
                new ChordRequestMessage() {
                    Type = ChordRequestType.UpdateSuccessor,
                    RequesterId = local.NodeId,
                    NewSuccessor = joiningNode
                },
                oldPredecessor
            );

            // TODO: figure out what to do when the 'update successor' operation failed

            // create the response to be returned
            var response = new ChordResponseMessage()
            {
                Responder = local,
                CommitSuccessful = updatePredResponse.CommitSuccessful,
                Predecessor = oldPredecessor,
                FingerTable = fingerTable.Values
            };

            // update the predecessor of this node
            predecessor = joiningNode;
            fingerTable.Add(predecessor.NodeId, predecessor);

            // update the state to 'idle' if this node was the initial node on the cluster
            if (fingerTable.Count == 1) { local.State = ChordHealthStatus.Idle; }

            // allow the next join/leave operation to start
            //nodeJoinLeaveMutex.ReleaseMutex();
            // info: mutex does not work because the join


            return response;
        }

        private async Task<IChordResponseMessage> processInitNodeLeave(
            IChordRequestMessage request)
        {
            // prepare for the predecessor leaving the network
            // inform the payload component that it will be sent payload data

            // wait until the concurrent join/leave operation is complete
            //nodeJoinLeaveMutex.WaitOne();

            return new ChordResponseMessage() {
                Responder = local,
                ReadyForDataCopy = true
            };
        }

        private async Task<IChordResponseMessage> processCommitNodeLeave(
            IChordRequestMessage request)
        {
            // request updating the leaving node's prodecessor's successor
            // -> predecessor.successor = this node

            var response = await sendRequest(
                new ChordRequestMessage() {
                    Type = ChordRequestType.UpdateSuccessor,
                    RequesterId = local.NodeId,
                    NewSuccessor = request.NewSuccessor
                },
                predecessor);

            // create the response to be returned
            response = new ChordResponseMessage() {
                Responder = local,
                CommitSuccessful = response.CommitSuccessful
            };

            // allow the next join/leave operation to start
            //nodeJoinLeaveMutex.ReleaseMutex();

            return response;
        }

        private async Task<IChordResponseMessage> processUpdateSuccessor(
            IChordRequestMessage request)
        {
            const int timeout = 10;

            Console.WriteLine($"updating successor of { local } from { successor } to { request.NewSuccessor }");

            // ping the new successor to make sure it is healthy
            var status = await CheckHealth(request.NewSuccessor, timeout, ChordHealthStatus.Dead);
            bool canUpdate = status != ChordHealthStatus.Dead;

            // update the successor
            if (canUpdate) { successor = request.NewSuccessor; }

            // respond whether the update was successful
            return new ChordResponseMessage() {
                Responder = local,
                CommitSuccessful = canUpdate
            };
        }

        #endregion Chord Server

        #region Common Code

        private long getBestFingerId(long key)
        {
            // cache the finger table ids to ensure operation consistency
            var cachedFingerIds = fingerTable.Keys.ToList();
            // TODO: think of only forwarding to healthy nodes

            // make sure the finger table is not empty
            if (cachedFingerIds.Count == 0 && local.State == ChordHealthStatus.Idle) {
                throw new InvalidOperationException(
                    "The finger table is empty! Make sure the node was properly initialized!"); }

            // termination case: forward to the successor if it is the manager of the key
            // recursion case: forward to the closest predecessing finger of the key
            return (long)restMod(successor.NodeId + key, maxId) >= key ? successor.NodeId
                : getClosestPredecessor(key, cachedFingerIds);
        }

        private long getClosestPredecessor(long key, IEnumerable<long> fingerIds)
        {
            // shift the finger ids such that the key's modulo identity becomes 0
            // then select the biggest id, which has to be the closest predecessor
            BigInteger closestNormPred = fingerIds
                .Select(x => restMod(x + key, maxId)).Max();

            // convert the shifted node id back to the actual node id
            return (long)restMod(closestNormPred - key, maxId);
        }

        #endregion Common Code

        #region Helpers

        // initialize the random number generator
        private static readonly Random rng = new Random();

        private long getRandId()
        {
            // concatenate two random 32-bit integers to a long value
            return ((long)rng.Next(int.MaxValue) << 32)
                 | (long)(uint)rng.Next(int.MinValue, int.MaxValue);
        }

        // handles all negative rest classes correctly by mapping
        // them to their positive identity within [0, classes-1]
        private BigInteger restMod(BigInteger element, BigInteger classes)
            => (element % classes + classes) % classes;

        #endregion Helpers
    }
}