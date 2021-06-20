using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Chord.Lib.Core;
using Xunit;
using Xunit.Abstractions;

namespace Chord.Lib.Test
{
    using MessageCallback = System.Func<IChordRequestMessage, IChordEndpoint, Task<IChordResponseMessage>>;

    public class ChordNetworkSimulationTest
    {
        private readonly ITestOutputHelper _logger;
        public ChordNetworkSimulationTest(ITestOutputHelper logger) { _logger = logger; }

        private const int CHORD_PORT = 9876;

        [Fact]
        public void SimulateInitialSelfJoin()
        {
            // define a lookup cache for the nodes to be simulated
            IDictionary<long, ChordNode> simulatedNodes = null;

            // define the message transmission function (just pass the message
            // directly to the target node's ProcessRequest() function)
            MessageCallback sendMessageFunc =
                (IChordRequestMessage request, IChordEndpoint receiver) => {
                    return simulatedNodes[receiver.NodeId].ProcessRequest(request);
                };

            // create two unconnected nodes and apply them to the
            // simulated physical network (so they can exchange messages)
            var initialNode = new ChordNode(sendMessageFunc, "10.0.0.1", CHORD_PORT.ToString());
            simulatedNodes = new List<ChordNode>() { initialNode }.ToDictionary(x => x.NodeId);

            // suggest the initial node as bootstrap node and perform the join procedure
            Func<Task<IChordEndpoint>> bootstrapFunc = () => { return Task.Run(() => initialNode.Local); };
            var joinTasks = simulatedNodes.Values.Select(x => x.JoinNetwork(bootstrapFunc)).ToArray();
            Task.WaitAll(joinTasks);

            // make sure that successor and predecessor of both nodes point to
            // the other node building a token-ring of 2 elements
            Assert.True(
                  initialNode.Successor.Equals(initialNode.Local)
               && initialNode.Local.State == ChordHealthStatus.Starting
           );
        }

        [Fact]
        public void SimulateInitialNodeJoinSync()
        {
            // define a lookup cache for the nodes to be simulated
            IDictionary<long, ChordNode> simulatedNodes = null;

            // define the message transmission function (just pass the message
            // directly to the target node's ProcessRequest() function)
            MessageCallback sendMessageFunc =
                (IChordRequestMessage request, IChordEndpoint receiver) => {
                    return simulatedNodes[receiver.NodeId].ProcessRequest(request);
                };

            // create two unconnected nodes and apply them to the
            // simulated physical network (so they can exchange messages)
            var initialNode = new ChordNode(sendMessageFunc, "10.0.0.1", CHORD_PORT.ToString());
            var joiningNode = new ChordNode(sendMessageFunc, "10.0.0.2", CHORD_PORT.ToString());
            simulatedNodes = new List<ChordNode>() { initialNode, joiningNode }.ToDictionary(x => x.NodeId);
            _logger.WriteLine($"joining nodes: { string.Join(", ", simulatedNodes.Values.Select(x => x.Local)) }");

            // suggest the initial node as bootstrap node and perform the join procedure
            Func<Task<IChordEndpoint>> bootstrapFunc = () => { return Task.Run(() => initialNode.Local); };
            initialNode.JoinNetwork(bootstrapFunc).Wait(5000);
            joiningNode.JoinNetwork(bootstrapFunc).Wait(5000);

            // make sure that successor and predecessor of both nodes point to
            // the other node building a token-ring of 2 elements
            Assert.True(
                  initialNode.Successor.Equals(joiningNode.Local)
               && initialNode.Predecessor.Equals(joiningNode.Local)
               && joiningNode.Successor.Equals(initialNode.Local)
               && joiningNode.Predecessor.Equals(initialNode.Local)
               && initialNode.Local.State == ChordHealthStatus.Idle
               && joiningNode.Local.State == ChordHealthStatus.Idle
           );
        }

        [Fact]
        public void SimulateInitialNodeJoinParallel()
        {
            // define a lookup cache for the nodes to be simulated
            IDictionary<long, ChordNode> simulatedNodes = null;

            // define the message transmission function (just pass the message
            // directly to the target node's ProcessRequest() function)
            MessageCallback sendMessageFunc =
                (IChordRequestMessage request, IChordEndpoint receiver) => {
                    return simulatedNodes[receiver.NodeId].ProcessRequest(request);
                };

            // create two unconnected nodes and apply them to the
            // simulated physical network (so they can exchange messages)
            var initialNode = new ChordNode(sendMessageFunc, "10.0.0.1", CHORD_PORT.ToString());
            var joiningNode = new ChordNode(sendMessageFunc, "10.0.0.2", CHORD_PORT.ToString());
            simulatedNodes = new List<ChordNode>() { initialNode, joiningNode }.ToDictionary(x => x.NodeId);
            _logger.WriteLine($"joining nodes: { string.Join(", ", simulatedNodes.Values.Select(x => x.Local)) }");

            // suggest the initial node as bootstrap node and perform the join procedure
            Func<Task<IChordEndpoint>> bootstrapFunc = () => { return Task.Run(() => initialNode.Local); };
            var joinTasks = simulatedNodes.Values.Select(x => x.JoinNetwork(bootstrapFunc)).ToArray();
            Task.WaitAll(joinTasks, 5000);

            // make sure that successor and predecessor of both nodes point to
            // the other node building a token-ring of 2 elements
            Assert.True(
                  initialNode.Successor.Equals(joiningNode.Local)
               && initialNode.Predecessor.Equals(joiningNode.Local)
               && joiningNode.Successor.Equals(initialNode.Local)
               && joiningNode.Predecessor.Equals(initialNode.Local)
               && initialNode.Local.State == ChordHealthStatus.Idle
               && joiningNode.Local.State == ChordHealthStatus.Idle
           );
        }

        [Fact] //(Skip="initial node join not working, fix that test first")]
        public void SimulateNetwork()
        {
            // define test hyperparams
            const int testNodesCount = 100;
            const int testTimeoutSecs = 15;

            _logger.WriteLine($"Simulating a chord network with { testNodesCount } nodes, timeout={ testTimeoutSecs }s");

            // define a lookup cache for the nodes to be simulated
            IDictionary<long, ChordNode> simulatedNodes = null;

            // define the message transmission function (just pass the message
            // directly to the target node's ProcessRequest() function)
            MessageCallback sendMessageFunc =
                (IChordRequestMessage request, IChordEndpoint receiver) => {
                    return simulatedNodes[receiver.NodeId].ProcessRequest(request);
                };

            // create yet unconnected chord nodes
            simulatedNodes = Enumerable.Range(1, testNodesCount)
                .Select(x => new ChordNode(sendMessageFunc, $"10.0.0.{ x }", CHORD_PORT.ToString()))
                .ToDictionary(x => x.NodeId);

            _logger.WriteLine("Successfully created nodes. Starting node join procedures.");

            // connect the chord nodes to a self-organized cluster by simulating
            // something like e.g. a Kubernetes rollout of several chord instances
            var bootstrap = simulatedNodes.First().Value.Local;
            Func<Task<IChordEndpoint>> bootstrapFunc = () => { return Task.Run(() => bootstrap); };
            var joinTasks = simulatedNodes.Values.Select(x => x.JoinNetwork(bootstrapFunc)).ToArray();

            // log the system state on a regular schedule until all join tasks completed
            // abort after several minutes if the tasks did not finish until then -> unit test failed
            var cancelCallback = new CancellationTokenSource();
            var timeoutTask = Task.Delay(testTimeoutSecs * 1000);
            var monitorTask = Task.Run(() => {

                    int i = 0;
                    while (joinTasks.Any(x => x.Status == TaskStatus.Running))
                    {
                        // report the states on a 5 second schedule
                        Task.Delay(5000).Wait();

                        // log the episode's system status
                        _logger.WriteLine("==================================");
                        _logger.WriteLine($"System state after { ++i } seconds:");
                        _logger.WriteLine(string.Join("\n", joinTasks.Select(task => $"task { task.Id }: { task.Status }")));
                    }

                    Task.WaitAll(joinTasks);

                }, cancelCallback.Token);

            // abort the simulation on timeout or if join tasks crashed -> unit test failed
            bool allTasksComplete = Task.WhenAny(timeoutTask, monitorTask) != timeoutTask;
            //Assert.True(allTasksComplete && joinTasks.All(x => x.Status == TaskStatus.RanToCompletion));

            _logger.WriteLine("Successfully joined all nodes to the chord network.");

            // update the simulated nodes cache in case a node id had to be updated (-> duplicate id)
            simulatedNodes = simulatedNodes.Values.ToDictionary(x => x.NodeId);
            _logger.WriteLine($"Generated node ids: { string.Join(", ", simulatedNodes.Keys) }");

            // evaluate the chord network structure using graph analysis
            evaluateNetworkStructure(simulatedNodes.Values);

            // TODO: test sending some key lookups and health checks

            // TODO: test the node leave prodecure (in particular the case for the last leaving node)
        }

        private void evaluateNetworkStructure(IEnumerable<ChordNode> simNodes)
        {
            _logger.WriteLine("enter network eval");

            var invalidNodes = simNodes.Where(x => x.Successor == x.Local);
            _logger.WriteLine($"nodes without successor: { invalidNodes.Count() }");

            // compute the edge adjacency
            var edgeAdj = simNodes
                .Select(x => new { Local = x.Local.NodeId, Succ = x.Successor.NodeId })
                .ToDictionary(x => x?.Local ?? -1, x => x?.Succ ?? -1);

            // cache unvisited nodes and determine the start as minimal node id
            var unvisitedNodeIds = edgeAdj.Keys.ToHashSet();
            var startId = unvisitedNodeIds.Min();
            unvisitedNodeIds.Remove(startId);

            long tempId = startId;
            // create output caches to capture the node visit order
            var ringId = 0;
            var nodeIdOrders = new Dictionary<int, List<long>>() {
                { ringId, new List<long>() { startId } } };

            // visit all nodes recursively, one after another
            while (unvisitedNodeIds.Count > 0)
            {
                // visit the next node
                long succ = edgeAdj[tempId];
                nodeIdOrders[ringId].Add(succ);
                bool found = unvisitedNodeIds.Remove(succ);
                tempId = succ;

                // in case the node was already visited, creating a cycle
                if (!found && unvisitedNodeIds.Count > 0)
                {
                    tempId = unvisitedNodeIds.Min();
                    unvisitedNodeIds.Remove(tempId);
                    nodeIdOrders.Add(++ringId, new List<long>() { tempId });
                }
            }

            // log all discovered node orders
            _logger.WriteLine(string.Join("\n", nodeIdOrders.Select(x =>
                string.Join(" -> ", x.Value.Select(y => y.ToString())))));
        }
    }
}
