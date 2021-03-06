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

        [Fact]
        public void SimulateNetwork()
        {
            // define test hyperparams
            const int testNodesCount = 100;
            const int chordPort = 9876;
            const int testTimeoutSecs = 5 * 60;

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
                .Select(x => new ChordNode(sendMessageFunc, $"10.0.0.{ x }", chordPort.ToString()))
                .ToDictionary(x => x.NodeId);

            _logger.WriteLine("Successfully created nodes. Starting node join procedures.");

            // connect the chord nodes to a self-organized cluster by simulating
            // something like e.g. a Kubernetes rollout of several chord instances
            var bootstrap = simulatedNodes.First().Value.Local;
            Func<Task<IChordEndpoint>> bootstrapFunc = () => { return Task.Run(() => bootstrap); };
            var joinTasks = simulatedNodes.Values.AsParallel()
                .Select(x => x.JoinNetwork(bootstrapFunc)).ToArray();

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

            // abort the simulation on timeout if needed -> unit test failed
            bool allTasksComplete = Task.WhenAny(timeoutTask, monitorTask) != timeoutTask;
            Assert.True(allTasksComplete);

            _logger.WriteLine("Successfully joined all nodes to the chord network.");

            // TODO: evaluate the network structure by some graph analysis

            // TODO: test sending some key lookups and health checks

            // TODO: test the node leave prodecure (in particular the case for the last leaving node)
        }
    }
}
