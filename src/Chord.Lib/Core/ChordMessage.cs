using System;
using System.Collections.Generic;

namespace Chord.Lib.Core
{
    public enum ChordRequestType
    {
        KeyLookup,
        UpdateSuccessor,
        HealthCheck,
        InitNodeJoin,
        CommitNodeJoin,
        InitNodeLeave,
        CommitNodeLeave,
    }

    public enum ChordHealthStatus
    {
        Starting,
        Idle,
        Leaving,
        Questionable,
        Dead
    }

    public class ChordEndpoint : IChordEndpoint
    {
        // summary of remote node features
        public long NodeId { get; set; }
        public string IpAddress { get; set; }
        public string Port { get; set; }
        public ChordHealthStatus State { get; set; } = ChordHealthStatus.Questionable;

        #region Equality Check

        public override bool Equals(object other)
        {
            return other?.GetType() == typeof(IChordEndpoint)
                && Equals((IChordEndpoint)other);
        }

        // return always 0 to enforce calling the Equals() function
        public override int GetHashCode() => 0;

        public bool Equals(IChordEndpoint other)
        {
            return other != null
                && other.NodeId.Equals(this.NodeId)
                && other.IpAddress.Equals(this.IpAddress)
                && other.Port.Equals(this.Port);
        }

        #endregion Equality Check

        #region ToString

        public override string ToString() => $"{ NodeId }@{ IpAddress }:{ Port }";

        #endregion ToSTring
    }

    public class ChordRequestMessage : IChordRequestMessage
    {
        // core request message features
        public ChordRequestType Type { get; set; }
        public long RequesterId { get; set; }
        public long RequestedResourceId { get; set; }

        // additional message features for the join/leave procedure
        public IChordEndpoint NewSuccessor { get; set; }
        public IChordEndpoint NewPredecessor { get; set; }
    }

    public class ChordResponseMessage : IChordResponseMessage
    {
        // core response message features
        public IChordEndpoint Responder { get; set; }

        // additional message features for the join/leave procedure
        public bool ReadyForDataCopy { get; set; }
        public bool CommitSuccessful { get; set; }
        public IChordEndpoint Predecessor { get; set; }
        public IEnumerable<IChordEndpoint> FingerTable { get; set; }
    }

    public interface IChordEndpoint : IEquatable<IChordEndpoint>
    {
        // summary of remote node features
        long NodeId { get; set; }
        string IpAddress { get; set; }
        string Port { get; set; }
        ChordHealthStatus State { get; set; }
    }

    public interface IChordRequestMessage
    {
        // core message features
        ChordRequestType Type { get; set; }
        long RequesterId { get; set; }
        long RequestedResourceId { get; set; }

        // additional message features for the join/leave procedure
        IChordEndpoint NewSuccessor { get; set; }
        IChordEndpoint NewPredecessor { get; set; }
    }

    public interface IChordResponseMessage
    {
        // core message features
        IChordEndpoint Responder { get; set; }

        // additional message features for the join/leave procedure
        bool ReadyForDataCopy { get; set; }
        bool CommitSuccessful { get; set; }
        IChordEndpoint Predecessor { get; set; }
        IEnumerable<IChordEndpoint> FingerTable { get; set; }
    }
}
