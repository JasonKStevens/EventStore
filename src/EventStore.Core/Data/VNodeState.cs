namespace EventStore.Core.Data {
	public enum VNodeState {
		Initializing,
		Unknown,
		PreReplica,
		CatchingUp,
		Clone,
		Slave,
		PreMaster,
		Master,
		Manager,
		ShuttingDown,
		Shutdown,
		ReadOnlyUnknown,
		ReadOnlyPreReplica,
		ReadOnlyReplica,
	}

	public static class VNodeStateExtensions {
		public static bool IsReplica(this VNodeState state) {
			return state == VNodeState.CatchingUp
			       || state == VNodeState.Clone
			       || state == VNodeState.Slave
				   || state == VNodeState.ReadOnlyReplica;
		}
		public static bool IsReadOnly(this VNodeState state) {
			return state == VNodeState.ReadOnlyUnknown
					|| state == VNodeState.ReadOnlyPreReplica
					|| state == VNodeState.ReadOnlyReplica;
		}
	}
}
