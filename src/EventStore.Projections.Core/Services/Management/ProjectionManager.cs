using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Common.Log;
using EventStore.Common.Options;
using EventStore.Common.Utils;
using EventStore.Core.Bus;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Services.TimerService;
using EventStore.Core.Services.UserManagement;
using EventStore.Projections.Core.Messages;
using EventStore.Core.Helpers;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Standard;
using EventStore.Projections.Core.Common;

namespace EventStore.Projections.Core.Services.Management {
	public class ProjectionManager
		: IDisposable,
			IHandle<SystemMessage.StateChangeMessage>,
			IHandle<SystemMessage.SystemCoreReady>,
			IHandle<SystemMessage.EpochWritten>,
			IHandle<ClientMessage.ReadStreamEventsBackwardCompleted>,
			IHandle<ClientMessage.ReadStreamEventsForwardCompleted>,
			IHandle<ClientMessage.WriteEventsCompleted>,
			IHandle<ClientMessage.DeleteStreamCompleted>,
			IHandle<ProjectionManagementMessage.Command.Post>,
			IHandle<ProjectionManagementMessage.Command.PostBatch>,
			IHandle<ProjectionManagementMessage.Command.UpdateQuery>,
			IHandle<ProjectionManagementMessage.Command.GetQuery>,
			IHandle<ProjectionManagementMessage.Command.Delete>,
			IHandle<ProjectionManagementMessage.Command.GetStatistics>,
			IHandle<ProjectionManagementMessage.Command.GetState>,
			IHandle<ProjectionManagementMessage.Command.GetResult>,
			IHandle<ProjectionManagementMessage.Command.Disable>,
			IHandle<ProjectionManagementMessage.Command.Enable>,
			IHandle<ProjectionManagementMessage.Command.Abort>,
			IHandle<ProjectionManagementMessage.Command.SetRunAs>,
			IHandle<ProjectionManagementMessage.Command.Reset>,
			IHandle<ProjectionManagementMessage.Command.StartSlaveProjections>,
			IHandle<ProjectionManagementMessage.Command.GetConfig>,
			IHandle<ProjectionManagementMessage.Command.UpdateConfig>,
			IHandle<ProjectionManagementMessage.Internal.CleanupExpired>,
			IHandle<ProjectionManagementMessage.Internal.Deleted>,
			IHandle<CoreProjectionStatusMessage.Started>,
			IHandle<CoreProjectionStatusMessage.Stopped>,
			IHandle<CoreProjectionStatusMessage.Faulted>,
			IHandle<CoreProjectionStatusMessage.Prepared>,
			IHandle<CoreProjectionStatusMessage.StateReport>,
			IHandle<CoreProjectionStatusMessage.ResultReport>,
			IHandle<CoreProjectionStatusMessage.StatisticsReport>,
			IHandle<CoreProjectionManagementMessage.SlaveProjectionReaderAssigned>,
			IHandle<ProjectionManagementMessage.RegisterSystemProjection>,
			IHandle<CoreProjectionStatusMessage.ProjectionWorkerStarted>,
			IHandle<ProjectionManagementMessage.ReaderReady> {
		public const int ProjectionQueryId = -2;
		public const int ProjectionCreationRetryCount = 1;

		private readonly ILogger _logger = LogManager.GetLoggerFor<ProjectionManager>();

		private readonly IPublisher _inputQueue;
		private readonly IPublisher _publisher;
		private readonly Tuple<Guid, IPublisher>[] _queues;
		private readonly Guid[] _workers;
		private readonly TimeSpan _projectionsQueryExpiry;

		private readonly ITimeProvider _timeProvider;
		private readonly ProjectionType _runProjections;
		private readonly bool _initializeSystemProjections;
		private readonly Dictionary<string, ManagedProjection> _projections;
		private readonly Dictionary<Guid, string> _projectionsMap;

		private readonly RequestResponseDispatcher<ClientMessage.WriteEvents, ClientMessage.WriteEventsCompleted>
			_writeDispatcher;

		private readonly RequestResponseDispatcher<ClientMessage.DeleteStream, ClientMessage.DeleteStreamCompleted>
			_streamDispatcher;

		private readonly
			RequestResponseDispatcher
			<ClientMessage.ReadStreamEventsForward, ClientMessage.ReadStreamEventsForwardCompleted>
			_readForwardDispatcher;

		private readonly
			RequestResponseDispatcher
			<ClientMessage.ReadStreamEventsBackward, ClientMessage.ReadStreamEventsBackwardCompleted>
			_readDispatcher;

		private int _readEventsBatchSize = 100;

		private int _lastUsedQueue = 0;
		private bool _started;
		private bool _projectionsStarted;
		private long _projectionsRegistrationExpectedVersion = 0;
		private readonly PublishEnvelope _publishEnvelope;

		private readonly
			RequestResponseDispatcher<CoreProjectionManagementMessage.GetState, CoreProjectionStatusMessage.StateReport>
			_getStateDispatcher;

		private readonly
			RequestResponseDispatcher
			<CoreProjectionManagementMessage.GetResult, CoreProjectionStatusMessage.ResultReport>
			_getResultDispatcher;

		private readonly IODispatcher _ioDispatcher;

		public ProjectionManager(
			IPublisher inputQueue,
			IPublisher publisher,
			IDictionary<Guid, IPublisher> queueMap,
			ITimeProvider timeProvider,
			ProjectionType runProjections,
			IODispatcher ioDispatcher,
			TimeSpan projectionQueryExpiry,
			bool initializeSystemProjections = true) {
			if (inputQueue == null) throw new ArgumentNullException("inputQueue");
			if (publisher == null) throw new ArgumentNullException("publisher");
			if (queueMap == null) throw new ArgumentNullException("queueMap");
			if (queueMap.Count == 0) throw new ArgumentException("At least one queue is required", "queueMap");

			_inputQueue = inputQueue;
			_publisher = publisher;
			_queues = queueMap.Select(v => Tuple.Create(v.Key, v.Value)).ToArray();
			_workers = _queues.Select(v => v.Item1).ToArray();

			_timeProvider = timeProvider;
			_runProjections = runProjections;
			_initializeSystemProjections = initializeSystemProjections;
			_ioDispatcher = ioDispatcher;
			_projectionsQueryExpiry = projectionQueryExpiry;

			_writeDispatcher =
				new RequestResponseDispatcher<ClientMessage.WriteEvents, ClientMessage.WriteEventsCompleted>(
					publisher,
					v => v.CorrelationId,
					v => v.CorrelationId,
					new PublishEnvelope(_inputQueue));
			_readDispatcher =
				new RequestResponseDispatcher
					<ClientMessage.ReadStreamEventsBackward, ClientMessage.ReadStreamEventsBackwardCompleted>(
						publisher,
						v => v.CorrelationId,
						v => v.CorrelationId,
						new PublishEnvelope(_inputQueue));
			_readForwardDispatcher =
				new RequestResponseDispatcher
					<ClientMessage.ReadStreamEventsForward, ClientMessage.ReadStreamEventsForwardCompleted>(
						publisher,
						v => v.CorrelationId,
						v => v.CorrelationId,
						new PublishEnvelope(_inputQueue));
			_streamDispatcher =
				new RequestResponseDispatcher<ClientMessage.DeleteStream, ClientMessage.DeleteStreamCompleted>(
					publisher,
					v => v.CorrelationId,
					v => v.CorrelationId,
					new PublishEnvelope(_inputQueue));

			_projections = new Dictionary<string, ManagedProjection>();
			_projectionsMap = new Dictionary<Guid, string>();
			_publishEnvelope = new PublishEnvelope(_inputQueue, crossThread: true);
			_getStateDispatcher =
				new RequestResponseDispatcher
					<CoreProjectionManagementMessage.GetState, CoreProjectionStatusMessage.StateReport>(
						_publisher,
						v => v.CorrelationId,
						v => v.CorrelationId,
						new PublishEnvelope(_inputQueue));
			_getResultDispatcher =
				new RequestResponseDispatcher
					<CoreProjectionManagementMessage.GetResult, CoreProjectionStatusMessage.ResultReport>(
						_publisher,
						v => v.CorrelationId,
						v => v.CorrelationId,
						new PublishEnvelope(_inputQueue));
		}

		private void Start() {
			if (_started)
				throw new InvalidOperationException();
			_started = true;
			_publisher.Publish(new ProjectionManagementMessage.Starting(_epochId));
		}

		public void Handle(ProjectionManagementMessage.ReaderReady message) {
			if (_runProjections >= ProjectionType.System)
				StartExistingProjections(
					() => {
						_projectionsStarted = true;
						ScheduleExpire();
						_publisher.Publish(new SystemMessage.SubSystemInitialized("Projections"));
					});
		}


		private void ScheduleExpire() {
			if (!_projectionsStarted)
				return;
			_publisher.Publish(
				TimerMessage.Schedule.Create(
					TimeSpan.FromSeconds(60),
					_publishEnvelope,
					new ProjectionManagementMessage.Internal.CleanupExpired()));
		}

		private void Stop() {
			_started = false;
			_projectionsStarted = false;

			_writeDispatcher.CancelAll();
			_readDispatcher.CancelAll();

			_projections.Clear();
			_projectionsMap.Clear();
		}

		public void Handle(ProjectionManagementMessage.Command.Post message) {
			if (!_projectionsStarted)
				return;
			
			var pendingProjections = new Dictionary<string, PendingProjection>{
				{message.Name, new PendingProjection(message)}
			};

			PostNewProjections(pendingProjections, message);
		}

		public void Handle(ProjectionManagementMessage.Command.PostBatch message) {
			if (!_projectionsStarted || !message.Projections.Any())
				return;

			var pendingProjections = new Dictionary<string, PendingProjection>();
			foreach (var projection in message.Projections) {
				pendingProjections.Add(projection.Name, new PendingProjection(projection));
			}

			PostNewProjections(pendingProjections, message);
		}

		private void PostNewProjections(
			IDictionary<string, PendingProjection> projections,
			ProjectionManagementMessage.Command.ControlMessage message) {
			var duplicateNames = new List<string>();
			foreach (var pair in projections) {
				var projection = pair.Value;
				if (
					!ProjectionManagementMessage.RunAs.ValidateRunAs(
						projection.Mode,
						ReadWrite.Write,
						null,
						message,
						replace: projection.EnableRunAs)) {
					_logger.Info("PROJECTIONS: Projections batch rejected due to invalid run as");
					return;
				}
				if (projection.Name == null) {
					message.Envelope.ReplyWith(
						new ProjectionManagementMessage.OperationFailed("Projection name is required"));
				} else {
					if (_projections.ContainsKey(projection.Name)) {
						duplicateNames.Add(projection.Name);
					}
				}
			}
			if (duplicateNames.Any()) {
				var duplicatesMsg = $"Duplicate projection names : {string.Join(", ", duplicateNames)}";
				_logger.Debug($"PROJECTIONS: Conflict. {duplicatesMsg}");
				message.Envelope.ReplyWith(
					new ProjectionManagementMessage.Conflict(duplicatesMsg));
			} else
				PostNewProjections(projections, message.Envelope);
		}

		public void Handle(ProjectionManagementMessage.Command.Delete message) {
			if (!_projectionsStarted)
				return;
			var projection = GetProjection(message.Name);
			if (projection == null)
				message.Envelope.ReplyWith(new ProjectionManagementMessage.NotFound());
			if (IsSystemProjection(message.Name)) {
				message.Envelope.ReplyWith(
					new ProjectionManagementMessage.OperationFailed(
						"We currently don't allow for the deletion of System Projections."));
				return;
			} else {
				if (!ProjectionManagementMessage.RunAs.ValidateRunAs(projection.Mode, ReadWrite.Write, projection.RunAs,
					message)) return;
				try {
					projection.Handle(message);
				} catch (InvalidOperationException ex) {
					message.Envelope.ReplyWith(new ProjectionManagementMessage.OperationFailed(ex.Message));
					return;
				}
			}
		}

		private bool IsSystemProjection(string name) {
			return name == ProjectionNamesBuilder.StandardProjections.EventByCategoryStandardProjection ||
			       name == ProjectionNamesBuilder.StandardProjections.EventByTypeStandardProjection ||
			       name == ProjectionNamesBuilder.StandardProjections.StreamByCategoryStandardProjection ||
			       name == ProjectionNamesBuilder.StandardProjections.StreamsStandardProjection ||
			       name == ProjectionNamesBuilder.StandardProjections.EventByCorrIdStandardProjection;
		}

		public void Handle(ProjectionManagementMessage.Command.GetQuery message) {
			if (!_projectionsStarted)
				return;
			var projection = GetProjection(message.Name);
			if (projection == null)
				message.Envelope.ReplyWith(new ProjectionManagementMessage.NotFound());
			else {
				if (!ProjectionManagementMessage.RunAs.ValidateRunAs(projection.Mode, ReadWrite.Read, projection.RunAs,
					message)) return;
				projection.Handle(message);
			}
		}

		public void Handle(ProjectionManagementMessage.Command.UpdateQuery message) {
			if (!_projectionsStarted)
				return;
			_logger.Info(
				"Updating '{projection}' projection source to '{source}' (Requested type is: '{type}')",
				message.Name,
				message.Query,
				message.HandlerType);
			var projection = GetProjection(message.Name);
			if (projection == null)
				message.Envelope.ReplyWith(new ProjectionManagementMessage.NotFound());
			else {
				if (!ProjectionManagementMessage.RunAs.ValidateRunAs(projection.Mode, ReadWrite.Write, projection.RunAs,
					message)) return;
				projection.Handle(message); // update query text
			}
		}

		public void Handle(ProjectionManagementMessage.Command.Disable message) {
			if (!_projectionsStarted)
				return;
			_logger.Info("Disabling '{projection}' projection", message.Name);

			var projection = GetProjection(message.Name);
			if (projection == null)
				message.Envelope.ReplyWith(new ProjectionManagementMessage.NotFound());
			else {
				if (!ProjectionManagementMessage.RunAs.ValidateRunAs(projection.Mode, ReadWrite.Write, projection.RunAs,
					message)) return;
				projection.Handle(message);
			}
		}

		public void Handle(ProjectionManagementMessage.Command.Enable message) {
			if (!_projectionsStarted)
				return;
			_logger.Info("Enabling '{projection}' projection", message.Name);

			var projection = GetProjection(message.Name);
			if (projection == null) {
				_logger.Error("DBG: PROJECTION *{projection}* NOT FOUND.", message.Name);
				message.Envelope.ReplyWith(new ProjectionManagementMessage.NotFound());
			} else {
				if (!ProjectionManagementMessage.RunAs.ValidateRunAs(projection.Mode, ReadWrite.Write, projection.RunAs,
					message)) return;
				projection.Handle(message);
			}
		}

		public void Handle(ProjectionManagementMessage.Command.Abort message) {
			if (!_projectionsStarted)
				return;
			_logger.Info("Aborting '{projection}' projection", message.Name);

			var projection = GetProjection(message.Name);
			if (projection == null)
				message.Envelope.ReplyWith(new ProjectionManagementMessage.NotFound());
			else {
				if (!ProjectionManagementMessage.RunAs.ValidateRunAs(projection.Mode, ReadWrite.Write, projection.RunAs,
					message)) return;
				projection.Handle(message);
			}
		}

		public void Handle(ProjectionManagementMessage.Command.SetRunAs message) {
			if (!_projectionsStarted)
				return;
			_logger.Info("Setting RunAs1 account for '{projection}' projection", message.Name);

			var projection = GetProjection(message.Name);
			if (projection == null) {
				_logger.Error("DBG: PROJECTION *{projection}* NOT FOUND.", message.Name);
				message.Envelope.ReplyWith(new ProjectionManagementMessage.NotFound());
			} else {
				if (
					!ProjectionManagementMessage.RunAs.ValidateRunAs(
						projection.Mode, ReadWrite.Write, projection.RunAs, message,
						message.Action == ProjectionManagementMessage.Command.SetRunAs.SetRemove.Set)) return;

				projection.Handle(message);
			}
		}

		public void Handle(ProjectionManagementMessage.Command.Reset message) {
			if (!_projectionsStarted)
				return;
			_logger.Info("Resetting '{projection}' projection", message.Name);

			var projection = GetProjection(message.Name);
			if (projection == null) {
				_logger.Error("DBG: PROJECTION *{projection}* NOT FOUND.", message.Name);
				message.Envelope.ReplyWith(new ProjectionManagementMessage.NotFound());
			} else {
				if (!ProjectionManagementMessage.RunAs.ValidateRunAs(projection.Mode, ReadWrite.Write, projection.RunAs,
					message)) return;
				projection.Handle(message);
			}
		}

		public void Handle(ProjectionManagementMessage.Command.GetStatistics message) {
			if (!_projectionsStarted)
				return;
			if (!string.IsNullOrEmpty(message.Name)) {
				var projection = GetProjection(message.Name);
				if (projection == null)
					message.Envelope.ReplyWith(new ProjectionManagementMessage.NotFound());
				else
					message.Envelope.ReplyWith(
						new ProjectionManagementMessage.Statistics(new[] {projection.GetStatistics()}));
			} else {
				var statuses = (from projectionNameValue in _projections
					let projection = projectionNameValue.Value
					where !projection.Deleted
					where
						message.Mode == null || message.Mode == projection.Mode
						                     || (message.Mode.GetValueOrDefault() == ProjectionMode.AllNonTransient
						                         && projection.Mode != ProjectionMode.Transient)
					let status = projection.GetStatistics()
					select status).ToArray();
				message.Envelope.ReplyWith(new ProjectionManagementMessage.Statistics(statuses));
			}
		}

		public void Handle(ProjectionManagementMessage.Command.GetState message) {
			if (!_projectionsStarted)
				return;
			var projection = GetProjection(message.Name);
			if (projection == null)
				message.Envelope.ReplyWith(new ProjectionManagementMessage.NotFound());
			else
				projection.Handle(message);
		}

		public void Handle(ProjectionManagementMessage.Command.GetResult message) {
			if (!_projectionsStarted)
				return;
			var projection = GetProjection(message.Name);
			if (projection == null)
				message.Envelope.ReplyWith(new ProjectionManagementMessage.NotFound());
			else
				projection.Handle(message);
		}

		public void Handle(ProjectionManagementMessage.Command.GetConfig message) {
			if (!_projectionsStarted)
				return;
			var projection = GetProjection(message.Name);
			if (projection == null)
				message.Envelope.ReplyWith(new ProjectionManagementMessage.NotFound());
			else {
				if (!ProjectionManagementMessage.RunAs.ValidateRunAs(projection.Mode, ReadWrite.Read, projection.RunAs,
					message)) return;
				projection.Handle(message);
			}
		}

		public void Handle(ProjectionManagementMessage.Command.UpdateConfig message) {
			if (!_projectionsStarted)
				return;
			var projection = GetProjection(message.Name);
			if (projection == null)
				message.Envelope.ReplyWith(new ProjectionManagementMessage.NotFound());
			else {
				if (!ProjectionManagementMessage.RunAs.ValidateRunAs(projection.Mode, ReadWrite.Read, projection.RunAs,
					message)) return;
				try {
					projection.Handle(message);
				} catch (InvalidOperationException ex) {
					message.Envelope.ReplyWith(new ProjectionManagementMessage.OperationFailed(ex.Message));
					return;
				}
			}
		}

		public void Handle(ProjectionManagementMessage.Internal.CleanupExpired message) {
			ScheduleExpire();
			CleanupExpired();
		}

		private void CleanupExpired() {
			foreach (var managedProjection in _projections.ToArray()) {
				managedProjection.Value.Handle(new ProjectionManagementMessage.Internal.CleanupExpired());
			}
		}

		public void Handle(CoreProjectionStatusMessage.Started message) {
			string name;
			if (_projectionsMap.TryGetValue(message.ProjectionId, out name)) {
				var projection = _projections[name];
				projection.Handle(message);
			}
		}

		public void Handle(CoreProjectionStatusMessage.Stopped message) {
			string name;
			if (_projectionsMap.TryGetValue(message.ProjectionId, out name)) {
				var projection = _projections[name];
				projection.Handle(message);
			}
		}

		public void Handle(CoreProjectionStatusMessage.Faulted message) {
			string name;
			if (_projectionsMap.TryGetValue(message.ProjectionId, out name)) {
				var projection = _projections[name];
				projection.Handle(message);
			}
		}

		public void Handle(CoreProjectionStatusMessage.Prepared message) {
			string name;
			if (_projectionsMap.TryGetValue(message.ProjectionId, out name)) {
				var projection = _projections[name];
				projection.Handle(message);
			}
		}

		public void Handle(CoreProjectionStatusMessage.StateReport message) {
			_getStateDispatcher.Handle(message);
		}

		public void Handle(CoreProjectionStatusMessage.ResultReport message) {
			_getResultDispatcher.Handle(message);
		}

		public void Handle(CoreProjectionStatusMessage.StatisticsReport message) {
			string name;
			if (_projectionsMap.TryGetValue(message.ProjectionId, out name)) {
				var projection = _projections[name];
				projection.Handle(message);
			}
		}

		public void Handle(CoreProjectionManagementMessage.SlaveProjectionReaderAssigned message) {
			Action<CoreProjectionManagementMessage.SlaveProjectionReaderAssigned> action;
			if (_awaitingSlaveProjections.TryGetValue(message.ProjectionId, out action)) {
				action(message);
			}
		}

		public void Handle(ClientMessage.ReadStreamEventsBackwardCompleted message) {
			_readDispatcher.Handle(message);
		}

		public void Handle(ClientMessage.ReadStreamEventsForwardCompleted message) {
			_readForwardDispatcher.Handle(message);
		}

		public void Handle(ClientMessage.WriteEventsCompleted message) {
			_writeDispatcher.Handle(message);
		}

		public void Handle(ClientMessage.DeleteStreamCompleted message) {
			_streamDispatcher.Handle(message);
		}

		private VNodeState _currentState = VNodeState.Unknown;
		private bool _systemIsReady = false;
		private bool _ready = false;
		private Guid _epochId = Guid.Empty;

		public void Handle(SystemMessage.SystemCoreReady message) {
			_systemIsReady = true;
			StartWhenConditionsAreMet();
		}

		public void Handle(SystemMessage.StateChangeMessage message) {
			_currentState = message.State;
			if (_currentState != VNodeState.Master)
				_ready = false;

			StartWhenConditionsAreMet();
		}

		public void Handle(SystemMessage.EpochWritten message) {
			if (_ready) return;

			if (_currentState == VNodeState.Master) {
				_epochId = message.Epoch.EpochId;
				_ready = true;
			}

			StartWhenConditionsAreMet();
		}

		private void StartWhenConditionsAreMet() {
			//run if and only if these conditions are met
			if (_systemIsReady && _ready) {
				if (!_started) {
					_logger.Debug("PROJECTIONS: Starting Projections Manager. (Node State : {state})", _currentState);
					Start();
				}
			} else {
				if (_started) {
					_logger.Debug("PROJECTIONS: Stopping Projections Manager. (Node State : {state})", _currentState);
					Stop();
				}
			}
		}

		public void Handle(ProjectionManagementMessage.Internal.Deleted message) {
			var deletedEventId = Guid.NewGuid();
			DeleteProjection(message,
				expVer => {
					_awaitingSlaveProjections.Remove(message.Id); // if any disconnected in error
					_projections.Remove(message.Name);
					_projectionsMap.Remove(message.Id);
					_projectionsRegistrationExpectedVersion = expVer;
				});
		}

		private void DeleteProjection(
			ProjectionManagementMessage.Internal.Deleted message, Action<long> completed, int retryCount = ProjectionCreationRetryCount) {
			var corrId = Guid.NewGuid();
			var writeDelete = new ClientMessage.WriteEvents(
					corrId,
					corrId,
					_writeDispatcher.Envelope,
					true,
					ProjectionNamesBuilder.ProjectionsRegistrationStream,
					_projectionsRegistrationExpectedVersion,
					new Event(
						Guid.NewGuid(),
						ProjectionEventTypes.ProjectionDeleted,
						false,
						Helper.UTF8NoBom.GetBytes(message.Name),
						Empty.ByteArray),
					SystemAccount.Principal);

			BeginWriteProjectionDeleted(writeDelete, message, completed);
		}

		public void Handle(ProjectionManagementMessage.RegisterSystemProjection message) {
			if (!_projections.ContainsKey(message.Name)) {
				Handle(
					new ProjectionManagementMessage.Command.Post(
						new PublishEnvelope(_inputQueue),
						ProjectionMode.Continuous,
						message.Name,
						ProjectionManagementMessage.RunAs.System,
						message.Handler,
						message.Query,
						true,
						true,
						true,
						true,
						enableRunAs: true));
			}
		}

		public void Dispose() {
			foreach (var projection in _projections.Values)
				projection.Dispose();
			_projections.Clear();
		}

		private ManagedProjection GetProjection(string name) {
			ManagedProjection result;
			return _projections.TryGetValue(name, out result) ? result : null;
		}

		private void StartExistingProjections(Action completed) {
			var registeredProjections = new Dictionary<string, long>();
			ReadProjectionsList(
				registeredProjections,
				r => StartRegisteredProjections(r, completed));
		}

		private void ReadProjectionsList(
			IDictionary<string, long> registeredProjections,
			Action<IDictionary<string, long>> completedAction,
			long from = 0) {

			_logger.Debug("PROJECTIONS: Reading Existing Projections from {stream}", ProjectionNamesBuilder.ProjectionsRegistrationStream);
			var corrId = Guid.NewGuid();
			_readForwardDispatcher.Publish(
				new ClientMessage.ReadStreamEventsForward(
					corrId,
					corrId,
					_readDispatcher.Envelope,
					ProjectionNamesBuilder.ProjectionsRegistrationStream,
					from,
					_readEventsBatchSize,
					resolveLinkTos: false,
					requireMaster: false,
					validationStreamVersion: null,
					user: SystemAccount.Principal),
				m => OnProjectionsListReadCompleted(m, registeredProjections, from, completedAction));
		}

		private void OnProjectionsListReadCompleted(
			ClientMessage.ReadStreamEventsForwardCompleted msg,
			IDictionary<string, long> registeredProjections,
			long requestedFrom,
			Action<IDictionary<string, long>> completedAction) {
			switch (msg.Result) {
				case ReadStreamResult.Success:
					foreach (var evnt in msg.Events) {
						var projectionId = evnt.Event.EventNumber;
						if (projectionId == 0)
							projectionId = Int32.MaxValue - 1;
						if (evnt.Event.EventType == ProjectionEventTypes.ProjectionsInitialized) {
							registeredProjections.Add(ProjectionEventTypes.ProjectionsInitialized, projectionId);
							continue;
						}

						var projectionName = Helper.UTF8NoBom.GetString(evnt.Event.Data);
						if (string.IsNullOrEmpty(projectionName)
						    || _projections.ContainsKey(projectionName)) {
							_logger.Warn(
								"PROJECTIONS: The following projection: {projection} has a duplicate registration event.",
								projectionName);
							continue;
						}

						if (evnt.Event.EventType == ProjectionEventTypes.ProjectionCreated) {
							if (registeredProjections.ContainsKey(projectionName)) {
								registeredProjections[projectionName] = projectionId;
								_logger.Warn(
									"PROJECTIONS: The following projection: {projection} has a duplicate created event. Using projection Id {projectionId}",
									projectionName, projectionId);
								continue;
							}

							registeredProjections.Add(projectionName, projectionId);
						} else if (evnt.Event.EventType == ProjectionEventTypes.ProjectionDeleted) {
							registeredProjections.Remove(projectionName);
						}
					}
					_projectionsRegistrationExpectedVersion = msg.LastEventNumber;

					if (!msg.IsEndOfStream) {
						ReadProjectionsList(registeredProjections, completedAction, @from: msg.NextEventNumber);
						return;
					}

					break;
				case ReadStreamResult.StreamDeleted:
				case ReadStreamResult.Error:
				case ReadStreamResult.AccessDenied:
					_logger.Fatal(
						"There was an error reading the projections list due to {e}. Projections could not be loaded.",
						msg.Result);
					return;
			}
			completedAction(registeredProjections);
		}

		private void StartRegisteredProjections(IDictionary<string, long> registeredProjections,
			Action completedAction) {
			if (!registeredProjections.Any()) {
				_logger.Debug("PROJECTIONS: No projections were found in {stream}, starting from empty stream",
					ProjectionNamesBuilder.ProjectionsRegistrationStream);
				WriteProjectionsInitialized(
					() => {
						completedAction();
						CreateSystemProjections();
					},
					Guid.NewGuid());
				return;
			}

			List<string> projections = registeredProjections
				.Where(x => x.Key != ProjectionEventTypes.ProjectionsInitialized)
				.Select(x => x.Key).ToList();

			_logger.Debug(
				"PROJECTIONS: Found the following projections in {stream}: " +
				(LogManager.StructuredLog ? "{@projections}" : "{projections}"),
				ProjectionNamesBuilder.ProjectionsRegistrationStream,
				LogManager.StructuredLog ? (object)projections : (object)String.Join(", ", projections));

			//create any missing system projections
			CreateSystemProjections(registeredProjections.Select(x => x.Key).ToList());

			foreach (var projectionRegistration in registeredProjections.Where(x =>
				x.Key != ProjectionEventTypes.ProjectionsInitialized)) {
				int queueIndex = GetNextWorkerIndex();
				var managedProjection = CreateManagedProjectionInstance(
					projectionRegistration.Key,
					projectionRegistration.Value,
					Guid.NewGuid(),
					_workers[queueIndex]);
				managedProjection.InitializeExisting(projectionRegistration.Key);
			}

			completedAction();
		}

		private bool IsProjectionEnabledToRunByMode(string projectionName) {
			return _runProjections >= ProjectionType.All
			       || _runProjections == ProjectionType.System && projectionName.StartsWith("$");
		}

		private void WriteProjectionsInitialized(Action action, Guid registrationEventId) {
			var corrId = Guid.NewGuid();
			_writeDispatcher.Publish(
				new ClientMessage.WriteEvents(
					corrId,
					corrId,
					_writeDispatcher.Envelope,
					true,
					ProjectionNamesBuilder.ProjectionsRegistrationStream,
					ExpectedVersion.NoStream,
					new Event(registrationEventId, ProjectionEventTypes.ProjectionsInitialized, false, Empty.ByteArray,
						Empty.ByteArray),
					SystemAccount.Principal),
				completed => WriteProjectionsInitializedCompleted(completed, registrationEventId, action));
		}

		private void WriteProjectionsInitializedCompleted(ClientMessage.WriteEventsCompleted completed,
			Guid registrationEventId, Action action) {
			switch (completed.Result) {
				case OperationResult.Success:
					action();
					break;
				case OperationResult.CommitTimeout:
				case OperationResult.ForwardTimeout:
				case OperationResult.PrepareTimeout:
					WriteProjectionsInitialized(action, registrationEventId);
					break;
				default:
					_logger.Fatal("Cannot initialize projections subsystem. Cannot write a fake projection");
					break;
			}
		}

		private void CreateSystemProjections() {
			CreateSystemProjections(new List<string>());
		}

		private void CreateSystemProjections(List<string> existingSystemProjections) {
			var systemProjections = new List<ProjectionManagementMessage.Command.PostBatch.ProjectionPost>();

			if (!_initializeSystemProjections) {
				return;
			}

			if (!existingSystemProjections.Contains(ProjectionNamesBuilder.StandardProjections
				.StreamsStandardProjection))
				systemProjections.Add(CreateSystemProjectionPost(
					ProjectionNamesBuilder.StandardProjections.StreamsStandardProjection,
					typeof(IndexStreams),
					""));

			if (!existingSystemProjections.Contains(ProjectionNamesBuilder.StandardProjections
				.StreamByCategoryStandardProjection))
				systemProjections.Add(CreateSystemProjectionPost(
					ProjectionNamesBuilder.StandardProjections.StreamByCategoryStandardProjection,
					typeof(CategorizeStreamByPath),
					"first\r\n-"));

			if (!existingSystemProjections.Contains(ProjectionNamesBuilder.StandardProjections
				.EventByCategoryStandardProjection))
				systemProjections.Add(CreateSystemProjectionPost(
					ProjectionNamesBuilder.StandardProjections.EventByCategoryStandardProjection,
					typeof(CategorizeEventsByStreamPath),
					"first\r\n-"));

			if (!existingSystemProjections.Contains(ProjectionNamesBuilder.StandardProjections
				.EventByTypeStandardProjection))
				systemProjections.Add(CreateSystemProjectionPost(
					ProjectionNamesBuilder.StandardProjections.EventByTypeStandardProjection,
					typeof(IndexEventsByEventType),
					""));

			if (!existingSystemProjections.Contains(ProjectionNamesBuilder.StandardProjections
				.EventByCorrIdStandardProjection))
				systemProjections.Add(CreateSystemProjectionPost(
					ProjectionNamesBuilder.StandardProjections.EventByCorrIdStandardProjection,
					typeof(ByCorrelationId),
					"{\"correlationIdProperty\":\"$correlationId\"}"));

			IEnvelope envelope = new NoopEnvelope();
			var postBatchMessage = new ProjectionManagementMessage.Command.PostBatch(
				envelope, ProjectionManagementMessage.RunAs.System, systemProjections.ToArray());
			_publisher.Publish(postBatchMessage);
		}

		private ProjectionManagementMessage.Command.PostBatch.ProjectionPost CreateSystemProjectionPost
			(string name, Type handlerType, string config) {
			return new ProjectionManagementMessage.Command.PostBatch.ProjectionPost(
				ProjectionMode.Continuous,
				ProjectionManagementMessage.RunAs.System,
				name,
				"native:" + handlerType.Namespace + "." + handlerType.Name,
				config,
				enabled: false,
				checkpointsEnabled: true,
				emitEnabled: true,
				trackEmittedStreams: false,
				enableRunAs: true);
		}

		private void InitializeNewProjection(NewProjectionInitializer initializer,
			long version, IEnvelope replyEnvelope) {
			try {
				int queueIndex = GetNextWorkerIndex();
				initializer.CreateAndInitializeNewProjection(this, Guid.NewGuid(), _workers[queueIndex],
					version: version);
			} catch (Exception ex) {
				replyEnvelope.ReplyWith(new ProjectionManagementMessage.OperationFailed(ex.Message));
			}
		}

		private void PostNewProjections
			(IDictionary<string, PendingProjection> newProjections, IEnvelope replyEnvelope) {
			var corrId = Guid.NewGuid();
			var events = new List<Event>();
			foreach (var pair in newProjections) {
				var projection = pair.Value;
				if (projection.Mode >= ProjectionMode.OneTime) {
					var eventId = Guid.NewGuid();
					events.Add(new Event(
						eventId,
						ProjectionEventTypes.ProjectionCreated,
						false,
						Helper.UTF8NoBom.GetBytes(projection.Name),
						Empty.ByteArray));
				} else {
					// Initialize transient projection
					var initializer = projection.CreateInitializer(ProjectionQueryId, replyEnvelope);
					ReadProjectionPossibleStream(projection.Name,
					m => ReadProjectionPossibleStreamCompleted
						(m, initializer, replyEnvelope));
				}
			}

			if (events.Any()) {
				var writeEvents = new ClientMessage.WriteEvents(
					corrId,
					corrId,
					_writeDispatcher.Envelope,
					true,
					ProjectionNamesBuilder.ProjectionsRegistrationStream,
					_projectionsRegistrationExpectedVersion,
					events.ToArray(),
					SystemAccount.Principal);

				BeginWriteNewProjections(writeEvents, newProjections, replyEnvelope);
			}
		}

		private void BeginWriteNewProjections(ClientMessage.WriteEvents writeEvents,
			IDictionary<string, PendingProjection> newProjections, IEnvelope replyEnvelope,
			int retryCount = ProjectionCreationRetryCount) {

			_writeDispatcher.Publish(
				writeEvents,
				m => WriteNewProjectionsCompleted(m, writeEvents, newProjections, replyEnvelope, retryCount));
		}

		private void WriteNewProjectionsCompleted(ClientMessage.WriteEventsCompleted completed,
			ClientMessage.WriteEvents write, IDictionary<string, PendingProjection> newProjections,
			IEnvelope envelope, int retryCount = ProjectionCreationRetryCount) {

			if (completed.Result == OperationResult.Success) {
				ReadProjectionsList(
					new Dictionary<string, long>(),
					r => StartNewlyRegisteredProjections(r, newProjections, OnProjectionsRegistrationCaughtUp, envelope),
					completed.FirstEventNumber);
				return;
			}

			_logger.Info(
				"PROJECTIONS: Created event for projections has not been written to {stream}: " +
				(LogManager.StructuredLog ? "{@projections}" : "{projections}") +
				". Error: {error}",
				ProjectionNamesBuilder.ProjectionsRegistrationStream,
				LogManager.StructuredLog ? (object)newProjections.Keys : (object)string.Join(", ", newProjections.Keys),
				Enum.GetName(typeof(OperationResult), completed.Result));
			
			if (completed.Result == OperationResult.ForwardTimeout ||
				completed.Result == OperationResult.PrepareTimeout ||
				completed.Result == OperationResult.CommitTimeout) {
				if (retryCount > 0) {
					_logger.Info("PROJECTIONS: Retrying write projection creations for " +
								 (LogManager.StructuredLog ? "{@projections}" : "{projections}"),
								 LogManager.StructuredLog ? (object)newProjections.Keys : (object)string.Join(", ", newProjections.Keys));
					BeginWriteNewProjections(write, newProjections, envelope, retryCount - 1);
					return;
				}
			}

			if (completed.Result == OperationResult.WrongExpectedVersion
				&& (completed.CurrentVersion < 0
				|| completed.CurrentVersion > _projectionsRegistrationExpectedVersion)) {
				ReadProjectionsList(
					new Dictionary<string, long>(),
					r => StartNewlyRegisteredProjections(r, newProjections, OnProjectionsRegistrationCaughtUp, envelope),
					_projectionsRegistrationExpectedVersion + 1);
				return;
			}

			envelope.ReplyWith(new ProjectionManagementMessage.OperationFailed(
				string.Format(
					"The projections '{0}' could not be created because the registration could not be written due to {1}",
					string.Join(", ", newProjections.Keys), completed.Result)));
		}

		private void StartNewlyRegisteredProjections
			(IDictionary<string, long> registeredProjections,
			IDictionary<string, PendingProjection> newProjections,
			Action completedAction, IEnvelope replyEnvelope) {

			if (!registeredProjections.Any()) {
				replyEnvelope.ReplyWith(
					new ProjectionManagementMessage.OperationFailed("Projections were invalid"));
				return;
			}

			var registrations = registeredProjections
				.Where(x => x.Key != ProjectionEventTypes.ProjectionsInitialized)
				.ToDictionary(p => p.Key, p => p.Value);

			_logger.Debug(
				"PROJECTIONS: Found the following new projections in {stream}: " +
				(LogManager.StructuredLog ? "{@projections}" : "{projections}"),
				ProjectionNamesBuilder.ProjectionsRegistrationStream,
				LogManager.StructuredLog ? (object)registrations.Keys : (object)String.Join(", ", registrations.Keys));

			try {
				foreach (var projectionRegistration in registrations) {
					if (!newProjections.TryGetValue(projectionRegistration.Key, out var posted)) {
						_logger.Trace("PROJECTIONS: The projection {projectionName} was registered, but has no configuration. Projection will not be added",
						projectionRegistration.Key);
						continue;
					}

					var initializer = posted.CreateInitializer(projectionRegistration.Value, replyEnvelope);
					ReadProjectionPossibleStream(projectionRegistration.Key,
						m => ReadProjectionPossibleStreamCompleted
							(m, initializer, replyEnvelope));
				}
			} catch (Exception ex) {
				replyEnvelope.ReplyWith(new ProjectionManagementMessage.OperationFailed(ex.Message));
			}

			completedAction();
		}

		private void ReadProjectionPossibleStream(
			string projectionName,
			Action<ClientMessage.ReadStreamEventsBackwardCompleted> onComplete) {
			var corrId = Guid.NewGuid();
			_readDispatcher.Publish(
				new ClientMessage.ReadStreamEventsBackward(
					corrId,
					corrId,
					_readDispatcher.Envelope,
					ProjectionNamesBuilder.ProjectionsStreamPrefix + projectionName,
					0,
					_readEventsBatchSize,
					resolveLinkTos: false,
					requireMaster: false,
					validationStreamVersion: null,
					user: SystemAccount.Principal),
				onComplete);
		}

		private void ReadProjectionPossibleStreamCompleted(
			ClientMessage.ReadStreamEventsBackwardCompleted completed,
			NewProjectionInitializer initializer,
			IEnvelope replyEnvelope) {

			long version = -1;
			if (completed.Result == ReadStreamResult.Success) {
				version = completed.LastEventNumber + 1;
			}

			InitializeNewProjection(initializer, version, replyEnvelope);
		}

		private void OnProjectionsRegistrationCaughtUp() {
			_logger.Debug($"PROJECTIONS: Caught up with projections registration. Next expected version: {_projectionsRegistrationExpectedVersion}");
		}

		private void BeginWriteProjectionDeleted(
			ClientMessage.WriteEvents writeDelete,
			ProjectionManagementMessage.Internal.Deleted message,
			Action<long> onCompleted,
			int retryCount = ProjectionCreationRetryCount) {
			_writeDispatcher.Publish(
				writeDelete,
				m => WriteProjectionDeletedCompleted(m, writeDelete, message, onCompleted, retryCount));
		}

		private void WriteProjectionDeletedCompleted(ClientMessage.WriteEventsCompleted writeCompleted,
			ClientMessage.WriteEvents writeDelete,
			ProjectionManagementMessage.Internal.Deleted message,
			Action<long> onCompleted,
			int retryCount) {

			if (writeCompleted.Result == OperationResult.Success) {
				onCompleted?.Invoke(writeCompleted.LastEventNumber);
				return;
			}

			_logger.Info(
				"PROJECTIONS: Projection '{projection}' deletion has not been written to {stream}. Error: {e}",
				message.Name,
				ProjectionNamesBuilder.ProjectionsRegistrationStream,
				Enum.GetName(typeof(OperationResult), writeCompleted.Result));

			if (writeCompleted.Result == OperationResult.CommitTimeout || writeCompleted.Result == OperationResult.ForwardTimeout
																	   || writeCompleted.Result == OperationResult.PrepareTimeout) {
				if (retryCount > 0) {
					_logger.Info("PROJECTIONS: Retrying write projection deletion for {projection}", message.Name);
					BeginWriteProjectionDeleted(writeDelete, message, onCompleted, retryCount - 1);
					return;
				}
			}

			if (writeCompleted.Result == OperationResult.WrongExpectedVersion
				&& (writeCompleted.CurrentVersion < 0
				|| writeCompleted.CurrentVersion > _projectionsRegistrationExpectedVersion)) {
				_logger.Info("PROJECTIONS: Got wrong expected version writing projection deletion for {projection}." +
					"Reading {registrationStream} before retrying", message.Name, ProjectionNamesBuilder.ProjectionsRegistrationStream);
				ReadProjectionsList(
					new Dictionary<string, long>(),
					m => DeleteProjection(message, onCompleted, retryCount: --retryCount),
					_projectionsRegistrationExpectedVersion + 1);
				return;
			}

			_logger.Error(
				"PROJECTIONS: The projection '{0}' could not be deleted because the deletion event could not be written due to {1}",
				message.Name, writeCompleted.Result);
		}

		public class NewProjectionInitializer {
			private readonly long _projectionId;
			private readonly bool _enabled;
			private readonly string _handlerType;
			private readonly string _query;
			private readonly ProjectionMode _projectionMode;
			private readonly bool _emitEnabled;
			private readonly bool _checkpointsEnabled;
			private readonly bool _trackEmittedStreams;
			private readonly bool _enableRunAs;
			private readonly ProjectionManagementMessage.RunAs _runAs;
			private readonly IEnvelope _replyEnvelope;
			private readonly string _name;

			public NewProjectionInitializer(
				long projectionId,
				string name,
				ProjectionMode projectionMode,
				string handlerType,
				string query,
				bool enabled,
				bool emitEnabled,
				bool checkpointsEnabled,
				bool enableRunAs,
				bool trackEmittedStreams,
				ProjectionManagementMessage.RunAs runAs,
				IEnvelope replyEnvelope) {
				if (projectionMode >= ProjectionMode.Continuous && !checkpointsEnabled)
					throw new InvalidOperationException("Continuous mode requires checkpoints");

				if (emitEnabled && !checkpointsEnabled)
					throw new InvalidOperationException("Emit requires checkpoints");

				_projectionId = projectionId;
				_enabled = enabled;
				_handlerType = handlerType;
				_query = query;
				_projectionMode = projectionMode;
				_emitEnabled = emitEnabled;
				_checkpointsEnabled = checkpointsEnabled;
				_trackEmittedStreams = trackEmittedStreams;
				_enableRunAs = enableRunAs;
				_runAs = runAs;
				_replyEnvelope = replyEnvelope;
				_name = name;
			}

			public void CreateAndInitializeNewProjection(
				ProjectionManager projectionManager,
				Guid projectionCorrelationId,
				Guid workerId,
				bool isSlave = false,
				Guid slaveMasterWorkerId = default(Guid),
				Guid slaveMasterCorrelationId = default(Guid),
				long? version = -1) {
				var projection = projectionManager.CreateManagedProjectionInstance(
					_name,
					_projectionId,
					projectionCorrelationId,
					workerId,
					isSlave,
					slaveMasterWorkerId,
					slaveMasterCorrelationId);
				projection.InitializeNew(
					new ManagedProjection.PersistedState {
						Enabled = _enabled,
						HandlerType = _handlerType,
						Query = _query,
						Mode = _projectionMode,
						EmitEnabled = _emitEnabled,
						CheckpointsDisabled = !_checkpointsEnabled,
						TrackEmittedStreams = _trackEmittedStreams,
						CheckpointHandledThreshold = ProjectionConsts.CheckpointHandledThreshold,
						CheckpointAfterMs = (int)ProjectionConsts.CheckpointAfterMs.TotalMilliseconds,
						MaxAllowedWritesInFlight = ProjectionConsts.MaxAllowedWritesInFlight,
						Epoch = -1,
						Version = version,
						RunAs = _enableRunAs ? SerializedRunAs.SerializePrincipal(_runAs) : null
					},
					_replyEnvelope);
			}
		}

		private ManagedProjection CreateManagedProjectionInstance(
			string name,
			long projectionId,
			Guid projectionCorrelationId,
			Guid workerID,
			bool isSlave = false,
			Guid slaveMasterWorkerId = default(Guid),
			Guid slaveMasterCorrelationId = default(Guid)) {
			var enabledToRun = IsProjectionEnabledToRunByMode(name);
			var workerId = workerID;
			var managedProjectionInstance = new ManagedProjection(
				workerId,
				projectionCorrelationId,
				projectionId,
				name,
				enabledToRun,
				_logger,
				_streamDispatcher,
				_writeDispatcher,
				_readDispatcher,
				_publisher,
				_timeProvider,
				_getStateDispatcher,
				_getResultDispatcher,
				_ioDispatcher,
				_projectionsQueryExpiry,
				isSlave,
				slaveMasterWorkerId,
				slaveMasterCorrelationId);

			_projectionsMap.Add(projectionCorrelationId, name);
			_projections.Add(name, managedProjectionInstance);
			_logger.Debug("Adding projection {projectionCorrelationId}@{projection} to list", projectionCorrelationId,
				name);
			return managedProjectionInstance;
		}

		private int GetNextWorkerIndex() {
			if (_lastUsedQueue >= _workers.Length)
				_lastUsedQueue = 0;
			var queueIndex = _lastUsedQueue;
			_lastUsedQueue++;
			return queueIndex;
		}

		private readonly Dictionary<Guid, Action<CoreProjectionManagementMessage.SlaveProjectionReaderAssigned>>
			_awaitingSlaveProjections =
				new Dictionary<Guid, Action<CoreProjectionManagementMessage.SlaveProjectionReaderAssigned>>();


		public void Handle(ProjectionManagementMessage.Command.StartSlaveProjections message) {
			var result = new Dictionary<string, SlaveProjectionCommunicationChannel[]>();
			var counter = 0;
			foreach (var g in message.SlaveProjections.Definitions) {
				var @group = g;
				switch (g.RequestedNumber) {
					case SlaveProjectionDefinitions.SlaveProjectionRequestedNumber.One:
					case SlaveProjectionDefinitions.SlaveProjectionRequestedNumber.OnePerNode: {
						var resultArray = new SlaveProjectionCommunicationChannel[1];
						result.Add(g.Name, resultArray);
						counter++;
						int queueIndex = GetNextWorkerIndex();
						CINP(
							message,
							@group,
							resultArray,
							queueIndex,
							0,
							() => CheckSlaveProjectionsStarted(message, ref counter, result));
						break;
					}
					case SlaveProjectionDefinitions.SlaveProjectionRequestedNumber.OnePerThread: {
						var resultArray = new SlaveProjectionCommunicationChannel[_workers.Length];
						result.Add(g.Name, resultArray);

						for (int index = 0; index < _workers.Length; index++) {
							counter++;
							CINP(
								message,
								@group,
								resultArray,
								index,
								index,
								() => CheckSlaveProjectionsStarted(message, ref counter, result));
						}

						break;
					}
					default:
						throw new NotSupportedException();
				}
			}
		}

		private static void CheckSlaveProjectionsStarted(
			ProjectionManagementMessage.Command.StartSlaveProjections message,
			ref int counter,
			Dictionary<string, SlaveProjectionCommunicationChannel[]> result) {
			counter--;
			if (counter == 0)
				message.Envelope.ReplyWith(
					new ProjectionManagementMessage.SlaveProjectionsStarted(
						message.MasterCorrelationId,
						message.MasterWorkerId,
						new SlaveProjectionCommunicationChannels(result)));
		}

		private void CINP(
			ProjectionManagementMessage.Command.StartSlaveProjections message,
			SlaveProjectionDefinitions.Definition @group,
			SlaveProjectionCommunicationChannel[] resultArray,
			int queueIndex,
			int arrayIndex,
			Action completed) {
			var projectionCorrelationId = Guid.NewGuid();
			var slaveProjectionName = message.Name + "-" + @group.Name + "-" + queueIndex;
			_awaitingSlaveProjections.Add(
				projectionCorrelationId,
				assigned => {
					var queueWorkerId = _workers[queueIndex];

					resultArray[arrayIndex] = new SlaveProjectionCommunicationChannel(
						slaveProjectionName,
						queueWorkerId,
						assigned.SubscriptionId);
					completed();

					_awaitingSlaveProjections.Remove(projectionCorrelationId);
				});


			var initializer = new NewProjectionInitializer(
				ProjectionQueryId,
				slaveProjectionName,
				@group.Mode,
				@group.HandlerType,
				@group.Query,
				true,
				@group.EmitEnabled,
				@group.CheckpointsEnabled,
				@group.EnableRunAs,
				@group.TrackEmittedStreams,
				@group.RunAs1,
				replyEnvelope: null);

			initializer.CreateAndInitializeNewProjection(
				this,
				projectionCorrelationId,
				_workers[queueIndex],
				true,
				message.MasterWorkerId,
				message.MasterCorrelationId);
		}

		public void Handle(CoreProjectionStatusMessage.ProjectionWorkerStarted message) {
			RebalanceWork();
		}

		private void RebalanceWork() {
			//
		}

		public class PendingProjection {
			public ProjectionMode Mode { get; }
			public SerializedRunAs RunAs { get; }
			public string Name { get; }
			public string HandlerType { get; }
			public string Query { get; }
			public bool Enabled { get; }
			public bool CheckpointsEnabled { get; }
			public bool EmitEnabled { get; }
			public bool EnableRunAs { get; }
			public bool TrackEmittedStreams { get; }

			public PendingProjection(
				ProjectionMode mode, SerializedRunAs runAs, string name, string handlerType, string query,
				bool enabled, bool checkpointsEnabled, bool emitEnabled, bool enableRunAs,
				bool trackEmittedStreams) {
				Mode = mode;
				RunAs = runAs;
				Name = name;
				HandlerType = handlerType;
				Query = query;
				Enabled = enabled;
				CheckpointsEnabled = checkpointsEnabled;
				EmitEnabled = emitEnabled;
				EnableRunAs = enableRunAs;
				TrackEmittedStreams = trackEmittedStreams;
			}

			public PendingProjection(ProjectionManagementMessage.Command.PostBatch.ProjectionPost projection)
				: this(projection.Mode, projection.RunAs, projection.Name, projection.HandlerType,
					projection.Query, projection.Enabled, projection.CheckpointsEnabled,
					projection.EmitEnabled, projection.EnableRunAs, projection.TrackEmittedStreams) { }

			public PendingProjection(ProjectionManagementMessage.Command.Post projection)
				: this(projection.Mode, projection.RunAs, projection.Name, projection.HandlerType,
					projection.Query, projection.Enabled, projection.CheckpointsEnabled,
					projection.EmitEnabled, projection.EnableRunAs, projection.TrackEmittedStreams) { }


			public NewProjectionInitializer CreateInitializer(long projectionId, IEnvelope replyEnvelope) =>
				new NewProjectionInitializer(
					projectionId,
					Name,
					Mode,
					HandlerType,
					Query,
					Enabled,
					EmitEnabled,
					CheckpointsEnabled,
					EnableRunAs,
					TrackEmittedStreams,
					RunAs,
					replyEnvelope);
		}
	}
}
