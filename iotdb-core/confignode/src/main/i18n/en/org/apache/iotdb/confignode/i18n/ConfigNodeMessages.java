/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.confignode.i18n;

public final class ConfigNodeMessages {

  public static final String ACQUIRE_TRIGGERTABLELOCK = "acquire TriggerTableLock";
  public static final String ACQUIRE_UDFTABLELOCK = "acquire UDFTableLock";
  public static final String ACTIVATING = "Activating {}...";
  public static final String ADJUSTREGIONGROUPNUM_THE_MAXIMUM_NUMBER_OF_DATAREGIONGROUPS_FOR =
      "[AdjustRegionGroupNum] The maximum number of DataRegionGroups for Database: {} is adjusted to: {}";
  public static final String ADJUSTREGIONGROUPNUM_THE_MAXIMUM_NUMBER_OF_SCHEMAREGIONGROUPS_FOR =
      "[AdjustRegionGroupNum] The maximum number of SchemaRegionGroups for Database: {} is adjusted to: {}";
  public static final String ADJUSTREGIONGROUPNUM_THE_MINIMUM_NUMBER_OF_DATAREGIONGROUPS_FOR =
      "[AdjustRegionGroupNum] The minimum number of DataRegionGroups for Database: {} is adjusted to: {}";
  public static final String ADJUSTREGIONGROUPNUM_THE_MINIMUM_NUMBER_OF_SCHEMAREGIONGROUPS_FOR =
      "[AdjustRegionGroupNum] The minimum number of SchemaRegionGroups for Database: {} is adjusted to: {}";
  public static final String CANNOT_FIND_REGIONGROUP_FOR_REGION_WHEN_ADDREGIONNEWLOCATION_IN =
      "Cannot find RegionGroup for region {} when addRegionNewLocation in {}";
  public static final String CANNOT_FIND_REGIONGROUP_FOR_REGION_WHEN_REMOVEREGIONOLDLOCATION_IN =
      "Cannot find RegionGroup for region {} when removeRegionOldLocation in {}";
  public static final String CAN_ONLY_ALTER_DATATYPE_OF_FIELD_COLUMNS =
      "Can only alter datatype of FIELD columns";
  public static final String CAN_T_CLOSE_STANDALONELOG_FOR_CONFIGNODE_SIMPLECONSENSUS_MODE =
      "Can't close StandAloneLog for ConfigNode SimpleConsensus mode, ";
  public static final String CAN_T_CONNECT_TO_DATA_NODE = "Can't connect to Data node: {}";
  public static final String CAN_T_CONSTRUCT_CLUSTERSCHEMAINFO =
      "Can't construct ClusterSchemaInfo";
  public static final String CAN_T_DELETE_TEMPORARY_SNAPSHOT_FILE_RETRYING =
      "Can't delete temporary snapshot file: {}, retrying...";
  public static final String CAN_T_FORCE_LOGWRITER_FOR_CONFIGNODE_FLUSHWALFORSIMPLECONSENSUS =
      "Can't force logWriter for ConfigNode flushWALForSimpleConsensus";
  public static final String CAN_T_FORCE_LOGWRITER_FOR_CONFIGNODE_SIMPLECONSENSUS_MODE =
      "Can't force logWriter for ConfigNode SimpleConsensus mode";
  public static final String CAN_T_SERIALIZE_CURRENT_CONFIGPHYSICALPLAN_FOR_CONFIGNODE_SIMPLECONSENSUS_MODE =
      "Can't serialize current ConfigPhysicalPlan for ConfigNode SimpleConsensus mode";
  public static final String CAN_T_START_CONFIGNODE_CONSENSUS_GROUP =
      "Can't start ConfigNode consensus group!";
  public static final String CHANGE_REGIONS_LEADER_ERROR_ON_DATE_NODE =
      "Change regions leader error on Date node: {}";
  public static final String CHECK_BEFORE_DROPPING_TOPIC_TOPIC_EXISTS =
      "Check before dropping topic: {}, topic exists: {}";
  public static final String CHECK_BEFORE_DROP_PIPE_PIPE_EXISTS =
      "Check before drop pipe {}, pipe exists: {}.";
  public static final String CLUSTERID_HAS_BEEN_GENERATED = "clusterID has been generated: {}";
  public static final String CLUSTERID_HAS_BEEN_RECOVERED_FROM_SNAPSHOT =
      "clusterID has been recovered from snapshot: {}";
  public static final String CLUSTERID_NOT_GENERATED_YET_SHOULD_NEVER_HAPPEN =
      "clusterId not generated yet, should never happen.";
  public static final String CONFIGNODESNAPSHOT_FINISH_TO_TAKE_SNAPSHOT_FOR_TIME_CONSUMPTION_MS =
      "[ConfigNodeSnapshot] Finish to take snapshot for {}, time consumption: {} ms";
  public static final String CONFIGNODESNAPSHOT_LOAD_SNAPSHOT_FOR_COST_MS =
      "[ConfigNodeSnapshot] Load snapshot for {} cost {} ms";
  public static final String CONFIGNODESNAPSHOT_LOAD_SNAPSHOT_SUCCESS_LATESTSNAPSHOTROOTDIR =
      "[ConfigNodeSnapshot] Load snapshot success, latestSnapshotRootDir: {}";
  public static final String CONFIGNODESNAPSHOT_START_TO_LOAD_SNAPSHOT_FOR_FROM =
      "[ConfigNodeSnapshot] Start to load snapshot for {} from {}";
  public static final String CONFIGNODESNAPSHOT_START_TO_TAKE_SNAPSHOT_FOR_INTO =
      "[ConfigNodeSnapshot] Start to take snapshot for {} into {}";
  public static final String CONFIGNODESNAPSHOT_TASK_SNAPSHOT_SUCCESS_SNAPSHOTDIR =
      "[ConfigNodeSnapshot] Task snapshot success, snapshotDir: {}";
  public static final String CONFIGNODE_EXITING = "ConfigNode exiting...";
  public static final String CONFIGNODE_NEED_REDIRECT_TO_RETRY =
      "ConfigNode need redirect to  {}, retry {} ...";
  public static final String CONFIGNODE_MEMORY_PROPORTION_SHOULD_BE_IN_THE_FORM_OF_PIPE_FREE =
      "The parameter confignode_memory_proportion should be in the form of Pipe:Free, "
          + "but got {}. Use default value 1:9.";
  public static final String INITIAL_CONFIGNODE_ALLOCATE_MEMORY_FOR_PIPE =
      "initial ConfigNode allocateMemoryForPipe = {}";
  public static final String INITIAL_CONFIGNODE_FREE_MEMORY =
      "initial ConfigNode freeMemory = {}";
  public static final String CONFIGNODE_PORT_CHECK_SUCCESSFUL = "configNode port check successful.";
  public static final String CONFIGNODE_RPC_SERVICE_FINISHED_TO_REMOVE_AINODE_RESULT =
      "ConfigNode RPC Service finished to remove AINode, result: {}";
  public static final String CONFIGNODE_RPC_SERVICE_FINISHED_TO_REMOVE_DATANODE_REQ_RESULT =
      "ConfigNode RPC Service finished to remove DataNode, req: {}, result: {}";
  public static final String CONFIGNODE_RPC_SERVICE_START_TO_REMOVE_AINODE =
      "ConfigNode RPC Service start to remove AINode";
  public static final String CONFIGNODE_RPC_SERVICE_START_TO_REMOVE_DATANODE_REQ =
      "ConfigNode RPC Service start to remove DataNode, req: {}";
  public static final String CONFIGNODE_SIMPLECONSENSUSFILE_HAS_EXISTED_FILEPATH =
      "ConfigNode SimpleConsensusFile has existed，filePath:{}";
  public static final String CONFIG_REGION_LISTENING_QUEUE_LISTEN_TO_SNAPSHOT_FAILED_THE_HISTORICAL =
      "Config Region Listening Queue Listen to snapshot failed, the historical data may not be transferred.";
  public static final String CONFIG_REGION_LISTENING_QUEUE_LISTEN_TO_SNAPSHOT_FAILED_WHEN_STARTUP =
      "Config Region Listening Queue Listen to snapshot failed when startup, snapshot will be tried again when starting schema transferring pipes";
  public static final String CONTINUOUS_QUERY_MIN_EVERY_INTERVAL_IN_MS_SHOULD_BE_GREATER =
      "continuous_query_min_every_interval_in_ms should be greater than 0, but current value is {}, ignore that and use the default value {}";
  public static final String CONTINUOUS_QUERY_SUBMIT_THREAD_SHOULD_BE_GREATER_THAN_0 =
      "continuous_query_submit_thread should be greater than 0, but current value is {}, ignore that and use the default value {}";
  public static final String COULDN_T_LOAD_CONFIGNODE_CONF_FILE_REJECT_CONFIGNODE_STARTUP =
      "Couldn't load ConfigNode conf file, reject ConfigNode startup.";
  public static final String COULDN_T_LOAD_THE_CONFIGURATION_FROM_ANY_OF_THE_KNOWN =
      "Couldn't load the configuration {} from any of the known sources.";
  public static final String CREATEREGIONGROUPS_DATABASE_HAS_BEEN_DELETED_CORRESPONDING_REGIONGROUPS =
      "[CreateRegionGroups] Database {} has been deleted, corresponding RegionGroups will not be created.";
  public static final String CREATE_CONFIGNODE_SIMPLECONSENSUSFILE =
      "Create ConfigNode SimpleConsensusFile: {}";
  public static final String CREATE_CONFIGNODE_SIMPLECONSENSUSFILE_FAILED_FILEPATH =
      "Create ConfigNode SimpleConsensusFile failed, filePath: {}";
  public static final String CURRENT_NODE_NODEID_IP_PORT_AS_CONFIG_REGION_LEADER_IS =
      "Current node [nodeId: {}, ip:port: {}] as config region leader is ready to work";
  public static final String CURRENT_NODE_NODEID_IP_PORT_BECOMES_CONFIG_REGION_LEADER =
      "Current node [nodeId: {}, ip:port: {}] becomes config region leader";
  public static final String CURRENT_NODE_NODEID_IP_PORT_IS_NO_LONGER_THE_LEADER =
      "Current node [nodeId:{}, ip:port: {}] is no longer the leader, ";
  public static final String MESSAGE_CURRENT_NODE_NODEID_ARG_IP_PORT_ARG_IS_NO_LONGER_THE_LEADER_SKIP_STARTING_LEADER_SERVICES_BECAUSE_THE_LEADER_EPOCH_IS_STALE_CCF39435 =
      "Current node [nodeId:{}, ip:port: {}] is no longer the leader, skip starting leader "
          + "services because the leader epoch is stale";
  public static final String DATABASE_INCONSISTENCY_DETECTED_WHEN_ADJUSTING_MAX_REGION_GROUP_COUNT_MESSAGE =
      "Database inconsistency detected when adjusting max region group count, message: {}, will be corrected by the following adjusting plans";
  public static final String DATABASE_NOT_EXIST = "Database not exist";
  public static final String DATA_REGION_CONSENSUS_PROTOCOL_CLASS =
      "data_region_consensus_protocol_class";
  public static final String DEACTIVATING = "Deactivating {}...";
  public static final String DEFAULT_CHARSET_IS = "{} default charset is: {}";
  public static final String DELETED_FAILED_TAKE_APPROPRIATE_ACTION =
      "{} deleted failed; take appropriate action.";
  public static final String DELETE_USELESS_PROCEDURE_WAL_DIR_FAIL =
      "Delete useless procedure wal dir fail.";
  public static final String DESERIALIZATION_ERROR_FOR_WRITE_PLAN_REQUEST_BYTEBUFFER =
      "Deserialization error for write plan, request: {}, bytebuffer: {}";
  public static final String DOES_NOT_EXIST = "%s does not exist";
  public static final String DROPPING_TAG_OR_TIME_COLUMN_IS_NOT_SUPPORTED =
      "Dropping tag or time column is not supported.";
  public static final String DROP_CQ_FAILED_BECAUSE_ITS_TOKEN_DOESN_T_MATCH =
      "Drop CQ {} failed, because its token doesn't match.";
  public static final String DROP_CQ_FAILED_BECAUSE_IT_DOESN_T_EXIST =
      "Drop CQ {} failed, because it doesn't exist.";
  public static final String DROP_CQ_SUCCESSFULLY = "Drop CQ {} successfully.";
  public static final String DUPLICATED_TEMPLATE_NAME = "Duplicated template name: ";
  public static final String ENABLESEPARATIONOFADMINPOWERS_IS_NOT_SUPPORTED =
      "EnableSeparationOfAdminPowers is not supported";
  public static final String ENVIRONMENT_VARIABLES = "{} environment variables: {}";
  public static final String ERROR_GET_MATCHED_PATHS_IN_GIVEN_LEVEL =
      "Error get matched paths in given level.";
  public static final String ERROR_GET_MATCHED_PATHS_IN_NEXT_LEVEL =
      "Error get matched paths in next level.";
  public static final String ERROR_OCCURRED_WHEN_GET_PATHS_SET_ON_TEMPLATE =
      "Error occurred when get paths set on template {}";
  public static final String ERROR_STARTING = "Error starting";
  public static final String EXECUTE_ALTERDATABASE_WITH_RESULT =
      "Execute AlterDatabase: {} with result: {}";
  public static final String EXECUTE_GETCLUSTERID_WITH_RESULT =
      "Execute getClusterId with result {}";
  public static final String EXECUTE_GETSYSTEMCONFIGURATION_WITH_RESULT =
      "Execute GetSystemConfiguration with result {}";
  public static final String EXECUTE_NON_QUERY_PLAN_FAILED = "Execute non-query plan failed";
  public static final String EXECUTE_QUERY_PLAN_FAILED = "Execute query plan failed";
  public static final String EXECUTE_REGISTERAINODEREQUEST_WITH_RESULT =
      "Execute RegisterAINodeRequest {} with result {}";
  public static final String EXECUTE_REGISTERCONFIGNODEREQUEST_WITH_RESULT =
      "Execute RegisterConfigNodeRequest {} with result {}";
  public static final String EXECUTE_REGISTERDATANODEREQUEST_WITH_RESULT =
      "Execute RegisterDataNodeRequest {} with result {}";
  public static final String EXECUTE_RESTARTAINODEREQUEST_WITH_RESULT =
      "Execute RestartAINodeRequest {} with result {}";
  public static final String EXECUTE_RESTARTDATANODEREQUEST_WITH_RESULT =
      "Execute RestartDataNodeRequest {} with result {}";
  public static final String EXECUTE_GET_METADATA_WITH_RESULT = "Execute getMetaDataCache with result {}";
  public static final String EXECUTE_SETDATABASE_WITH_RESULT =
      "Execute SetDatabase: {} with result: {}";
  public static final String FAILED_IN_THE_READ_API_EXECUTING_THE_CONSENSUS_LAYER_DUE =
      "Failed in the read API executing the consensus layer due to: ";
  public static final String FAILED_IN_THE_WRITE_API_EXECUTING_THE_CONSENSUS_LAYER_DUE =
      "Failed in the write API executing the consensus layer due to: ";
  public static final String FAILED_ON_AINODE = "{} failed on AINode {}";
  public static final String FAILED_ON_AINODE_RETRYING = "{} failed on AINode {}, retrying {}...";
  public static final String FAILED_ON_CONFIGNODE = "{} failed on ConfigNode {}";
  public static final String FAILED_ON_CONFIGNODE_BECAUSE_RETRYING =
      "{} failed on ConfigNode {}, because {}, retrying {}...";
  public static final String FAILED_ON_DATANODE = "{} failed on DataNode {}";
  public static final String FAILED_ON_DATANODE_RETRYING =
      "{} failed on DataNode {}, retrying {}...";
  public static final String FAILED_TO_ALTER_PIPE = "Failed to alter pipe";
  public static final String FAILED_TO_CHECK_SCHEMA_REGION_USING_TEMPLATE_ON_DATANODE =
      "Failed to check schema region using template on DataNode {}, {}";
  public static final String FAILED_TO_CHECK_TIMESERIES_EXISTENCE_ON_DATANODE =
      "Failed to check timeseries existence on DataNode {}, {}";
  public static final String FAILED_TO_COUNT_PATHS_USING_TEMPLATE_ON_DATANODE =
      "Failed to count paths using template on DataNode {}, {}";
  public static final String FAILED_TO_CREATE_MULTIPLE_PIPES = "Failed to create multiple pipes";
  public static final String FAILED_TO_CREATE_PIPE = "Failed to create pipe";
  public static final String FAILED_TO_CREATE_PIPEPLUGIN_SOURCE_PIPEPLUGIN_FAILED_TO_LOAD =
      "Failed to create PipePlugin [%s], source PipePlugin [%s] failed to load: %s";
  public static final String FAILED_TO_CREATE_PIPEPLUGIN_SOURCE_PIPEPLUGIN_JAR_DOES_NOT_EXIST =
      "Failed to create PipePlugin [%s], source PipePlugin [%s] jar [%s] does not exist in install dir.";
  public static final String FAILED_TO_CREATE_PIPEPLUGIN_THE_SAME_NAME_PIPEPLUGIN_HAS_BEEN =
      "Failed to create PipePlugin [%s], the same name PipePlugin has been created";
  public static final String FAILED_TO_CREATE_PIPEPLUGIN_THIS_PIPEPLUGIN_EXISTS_BUT_FAILED_TO =
      "Failed to create PipePlugin [%s], this PipePlugin exists but failed to load: %s";
  public static final String FAILED_TO_CREATE_TEMPLATE_BECAUSE_TEMPLATE_NAME_EXISTS =
      "Failed to create template, because template name {} exists";
  public static final String FAILED_TO_CREATE_TRIGGER_THE_SAME_NAME_JAR_BUT_DIFFERENT =
      "Failed to create trigger [%s], the same name Jar [%s] but different MD5 [%s] has existed";
  public static final String FAILED_TO_CREATE_TRIGGER_THE_SAME_NAME_TRIGGER_HAS_BEEN =
      "Failed to create trigger [%s], the same name trigger has been created";
  public static final String FAILED_TO_CREATE_UDF_THE_SAME_NAME_JAR_BUT_DIFFERENT =
      "Failed to create UDF [%s], the same name Jar [%s] but different MD5 [%s] has existed";
  public static final String FAILED_TO_CREATE_UDF_THE_SAME_NAME_UDF_HAS_BEEN =
      "Failed to create UDF [%s], the same name UDF has been created";
  public static final String FAILED_TO_DECREASE_LISTENER_REFERENCE =
      "Failed to decrease listener reference";
  public static final String FAILED_TO_DROP_PIPE = "Failed to drop pipe";
  public static final String FAILED_TO_DROP_PIPEPLUGIN_THE_PIPEPLUGIN_IS_A_BUILT_IN =
      "Failed to drop PipePlugin [%s], the PipePlugin is a built-in PipePlugin";
  public static final String FAILED_TO_DROP_PIPEPLUGIN_THIS_PIPEPLUGIN_HAS_NOT_BEEN_CREATED =
      "Failed to drop PipePlugin [%s], this PipePlugin has not been created";
  public static final String FAILED_TO_DROP_TRIGGER_THIS_TRIGGER_HAS_NOT_BEEN_CREATED =
      "Failed to drop trigger [%s], this trigger has not been created";
  public static final String FAILED_TO_DROP_UDF_THIS_UDF_HAS_NOT_BEEN_CREATED =
      "Failed to drop UDF [%s], this UDF has not been created";
  public static final String
      FAILED_TO_ENRICH_PIPE_WITH_ROOT_USER_FOR_COMPATIBILITY_BECAUSE_ROOT_USER_DOES_NOT_EXIST =
          "Failed to enrich pipe %s with root user for compatibility because root user %s does not exist.";
  public static final String FAILED_TO_FETCH_SCHEMAENGINE_BLACK_LIST_ON_DATANODE =
      "Failed to fetch schemaengine black list on DataNode {}, {}";
  public static final String FAILED_TO_GET_FIELD = "Failed to get field {}";
  public static final String FAILED_TO_HANDLE_LEADER_CHANGE = "Failed to handle leader change";
  public static final String FAILED_TO_HANDLE_META_CHANGES = "Failed to handle meta changes";
  public static final String FAILED_TO_INCREASE_LISTENER_REFERENCE =
      "Failed to increase listener reference";
  public static final String FAILED_TO_LOAD_PIPE_INFO_FROM_SNAPSHOT =
      "Failed to load pipe info from snapshot, ";
  public static final String FAILED_TO_LOAD_PIPE_PLUGIN_INFO_FROM_SNAPSHOT =
      "Failed to load pipe plugin info from snapshot";
  public static final String FAILED_TO_LOAD_PIPE_TASK_INFO_FROM_SNAPSHOT =
      "Failed to load pipe task info from snapshot";
  public static final String FAILED_TO_LOAD_PLUGIN_CLASS_FOR_PLUGIN_WHEN_LOADING_SNAPSHOT =
      "Failed to load plugin class for plugin [{}] when loading snapshot [{}] ";
  public static final String FAILED_TO_LOAD_SNAPSHOT_BECAUSE_GET_NULL_DATABASE_NAME =
      "Failed to load snapshot because get null database name";
  public static final String FAILED_TO_LOAD_SNAPSHOT_BECAUSE_SNAPSHOT_DIR_NOT_EXISTS =
      "Failed to load snapshot, because snapshot dir [{}] not exists.";
  public static final String FAILED_TO_LOAD_SNAPSHOT_OF_CQINFO_SNAPSHOT_FILE_DOES_NOT =
      "Failed to load snapshot of CQInfo, snapshot file [{}] does not exist.";
  public static final String FAILED_TO_LOAD_SNAPSHOT_OF_TEMPLATEPRESETTABLE_SNAPSHOT_FILE_IS_NOT =
      "Failed to load snapshot of TemplatePreSetTable,snapshot file [{}] is not a valid file.";
  public static final String FAILED_TO_LOAD_SNAPSHOT_OF_TTLINFO_SNAPSHOT_FILE_DOES_NOT =
      "Failed to load snapshot of TTLInfo, snapshot file [{}] does not exist.";
  public static final String FAILED_TO_LOAD_SNAPSHOT_SNAPSHOT_FILE_IS_NOT_EXIST =
      "Failed to load snapshot, snapshot file [{}] is not exist.";
  public static final String FAILED_TO_LOAD_SNAPSHOT_SNAPSHOT_FILE_IS_NOT_EXIST_2 =
      "Failed to load snapshot,snapshot file [{}] is not exist.";
  public static final String FAILED_TO_LOAD_SUBSCRIPTION_SNAPSHOT_SNAPSHOT_FILE_IS_NOT_EXIST =
      "Failed to load subscription snapshot, snapshot file {} is not exist.";
  public static final String FAILED_TO_ON_CONFIGNODE_RESPONSE =
      "Failed to {} on ConfigNode: {}, response: {}";
  public static final String FAILED_TO_ON_DATANODE = "Failed to {} on DataNode {}, {}";
  public static final String FAILED_TO_ON_DATANODE_EXCEPTION =
      "Failed to {} on DataNode: {}, exception: {}";
  public static final String FAILED_TO_ON_DATANODE_RESPONSE =
      "Failed to {} on DataNode: {}, response: {}";
  public static final String FAILED_TO_OPERATE_PIPE = "Failed to operate pipe";
  public static final String FAILED_TO_SET_PIPE_STATUS = "Failed to set pipe status";
  public static final String FAILED_TO_SET_PIPE_STATUS_WITH_STOPPED_BY_RUNTIME_EXCEPTION =
      "Failed to set pipe status with stopped-by-runtime-exception flag";
  public static final String FAILED_TO_TAKE_SNAPSHOT_BECAUSE_CREATE_TMP_DIR_FAIL =
      "Failed to take snapshot, because create tmp dir [{}] fail.";
  public static final String FAILED_TO_TAKE_SNAPSHOT_BECAUSE_SNAPSHOT_DIR_IS_ALREADY_EXIST =
      "Failed to take snapshot, because snapshot dir [{}] is already exist.";
  public static final String FAILED_TO_TAKE_SNAPSHOT_BECAUSE_SNAPSHOT_FILE_IS_ALREADY_EXIST =
      "Failed to take snapshot, because snapshot file [{}] is already exist.";
  public static final String FAILED_TO_TAKE_SNAPSHOT_OF_CQINFO_BECAUSE_SNAPSHOT_FILE_IS =
      "Failed to take snapshot of CQInfo, because snapshot file [{}] is already exist.";
  public static final String FAILED_TO_TAKE_SNAPSHOT_OF_TEMPLATEPRESETTABLE_BECAUSE_SNAPSHOT_FILE_IS =
      "Failed to take snapshot of TemplatePreSetTable, because snapshot file [{}] is already exist.";
  public static final String FAILED_TO_TAKE_SNAPSHOT_OF_TTLINFO_BECAUSE_SNAPSHOT_FILE_IS =
      "Failed to take snapshot of TTLInfo, because snapshot file [{}] is already exist.";
  public static final String FAILED_TO_TAKE_SUBSCRIPTION_SNAPSHOT_BECAUSE_SNAPSHOT_FILE_IS_ALREADY =
      "Failed to take subscription snapshot, because snapshot file {} is already exist.";
  public static final String FAILED_TO_UPDATE_CONFIG_FILE = "Failed to update config file";
  public static final String FILE_NOT_EXISTS = "File {} not exists";
  public static final String FOR_RECEIVES = "{} for {} receives: {}";
  public static final String GET_DATANODE_CPU_CORE_FAIL_WILL_BE_TREATED_AS_ZERO =
      "Get DataNode {} cpu core fail, will be treated as zero.";
  public static final String GET_PIPEPLUGIN_JAR_FAILED = "Get PipePlugin_Jar failed";
  public static final String GET_TRIGGERJAR_FAILED = "Get TriggerJar failed";
  public static final String GET_UDF_JAR_FAILED = "Get UDF_Jar failed";
  public static final String GET_URL_FAILED = "get url failed";
  public static final String GET_USER_OR_ROLE_PERMISSIONINFO_FAILED_BECAUSE =
      "get user or role permissionInfo failed because ";
  public static final String HANDLING_CONSUMER_GROUP_META_CHANGES =
      "Handling consumer group meta changes ...";
  public static final String HANDLING_PIPE_META_CHANGES = "Handling pipe meta changes ...";
  public static final String HANDLING_TOPIC_META_CHANGES = "Handling topic meta changes ...";
  public static final String HAS_REGISTERED_SUCCESSFULLY_WAITING_FOR_THE_LEADER_S_SCHEDULING_TO =
      "{} {} has registered successfully. Waiting for the leader's scheduling to join the cluster: {}.";
  public static final String HAS_SUCCESSFULLY_RESTARTED_AND_JOINED_THE_CLUSTER =
      "{} has successfully restarted and joined the cluster: {}.";
  public static final String HAS_SUCCESSFULLY_STARTED_AND_JOINED_THE_CLUSTER =
      "{} has successfully started and joined the cluster: {}.";
  public static final String ID_TOOK_SNAPSHOT_FAIL = "{} id {} took snapshot fail";
  public static final String INITSTANDALONECONFIGNODE_MEETS_ERROR_CAN_T_FIND_STANDALONE_LOG_FILES_FILEPATH =
      "InitStandAloneConfigNode meets error, can't find standalone log files, filePath: {}";
  public static final String INVALID_AUTHOR_TYPE_ORDINAL = "Invalid Author Type ordinal";
  public static final String IOTDB_STARTED = "IoTDB started";
  public static final String IS_DEACTIVATED = "{} is deactivated.";
  public static final String IS_IN_RESTARTING_PROCESS = "{} is in restarting process...";
  public static final String LEADER_DISTRIBUTION_POLICY = "leader_distribution_policy";
  public static final String LEADER_HAS_NOT_BEEN_ELECTED_YET_WAIT_FOR_1_SECOND =
      "Leader has not been elected yet, wait for 1 second";
  public static final String LOAD_FAILED_IT_WILL_BE_DELETED = "Load {} failed, it will be deleted.";
  public static final String LOAD_PROCEDURE_WAL_FAILED = "Load procedure wal failed.";
  public static final String LOAD_SNAPSHOT_ERROR = "Load snapshot error";
  public static final String MAKE_DIRS = "Make dirs: {}";
  public static final String MEET_ERROR_WHEN_DEACTIVATE_CONFIGNODE =
      "Meet error when deactivate ConfigNode";
  public static final String MEET_ERROR_WHEN_DOING_START_CHECKING =
      "Meet error when doing start checking";
  public static final String MEET_ERROR_WHILE_STARTING_UP = "Meet error while starting up.";
  public static final String NEW_TYPE_IS_NOT_COMPATIBLE_WITH_THE_EXISTING_ONE =
      "New type %s is not compatible with the existing one %s";
  public static final String NODE_IS_ALREADY_IN_REGION_LOCATIONS_WHEN_ADDREGIONNEWLOCATION_IN =
      "Node is already in region locations when addRegionNewLocation in {}, ";
  public static final String NODE_IS_NOT_IN_REGION_LOCATIONS_WHEN_REMOVEREGIONOLDLOCATION_IN =
      "Node is not in region locations when removeRegionOldLocation in {}, ";
  public static final String OLD_PROCEDURE_FILES_HAVE_BEEN_LOADED_SUCCESSFULLY_TAKING_SNAPSHOT =
      "Old procedure files have been loaded successfully, taking snapshot...";
  public static final String PARTITIONTABLECLEANER_THE_TIMEPARTITIONS_ARE_REMOVED_FROM_DATABASE =
      "[PartitionTableCleaner] The TimePartitions: {} are removed from Database: {}";
  public static final String PATH1_SHOULD_NOT_BE_NULL = "Path1 should not be null";
  public static final String PIPEMETASYNCER_IS_TRYING_TO_RESTART_THE_PIPES =
      "PipeMetaSyncer is trying to restart the pipes: {}";
  public static final String PIPE_IS_USING_EXTERNAL_SOURCE_SKIP_REGION =
      "Pipe {} is using external source, skip region leader change. PipeHandleLeaderChangePlan: {}";
  public static final String PLAN_TYPE_IS_NOT_SUPPORTED = "Plan type %s is not supported.";
  public static final String PLEASE_SET_THE_CN_SEED_CONFIG_NODE_PARAMETER_IN_IOTDB =
      "Please set the cn_seed_config_node parameter in iotdb-system.properties file.";
  public static final String PORTS_USED_IN_CONFIGNODE_HAVE_REPEAT =
      "ports used in configNode have repeat.";
  public static final String REACH_EOF = "Reach eof";
  public static final String RECORDING_CONSUMER_GROUP_META = "Recording consumer group meta: {}";
  public static final String RECORDING_TOPIC_META = "Recording topic meta: {}";
  public static final String RECOVERED_CONSENSUS_PIPES_AS_RUNNING_DURING_SNAPSHOT_LOAD =
      "Recovered consensus pipes {} as RUNNING during snapshot load.";
  public static final String RELEASE_TRIGGERTABLELOCK = "release TriggerTableLock";
  public static final String RELEASE_UDFTABLELOCK = "release UDFTableLock";
  public static final String REMOVED_THE_AINODE_FROM_CLUSTER = "Removed the AINode {} from cluster";
  public static final String REMOVED_THE_DATANODE_FROM_CLUSTER =
      "Removed the datanode {} from cluster";
  public static final String REMOVE_ONLINE_CONFIGNODE_FAILED = "Remove online ConfigNode failed.";
  public static final String REPORTING_CONFIGNODE_SHUTDOWN_FAILED_THE_CLUSTER_WILL_STILL_TAKE_THE =
      "Reporting ConfigNode shutdown failed. The cluster will still take the current ConfigNode as Running for a few seconds.";
  public static final String RETRY_WAIT_FAILED = "Retry wait failed.";
  public static final String ROUTE_PRIORITY_POLICY = "route_priority_policy";
  public static final String SCHEMA_OF_MEASUREMENT_IS_NOT_COMPATIBLE_WITH_EXISTING_MEASUREMENT_IN =
      "Schema of measurement %s is not compatible with existing measurement in template %s";
  public static final String SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS =
      "schema_region_consensus_protocol_class";
  public static final String SEND_RPC_TO_DATA_NODE_FOR_CHANGING_REGIONS_LEADER_ON =
      "Send RPC to data node: {} for changing regions leader on it";
  public static final String SETTTL_THE_TTL_OF_DATABASE_IS_ADJUSTED_TO =
      "[SetTTL] The ttl of Database: {} is adjusted to: {}";
  public static final String SNAPSHOT_DIRECTORY_CAN_NOT_BE_CREATED =
      "snapshot directory [{}] can not be created.";
  public static final String SNAPSHOT_DIRECTORY_IS_NOT_EMPTY =
      "Snapshot directory [{}] is not empty.";
  public static final String SNAPSHOT_DIRECTORY_IS_NOT_EXIST_CAN_NOT_LOAD_SNAPSHOT_WITH =
      "snapshot directory [{}] is not exist, can not load snapshot with this directory.";
  public static final String SNAPSHOT_DIRECTORY_IS_NOT_EXIST_START_TO_CREATE_IT =
      "snapshot directory [{}] is not exist,start to create it.";
  public static final String STARTING_IOTDB = "Starting IoTDB {}";
  public static final String START_CONFIGNODE_FAILED_BECAUSE_COULDN_T_MAKE_SYSTEM_DIRS =
      "Start ConfigNode failed, because couldn't make system dirs: %s.";
  public static final String START_READING_CONFIGNODE_CONF_FILE =
      "start reading ConfigNode conf file: {}";
  public static final String SUCCESSFULLY_APPLY_CONFIGNODE_CURRENT_CONFIGNODEGROUP =
      "Successfully apply ConfigNode: {}. Current ConfigNodeGroup: {}";
  public static final String SUCCESSFULLY_CHECK_SCHEMA_REGION_USING_TEMPLATE_ON_DATANODE =
      "Successfully check schema region using template on DataNode: {}";
  public static final String SUCCESSFULLY_CHECK_TIMESERIES_EXISTENCE_ON_DATANODE =
      "Successfully check timeseries existence on DataNode: {}";
  public static final String SUCCESSFULLY_COUNT_PATHS_USING_TEMPLATE_ON_DATANODE =
      "Successfully count paths using template on DataNode: {}";
  public static final String SUCCESSFULLY_FETCH_SCHEMAENGINE_BLACK_LIST_ON_DATANODE =
      "Successfully fetch schemaengine black list on DataNode: {}";
  public static final String SUCCESSFULLY_INITIALIZE_CONFIGMANAGER =
      "Successfully initialize ConfigManager.";
  public static final String SUCCESSFULLY_ON_CONFIGNODE = "Successfully {} on ConfigNode: {}";
  public static final String SUCCESSFULLY_ON_DATANODE = "Successfully {} on DataNode: {}";
  public static final String SUCCESSFULLY_REMOVE_CONFIGNODE_CURRENT_CONFIGNODEGROUP =
      "Successfully remove ConfigNode: {}. Current ConfigNodeGroup: {}";
  public static final String SUCCESSFULLY_SETUP_INTERNAL_SERVICES =
      "Successfully setup internal services.";
  public static final String SUCCESSFULLY_UPDATE_NODE_S_VERSION =
      "Successfully update Node {} 's version.";
  public static final String SYSTEMPROPERTIES_NORMALIZE_FROM_TO_FOR_COMPATIBILITY =
      "[SystemProperties] Normalize {} from {} to {} for compatibility.";
  public static final String SYSTEMPROPERTIES_STORE_CONFIG_NODE_ID =
      "[SystemProperties] store config_node_id: {}";
  public static final String SYSTEMPROPERTIES_STORE_IS_SEED_CONFIG_NODE =
      "[SystemProperties] store is_seed_config_node: {}";
  public static final String TAKE_SNAPSHOT_ERROR = "Take snapshot error";
  public static final String TAKING_SNAPSHOT_FAIL_PROCEDURE_UPGRADE_FAIL =
      "Taking snapshot fail, procedure upgrade fail";
  public static final String TEMPLATE_ALREADY_EXISTS_ON = "Template already exists on ";
  public static final String TEMPLATE_DOES_NOT_EXIST = "Template %s does not exist";
  public static final String TEMPLATE_FAILED_TO_TAKE_SNAPSHOT_BECAUSE_SNAPSHOT_FILE_IS_ALREADY =
      "template failed to take snapshot, because snapshot file [{}] is already exist.";
  public static final String TEMPLATE_IS_NOT_SET_ON_PATH = "Template %s is not set on path %s";
  public static final String TEMPLATE_WITH_ID_DOES_NOT_EXIST = "Template with id=%s does not exist";
  public static final String THERE_ARE_AI_NODES_IN_CLUSTER_AFTER_EXECUTED_REMOVEAINODEPLAN =
      "{}, There are {} AI nodes in cluster after executed RemoveAINodePlan";
  public static final String THERE_ARE_AI_NODES_IN_CLUSTER_BEFORE_EXECUTED_REMOVEAINODEPLAN =
      "{}, There are {} AI nodes in cluster before executed RemoveAINodePlan";
  public static final String THERE_ARE_DATA_NODE_IN_CLUSTER_AFTER_EXECUTED_REMOVEDATANODEPLAN =
      "{}, There are {} data node in cluster after executed RemoveDataNodePlan";
  public static final String THERE_ARE_DATA_NODE_IN_CLUSTER_BEFORE_EXECUTED_REMOVEDATANODEPLAN =
      "{}, There are {} data node in cluster before executed RemoveDataNodePlan";
  public static final String THESE_REQUEST_TYPES_SHOULD_BE_ADDED_TO_ACTIONMAP =
      "These request types should be added to actionMap: %s";
  public static final String THE_CHECK_SUM_OF_THE_NO_LOG_BATCH_IS_INCORRECT =
      "The check sum of the No.%d log batch is incorrect! In ";
  public static final String THE_CURRENT_CONFIGNODE_CAN_T_JOINED_THE_CLUSTER_BECAUSE_LEADER =
      "The current ConfigNode can't joined the cluster because leader's scheduling failed. The possible cause is that the ip:port configuration is incorrect.";
  public static final String THE_CURRENT_CONFIGNODE_CAN_T_SEND_REGISTER_REQUEST_TO_THE =
      "The current ConfigNode can't send register request to the ConfigNode-leader after all retries!";
  public static final String THE_CURRENT_IS_NOW_STARTING_AS_THE_SEED_CONFIGNODE =
      "The current {} is now starting as the Seed-ConfigNode.";
  public static final String THE_DATA_REPLICATION_FACTOR_SHOULD_BE_POSITIVE =
      "The data_replication_factor should be positive";
  public static final String THE_DEFAULT_DATA_REGION_GROUP_NUM_SHOULD_BE_POSITIVE =
      "The default_data_region_group_num should be positive";
  public static final String THE_DEFAULT_SCHEMA_REGION_GROUP_NUM_SHOULD_BE_POSITIVE =
      "The default_schema_region_group_num should be positive";
  public static final String THE_PARAMETER_CN_TARGET_CONFIG_NODE_LIST_HAS_BEEN_ABANDONED =
      "The parameter cn_target_config_node_list has been abandoned, only the first ConfigNode address will be used to join in the cluster. Please use cn_seed_config_node instead.";
  public static final String THE_PARAMETER_CONFIG_NODE_ID_DOESN_T_EXIST_IN =
      "The parameter config_node_id doesn't exist in ";
  public static final String THE_PROCEDURE_FRAMEWORK_HAS_BEEN_SUCCESSFULLY_UPGRADED_NOW_IT_USES =
      "The Procedure framework has been successfully upgraded. Now it uses the consensus layer's services instead of maintaining the WAL itself.";
  public static final String THE_REMOVE_CONFIGNODE_SCRIPT_HAS_BEEN_DEPRECATED_PLEASE_CONNECT_TO =
      "The remove-confignode script has been deprecated. Please connect to the CLI and use SQL: remove confignode [confignode_id].";
  public static final String THE_RESULT_OF_REGISTER_CONFIGNODE_IS_EMPTY =
      "The result of register ConfigNode is empty!";
  public static final String THE_RESULT_OF_REGISTER_SELF_CONFIGNODE_IS_RETRY =
      "The result of register self ConfigNode is {}, retry {} ...";
  public static final String THE_RESULT_OF_SUBMITTING_REMOVECONFIGNODE_JOB_IS_REMOVECONFIGNODEREQUEST =
      "The result of submitting RemoveConfigNode job is {}. RemoveConfigNodeRequest: {}";
  public static final String THE_SCHEMA_REPLICATION_FACTOR_SHOULD_BE_POSITIVE =
      "The schema_replication_factor should be positive";
  public static final String THE_SEEDCONFIGNODE_SETTING_IN_CONF_IS_EMPTY =
      "The seedConfigNode setting in conf is empty";
  public static final String THE_S_CREATION_HAS_NOT_PASSED_IN_JARNAME_WHICH_DOES =
      "The %s's creation has not passed in jarName, which does not exist in other pipePlugins. Please check";
  public static final String THE_TIMESTAMP_PRECISION_SHOULD_BE_MS_US_OR_NS =
      "The timestamp_precision should be ms, us or ns";
  public static final String THE_TIME_PARTITION_INTERVAL_SHOULD_BE_POSITIVE =
      "The time_partition_interval should be positive";
  public static final String THE_TIME_PARTITION_ORIGIN_SHOULD_BE_NON_NEGATIVE =
      "The time_partition_origin should be non-negative";
  public static final String TRY_LISTEN_TO_PLAN_FAILED = "Try listen to plan failed";
  public static final String UNDEFINED_TEMPLATE = "Undefined template {}";
  public static final String UNEXPECTED_INTERRUPTION_DURING_THE_CLOSE_METHOD_OF_LOGWRITER =
      "Unexpected interruption during the close method of logWriter";
  public static final String UNEXPECTED_INTERRUPTION_DURING_WAITING_FOR_LEADER_ELECTION =
      "Unexpected interruption during waiting for leader election.";
  public static final String UNEXPECTED_READ_PLAN = "Unexpected read plan : {}";
  public static final String UNEXPECTED_WRITE_PLAN_REQUEST_BYTEBUFFER =
      "Unexpected write plan, request: {}, bytebuffer: {}";
  public static final String UNKNOWN_FAILURE_DETECTOR = "Unknown failure_detector: %s, please set to \"fixed\" or \"phi_accrual\"";
  public static final String UNKNOWN_HOST_WHEN_CHECKING_SEED_CONFIGNODE_IP =
      "Unknown host when checking seed configNode IP {}";
  public static final String UNKNOWN_LEADER_DISTRIBUTION_POLICY =
      "Unknown leader_distribution_policy: %s, please set to \"GREEDY\" or \"CFS\" or \"HASH\"";
  public static final String UNKNOWN_PHYSICALPLAN_CONFIGPHYSICALPLANTYPE =
      "unknown PhysicalPlan configPhysicalPlanType: ";
  public static final String UNKNOWN_READ_CONSISTENCY_LEVEL_PLEASE_SET_TO =
      "Unknown read_consistency_level: %s, please set to \"strong\" or \"weak\"";
  public static final String UNKNOWN_ROUTE_PRIORITY_POLICY_PLEASE_SET_TO =
      "Unknown route_priority_policy: %s, please set to \"LEADER\" or \"GREEDY\"";
  public static final String UNRECOGNIZED_LOG_CONFIGPHYSICALPLANTYPE =
      "Unrecognized log configPhysicalPlanType: ";
  public static final String UNRECOGNIZED_REGIONMAINTAINTYPE = "Unrecognized RegionMaintainType: ";
  public static final String UNSUPPORTED_SUBPLAN_TYPE = "Unsupported subPlan type: %s";
  public static final String UNSUPPORTED_SUB_PLAN_TYPE = "Unsupported sub plan type: ";
  public static final String UPDATE_ONLINE_CONFIGNODE_FAILED = "Update online ConfigNode failed.";
  public static final String UPDATE_PROCEDURE_PID_WAL_FAILED =
      "Update Procedure (pid={}) wal failed";
  public static final String UTILITY_CLASS_SYSTEMPROPERTIESUTILS =
      "Utility class: SystemPropertiesUtils.";
  public static final String VIEW_IS_NOT_SUPPORTED = "View is not supported.";
  public static final String WRITE_CONFIGNODE_SYSTEM_PROPERTIES_FAILED =
      "Write confignode-system.properties failed";
  public static final String WRONG_MNODE_TYPE = "Wrong MNode Type";
  public static final String WRONG_NODE_TYPE = "Wrong node type";
  public static final String YOU_SHOULD_MANUALLY_DELETE_THE_PROCEDURE_WAL_DIR_BEFORE_CONFIGNODE =
      "You should manually delete the procedure wal dir before ConfigNode restart. {}";
  public static final String NOT_SUPPORT = "not support";

    public static final String THE_TTL_SHOULD_BE_POSITIVE = "The TTL should be positive.";
  public static final String CONFIGPROCEDURESTORE_START_FAILED = "ConfigProcedureStore start failed ";
  public static final String MAKE_PROCEDURE_WAL_DIR = "Make procedure wal dir: {}";
  public static final String FAIL_TO_GET_LOCATION_TRIGGER = "Fail to get Location trigger[%s]";
  public static final String GET_TRIGGERJAR_FAILED_BECAUSE = "Get TriggerJar failed, because ";
  public static final String GET_UDF_JAR_FAILED_BECAUSE = "Get UDF_Jar failed, because ";
  public static final String FAILED_TO_CREATE_PIPE_BECAUSE = "Failed to create pipe, because ";
  public static final String FAILED_TO_SET_PIPE_STATUS_BECAUSE = "Failed to set pipe status, because ";
  public static final String FAILED_TO_DROP_PIPE_BECAUSE = "Failed to drop pipe, because ";
  public static final String FAILED_TO_ALTER_PIPE_BECAUSE = "Failed to alter pipe, because ";
  public static final String FAILED_TO_CREATE_MULTIPLE_PIPES_BECAUSE = "Failed to create multiple pipes, because ";
  public static final String FAILED_TO_START_PIPE_BECAUSE_PIPE_DOES_NOT_EXIST =
      "Failed to start pipe %s, the pipe does not exist";
  public static final String FAILED_TO_START_PIPE_BECAUSE_PIPE_IS_ALREADY_DROPPED =
      "Failed to start pipe %s, the pipe is already dropped";
  public static final String FAILED_TO_STOP_PIPE_BECAUSE_PIPE_DOES_NOT_EXIST =
      "Failed to stop pipe %s, the pipe does not exist";
  public static final String FAILED_TO_STOP_PIPE_BECAUSE_PIPE_IS_ALREADY_DROPPED =
      "Failed to stop pipe %s, the pipe is already dropped";
  public static final String FAILED_TO_HANDLE_LEADER_CHANGE_BECAUSE = "Failed to handle leader change, because ";
  public static final String FAILED_TO_HANDLE_META_CHANGES_BECAUSE = "Failed to handle meta changes, because ";
  public static final String GET_PIPEPLUGIN_JAR_FAILED_BECAUSE = "Get PipePlugin_Jar failed, because ";
  public static final String RECORDING_PIPE_META = "Recording pipe meta: {}";
  public static final String UNRECOGNIZED_NODE_TYPE_WHEN_RECOVERING_THE_MTREE = "Unrecognized node type {} when recovering the mTree.";
  public static final String IOTDB_CLUSTER_COULD_PROVIDE_DATA_SERVICE_NOW_ENJOY_YOURSELF = "IoTDB-Cluster could provide data service, now enjoy yourself!";
  public static final String FAILED_TO_ALTER_DATABASE_DOESN_T_SUPPORT_ALTER_TTL_YET = "Failed to alter database. Doesn't support ALTER TTL yet.";
  public static final String NO_REGISTERED_AINODE_FOUND = "No registered AINode found";
  public static final String AINODE_LOCATION_RESOLVED = "AINode location resolved";
  public static final String GETAINODELOCATION_FAILED = "getAINodeLocation failed: ";
  public static final String REMOVE_CONSENSUSGROUP_SUCCESS = "remove ConsensusGroup success.";
  public static final String STOP_AND_CLEAR_CONFIGNODE_SUCCESS = "Stop And Clear ConfigNode Success.";
  public static final String CANNOT_CLOSE_LOG_FILE = "Cannot close log file {}";
  public static final String OPEN_WAL_FILE_SIZE_IS = "open WAL file: {} size is {}";
  public static final String FAIL_TO_TRUNCATE_LOG_FILE_TO_SIZE = "Fail to truncate log file to size {}";
  public static final String ALL_RETRY_FAILED_DUE_TO = "All retry failed due to: ";

  public static final String AUTHENTICATION_FAILED = "Authentication failed.";
  private ConfigNodeMessages() {}
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String MESSAGE_ILLEGAL_PATTERN_PATH_ARG_PATTERN_PATH_SHOULD_END_OTHERWISE_IT_50E969BB =
      "Illegal pattern path: %s, pattern path should end with **, otherwise, it should be a specific"
      + " database or device path without *";
  public static final String MESSAGE_NUMBER_TTL_RULES_HAS_REACHED_LIMIT_8575FF1E = "The number of TTL rules has reached the limit ";
  public static final String MESSAGE_CAPACITY_ARG_REQUESTED_TOTAL_ARG_PLEASE_DELETE_SOME_EXISTING_RULES_35B24B22 = "(capacity: %d, requested total: %d). Please delete some existing rules first.";
  public static final String LOG_EXITS_JVM_MEMORY_USAGE_ARG_0BCD1CCF = " exits. Jvm memory usage: {}";
  public static final String EXCEPTION_TOPOLOGY_PROBING_BASE_INTERVAL_MS_MUST_POSITIVE_BUT_GOT_18C9B7A2 = "topology_probing_base_interval_in_ms must be positive, but got: ";
  public static final String EXCEPTION_TOPOLOGY_PROBING_TIMEOUT_RATIO_MUST_0_1_BUT_GOT_FBD0E28B = "topology_probing_timeout_ratio must be in (0, 1), but got: ";
  public static final String EXCEPTION_DATA_CONFIGNODE_SYSTEM_CONFIGNODE_SYSTEM_PROPERTIES_786349AB = "data/confignode/system/confignode-system.properties. ";
  public static final String EXCEPTION_PLEASE_DELETE_DATA_DIR_DATA_CONFIGNODE_RESTART_AGAIN_8527BE66 = "Please delete data dir data/confignode and restart again.";
  public static final String EXCEPTION_CONFIG_NODE_CONSENSUS_PROTOCOL_CLASS_SET_E7A83ED6 = "the config_node_consensus_protocol_class is set to";
  public static final String EXCEPTION_AVAILABLE_ONLY_SCHEMA_REPLICATION_FACTOR_SET_1_45667207 = "available only when schema_replication_factor is set to 1";
  public static final String EXCEPTION_MESSAGE_E81C4E4F = "or";
  public static final String EXCEPTION_AVAILABLE_ONLY_DATA_REPLICATION_FACTOR_SET_1_71748D3D = "available only when data_replication_factor is set to 1";
  public static final String EXCEPTION_ARG_ARG_6E068B23 = "%s or %s";
  public static final String EXCEPTION_SCHEMAREGION_DOESN_T_SUPPORT_ORG_APACHE_IOTDB_CONSENSUS_IOT_IOTCONSENSUS_84350FD1 = "the SchemaRegion doesn't support org.apache.iotdb.consensus.iot.IoTConsensus";
  public static final String EXCEPTION_SCHEMAREGION_DOESN_T_SUPPORT_ORG_APACHE_IOTDB_CONSENSUS_IOT_IOTCONSENSUSV2_BA353C6D = "the SchemaRegion doesn't support org.apache.iotdb.consensus.iot.IoTConsensusV2";
  public static final String EXCEPTION_GREEDY_MIN_COST_FLOW_HASH_C07DA2EE = "GREEDY or MIN_COST_FLOW or HASH";
  public static final String EXCEPTION_UNRECOGNIZED_LEADER_DISTRIBUTION_POLICY_SET_F9FFB410 = "an unrecognized leader_distribution_policy is set";
  public static final String EXCEPTION_LEADER_GREEDY_55C6B994 = "LEADER or GREEDY";
  public static final String EXCEPTION_UNRECOGNIZED_ROUTE_PRIORITY_POLICY_SET_C0012AE4 = "an unrecognized route_priority_policy is set";
  public static final String EXCEPTION_THERE_NO_AVAILABLE_ARG_DATABASES_ARG_CURRENTLY_9B8297B3 = "There are no available %s for databases: %s currently, ";
  public static final String EXCEPTION_PLEASE_USE_SHOW_CLUSTER_SHOW_REGIONS_CHECK_CLUSTER_STATUS_611120DF = "please use \"show cluster\" or \"show regions\" to check the cluster status";
  public static final String EXCEPTION_SCHEMAREGIONGROUPS_3C409207 = "SchemaRegionGroups";
  public static final String EXCEPTION_DATAREGIONGROUPS_945CCE78 = "DataRegionGroups";
  public static final String EXCEPTION_DATANODE_NOT_ENOUGH_PLEASE_REGISTER_MORE_9F5EEDF5 = "DataNode is not enough, please register more. ";
  public static final String EXCEPTION_CURRENT_DATANODES_ARG_REPLICATIONFACTOR_ARG_5D686D2B = "Current DataNodes: %s, replicationFactor: %d";
  public static final String EXCEPTION_THERE_NO_AVAILABLE_AINODES_CURRENTLY_PLEASE_USE_SHOW_CLUSTER_CHECK_FD32EB52 =
      "There are no available AINodes currently, please use \"show cluster\" to check the cluster"
      + " status.";
  public static final String EXCEPTION_ADD_CONSENSUSGROUP_ARG_FAILED_D3FDDC1B = "Add ConsensusGroup to: %s failed.";
  public static final String EXCEPTION_ADD_PEER_ARG_FAILED_17DEB6CA = "Add peer: %s failed.";
  public static final String EXCEPTION_DATABASE_ARG_DOESN_T_EXIST_778BBF66 = "Database: %s doesn't exist.";
  public static final String LOG_REDIRECTION_RECOMMENDED_REMOVECONFIGNODE_BUT_NO_LEADER_ENDPOINT_PROVIDED_ABORT_RETRY_520A4C64 = "Redirection recommended for removeConfigNode but no leader endpoint provided, abort retry.";
  public static final String LOG_FAILED_WRITE_AUDIT_LOG_DATANODE_ARG_RESPONSE_ARG_691ABC90 = "Failed to write audit log to DataNode {}, response: {}";
  public static final String LOG_FAILED_WRITE_AUDIT_LOG_DATANODE_ARG_90F15E13 = "Failed to write audit log to DataNode {}";
  public static final String EXCEPTION_UNKNOWN_PHYSICALPLANTYPE_ARG_7F21B699 = "Unknown PhysicalPlanType: %d";
  public static final String LOG_CANNOT_READ_MORE_PHYSICALPLANS_ARG_SUCCESSFULLY_READ_INDEX_ARG_REASON_2EC90E78 = "Cannot read more PhysicalPlans from {}, successfully read index is {}. The reason is";
  public static final String EXCEPTION_FILE_11296840 = "file: ";
  public static final String EXCEPTION_ARG_CALCULATED_ARG_0EEEE191 = "%d Calculated: %d.";
  public static final String LOG_CANNOT_DESERIALIZE_PHYSICALPLANS_BYTEBUFFER_IGNORE_REMAINING_LOGS_06AE778F = "Cannot deserialize PhysicalPlans from ByteBuffer, ignore remaining logs";
  public static final String MESSAGE_FAILED_ALTER_DATABASE_DOESN_T_SUPPORT_ALTER_SCHEMAREPLICATIONFACTOR_YET_AD96111F = "Failed to alter database. Doesn't support ALTER SchemaReplicationFactor yet.";
  public static final String MESSAGE_FAILED_ALTER_DATABASE_DOESN_T_SUPPORT_ALTER_DATAREPLICATIONFACTOR_YET_2E7FF6E7 = "Failed to alter database. Doesn't support ALTER DataReplicationFactor yet.";
  public static final String MESSAGE_FAILED_ALTER_DATABASE_DOESN_T_SUPPORT_ALTER_TIMEPARTITIONORIGIN_YET_B315F2E3 = "Failed to alter database. Doesn't support ALTER TimePartitionOrigin yet.";
  public static final String MESSAGE_FAILED_ALTER_DATABASE_DOESN_T_SUPPORT_ALTER_TIMEPARTITIONINTERVAL_YET_F539A76F = "Failed to alter database. Doesn't support ALTER TimePartitionInterval yet.";
  public static final String MESSAGE_REMOVE_CONSENSUSGROUP_FAILED_BECAUSE_TARGET_CONFIGNODE_NOT_CURRENT_CONFIGNODE_608E64F9 = "remove ConsensusGroup failed because the target ConfigNode is not current ConfigNode.";
  public static final String MESSAGE_REMOVE_CONSENSUSGROUP_FAILED_BECAUSE_INTERNAL_FAILURE_SEE_OTHER_LOGS_MORE_51858EC2 = "remove ConsensusGroup failed because internal failure. See other logs for more details";
  public static final String EXCEPTION_LOADPIPETASKINFOEXCEPTION_2270468E = "loadPipeTaskInfoException=";
  public static final String EXCEPTION_LOADPIPEPLUGININFOEXCEPTION_40362E11 = ", loadPipePluginInfoException=";
  public static final String MESSAGE_FAILED_SET_PIPE_STATUS_STOPPED_RUNTIME_EXCEPTION_FLAG_BECAUSE_BFEA15AA = "Failed to set pipe status with stopped-by-runtime-exception flag, because ";
  public static final String EXCEPTION_UNKNOWN_TYPE_7618F8F4 = "unknown type: ";
  public static final String EXCEPTION_NO_SUCH_USER_ARG_D11B1046 = "No such user : %s";
  public static final String EXCEPTION_NO_SUCH_USER_ID_99CA691B = "No such user id: ";
  public static final String LOG_HANDLING_COMMIT_PROGRESS_META_CHANGES_FA21A080 = "Handling commit progress meta changes ...";
  public static final String EXCEPTION_FAILED_CREATE_ALTER_TOPIC_ILLEGAL_ARG_ARG_EXPECTED_1_POSITIVE_A33070FB = "Failed to create or alter topic, illegal %s=%s, expected -1 or a positive long value";
  public static final String LOG_TRYING_GET_MAX_TTL_UNDER_ONE_DATABASE_USE_LONG_MAX_9D70ACB2 = " when trying to get max ttl under one database, use Long.MAX_VALUE.";
  public static final String MESSAGE_ENABLE_IOTDB_CLUSTER_S_DATA_SERVICE_PLEASE_REGISTER_ARG_MORE_F48F3890 = "To enable IoTDB-Cluster's data service, please register %d more IoTDB-DataNode";
  public static final String MESSAGE_APPLY_NEW_CONFIGNODE_FAILED_BECAUSE_CURRENT_CONFIGNODE_CAN_T_STORE_1BB6A6BF = "Apply new ConfigNode failed because current ConfigNode can't store ConfigNode information.";
  public static final String MESSAGE_REMOVE_CONFIGNODE_FAILED_BECAUSE_CURRENT_CONFIGNODE_CAN_T_STORE_CONFIGNODE_8AB3BCB4 = "Remove ConfigNode failed because current ConfigNode can't store ConfigNode information.";
  public static final String LOG_NODE_ARG_REGION_ARG_70A7CD4F = "node: {}, region: {}";
  public static final String LOG_NO_NEED_REMOVE_IT_NODE_ARG_REGION_ARG_D14062CE = "no need to remove it, node: {}, region: {}";
  public static final String LOG_PID_ARG_FAILED_WRITE_UPDATE_API_EXECUTING_CONSENSUS_LAYER_824FB30E = "pid={} Failed in the write update API executing the consensus layer due to: ";
  public static final String LOG_PID_ARG_FAILED_WRITE_DELETE_API_EXECUTING_CONSENSUS_LAYER_0E758BF5 = "pid={} Failed in the write delete API executing the consensus layer due to: ";
  public static final String LOG_NEW_LEADER_NODEID_ARG_0A63760B = "the new leader is [nodeId:{}]";
  public static final String LOG_START_CLEANING_UP_RELATED_SERVICES_A409E261 = "start cleaning up related services";
  public static final String LOG_ALL_SERVICES_OLD_LEADER_UNAVAILABLE_NOW_8A22E60F = "all services on old leader are unavailable now.";
  public static final String LOG_FILEPATH_ARG_RETRY_ARG_16284354 = "filePath: {}, retry: {}";
  public static final String EXCEPTION_COLON_5D70AD09 = ":";
  public static final String MESSAGE_COLON_CEFF3F4D = ": ";
  public static final String EMPTY_MESSAGE = "";
  public static final String EXCEPTION_PROCEDURE_TYPE_IS_NULL_93147BD3 = "Procedure type is null";
  public static final String EXCEPTION_DOT_9D9B854A = ".";
  public static final String MESSAGE_CURRENT_CONFIGNODE_NODEID_ARG_IP_ARG_FAILED_TO_START_LEADER_SERVICE_ARG_THE_1754011A =
      "Current ConfigNode(nodeId: {}, ip: {}) failed to start leader service [{}], the";
  public static final String MESSAGE_CURRENT_CONFIGNODE_NODEID_ARG_IP_ARG_FAILED_TO_START_LEADER_SERVICE_ARG_THE_NODE_WILL_STILL_FINISH_WARMING_UP_THIS_SERVICE_STAYS_UNAVAILABLE_UNTIL_THE_NEXT_LEADERSHIP_TRANSITION_E89A98E7 =
      "Current ConfigNode(nodeId: {}, ip: {}) failed to start leader service [{}], the node will "
          + "still finish warming up; this service stays unavailable until the next leadership "
          + "transition.";
  public static final String MESSAGE_CURRENT_CONFIGNODE_NODEID_ARG_IP_ARG_FINISHED_STARTING_LEADER_SERVICES_WHILE_LOAD_0C57A408 =
      "Current ConfigNode(nodeId: {}, ip: {}) finished starting leader services while load";
  public static final String MESSAGE_CURRENT_CONFIGNODE_NODEID_ARG_IP_ARG_FINISHED_STARTING_LEADER_SERVICES_WHILE_LOAD_WARM_UP_IS_STILL_IN_PROGRESS_ARG_17C09A31 =
      "Current ConfigNode(nodeId: {}, ip: {}) finished starting leader services while load warm-up "
          + "is still in progress: {}";
  public static final String MESSAGE_UNEXPECTED_INTERRUPTION_WHILE_WAITING_FOR_CONFIGNODE_LEADER_LOAD_WARM_UP_BB5AA4F7 =
      "Unexpected interruption while waiting for ConfigNode leader load warm-up.";
  public static final String EXCEPTION_PROCEDURE_FILE_ARG_EXCEEDS_THE_LOAD_BUFFER_LIMIT_ARG_ACTUAL_SIZE_ARG_62375B4C =
      "Procedure file %s exceeds the load buffer limit %s, actual size %s";
  public static final String MESSAGE_DROPPING_LEGACY_REGION_DELETE_TASK_FOR_ARG_WHILE_REPLAYING_OFFER_PLAN_REGION_DELETION_IS_NOW_HANDLED_BY_REMOVEREGIONGROUPPROCEDURE_2A81A649 =
      "Dropping legacy region-delete task for {} while replaying offer plan; region deletion is now handled by RemoveRegionGroupProcedure.";
  public static final String MESSAGE_DROPPING_LEGACY_REGION_DELETE_TASK_FOR_ARG_WHILE_LOADING_SNAPSHOT_REGION_DELETION_IS_NOW_HANDLED_BY_REMOVEREGIONGROUPPROCEDURE_A9D409A0 =
      "Dropping legacy region-delete task for {} while loading snapshot; region deletion is now handled by RemoveRegionGroupProcedure.";
  public static final String MESSAGE_CONFIGNODE_LEADER_IS_WARMING_UP_BEFORE_SERVING_THE_REGISTERING_CONFIGNODE_WILL_WAIT_2E051639 =
      "ConfigNode leader is warming up before serving the registering ConfigNode, will wait";
  public static final String MESSAGE_CONFIGNODE_LEADER_IS_WARMING_UP_BEFORE_SERVING_THE_REGISTERING_CONFIGNODE_WILL_WAIT_AND_RETRY_STATUS_ARG_RETRY_ARG_3C924873 =
      "ConfigNode leader is warming up before serving the registering ConfigNode, will wait and "
          + "retry. Status: {}, retry: {}";
  public static final String EXCEPTION_ARG_SHOULD_BE_AN_INTEGER_BUT_WAS_ARG_56B5D91B =
      "%s should be an integer, but was %s.";
  public static final String EXCEPTION_HEARTBEAT_INTERVAL_IN_MS_SHOULD_BE_GREATER_THAN_0_BUT_WAS_2269997B =
      "heartbeat_interval_in_ms should be greater than 0, but was ";
  public static final String EXCEPTION_CONTINUOUS_QUERY_MIN_EVERY_INTERVAL_IN_MS_SHOULD_BE_GREATER_THAN_0_BUT_CURRENT_VALUE_IS_F9A1BEC4 =
      "continuous_query_min_every_interval_in_ms should be greater than 0, but current value is ";
  public static final String EXCEPTION_DEFAULT_SCHEMA_REGION_GROUP_NUM_PER_DATABASE_SHOULD_BE_POSITIVE_C8F77774 =
      "default_schema_region_group_num_per_database should be positive.";
  public static final String EXCEPTION_SCHEMA_REGION_PER_DATA_NODE_SHOULD_BE_POSITIVE_CDEB9FC1 =
      "schema_region_per_data_node should be positive.";
  public static final String EXCEPTION_DEFAULT_DATA_REGION_GROUP_NUM_PER_DATABASE_SHOULD_BE_POSITIVE_8E68B5A0 =
      "default_data_region_group_num_per_database should be positive.";
  public static final String EXCEPTION_DATA_REGION_PER_DATA_NODE_SHOULD_BE_NON_NEGATIVE_D2960368 =
      "data_region_per_data_node should be non-negative.";
  public static final String EXCEPTION_PROCEDURE_COMPLETED_CLEAN_INTERVAL_SHOULD_BE_GREATER_THAN_0_BUT_WAS_8781558E =
      "procedure_completed_clean_interval should be greater than 0, but was ";
  public static final String EXCEPTION_PROCEDURE_COMPLETED_EVICT_TTL_SHOULD_BE_GREATER_THAN_0_BUT_WAS_5A4D0CF6 =
      "procedure_completed_evict_ttl should be greater than 0, but was ";

}
