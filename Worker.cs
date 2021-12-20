using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using System;
using System.Reflection;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using Newtonsoft.Json;
using Npgsql;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Net;
using System.Text;
using SendGrid;
using SendGrid.Helpers.Mail;
using SendGrid.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.AspNetCore.Mvc;

namespace TequaCreek.BloxGuardianMessageProcessingService
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> logger;
        private IConfiguration configuration;
        private ISendGridClient sendGridClient;

        private Timer timer = null!;

        private List<RabbitMQ.Client.Events.AsyncEventingBasicConsumer> consumers;
        private List<RabbitMQ.Client.IModel> channels;
        private List<RabbitMQ.Client.IConnection> connections;

        private RabbitMQ.Client.ConnectionFactory rmqFromBloxChannelConnectionFactory;
        private RabbitMQ.Client.IConnection rmqFromBloxChannelConnection;
        private RabbitMQ.Client.IModel rmqFromBloxChannelChannel;
        private RabbitMQ.Client.Events.AsyncEventingBasicConsumer rmqFromBloxChannelConsumer;

        public string processingServerId { get; set; }
        public Worker(IConfiguration configuration, ILogger<Worker> logger, [FromServices] ISendGridClient sendGridClient)
        {
            this.logger = logger;
            this.configuration = configuration;
            this.sendGridClient = sendGridClient;

            this.processingServerId = configuration["AppSettings:ProcessingServerId"];

            consumers = new List<AsyncEventingBasicConsumer>();
            channels = new List<IModel>();
            connections = new List<IConnection>();

        }

        public override async Task StartAsync(CancellationToken cancellationToken)
        {

            string exchangeName;
            string queueName;

            try
            {

                logger.LogInformation("Starting BloxGuardian Message Processing Service (Assigned Processing Server ID = {0})", this.processingServerId);
                logger.LogInformation("Assembly version {0}", Assembly.GetEntryAssembly().GetName().Version);
                logger.LogInformation("File version {0}", Assembly.GetEntryAssembly().GetCustomAttribute<AssemblyFileVersionAttribute>().Version);
                logger.LogInformation("Product version {0}", Assembly.GetEntryAssembly().GetCustomAttribute<AssemblyInformationalVersionAttribute>().InformationalVersion);

                logger.LogInformation("Setting up Production messaging queue handling");
                logger.LogInformation("Opening production connection to From-BloxChannel message queue host");
                logger.LogInformation("  -- Host Name = {0}, Port = {1}, Virtual Host = {0}",
                                        configuration["AppSettings:BloxChannelToBloxGuardianMQHostName"],
                                        configuration["AppSettings:BloxChannelToBloxGuardianMQPort"],
                                        configuration["AppSettings:BloxChannelToBloxGuardianMQVirtualHostName"]);
                rmqFromBloxChannelConnectionFactory = new RabbitMQ.Client.ConnectionFactory()
                {
                    HostName = configuration["AppSettings:BloxChannelToBloxGuardianMQHostName"],
                    Port = int.Parse(configuration["AppSettings:BloxChannelToBloxGuardianMQPort"]),
                    VirtualHost = configuration["AppSettings:BloxChannelToBloxGuardianMQVirtualHostName"],
                    UserName = configuration["AppSettings:BloxChannelToBloxGuardianMQUserName"],
                    Password = configuration["AppSettings:BloxChannelToBloxGuardianMQUserPassword"],
                    DispatchConsumersAsync = true
                };

                rmqFromBloxChannelConnection = rmqFromBloxChannelConnectionFactory.CreateConnection();
                rmqFromBloxChannelChannel = rmqFromBloxChannelConnection.CreateModel();

                exchangeName = configuration["AppSettings:BloxChannelToBloxGuardianMQExchangeName"];

                // PROGRAMMER'S NOTE:  Best practice is to just declare definitions.  If Exchange and Queue already exist, the functions
                //                     simply return without action.
                rmqFromBloxChannelChannel.ExchangeDeclare(exchangeName, RabbitMQ.Client.ExchangeType.Direct, true, false, null);

                /////////////////////////////////////////
                /// From BloxChannel messages channel ///
                /////////////////////////////////////////

                queueName = string.Format(TequaCreek.BloxChannelDataModelLibrary.SharedConstantValues.MESSAGE_QUEUE_NAME_PATTERN_INTERSYSTEM_TO_BLOXGUARDIAN,
                                            this.processingServerId);
                logger.LogInformation("Setting up From-BloxGuardian queue {0} definition (if not already exists)", queueName);

                Dictionary<string, object> args = new Dictionary<string, object>();

                // PROGRAMMER'S NOTE:  Add any queue definition arguments here (NOTE:  this may cause a conflict if queue already defined
                //                                                                     with different args)

                rmqFromBloxChannelChannel.QueueDeclare(queueName, true, false, false, args);

                logger.LogInformation("Attaching message handler Worker::MessageReceivedAsync() to To-BloxGuardian queue {0}", queueName);

                rmqFromBloxChannelConsumer = new RabbitMQ.Client.Events.AsyncEventingBasicConsumer(rmqFromBloxChannelChannel);
                rmqFromBloxChannelConsumer.Received += MessageReceivedAsync;

                // PROGRAMMER'S NOTE:  May want to add other event handlers here for shutdown, etc.

                rmqFromBloxChannelChannel.BasicConsume(queueName, false, "", false, false, null, rmqFromBloxChannelConsumer);


                logger.LogInformation("Setting up timed activities handler to fire every {0} seconds", configuration["AppSettings:TimedWorkActivationSecondsCount"]);
                timer = new Timer(async o => { await PerformTimedWorkActivities(null); }, null, TimeSpan.Zero,
                                  TimeSpan.FromSeconds(int.Parse(configuration["AppSettings:TimedWorkActivationSecondsCount"])));

            }
            catch (Exception ex1)
            {
                logger.LogError("Error occurred in BloxGuardianMessageProcessingService.Worker::StartAsync() - {0} \n {1}", ex1.Message, ex1.StackTrace);
            }       // handler 1

            await Task.Delay(0);

        }       // StartAsync();

        public override async Task StopAsync(CancellationToken cancellationToken)
        {

            logger.LogInformation("Stopped BloxChannel Message Processing Service (Assigned Processing Server ID = {0})", this.processingServerId);

            timer.Change(Timeout.Infinite, 0);

            await Task.Delay(0);

        }       // StopAsync()


        protected override async Task ExecuteAsync(CancellationToken cancellationToken)
        {

            /*

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {

                    await streamWriter.WriteLineAsync(string.Format("Executed function at {0}", System.DateTime.Now.ToString()));
                    await streamWriter.FlushAsync();

                }
                catch (Exception ex)
                {
                    _logger.LogError("Exception occurred {0}.", ex.Message);
                }
                finally
                {
                    await Task.Delay(1000 * 5, cancellationToken);
                }
            }

            */

            // TEMP CODE
            await Task.Delay(0);

        }       // ExecuteAsync()

        private async Task PerformTimedWorkActivities(object state)
        {

            // NOP

        }       // PerformTimedWorkActivities()

        public async Task MessageReceivedAsync(object sender, BasicDeliverEventArgs @event)
        {

            System.Text.StringBuilder sqlStatement;
            DateTime processingRunDateTime;
            string baseFileDirectory;
            string baseMessageFileName;
            TequaCreek.BloxChannelDotNetAPI.Models.BloxGuardian.FromBloxGuardianMessagePacket fromBloxGuardianMessagePacket;

            int messageToBloxGuardianInternalId;
            string messageToBloxGuardianExternalId = "";
            int messageOriginationTypeCode = 0;
            string payload = null;
            int allowedCommunicationPathInternalId = 0;
            string inGameUserId = null;
            int inGameEndpointInternalId = 0;
            int externalEndpointInternalId = 0;
            bool continueProcessing = true;
            int inGameUserBGAccountPairingInternalId = 0;
            int bloxGuardianAccountInternalId = 0;

            NpgsqlConnection sqlConnection1;
            NpgsqlConnection sqlConnection2;

            NpgsqlCommand sqlCommandGetMessageToBloxGuardian;
            NpgsqlDataReader sqlDataReaderGetMessageToBloxGuardian;
            NpgsqlCommand sqlCommandGetBloxGuardianAccount;
            NpgsqlDataReader sqlDataReaderGetBloxGuardianAccount;
            NpgsqlCommand sqlCommandGetInGameToAccountPairing;
            NpgsqlDataReader sqlDataReaderGetInGameToAccountPairing;
            NpgsqlCommand sqlCommandInsertInGameToAccountPairing;
            NpgsqlCommand sqlCommandUpdateInGameToAccountPairing;
            NpgsqlCommand sqlCommandDeleteInGameToAccountPairing;
            NpgsqlCommand sqlCommandGetPendingMessagesForPairedAccount;
            NpgsqlDataReader sqlDataReaderGetPendingMessagesForPairedAccount;
            NpgsqlCommand sqlCommandRemovePendingMessagesForPairedAccount;
            NpgsqlCommand sqlCommandInsertPendingMessageForPairedAccount;
            NpgsqlCommand sqlCommandUpdateMessageToBloxGuardian;

            // TEMP CODE
            logger.LogDebug("Entering Worker:MessageReceivedAsync()");
            // END TEMP CODE

            try
            {
                processingRunDateTime = System.DateTime.Now;

                using (sqlConnection1 = new NpgsqlConnection(configuration["ConnectionStrings:BloxChannel"]))
                {
                    await sqlConnection1.OpenAsync();

                    sqlStatement = new System.Text.StringBuilder();
                    sqlStatement.Append("SELECT MSG.message_to_bloxguardian_internal_id, MSG.message_to_bloxguardian_external_id, ");
                    sqlStatement.Append("       MSG.message_origination_type_code, MSG.payload, MSG.allowed_communication_path_internal_id, ");
                    sqlStatement.Append("       MSG.ingame_user_id, ACP.ingame_endpoint_internal_id, ACP.external_endpoint_internal_ID ");
                    sqlStatement.Append("  FROM message_to_bloxguardian MSG INNER JOIN allowed_communication_path ACP ");
                    sqlStatement.Append("         ON MSG.allowed_communication_path_internal_id = ACP.allowed_communication_path_internal_id ");
                    sqlStatement.Append("  WHERE MSG.message_to_bloxguardian_internal_id = @MessageToBloxGuardianInternalID ");

                    sqlCommandGetMessageToBloxGuardian = sqlConnection1.CreateCommand();
                    sqlCommandGetMessageToBloxGuardian.CommandText = sqlStatement.ToString();
                    sqlCommandGetMessageToBloxGuardian.CommandTimeout = 600;
                    sqlCommandGetMessageToBloxGuardian.Parameters.Add(new NpgsqlParameter("@MessageToBloxGuardianInternalID", NpgsqlTypes.NpgsqlDbType.Integer));

                    sqlCommandGetMessageToBloxGuardian.Parameters["@MessageToBloxGuardianInternalID"].Value = 0;
                    await sqlCommandGetMessageToBloxGuardian.PrepareAsync();

                    sqlStatement = new System.Text.StringBuilder();
                    sqlStatement.Append("UPDATE message_to_bloxguardian ");
                    sqlStatement.Append("  SET processing_status = @ProcessingStatus ");
                    sqlStatement.Append("  WHERE message_to_bloxguardian_internal_id = @MessageToBloxGuardianInternalID ");

                    sqlCommandUpdateMessageToBloxGuardian = sqlConnection1.CreateCommand();
                    sqlCommandUpdateMessageToBloxGuardian.CommandText = sqlStatement.ToString();
                    sqlCommandUpdateMessageToBloxGuardian.CommandTimeout = 600;
                    sqlCommandUpdateMessageToBloxGuardian.Parameters.Add(new NpgsqlParameter("@ProcessingStatus", NpgsqlTypes.NpgsqlDbType.Integer));
                    sqlCommandUpdateMessageToBloxGuardian.Parameters.Add(new NpgsqlParameter("@MessageToBloxGuardianInternalID", NpgsqlTypes.NpgsqlDbType.Integer));

                    sqlCommandUpdateMessageToBloxGuardian.Parameters["@ProcessingStatus"].Value = 0;
                    sqlCommandUpdateMessageToBloxGuardian.Parameters["@MessageToBloxGuardianInternalID"].Value = 0;
                    await sqlCommandUpdateMessageToBloxGuardian.PrepareAsync();

                    // 1.)  Get message body and determine message source to determine appropriate processing.
                    //
                    // Message body will be in format {Message Source Constant},{BloxGuardian message internal identifier}
                    //
                    //                                example:  2,3377
                    //
                    //
                    // {Message Source Constant} is one of the following values:  MESSAGE_TYPE_ID_INGAME_TO_BLOXGUARDIAN
                    //                                                            MESSAGE_TYPE_ID_EXTERNAL_TO_BLOXGUARDIAN
                    //                                                            MESSAGE_TYPE_ID_INTERSYSTEM_TO_BLOXGUARDIAN
                    //
                    // defined in TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues

                    string messageBody = System.Text.Encoding.UTF8.GetString(@event.Body.ToArray());
                    string messageType = messageBody.Substring(0, messageBody.IndexOf(","));


                    switch (messageType)
                    {
                        case TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.MESSAGE_TYPE_ID_INGAME_TO_BLOXGUARDIAN:

                            messageToBloxGuardianInternalId = int.Parse(messageBody.Substring(messageBody.IndexOf(",") + 1));

                            // TEMP CODE
                            logger.LogDebug("Processing message with Internal ID = {0}", messageToBloxGuardianInternalId);
                            // END TEMP CODE


                            // Determine message origination source (i.e. function that called BloxGuardian API from In-Game endpoint)
                            sqlCommandGetMessageToBloxGuardian.Parameters["@MessageToBloxGuardianInternalID"].Value = messageToBloxGuardianInternalId;
                            sqlDataReaderGetMessageToBloxGuardian = await sqlCommandGetMessageToBloxGuardian.ExecuteReaderAsync();
                            if (await sqlDataReaderGetMessageToBloxGuardian.ReadAsync())
                            {

                                messageToBloxGuardianExternalId = sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_EXTERNAL_ID);
                                messageOriginationTypeCode = sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_MESSAGE_ORIGINATION_TYPE_CODE);

                                if (!await sqlDataReaderGetMessageToBloxGuardian.IsDBNullAsync(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_PAYLOAD))
                                {
                                    payload = sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_PAYLOAD);
                                }

                                allowedCommunicationPathInternalId = sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_ALLOWED_COMMUNICATION_PATH_INTERNAL_ID);

                                if (!await sqlDataReaderGetMessageToBloxGuardian.IsDBNullAsync(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INGAME_USER_ID))
                                {
                                    inGameUserId = sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INGAME_USER_ID);
                                }

                                inGameEndpointInternalId = sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INGAME_ENDPOINT_INTERNAL_ID);
                                externalEndpointInternalId = sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_EXTERNAL_ENDPOINT_INTERNAL_ID);
                            }
                            else
                            {

                                /// Database/System integrity issue
                                logger.LogError("Unable to find record with Internal ID = {0} for Message to BloxGuardian", messageToBloxGuardianInternalId);

                                continueProcessing = false;
                            }
                            await sqlDataReaderGetMessageToBloxGuardian.CloseAsync();

                            if (continueProcessing)
                            {

                                // TEMP CODE
                                logger.LogDebug("Message is In-Game to BloxGuardian");
                                // END TEMP CODE

                                switch ((TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType)messageOriginationTypeCode)
                                {
                                    case TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType.RequestToPairToAccount:

                                        // TEMP CODE
                                        logger.LogDebug("Request Pair to Account");
                                        // END TEMP CODE

                                        /////////////////////////////////////////////////////
                                        //////  In-Game to BG - Request Pair to Account /////
                                        /////////////////////////////////////////////////////

                                        using (sqlConnection2 = new NpgsqlConnection(configuration["ConnectionStrings:BloxChannel"]))
                                        {
                                            await sqlConnection2.OpenAsync();

                                            sqlStatement = new System.Text.StringBuilder();
                                            sqlStatement.Append("SELECT IGAP.ingame_user_bg_account_pairing_internal_id, BGA.bloxguardian_account_external_id, ");
                                            sqlStatement.Append("       BGA.account_holder_last_name, BGA.account_holder_first_name, IGAP.pairing_status ");
                                            sqlStatement.Append("  FROM ingame_user_bg_account_pairing IGAP ");
                                            sqlStatement.Append("    LEFT OUTER JOIN bloxguardian_account BGA ");
                                            sqlStatement.Append("                    ON IGAP.bloxguardian_account_internal_id = BGA.bloxguardian_account_internal_id ");
                                            sqlStatement.Append("  WHERE IGAP.allowed_communication_path_internal_id = @AllowedCommunicationPathInternalID AND ");
                                            sqlStatement.Append("        IGAP.ingame_user_id = @InGameUserId ");

                                            sqlCommandGetInGameToAccountPairing = sqlConnection1.CreateCommand();
                                            sqlCommandGetInGameToAccountPairing.CommandText = sqlStatement.ToString();
                                            sqlCommandGetInGameToAccountPairing.CommandTimeout = 600;
                                            sqlCommandGetInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@AllowedCommunicationPathInternalID", NpgsqlTypes.NpgsqlDbType.Integer));
                                            sqlCommandGetInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@InGameUserId", NpgsqlTypes.NpgsqlDbType.Varchar, 20));

                                            sqlCommandGetInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value = 0;
                                            sqlCommandGetInGameToAccountPairing.Parameters["@InGameUserId"].Value = "";
                                            await sqlCommandGetInGameToAccountPairing.PrepareAsync();

                                            sqlStatement = new System.Text.StringBuilder();
                                            sqlStatement.Append("DELETE FROM ingame_user_bg_account_pairing ");
                                            sqlStatement.Append("  WHERE allowed_communication_path_internal_id = @AllowedCommunicationPathInternalID AND ");
                                            sqlStatement.Append("        ingame_user_id = @InGameUserId ");

                                            sqlCommandDeleteInGameToAccountPairing = sqlConnection2.CreateCommand();
                                            sqlCommandDeleteInGameToAccountPairing.CommandText = sqlStatement.ToString();
                                            sqlCommandDeleteInGameToAccountPairing.CommandTimeout = 600;
                                            sqlCommandDeleteInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@AllowedCommunicationPathInternalID", NpgsqlTypes.NpgsqlDbType.Integer));
                                            sqlCommandDeleteInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@InGameUserId", NpgsqlTypes.NpgsqlDbType.Varchar, 20));

                                            sqlCommandDeleteInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value = 0;
                                            sqlCommandDeleteInGameToAccountPairing.Parameters["@InGameUserId"].Value = "";
                                            await sqlCommandDeleteInGameToAccountPairing.PrepareAsync();

                                            sqlStatement = new System.Text.StringBuilder();
                                            sqlStatement.Append("INSERT INTO ingame_user_bg_account_pairing ");
                                            sqlStatement.Append("  (allowed_communication_path_internal_id, ingame_user_id, bloxguardian_account_internal_id, ");
                                            sqlStatement.Append("   pairing_status, record_added_date_time, record_last_updated_date_time) ");
                                            sqlStatement.Append("  VALUES (@AllowedCommunicationPathInternalID, @InGameUserId, @BloxGuardianAccountInternalID, ");
                                            sqlStatement.Append("          @PairingStatus, @RecordAddedDateTime, @RecordLastUpdatedDateTime) ");

                                            sqlCommandInsertInGameToAccountPairing = sqlConnection2.CreateCommand();
                                            sqlCommandInsertInGameToAccountPairing.CommandText = sqlStatement.ToString();
                                            sqlCommandInsertInGameToAccountPairing.CommandTimeout = 600;
                                            sqlCommandInsertInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@AllowedCommunicationPathInternalID", NpgsqlTypes.NpgsqlDbType.Integer));
                                            sqlCommandInsertInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@InGameUserId", NpgsqlTypes.NpgsqlDbType.Varchar, 20));
                                            sqlCommandInsertInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@BloxGuardianAccountInternalID", NpgsqlTypes.NpgsqlDbType.Integer));
                                            sqlCommandInsertInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@PairingStatus", NpgsqlTypes.NpgsqlDbType.Integer));
                                            sqlCommandInsertInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@RecordAddedDateTime", NpgsqlTypes.NpgsqlDbType.Date));
                                            sqlCommandInsertInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@RecordLastUpdatedDateTime", NpgsqlTypes.NpgsqlDbType.Date));

                                            sqlCommandInsertInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value = 0;
                                            sqlCommandInsertInGameToAccountPairing.Parameters["@InGameUserId"].Value = "";
                                            sqlCommandInsertInGameToAccountPairing.Parameters["@BloxGuardianAccountInternalID"].Value = 0;
                                            sqlCommandInsertInGameToAccountPairing.Parameters["@PairingStatus"].Value = 0;
                                            sqlCommandInsertInGameToAccountPairing.Parameters["@RecordAddedDateTime"].Value = DateTime.MinValue;
                                            sqlCommandInsertInGameToAccountPairing.Parameters["@RecordLastUpdatedDateTime"].Value = DateTime.MinValue;
                                            await sqlCommandInsertInGameToAccountPairing.PrepareAsync();

                                            sqlStatement = new System.Text.StringBuilder();
                                            sqlStatement.Append("UPDATE ingame_user_bg_account_pairing ");
                                            sqlStatement.Append("  SET pairing_status = @PairingStatus, record_last_updated_date_time = @RecordLastUpdatedDateTime ");
                                            sqlStatement.Append("  WHERE allowed_communication_path_internal_id = @AllowedCommunicationPathInternalID AND ");
                                            sqlStatement.Append("        ingame_user_id = @InGameUserId ");

                                            sqlCommandUpdateInGameToAccountPairing = sqlConnection2.CreateCommand();
                                            sqlCommandUpdateInGameToAccountPairing.CommandText = sqlStatement.ToString();
                                            sqlCommandUpdateInGameToAccountPairing.CommandTimeout = 600;
                                            sqlCommandUpdateInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@PairingStatus", NpgsqlTypes.NpgsqlDbType.Integer));
                                            sqlCommandUpdateInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@RecordLastUpdatedDateTime", NpgsqlTypes.NpgsqlDbType.Timestamp, 32));
                                            sqlCommandUpdateInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@AllowedCommunicationPathInternalID", NpgsqlTypes.NpgsqlDbType.Integer));
                                            sqlCommandUpdateInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@InGameUserId", NpgsqlTypes.NpgsqlDbType.Varchar, 20));

                                            sqlCommandUpdateInGameToAccountPairing.Parameters["@PairingStatus"].Value = 0;
                                            sqlCommandUpdateInGameToAccountPairing.Parameters["@RecordLastUpdatedDateTime"].Value = DateTime.MinValue;
                                            sqlCommandUpdateInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value = 0;
                                            sqlCommandUpdateInGameToAccountPairing.Parameters["@InGameUserId"].Value = "";
                                            await sqlCommandUpdateInGameToAccountPairing.PrepareAsync();

                                            TequaCreek.BloxChannelDotNetAPI.Models.BloxGuardian.RequestPairingPayload requestPairingPayload = new BloxChannelDotNetAPI.Models.BloxGuardian.RequestPairingPayload();

                                            sqlCommandGetInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value = allowedCommunicationPathInternalId;
                                            sqlCommandGetInGameToAccountPairing.Parameters["@InGameUserId"].Value = inGameUserId;
                                            sqlDataReaderGetInGameToAccountPairing = await sqlCommandGetInGameToAccountPairing.ExecuteReaderAsync();
                                            if (await sqlDataReaderGetInGameToAccountPairing.ReadAsync())
                                            {

                                                switch ((TequaCreek.BloxGuardianDataModelLibrary.PairingStatus)
                                                         sqlDataReaderGetInGameToAccountPairing.GetInt32(ApplicationValues.INGAME_USER_ID_BG_ACCOUNT_PAIRING_QUERY_RESULT_COLUMN_OFFSET_PAIRING_STATUS))
                                                {

                                                    // 1.)  Check for existing completed pairing.  If found, return error that
                                                    //      pairing already in place.  If found, return formatted message
                                                    //      with BloxGuardian Account External ID and Account Holder Name
                                                    case TequaCreek.BloxGuardianDataModelLibrary.PairingStatus.CompletedPairing:

                                                        requestPairingPayload.bloxGuardianAccountId = sqlDataReaderGetInGameToAccountPairing.GetString(ApplicationValues.INGAME_USER_ID_BG_ACCOUNT_PAIRING_QUERY_RESULT_COLUMN_OFFSET_BLOXGUARDIAN_ACCOUNT_EXTERNAL_ID);
                                                        requestPairingPayload.accountHolderLastName = sqlDataReaderGetInGameToAccountPairing.GetString(ApplicationValues.INGAME_USER_ID_BG_ACCOUNT_PAIRING_QUERY_RESULT_COLUMN_OFFSET_ACCOUNT_HOLDER_LAST_NAME);
                                                        requestPairingPayload.accountHolderFirstName = sqlDataReaderGetInGameToAccountPairing.GetString(ApplicationValues.INGAME_USER_ID_BG_ACCOUNT_PAIRING_QUERY_RESULT_COLUMN_OFFSET_ACCOUNT_HOLDER_FIRST_NAME);
                                                        requestPairingPayload.pairingStatus = (int)TequaCreek.BloxGuardianDataModelLibrary.PairingStatus.PairingAlreadyExists;
                                                        break;

                                                    // 2.)  If prior half-open pairing (from In-Game) exists, remove it and set
                                                    //      up new half-pairing
                                                    case TequaCreek.BloxGuardianDataModelLibrary.PairingStatus.HalfOpenFromInGame:

                                                        // Delete existing pairing
                                                        sqlCommandDeleteInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value = allowedCommunicationPathInternalId;
                                                        sqlCommandDeleteInGameToAccountPairing.Parameters["@InGameUserId"].Value = inGameUserId;
                                                        await sqlCommandDeleteInGameToAccountPairing.ExecuteNonQueryAsync();

                                                        // Add new half-open pairing
                                                        sqlCommandInsertInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value = allowedCommunicationPathInternalId;
                                                        sqlCommandInsertInGameToAccountPairing.Parameters["@InGameUserId"].Value = inGameUserId;
                                                        sqlCommandInsertInGameToAccountPairing.Parameters["@BloxGuardianAccountInternalID"].Value = System.DBNull.Value;
                                                        sqlCommandInsertInGameToAccountPairing.Parameters["@PairingStatus"].Value = (int)TequaCreek.BloxGuardianDataModelLibrary.PairingStatus.HalfOpenFromInGame;
                                                        sqlCommandInsertInGameToAccountPairing.Parameters["@RecordAddedDateTime"].Value = processingRunDateTime;
                                                        sqlCommandInsertInGameToAccountPairing.Parameters["@RecordLastUpdatedDateTime"].Value = processingRunDateTime;
                                                        await sqlCommandInsertInGameToAccountPairing.ExecuteNonQueryAsync();

                                                        requestPairingPayload.pairingStatus = (int)TequaCreek.BloxGuardianDataModelLibrary.PairingStatus.HalfOpenFromInGame;
                                                        break;

                                                    // 3.)  Check for half-open pairing (from Mobile Device) that references Allowed Comm Path
                                                    //      plus In-Game User ID and complete pairing.  PROGRAMMER'S NOTE:  In theory this 
                                                    //      could happen if the in-game pairing request message arrived after an immediate
                                                    //      action on the mobile device to approve the pairing.  If found, return formatted
                                                    //      message with BloxGuardian Account External ID and Account Holder Name
                                                    case TequaCreek.BloxGuardianDataModelLibrary.PairingStatus.HalfOpenFromMobileDevice:

                                                        // Complete half-open pairing from device
                                                        sqlCommandUpdateInGameToAccountPairing.Parameters["@PairingStatus"].Value =
                                                            (int)TequaCreek.BloxGuardianDataModelLibrary.PairingStatus.CompletedPairing;
                                                        sqlCommandUpdateInGameToAccountPairing.Parameters["@RecordLastUpdatedDateTime"].Value = processingRunDateTime;
                                                        sqlCommandUpdateInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value = allowedCommunicationPathInternalId;
                                                        sqlCommandUpdateInGameToAccountPairing.Parameters["@InGameUserId"].Value = inGameUserId;
                                                        await sqlCommandUpdateInGameToAccountPairing.ExecuteNonQueryAsync();

                                                        requestPairingPayload.bloxGuardianAccountId = sqlDataReaderGetInGameToAccountPairing.GetString(ApplicationValues.INGAME_USER_ID_BG_ACCOUNT_PAIRING_QUERY_RESULT_COLUMN_OFFSET_BLOXGUARDIAN_ACCOUNT_EXTERNAL_ID);
                                                        requestPairingPayload.accountHolderLastName = sqlDataReaderGetInGameToAccountPairing.GetString(ApplicationValues.INGAME_USER_ID_BG_ACCOUNT_PAIRING_QUERY_RESULT_COLUMN_OFFSET_ACCOUNT_HOLDER_LAST_NAME);
                                                        requestPairingPayload.accountHolderFirstName = sqlDataReaderGetInGameToAccountPairing.GetString(ApplicationValues.INGAME_USER_ID_BG_ACCOUNT_PAIRING_QUERY_RESULT_COLUMN_OFFSET_ACCOUNT_HOLDER_FIRST_NAME);
                                                        requestPairingPayload.pairingStatus = (int)TequaCreek.BloxGuardianDataModelLibrary.PairingStatus.CompletedPairing;


                                                        // PROGRAMMER'S NOTE:  Send message to endpoints of completed pairing
                                                        logger.LogDebug("Will send message to endpoints of completed pairing from code to be inserted here");


                                                        break;

                                                }

                                            }
                                            else
                                            {

                                                // 4.)  If passed all prior validations and not a completed pairing (Step 3),
                                                //      write a half-open pairing record to database table

                                                // Add new half-open pairing
                                                sqlCommandInsertInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value = allowedCommunicationPathInternalId;
                                                sqlCommandInsertInGameToAccountPairing.Parameters["@InGameUserId"].Value = inGameUserId;
                                                sqlCommandInsertInGameToAccountPairing.Parameters["@BloxGuardianAccountInternalID"].Value = System.DBNull.Value;
                                                sqlCommandInsertInGameToAccountPairing.Parameters["@PairingStatus"].Value = (int)TequaCreek.BloxGuardianDataModelLibrary.PairingStatus.HalfOpenFromInGame;
                                                sqlCommandInsertInGameToAccountPairing.Parameters["@RecordAddedDateTime"].Value = processingRunDateTime;
                                                sqlCommandInsertInGameToAccountPairing.Parameters["@RecordLastUpdatedDateTime"].Value = processingRunDateTime;
                                                await sqlCommandInsertInGameToAccountPairing.ExecuteNonQueryAsync();

                                                requestPairingPayload.pairingStatus = (int)TequaCreek.BloxGuardianDataModelLibrary.PairingStatus.HalfOpenFromInGame;

                                            }       // (await sqlDataReaderGetInGameToAccountPairing.ReadAsync())
                                            await sqlDataReaderGetInGameToAccountPairing.CloseAsync();

                                            // 5.)  Write return message to file on file system
                                            baseMessageFileName = PadZeroLeft(messageToBloxGuardianInternalId, 10);
                                            baseFileDirectory = this.configuration["AppSettings:ToInGameFileStorePrefix"] + Path.DirectorySeparatorChar +
                                                                PadZeroLeft(inGameEndpointInternalId, 6);
                                            Directory.CreateDirectory(baseFileDirectory);       // PROGRAMMER'S NOTE:  This function will return if directory already created

                                            fromBloxGuardianMessagePacket = new BloxChannelDotNetAPI.Models.BloxGuardian.FromBloxGuardianMessagePacket();

                                            fromBloxGuardianMessagePacket.respondingToMessageToBloxGuardianExternalId = messageToBloxGuardianExternalId;
                                            fromBloxGuardianMessagePacket.responsePayloadType = (int)TequaCreek.BloxChannelDotNetAPI.ResponsePayloadType.RequestToPairToAccount;
                                            fromBloxGuardianMessagePacket.responsePayload = Base64Encode(JsonConvert.SerializeObject(requestPairingPayload));

                                            // Format return message as JSON object and save to file on filesystem
                                            await File.WriteAllTextAsync(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                                         JsonConvert.SerializeObject(fromBloxGuardianMessagePacket));
                                            File.Move(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                      baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_FILE_EXTENSION);

                                            // Update status to show that file with message was written
                                            sqlCommandUpdateMessageToBloxGuardian.Parameters["@ProcessingStatus"].Value = (int)TequaCreek.BloxGuardianDataModelLibrary.MessageProcessingStatus.QueuedForMessagingSubsystem;
                                            sqlCommandUpdateMessageToBloxGuardian.Parameters["@MessageToBloxGuardianInternalID"].Value =
                                                messageToBloxGuardianInternalId;
                                            await sqlCommandUpdateMessageToBloxGuardian.ExecuteNonQueryAsync();

                                            await sqlConnection2.CloseAsync();
                                        }       // using (sqlConnection2 = new NpgsqlConnection(configuration["ConnectionStrings:BloxChannel"]))

                                        break;

                                    case TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType.GetPairedAccount:

                                        // TEMP CODE
                                        logger.LogDebug("Message is Get Paired Account");
                                        // END TEMP CODE

                                        ////////////////////////////////////////////////
                                        //////  In-Game to BG - Get Paired Account /////
                                        ////////////////////////////////////////////////

                                        sqlStatement = new System.Text.StringBuilder();
                                        sqlStatement.Append("SELECT IGAP.ingame_user_bg_account_pairing_internal_id, BGA.bloxguardian_account_external_id, ");
                                        sqlStatement.Append("       BGA.account_holder_last_name, BGA.account_holder_first_name, IGAP.pairing_status ");
                                        sqlStatement.Append("  FROM ingame_user_bg_account_pairing IGAP ");
                                        sqlStatement.Append("    LEFT OUTER JOIN bloxguardian_account BGA ");
                                        sqlStatement.Append("                    ON IGAP.bloxguardian_account_internal_id = BGA.bloxguardian_account_internal_id ");
                                        sqlStatement.Append("  WHERE IGAP.allowed_communication_path_internal_id = @AllowedCommunicationPathInternalID AND ");
                                        sqlStatement.Append("        IGAP.ingame_user_id = @InGameUserId ");

                                        sqlCommandGetInGameToAccountPairing = sqlConnection1.CreateCommand();
                                        sqlCommandGetInGameToAccountPairing.CommandText = sqlStatement.ToString();
                                        sqlCommandGetInGameToAccountPairing.CommandTimeout = 600;
                                        sqlCommandGetInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@AllowedCommunicationPathInternalID", NpgsqlTypes.NpgsqlDbType.Integer));
                                        sqlCommandGetInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@InGameUserId", NpgsqlTypes.NpgsqlDbType.Varchar, 20));

                                        sqlCommandGetInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value = 0;
                                        sqlCommandGetInGameToAccountPairing.Parameters["@InGameUserId"].Value = "";
                                        await sqlCommandGetInGameToAccountPairing.PrepareAsync();

                                        // 1.)  Check for existing completed pairing.  If found, return formatted message
                                        //      with BloxGuardian Account External ID and Account Holder Name

                                        TequaCreek.BloxChannelDotNetAPI.Models.BloxGuardian.GetPairingPayload getPairingPayload = new BloxChannelDotNetAPI.Models.BloxGuardian.GetPairingPayload();

                                        sqlCommandGetInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value = allowedCommunicationPathInternalId;
                                        sqlCommandGetInGameToAccountPairing.Parameters["@InGameUserId"].Value = inGameUserId;

                                        sqlDataReaderGetInGameToAccountPairing = await sqlCommandGetInGameToAccountPairing.ExecuteReaderAsync();
                                        if (await sqlDataReaderGetInGameToAccountPairing.ReadAsync())
                                        {

                                            if (!await sqlDataReaderGetInGameToAccountPairing.IsDBNullAsync(ApplicationValues.INGAME_USER_ID_BG_ACCOUNT_PAIRING_QUERY_RESULT_COLUMN_OFFSET_BLOXGUARDIAN_ACCOUNT_EXTERNAL_ID))
                                            {
                                                getPairingPayload.bloxGuardianAccountId = sqlDataReaderGetInGameToAccountPairing.GetString(ApplicationValues.INGAME_USER_ID_BG_ACCOUNT_PAIRING_QUERY_RESULT_COLUMN_OFFSET_BLOXGUARDIAN_ACCOUNT_EXTERNAL_ID);
                                            }

                                            if (!await sqlDataReaderGetInGameToAccountPairing.IsDBNullAsync(ApplicationValues.INGAME_USER_ID_BG_ACCOUNT_PAIRING_QUERY_RESULT_COLUMN_OFFSET_ACCOUNT_HOLDER_LAST_NAME))
                                            {
                                                getPairingPayload.accountHolderLastName = sqlDataReaderGetInGameToAccountPairing.GetString(ApplicationValues.INGAME_USER_ID_BG_ACCOUNT_PAIRING_QUERY_RESULT_COLUMN_OFFSET_ACCOUNT_HOLDER_LAST_NAME);
                                            }

                                            if (!await sqlDataReaderGetInGameToAccountPairing.IsDBNullAsync(ApplicationValues.INGAME_USER_ID_BG_ACCOUNT_PAIRING_QUERY_RESULT_COLUMN_OFFSET_ACCOUNT_HOLDER_FIRST_NAME))
                                            {
                                                getPairingPayload.accountHolderLastName = sqlDataReaderGetInGameToAccountPairing.GetString(ApplicationValues.INGAME_USER_ID_BG_ACCOUNT_PAIRING_QUERY_RESULT_COLUMN_OFFSET_ACCOUNT_HOLDER_FIRST_NAME);
                                            }

                                            getPairingPayload.pairingStatus = sqlDataReaderGetInGameToAccountPairing.GetInt32(ApplicationValues.INGAME_USER_ID_BG_ACCOUNT_PAIRING_QUERY_RESULT_COLUMN_OFFSET_PAIRING_STATUS);

                                        }
                                        else
                                        {
                                            getPairingPayload.pairingStatus = (int)TequaCreek.BloxGuardianDataModelLibrary.PairingStatus.NoPairingExists;
                                        }
                                        await sqlDataReaderGetInGameToAccountPairing.CloseAsync();

                                        // 2.)  Write return message to file on file system
                                        baseMessageFileName = PadZeroLeft(messageToBloxGuardianInternalId, 10);
                                        baseFileDirectory = this.configuration["AppSettings:ToInGameFileStorePrefix"] + Path.DirectorySeparatorChar +
                                                            PadZeroLeft(inGameEndpointInternalId, 6);
                                        Directory.CreateDirectory(baseFileDirectory);       // PROGRAMMER'S NOTE:  This function will return if directory already created

                                        fromBloxGuardianMessagePacket = new BloxChannelDotNetAPI.Models.BloxGuardian.FromBloxGuardianMessagePacket();

                                        fromBloxGuardianMessagePacket.respondingToMessageToBloxGuardianExternalId = messageToBloxGuardianExternalId;
                                        fromBloxGuardianMessagePacket.responsePayloadType = (int)TequaCreek.BloxChannelDotNetAPI.ResponsePayloadType.GetPairedAccount;
                                        fromBloxGuardianMessagePacket.responsePayload = Base64Encode(JsonConvert.SerializeObject(getPairingPayload));

                                        // Format return message as JSON object and save to file on filesystem
                                        await File.WriteAllTextAsync(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                                     JsonConvert.SerializeObject(fromBloxGuardianMessagePacket));
                                        File.Move(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                  baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_FILE_EXTENSION);

                                        // 3.)  Update status to show that file with message was written
                                        sqlCommandUpdateMessageToBloxGuardian.Parameters["@ProcessingStatus"].Value = (int)TequaCreek.BloxGuardianDataModelLibrary.MessageProcessingStatus.QueuedForMessagingSubsystem;
                                        sqlCommandUpdateMessageToBloxGuardian.Parameters["@MessageToBloxGuardianInternalID"].Value = messageToBloxGuardianInternalId;
                                        await sqlCommandUpdateMessageToBloxGuardian.ExecuteNonQueryAsync();

                                        break;

                                    case TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType.RemovePairedAccount:

                                        // TEMP CODE
                                        logger.LogDebug("Remove Paired Account");
                                        // END TEMP CODE

                                        ///////////////////////////////////////////////////
                                        //////  In-Game to BG - Remove Paired Account /////
                                        ///////////////////////////////////////////////////

                                        // PROGRAMMER'S NOTE:  Need to check for pending messages (in database) and remove them
                                        //                     Will not send push message to device requesting removal,
                                        //                     but instead will allow message to time out or notify device via
                                        //                     API return code if attempt to interact with a previously-removed
                                        //                     message (for security purposes)

                                        using (sqlConnection2 = new NpgsqlConnection(configuration["ConnectionStrings:BloxChannel"]))
                                        {
                                            await sqlConnection2.OpenAsync();

                                            sqlStatement = new System.Text.StringBuilder();
                                            sqlStatement.Append("DELETE FROM ingame_user_bg_account_pairing ");
                                            sqlStatement.Append("  WHERE allowed_communication_path_internal_id = @AllowedCommunicationPathInternalID AND ");
                                            sqlStatement.Append("        ingame_user_id = @InGameUserId ");

                                            sqlCommandDeleteInGameToAccountPairing = sqlConnection2.CreateCommand();
                                            sqlCommandDeleteInGameToAccountPairing.CommandText = sqlStatement.ToString();
                                            sqlCommandDeleteInGameToAccountPairing.CommandTimeout = 600;
                                            sqlCommandDeleteInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@AllowedCommunicationPathInternalID", NpgsqlTypes.NpgsqlDbType.Integer));
                                            sqlCommandDeleteInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@InGameUserId", NpgsqlTypes.NpgsqlDbType.Varchar, 20));

                                            sqlCommandDeleteInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value = 0;
                                            sqlCommandDeleteInGameToAccountPairing.Parameters["@InGameUserId"].Value = "";
                                            await sqlCommandDeleteInGameToAccountPairing.PrepareAsync();

                                            sqlStatement = new System.Text.StringBuilder();
                                            sqlStatement.Append("UPDATE message_to_mobile_device ");
                                            sqlStatement.Append("  SET processing_status = @UpdateToProcessingStatus ");
                                            sqlStatement.Append("  WHERE message_to_mobile_device_internal_id IN ");
                                            sqlStatement.Append("(SELECT MTMD.message_to_mobile_device_internal_id ");
                                            sqlStatement.Append("   FROM message_to_mobile_device MTMD INNER JOIN message_to_bloxguardian MTBG ON  ");
                                            sqlStatement.Append("        MTMD.message_to_bloxguardian_internal_id = MTBG.message_to_bloxguardian_internal_id ");
                                            sqlStatement.Append("   WHERE MTBG.allowed_communication_path_internal_id = @AllowedCommunicationPathInternalID AND ");
                                            sqlStatement.Append("         MTBG.ingame_user_id = @InGameUserId AND MTMD.processing_status = @ProcessingStatus AND ");
                                            sqlStatement.Append("         MTBG.message_source = @MessageSource) ");

                                            sqlCommandRemovePendingMessagesForPairedAccount = sqlConnection2.CreateCommand();
                                            sqlCommandRemovePendingMessagesForPairedAccount.CommandText = sqlStatement.ToString();
                                            sqlCommandRemovePendingMessagesForPairedAccount.CommandTimeout = 600;
                                            sqlCommandRemovePendingMessagesForPairedAccount.Parameters.Add(new NpgsqlParameter("@UpdateToProcessingStatus", NpgsqlTypes.NpgsqlDbType.Integer));
                                            sqlCommandRemovePendingMessagesForPairedAccount.Parameters.Add(new NpgsqlParameter("@AllowedCommunicationPathInternalID", NpgsqlTypes.NpgsqlDbType.Integer));
                                            sqlCommandRemovePendingMessagesForPairedAccount.Parameters.Add(new NpgsqlParameter("@InGameUserId", NpgsqlTypes.NpgsqlDbType.Varchar, 20));
                                            sqlCommandRemovePendingMessagesForPairedAccount.Parameters.Add(new NpgsqlParameter("@ProcessingStatus", NpgsqlTypes.NpgsqlDbType.Integer));
                                            sqlCommandRemovePendingMessagesForPairedAccount.Parameters.Add(new NpgsqlParameter("@MessageSource", NpgsqlTypes.NpgsqlDbType.Integer));

                                            sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@UpdateToProcessingStatus"].Value = 0;
                                            sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@AllowedCommunicationPathInternalID"].Value = "";
                                            sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@InGameUserId"].Value = "";
                                            sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@ProcessingStatus"].Value = 0;
                                            sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@MessageSource"].Value = 0;
                                            await sqlCommandRemovePendingMessagesForPairedAccount.PrepareAsync();

                                            sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@UpdateToProcessingStatus"].Value =
                                                (int)TequaCreek.BloxGuardianDataModelLibrary.MessageToDeviceProcessingStatus.Removed;
                                            sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@AllowedCommunicationPathInternalID"].Value = allowedCommunicationPathInternalId;
                                            sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@InGameUserId"].Value = inGameUserId;
                                            sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@ProcessingStatus"].Value =
                                                (int)TequaCreek.BloxGuardianDataModelLibrary.MessageToDeviceProcessingStatus.PushedToDevice;
                                            sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@MessageSource"].Value =
                                                (int)TequaCreek.BloxGuardianDataModelLibrary.MessageSource.FromInGame;
                                            await sqlCommandRemovePendingMessagesForPairedAccount.ExecuteNonQueryAsync();

                                            // 1.)  Issue a delete command and return succes, regardless of whether a pairing record
                                            //      was found
                                            sqlCommandDeleteInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value = allowedCommunicationPathInternalId;
                                            sqlCommandDeleteInGameToAccountPairing.Parameters["@InGameUserId"].Value = inGameUserId;

                                            await sqlCommandDeleteInGameToAccountPairing.ExecuteNonQueryAsync();

                                            // 2.)  Write return message to file on file system
                                            baseMessageFileName = PadZeroLeft(messageToBloxGuardianInternalId, 10);
                                            baseFileDirectory = this.configuration["AppSettings:ToInGameFileStorePrefix"] + Path.DirectorySeparatorChar +
                                                                PadZeroLeft(inGameEndpointInternalId, 6);
                                            Directory.CreateDirectory(baseFileDirectory);       // PROGRAMMER'S NOTE:  This function will return if directory already created

                                            fromBloxGuardianMessagePacket = new BloxChannelDotNetAPI.Models.BloxGuardian.FromBloxGuardianMessagePacket();

                                            fromBloxGuardianMessagePacket.respondingToMessageToBloxGuardianExternalId = messageToBloxGuardianExternalId;
                                            fromBloxGuardianMessagePacket.responsePayloadType = (int)TequaCreek.BloxChannelDotNetAPI.ResponsePayloadType.RemovePairedAccount;

                                            // Format return message as JSON object and save to file on filesystem
                                            await File.WriteAllTextAsync(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                                         JsonConvert.SerializeObject(fromBloxGuardianMessagePacket));
                                            File.Move(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                      baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_FILE_EXTENSION);

                                            // 3.)  Update status to show that file with message was written
                                            sqlCommandUpdateMessageToBloxGuardian.Parameters["@ProcessingStatus"].Value = (int)TequaCreek.BloxGuardianDataModelLibrary.MessageProcessingStatus.QueuedForMessagingSubsystem;
                                            sqlCommandUpdateMessageToBloxGuardian.Parameters["@MessageToBloxGuardianInternalID"].Value = messageToBloxGuardianInternalId;
                                            await sqlCommandUpdateMessageToBloxGuardian.ExecuteNonQueryAsync();

                                            await sqlConnection2.CloseAsync();
                                        }       // using (sqlConnection2 = new NpgsqlConnection(configuration["ConnectionStrings:BloxChannel"]))

                                        break;

                                    case TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType.GetPendingMessagesForAccount:

                                        // TEMP CODE
                                        logger.LogDebug("Get Pending Messages for Account");
                                        // END TEMP CODE

                                        //////////////////////////////////////////////////////////////
                                        //////  In-Game to BG - Get Pending Messages for Account /////
                                        //////////////////////////////////////////////////////////////

                                        sqlStatement = new System.Text.StringBuilder();
                                        sqlStatement.Append("SELECT MTBG.message_to_bloxguardian_external_id, MTMD.message_pushed_date_time, MTBG.payload ");
                                        sqlStatement.Append("  FROM message_to_mobile_device MTMD INNER JOIN message_to_bloxguardian MTBG ON  ");
                                        sqlStatement.Append("       MTMD.message_to_bloxguardian_internal_id = MTBG.message_to_bloxguardian_internal_id ");
                                        sqlStatement.Append("  WHERE MTBG.allowed_communication_path_internal_id = @AllowedCommunicationPathInternalID AND ");
                                        sqlStatement.Append("        MTBG.ingame_user_id = @InGameUserId AND MTMD.processing_status = @ProcessingStatus AND ");
                                        sqlStatement.Append("        MTBG.message_source = @MessageSource ");
                                        sqlStatement.Append("  ORDER BY MTMD.message_pushed_date_time ");

                                        sqlCommandGetPendingMessagesForPairedAccount = sqlConnection1.CreateCommand();
                                        sqlCommandGetPendingMessagesForPairedAccount.CommandText = sqlStatement.ToString();
                                        sqlCommandGetPendingMessagesForPairedAccount.CommandTimeout = 600;
                                        sqlCommandGetPendingMessagesForPairedAccount.Parameters.Add(new NpgsqlParameter("@AllowedCommunicationPathInternalID", NpgsqlTypes.NpgsqlDbType.Integer));
                                        sqlCommandGetPendingMessagesForPairedAccount.Parameters.Add(new NpgsqlParameter("@InGameUserId", NpgsqlTypes.NpgsqlDbType.Varchar, 20));
                                        sqlCommandGetPendingMessagesForPairedAccount.Parameters.Add(new NpgsqlParameter("@ProcessingStatus", NpgsqlTypes.NpgsqlDbType.Integer));
                                        sqlCommandGetPendingMessagesForPairedAccount.Parameters.Add(new NpgsqlParameter("@MessageSource", NpgsqlTypes.NpgsqlDbType.Integer));

                                        sqlCommandGetPendingMessagesForPairedAccount.Parameters["@AllowedCommunicationPathInternalID"].Value = 0;
                                        sqlCommandGetPendingMessagesForPairedAccount.Parameters["@InGameUserId"].Value = "";
                                        sqlCommandGetPendingMessagesForPairedAccount.Parameters["@ProcessingStatus"].Value = 0;
                                        sqlCommandGetPendingMessagesForPairedAccount.Parameters["@MessageSource"].Value = 0;
                                        await sqlCommandGetPendingMessagesForPairedAccount.PrepareAsync();

                                        List<TequaCreek.BloxChannelDotNetAPI.Models.BloxGuardian.PendingMessagePayloadItem> messageList =
                                            new List<BloxChannelDotNetAPI.Models.BloxGuardian.PendingMessagePayloadItem>();

                                        // 1.)  Get pending messages from database table for Paired Account

                                        sqlCommandGetPendingMessagesForPairedAccount.Parameters["@AllowedCommunicationPathInternalID"].Value =
                                          allowedCommunicationPathInternalId;
                                        sqlCommandGetPendingMessagesForPairedAccount.Parameters["@InGameUserId"].Value =
                                          inGameUserId;
                                        sqlCommandGetPendingMessagesForPairedAccount.Parameters["@ProcessingStatus"].Value =
                                            (int)TequaCreek.BloxGuardianDataModelLibrary.MessageToDeviceProcessingStatus.PushedToDevice;
                                        sqlCommandGetPendingMessagesForPairedAccount.Parameters["@MessageSource"].Value =
                                            (int)TequaCreek.BloxGuardianDataModelLibrary.MessageSource.FromInGame;

                                        sqlDataReaderGetPendingMessagesForPairedAccount = await sqlCommandGetPendingMessagesForPairedAccount.ExecuteReaderAsync();
                                        while (await sqlDataReaderGetPendingMessagesForPairedAccount.ReadAsync())
                                        {
                                            TequaCreek.BloxChannelDotNetAPI.Models.BloxGuardian.PendingMessagePayloadItem pendingMessage =
                                                new BloxChannelDotNetAPI.Models.BloxGuardian.PendingMessagePayloadItem();

                                            pendingMessage.messageToBloxGuardianExternalId =
                                                sqlDataReaderGetPendingMessagesForPairedAccount.GetString(ApplicationValues.PENDING_MESSAGE_FOR_ACCOUNT_LIST_QUERY_RESULT_COLUMN_OFFSET_MESSAGE_TO_BLOXGUARDIAN_EXTERNAL_ID);
                                            pendingMessage.pushedToDeviceDateTime =
                                                sqlDataReaderGetPendingMessagesForPairedAccount.GetDateTime(ApplicationValues.PENDING_MESSAGE_FOR_ACCOUNT_LIST_QUERY_RESULT_COLUMN_OFFSET_MESSAGE_PUSHED_DATE_TIME);
                                            pendingMessage.payload = sqlDataReaderGetPendingMessagesForPairedAccount.GetString(ApplicationValues.PENDING_MESSAGE_FOR_ACCOUNT_LIST_QUERY_RESULT_COLUMN_OFFSET_PAYLOAD);

                                            messageList.Add(pendingMessage);

                                        }       // while (await sqlDataReaderGetPendingMessagesForPairedAccount.ReadAsync())
                                        await sqlDataReaderGetPendingMessagesForPairedAccount.CloseAsync();

                                        // 2.)  Write return message to file on file system
                                        baseMessageFileName = PadZeroLeft(messageToBloxGuardianInternalId, 10);
                                        baseFileDirectory = this.configuration["AppSettings:ToInGameFileStorePrefix"] + Path.DirectorySeparatorChar +
                                                            PadZeroLeft(inGameEndpointInternalId, 6);
                                        Directory.CreateDirectory(baseFileDirectory);       // PROGRAMMER'S NOTE:  This function will return if directory already created

                                        fromBloxGuardianMessagePacket = new BloxChannelDotNetAPI.Models.BloxGuardian.FromBloxGuardianMessagePacket();

                                        fromBloxGuardianMessagePacket.respondingToMessageToBloxGuardianExternalId =
                                            messageToBloxGuardianExternalId;
                                        fromBloxGuardianMessagePacket.responsePayloadType = (int)TequaCreek.BloxChannelDotNetAPI.ResponsePayloadType.GetPendingMessagesForAccount;
                                        fromBloxGuardianMessagePacket.responsePayload = Base64Encode(JsonConvert.SerializeObject(messageList));

                                        // Format return message as JSON object and save to file on filesystem
                                        await File.WriteAllTextAsync(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                                     JsonConvert.SerializeObject(fromBloxGuardianMessagePacket));
                                        File.Move(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                  baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_FILE_EXTENSION);

                                        // 3.)  Update status to show that file with message was written
                                        sqlCommandUpdateMessageToBloxGuardian.Parameters["@ProcessingStatus"].Value = (int)TequaCreek.BloxGuardianDataModelLibrary.MessageProcessingStatus.QueuedForMessagingSubsystem;
                                        sqlCommandUpdateMessageToBloxGuardian.Parameters["@MessageToBloxGuardianInternalID"].Value =
                                            messageToBloxGuardianInternalId;
                                        await sqlCommandUpdateMessageToBloxGuardian.ExecuteNonQueryAsync();

                                        break;

                                    case TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType.RemovePendingMessagesForAccount:

                                        /////////////////////////////////////////////////////////////////
                                        //////  In-Game to BG - Remove Pending Messages for Account /////
                                        /////////////////////////////////////////////////////////////////

                                        sqlStatement = new System.Text.StringBuilder();
                                        sqlStatement.Append("UPDATE message_to_mobile_device ");
                                        sqlStatement.Append("  SET processing_status = @UpdateToProcessingStatus ");
                                        sqlStatement.Append("  WHERE message_to_mobile_device_internal_id IN ");
                                        sqlStatement.Append("(SELECT MTMD.message_to_mobile_device_internal_id ");
                                        sqlStatement.Append("   FROM message_to_mobile_device MTMD INNER JOIN message_to_bloxguardian MTBG ON  ");
                                        sqlStatement.Append("        MTMD.message_to_bloxguardian_internal_id = MTBG.message_to_bloxguardian_internal_id ");
                                        sqlStatement.Append("   WHERE MTBG.allowed_communication_path_internal_id = @AllowedCommunicationPathInternalID AND ");
                                        sqlStatement.Append("         MTBG.ingame_user_id = @InGameUserId AND MTMD.processing_status = @ProcessingStatus AND ");
                                        sqlStatement.Append("         MTBG.message_source = @MessageSource) ");

                                        sqlCommandRemovePendingMessagesForPairedAccount = sqlConnection1.CreateCommand();
                                        sqlCommandRemovePendingMessagesForPairedAccount.CommandText = sqlStatement.ToString();
                                        sqlCommandRemovePendingMessagesForPairedAccount.CommandTimeout = 600;
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters.Add(new NpgsqlParameter("@UpdateToProcessingStatus", NpgsqlTypes.NpgsqlDbType.Integer));
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters.Add(new NpgsqlParameter("@AllowedCommunicationPathInternalID", NpgsqlTypes.NpgsqlDbType.Integer));
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters.Add(new NpgsqlParameter("@InGameUserId", NpgsqlTypes.NpgsqlDbType.Varchar, 20));
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters.Add(new NpgsqlParameter("@ProcessingStatus", NpgsqlTypes.NpgsqlDbType.Integer));
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters.Add(new NpgsqlParameter("@MessageSource", NpgsqlTypes.NpgsqlDbType.Integer));

                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@UpdateToProcessingStatus"].Value = 0;
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@AllowedCommunicationPathInternalID"].Value = 0;
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@InGameUserId"].Value = "";
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@ProcessingStatus"].Value = 0;
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@MessageSource"].Value = 0;

                                        // 1.)  Set status for pending messages for Account to "removed"
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@UpdateToProcessingStatus"].Value =
                                            (int)TequaCreek.BloxGuardianDataModelLibrary.MessageToDeviceProcessingStatus.Removed;
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@AllowedCommunicationPathInternalID"].Value =
                                          allowedCommunicationPathInternalId;
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@InGameUserId"].Value =
                                          inGameUserId;
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@ProcessingStatus"].Value =
                                            (int)TequaCreek.BloxGuardianDataModelLibrary.MessageToDeviceProcessingStatus.Removed;
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@MessageSource"].Value =
                                            (int)TequaCreek.BloxGuardianDataModelLibrary.MessageSource.FromInGame;
                                        await sqlCommandRemovePendingMessagesForPairedAccount.ExecuteNonQueryAsync();

                                        // 2.)  Write return message to file on file system
                                        baseMessageFileName = PadZeroLeft(messageToBloxGuardianInternalId, 10);
                                        baseFileDirectory = this.configuration["AppSettings:ToInGameFileStorePrefix"] + Path.DirectorySeparatorChar +
                                                            PadZeroLeft(inGameEndpointInternalId, 6);
                                        Directory.CreateDirectory(baseFileDirectory);       // PROGRAMMER'S NOTE:  This function will return if directory already created

                                        fromBloxGuardianMessagePacket = new BloxChannelDotNetAPI.Models.BloxGuardian.FromBloxGuardianMessagePacket();

                                        fromBloxGuardianMessagePacket.respondingToMessageToBloxGuardianExternalId =
                                            messageToBloxGuardianExternalId;
                                        fromBloxGuardianMessagePacket.responsePayloadType = (int)TequaCreek.BloxChannelDotNetAPI.ResponsePayloadType.RemovePendingMessagesForAccount;

                                        // Format return message as JSON object and save to file on filesystem
                                        await File.WriteAllTextAsync(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                                     JsonConvert.SerializeObject(fromBloxGuardianMessagePacket));
                                        File.Move(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                  baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_FILE_EXTENSION);

                                        // 3.)  Update status to show that file with message was written
                                        sqlCommandUpdateMessageToBloxGuardian.Parameters["@ProcessingStatus"].Value = (int)TequaCreek.BloxGuardianDataModelLibrary.MessageProcessingStatus.QueuedForMessagingSubsystem;
                                        sqlCommandUpdateMessageToBloxGuardian.Parameters["@MessageToBloxGuardianInternalID"].Value =
                                            messageToBloxGuardianInternalId;
                                        await sqlCommandUpdateMessageToBloxGuardian.ExecuteNonQueryAsync();

                                        break;

                                    case TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType.SendMessageToMobileDevice:

                                        // TEMP CODE
                                        logger.LogDebug("Send Message to Mobile Device");
                                        // END TEMP CODE

                                        ///////////////////////////////////////////////////////////
                                        //////  In-Game to BG - Send Message to Mobile Device /////
                                        ///////////////////////////////////////////////////////////

                                        using (sqlConnection2 = new NpgsqlConnection(configuration["ConnectionStrings:BloxChannel"]))
                                        {
                                            await sqlConnection2.OpenAsync();

                                            sqlStatement = new System.Text.StringBuilder();
                                            sqlStatement.Append("SELECT IGAP.ingame_user_bg_account_pairing_internal_id, BGA.bloxguardian_account_external_id, ");
                                            sqlStatement.Append("       BGA.account_holder_last_name, BGA.account_holder_first_name, IGAP.pairing_status ");
                                            sqlStatement.Append("  FROM ingame_user_bg_account_pairing IGAP ");
                                            sqlStatement.Append("    LEFT OUTER JOIN bloxguardian_account BGA ");
                                            sqlStatement.Append("                    ON IGAP.bloxguardian_account_internal_id = BGA.bloxguardian_account_internal_id ");
                                            sqlStatement.Append("  WHERE IGAP.allowed_communication_path_internal_id = @AllowedCommunicationPathInternalID AND ");
                                            sqlStatement.Append("        IGAP.ingame_user_id = @InGameUserId ");

                                            sqlCommandGetInGameToAccountPairing = sqlConnection1.CreateCommand();
                                            sqlCommandGetInGameToAccountPairing.CommandText = sqlStatement.ToString();
                                            sqlCommandGetInGameToAccountPairing.CommandTimeout = 600;
                                            sqlCommandGetInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@AllowedCommunicationPathInternalID", NpgsqlTypes.NpgsqlDbType.Integer));
                                            sqlCommandGetInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@InGameUserId", NpgsqlTypes.NpgsqlDbType.Varchar, 20));

                                            sqlCommandGetInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value = 0;
                                            sqlCommandGetInGameToAccountPairing.Parameters["@InGameUserId"].Value = "";
                                            await sqlCommandGetInGameToAccountPairing.PrepareAsync();

                                            sqlStatement = new System.Text.StringBuilder();
                                            sqlStatement.Append("INSERT INTO message_to_mobile_device ");
                                            sqlStatement.Append("   (ingame_user_bg_account_pairing_internal_id, message_to_bloxguardian_internal_id, ");
                                            sqlStatement.Append("    message_pushed_date_time, processing_status) ");
                                            sqlStatement.Append("   VALUES (@InGameUserBGAccountPairingInternalID, @MessageToBloxGuardianInternalID, ");
                                            sqlStatement.Append("           @MessagePushedDateTime, @ProcessingStatus) ");

                                            sqlCommandInsertPendingMessageForPairedAccount = sqlConnection2.CreateCommand();
                                            sqlCommandInsertPendingMessageForPairedAccount.CommandText = sqlStatement.ToString();
                                            sqlCommandInsertPendingMessageForPairedAccount.CommandTimeout = 600;
                                            sqlCommandInsertPendingMessageForPairedAccount.Parameters.Add(new NpgsqlParameter("@InGameUserBGAccountPairingInternalID", NpgsqlTypes.NpgsqlDbType.Integer));
                                            sqlCommandInsertPendingMessageForPairedAccount.Parameters.Add(new NpgsqlParameter("@MessageToBloxGuardianInternalID", NpgsqlTypes.NpgsqlDbType.Integer));
                                            sqlCommandInsertPendingMessageForPairedAccount.Parameters.Add(new NpgsqlParameter("@MessagePushedDateTime", NpgsqlTypes.NpgsqlDbType.Timestamp));
                                            sqlCommandInsertPendingMessageForPairedAccount.Parameters.Add(new NpgsqlParameter("@ProcessingStatus", NpgsqlTypes.NpgsqlDbType.Integer));

                                            sqlCommandInsertPendingMessageForPairedAccount.Parameters["@InGameUserBGAccountPairingInternalID"].Value = 0;
                                            sqlCommandInsertPendingMessageForPairedAccount.Parameters["@MessageToBloxGuardianInternalID"].Value = "";
                                            sqlCommandInsertPendingMessageForPairedAccount.Parameters["@MessagePushedDateTime"].Value = DateTime.MinValue;
                                            sqlCommandInsertPendingMessageForPairedAccount.Parameters["@ProcessingStatus"].Value = 0;
                                            await sqlCommandInsertPendingMessageForPairedAccount.PrepareAsync();

                                            // 1.)  Obtain paired Account for specified Allowed Communication Path and In-Game User ID
                                            sqlCommandGetInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value =
                                                allowedCommunicationPathInternalId;
                                            sqlCommandGetInGameToAccountPairing.Parameters["@InGameUserId"].Value =
                                                inGameUserId;

                                            sqlDataReaderGetInGameToAccountPairing = await sqlCommandGetInGameToAccountPairing.ExecuteReaderAsync();
                                            if (await sqlDataReaderGetInGameToAccountPairing.ReadAsync())
                                            {
                                                if (sqlDataReaderGetInGameToAccountPairing.GetInt32(ApplicationValues.INGAME_USER_ID_BG_ACCOUNT_PAIRING_QUERY_RESULT_COLUMN_OFFSET_PAIRING_STATUS) ==
                                                    (int)TequaCreek.BloxGuardianDataModelLibrary.PairingStatus.CompletedPairing)
                                                {
                                                    inGameUserBGAccountPairingInternalId =
                                                        sqlDataReaderGetInGameToAccountPairing.GetInt32(ApplicationValues.INGAME_USER_ID_BG_ACCOUNT_PAIRING_QUERY_RESULT_COLUMN_OFFSET_PAIRING_INTERNAL_ID); ;
                                                    continueProcessing = true;
                                                }
                                                else
                                                {
                                                    logger.LogWarning("Invalid account pairing condition to process message with ID = {0}", messageToBloxGuardianInternalId);
                                                    continueProcessing = false;
                                                }
                                            }
                                            else
                                            {
                                                logger.LogWarning("Invalid account pairing condition to process message with ID = {0}", messageToBloxGuardianInternalId);
                                                continueProcessing = false;
                                            }
                                            await sqlDataReaderGetInGameToAccountPairing.CloseAsync();


                                            // 2.)  Write new record to Pending Messages to Mobile Device table
                                            if (continueProcessing)
                                            {

                                                sqlCommandInsertPendingMessageForPairedAccount.Parameters["@InGameUserBGAccountPairingInternalID"].Value =
                                                    inGameUserBGAccountPairingInternalId;
                                                sqlCommandInsertPendingMessageForPairedAccount.Parameters["@MessageToBloxGuardianInternalID"].Value =
                                                    messageToBloxGuardianInternalId;
                                                sqlCommandInsertPendingMessageForPairedAccount.Parameters["@MessagePushedDateTime"].Value = processingRunDateTime;
                                                sqlCommandInsertPendingMessageForPairedAccount.Parameters["@ProcessingStatus"].Value =
                                                    (int)TequaCreek.BloxGuardianDataModelLibrary.MessageToDeviceProcessingStatus.New;
                                                await sqlCommandInsertPendingMessageForPairedAccount.ExecuteNonQueryAsync();


                                                // 3.)  Push message to Mobile Device for Account
                                                // TEMP CODE
                                                logger.LogDebug("Message will be sent to mobile device from code to be inserted here");
                                                // END TEMP CODE


                                                // 4.)  Write return message to file on file system
                                                baseMessageFileName = PadZeroLeft(messageToBloxGuardianInternalId, 10);
                                                baseFileDirectory = this.configuration["AppSettings:ToInGameFileStorePrefix"] + Path.DirectorySeparatorChar +
                                                                    PadZeroLeft(inGameEndpointInternalId, 6);
                                                Directory.CreateDirectory(baseFileDirectory);       // PROGRAMMER'S NOTE:  This function will return if directory already created

                                                fromBloxGuardianMessagePacket = new BloxChannelDotNetAPI.Models.BloxGuardian.FromBloxGuardianMessagePacket();

                                                fromBloxGuardianMessagePacket.respondingToMessageToBloxGuardianExternalId =
                                                    messageToBloxGuardianExternalId;
                                                fromBloxGuardianMessagePacket.responsePayloadType = (int)TequaCreek.BloxChannelDotNetAPI.ResponsePayloadType.SendMessageToBloxGuardian;

                                                // Format return message as JSON object and save to file on filesystem
                                                await File.WriteAllTextAsync(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                                             JsonConvert.SerializeObject(fromBloxGuardianMessagePacket));
                                                File.Move(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                          baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_FILE_EXTENSION);

                                                // 5.)  Update status to show that file with message was written
                                                sqlCommandUpdateMessageToBloxGuardian.Parameters["@ProcessingStatus"].Value = (int)TequaCreek.BloxGuardianDataModelLibrary.MessageProcessingStatus.QueuedForMessagingSubsystem;
                                                sqlCommandUpdateMessageToBloxGuardian.Parameters["@MessageToBloxGuardianInternalID"].Value =
                                                    messageToBloxGuardianInternalId;
                                                await sqlCommandUpdateMessageToBloxGuardian.ExecuteNonQueryAsync();

                                            }       // (continueProcessing)

                                            await sqlConnection2.CloseAsync();
                                        }       // using (sqlConnection2 = new NpgsqlConnection(configuration["ConnectionStrings:BloxChannel"]))

                                        break;
                                }

                            }




                            break;

                        case TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.MESSAGE_TYPE_ID_EXTERNAL_TO_BLOXGUARDIAN:

                            // TEMP CODE
                            logger.LogDebug("Message is External Endpoint to BloxGuardian");
                            // END TEMP CODE

                            messageToBloxGuardianInternalId = int.Parse(messageBody.Substring(messageBody.IndexOf(",") + 1));

                            // Determine message origination source (i.e. function that called BloxGuardian API from In-Game endpoint)
                            sqlCommandGetMessageToBloxGuardian.Parameters["@MessageToBloxGuardianInternalID"].Value = messageToBloxGuardianInternalId;
                            sqlDataReaderGetMessageToBloxGuardian = await sqlCommandGetMessageToBloxGuardian.ExecuteReaderAsync();
                            if (await sqlDataReaderGetMessageToBloxGuardian.ReadAsync())
                            {

                                messageToBloxGuardianExternalId = sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_EXTERNAL_ID);
                                messageOriginationTypeCode = sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_MESSAGE_ORIGINATION_TYPE_CODE);

                                if (!await sqlDataReaderGetMessageToBloxGuardian.IsDBNullAsync(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_PAYLOAD))
                                {
                                    payload = sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_PAYLOAD);
                                }

                                allowedCommunicationPathInternalId = sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_ALLOWED_COMMUNICATION_PATH_INTERNAL_ID);

                                if (!await sqlDataReaderGetMessageToBloxGuardian.IsDBNullAsync(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INGAME_USER_ID))
                                {
                                    inGameUserId = sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INGAME_USER_ID);
                                }

                                inGameEndpointInternalId = sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INGAME_ENDPOINT_INTERNAL_ID);
                                externalEndpointInternalId = sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_EXTERNAL_ENDPOINT_INTERNAL_ID);
                            }
                            else
                            {

                                /// Database/System integrity issue
                                logger.LogError("Unable to find record with Internal ID = {0} for Message to BloxGuardian", messageToBloxGuardianInternalId);

                                continueProcessing = false;
                            }
                            await sqlDataReaderGetMessageToBloxGuardian.CloseAsync();

                            if (continueProcessing)
                            {

                                switch ((TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType)messageOriginationTypeCode)
                                {
                                    case TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType.GetPairedAccount:

                                        // TEMP CODE
                                        logger.LogDebug("Get Paired Account");
                                        // END TEMP CODE

                                        ///////////////////////////////////////////
                                        //////  EE to BG - Get Paired Account /////
                                        ////////////////////////////////////////////

                                        sqlStatement = new System.Text.StringBuilder();
                                        sqlStatement.Append("SELECT IGAP.ingame_user_bg_account_pairing_internal_id, BGA.bloxguardian_account_external_id, ");
                                        sqlStatement.Append("       BGA.account_holder_last_name, BGA.account_holder_first_name, IGAP.pairing_status ");
                                        sqlStatement.Append("  FROM ingame_user_bg_account_pairing IGAP ");
                                        sqlStatement.Append("    LEFT OUTER JOIN bloxguardian_account BGA ");
                                        sqlStatement.Append("                    ON IGAP.bloxguardian_account_internal_id = BGA.bloxguardian_account_internal_id ");
                                        sqlStatement.Append("  WHERE IGAP.allowed_communication_path_internal_id = @AllowedCommunicationPathInternalID AND ");
                                        sqlStatement.Append("        IGAP.ingame_user_id = @InGameUserId ");

                                        sqlCommandGetInGameToAccountPairing = sqlConnection1.CreateCommand();
                                        sqlCommandGetInGameToAccountPairing.CommandText = sqlStatement.ToString();
                                        sqlCommandGetInGameToAccountPairing.CommandTimeout = 600;
                                        sqlCommandGetInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@AllowedCommunicationPathInternalID", NpgsqlTypes.NpgsqlDbType.Integer));
                                        sqlCommandGetInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@InGameUserId", NpgsqlTypes.NpgsqlDbType.Varchar, 20));

                                        sqlCommandGetInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value = 0;
                                        sqlCommandGetInGameToAccountPairing.Parameters["@InGameUserId"].Value = "";
                                        await sqlCommandGetInGameToAccountPairing.PrepareAsync();

                                        // 1.)  Check for existing completed pairing.  If found, return formatted message
                                        //      with BloxGuardian Account External ID and Account Holder Name

                                        TequaCreek.BloxChannelDotNetAPI.Models.BloxGuardian.GetPairingPayload getPairingPayload = new BloxChannelDotNetAPI.Models.BloxGuardian.GetPairingPayload();

                                        sqlCommandGetInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value =
                                            allowedCommunicationPathInternalId;
                                        sqlCommandGetInGameToAccountPairing.Parameters["@InGameUserId"].Value =
                                            inGameUserId;
                                        sqlDataReaderGetInGameToAccountPairing = await sqlCommandGetInGameToAccountPairing.ExecuteReaderAsync();
                                        if (await sqlDataReaderGetInGameToAccountPairing.ReadAsync())
                                        {

                                            if (!await sqlDataReaderGetInGameToAccountPairing.IsDBNullAsync(ApplicationValues.INGAME_USER_ID_BG_ACCOUNT_PAIRING_QUERY_RESULT_COLUMN_OFFSET_BLOXGUARDIAN_ACCOUNT_EXTERNAL_ID))
                                            {
                                                getPairingPayload.bloxGuardianAccountId = sqlDataReaderGetInGameToAccountPairing.GetString(ApplicationValues.INGAME_USER_ID_BG_ACCOUNT_PAIRING_QUERY_RESULT_COLUMN_OFFSET_BLOXGUARDIAN_ACCOUNT_EXTERNAL_ID);
                                            }

                                            if (!await sqlDataReaderGetInGameToAccountPairing.IsDBNullAsync(ApplicationValues.INGAME_USER_ID_BG_ACCOUNT_PAIRING_QUERY_RESULT_COLUMN_OFFSET_ACCOUNT_HOLDER_LAST_NAME))
                                            {
                                                getPairingPayload.accountHolderLastName = sqlDataReaderGetInGameToAccountPairing.GetString(ApplicationValues.INGAME_USER_ID_BG_ACCOUNT_PAIRING_QUERY_RESULT_COLUMN_OFFSET_ACCOUNT_HOLDER_LAST_NAME);
                                            }

                                            if (!await sqlDataReaderGetInGameToAccountPairing.IsDBNullAsync(ApplicationValues.INGAME_USER_ID_BG_ACCOUNT_PAIRING_QUERY_RESULT_COLUMN_OFFSET_ACCOUNT_HOLDER_FIRST_NAME))
                                            {
                                                getPairingPayload.accountHolderLastName = sqlDataReaderGetInGameToAccountPairing.GetString(ApplicationValues.INGAME_USER_ID_BG_ACCOUNT_PAIRING_QUERY_RESULT_COLUMN_OFFSET_ACCOUNT_HOLDER_FIRST_NAME);
                                            }

                                            getPairingPayload.pairingStatus = sqlDataReaderGetInGameToAccountPairing.GetInt32(ApplicationValues.INGAME_USER_ID_BG_ACCOUNT_PAIRING_QUERY_RESULT_COLUMN_OFFSET_PAIRING_STATUS);

                                        }
                                        else
                                        {
                                            getPairingPayload.pairingStatus = (int)TequaCreek.BloxGuardianDataModelLibrary.PairingStatus.NoPairingExists;
                                        }
                                        await sqlDataReaderGetInGameToAccountPairing.CloseAsync();

                                        // 2.)  Write return message to file on file system
                                        baseMessageFileName = PadZeroLeft(messageToBloxGuardianInternalId, 10);
                                        baseFileDirectory = this.configuration["AppSettings:ToExternalFileStorePrefix"] + Path.DirectorySeparatorChar +
                                                            PadZeroLeft(externalEndpointInternalId, 6);
                                        Directory.CreateDirectory(baseFileDirectory);       // PROGRAMMER'S NOTE:  This function will return if directory already created

                                        fromBloxGuardianMessagePacket = new BloxChannelDotNetAPI.Models.BloxGuardian.FromBloxGuardianMessagePacket();

                                        fromBloxGuardianMessagePacket.respondingToMessageToBloxGuardianExternalId =
                                            messageToBloxGuardianExternalId;
                                        fromBloxGuardianMessagePacket.responsePayloadType = (int)TequaCreek.BloxChannelDotNetAPI.ResponsePayloadType.GetPairedAccount;
                                        fromBloxGuardianMessagePacket.responsePayload = Base64Encode(JsonConvert.SerializeObject(getPairingPayload));

                                        // Format return message as JSON object and save to file on filesystem
                                        await File.WriteAllTextAsync(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                                     JsonConvert.SerializeObject(fromBloxGuardianMessagePacket));
                                        File.Move(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                  baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_FILE_EXTENSION);

                                        // 3.)  Update status to show that file with message was written
                                        sqlCommandUpdateMessageToBloxGuardian.Parameters["@ProcessingStatus"].Value = (int)TequaCreek.BloxGuardianDataModelLibrary.MessageProcessingStatus.QueuedForMessagingSubsystem;
                                        sqlCommandUpdateMessageToBloxGuardian.Parameters["@MessageToBloxGuardianInternalID"].Value =
                                            messageToBloxGuardianInternalId;
                                        await sqlCommandUpdateMessageToBloxGuardian.ExecuteNonQueryAsync();

                                        break;

                                    case TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType.RemovePairedAccount:

                                        // TEMP CODE
                                        logger.LogDebug("Remove Paired Account");
                                        // END TEMP CODE

                                        //////////////////////////////////////////////
                                        //////  EE to BG - Remove Paired Account /////
                                        //////////////////////////////////////////////

                                        // PROGRAMMER'S NOTE:  Need to check for pending messages (in database) and remove them
                                        //                     Will not send push message to device requesting removal,
                                        //                     but instead will allow message to time out or notify device via
                                        //                     API return code if attempt to interact with a previously-removed
                                        //                     message (for security purposes)

                                        sqlStatement = new System.Text.StringBuilder();
                                        sqlStatement.Append("DELETE FROM ingame_user_bg_account_pairing ");
                                        sqlStatement.Append("  WHERE allowed_communication_path_internal_id = @AllowedCommunicationPathInternalID AND ");
                                        sqlStatement.Append("        ingame_user_id = @InGameUserId ");

                                        sqlCommandDeleteInGameToAccountPairing = sqlConnection1.CreateCommand();
                                        sqlCommandDeleteInGameToAccountPairing.CommandText = sqlStatement.ToString();
                                        sqlCommandDeleteInGameToAccountPairing.CommandTimeout = 600;
                                        sqlCommandDeleteInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@AllowedCommunicationPathInternalID", NpgsqlTypes.NpgsqlDbType.Integer));
                                        sqlCommandDeleteInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@InGameUserId", NpgsqlTypes.NpgsqlDbType.Varchar, 20));

                                        sqlCommandDeleteInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value = 0;
                                        sqlCommandDeleteInGameToAccountPairing.Parameters["@InGameUserId"].Value = "";
                                        await sqlCommandDeleteInGameToAccountPairing.PrepareAsync();

                                        sqlStatement = new System.Text.StringBuilder();
                                        sqlStatement.Append("UPDATE message_to_mobile_device ");
                                        sqlStatement.Append("  SET processing_status = @UpdateToProcessingStatus ");
                                        sqlStatement.Append("  WHERE message_to_mobile_device_internal_id IN ");
                                        sqlStatement.Append("(SELECT MTMD.message_to_mobile_device_internal_id ");
                                        sqlStatement.Append("   FROM message_to_mobile_device MTMD INNER JOIN message_to_bloxguardian MTBG ON  ");
                                        sqlStatement.Append("        MTMD.message_to_bloxguardian_internal_id = MTBG.message_to_bloxguardian_internal_id ");
                                        sqlStatement.Append("   WHERE MTBG.allowed_communication_path_internal_id = @AllowedCommunicationPathInternalID AND ");
                                        sqlStatement.Append("         MTBG.ingame_user_id = @InGameUserId AND MTMD.processing_status = @ProcessingStatus AND ");
                                        sqlStatement.Append("         MTBG.message_source = @MessageSource) ");

                                        sqlCommandRemovePendingMessagesForPairedAccount = sqlConnection1.CreateCommand();
                                        sqlCommandRemovePendingMessagesForPairedAccount.CommandText = sqlStatement.ToString();
                                        sqlCommandRemovePendingMessagesForPairedAccount.CommandTimeout = 600;
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters.Add(new NpgsqlParameter("@UpdateToProcessingStatus", NpgsqlTypes.NpgsqlDbType.Integer));
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters.Add(new NpgsqlParameter("@AllowedCommunicationPathInternalID", NpgsqlTypes.NpgsqlDbType.Integer));
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters.Add(new NpgsqlParameter("@InGameUserId", NpgsqlTypes.NpgsqlDbType.Varchar, 20));
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters.Add(new NpgsqlParameter("@ProcessingStatus", NpgsqlTypes.NpgsqlDbType.Integer));
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters.Add(new NpgsqlParameter("@MessageSource", NpgsqlTypes.NpgsqlDbType.Integer));

                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@UpdateToProcessingStatus"].Value = 0;
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@AllowedCommunicationPathInternalID"].Value = 0;
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@InGameUserId"].Value = "";
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@ProcessingStatus"].Value = 0;
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@MessageSource"].Value = 0;
                                        await sqlCommandRemovePendingMessagesForPairedAccount.PrepareAsync();

                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@UpdateToProcessingStatus"].Value =
                                            (int)TequaCreek.BloxGuardianDataModelLibrary.MessageToDeviceProcessingStatus.Removed;
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@AllowedCommunicationPathInternalID"].Value =
                                          allowedCommunicationPathInternalId;
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@InGameUserId"].Value =
                                          inGameUserId;
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@ProcessingStatus"].Value =
                                            (int)TequaCreek.BloxGuardianDataModelLibrary.MessageToDeviceProcessingStatus.Removed;
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@MessageSource"].Value =
                                            (int)TequaCreek.BloxGuardianDataModelLibrary.MessageSource.FromExternal;
                                        await sqlCommandRemovePendingMessagesForPairedAccount.ExecuteNonQueryAsync();

                                        // 1.)  Issue a delete command and return succes, regardless of whether a pairing record
                                        //      was found
                                        sqlCommandDeleteInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value =
                                          allowedCommunicationPathInternalId;
                                        sqlCommandDeleteInGameToAccountPairing.Parameters["@InGameUserId"].Value =
                                          inGameUserId;

                                        await sqlCommandDeleteInGameToAccountPairing.ExecuteNonQueryAsync();

                                        // 2.)  Write return message to file on file system
                                        baseMessageFileName = PadZeroLeft(messageToBloxGuardianInternalId, 10);
                                        baseFileDirectory = this.configuration["AppSettings:ToExternalFileStorePrefix"] + Path.DirectorySeparatorChar +
                                                            PadZeroLeft(externalEndpointInternalId, 6);
                                        Directory.CreateDirectory(baseFileDirectory);       // PROGRAMMER'S NOTE:  This function will return if directory already created

                                        fromBloxGuardianMessagePacket = new BloxChannelDotNetAPI.Models.BloxGuardian.FromBloxGuardianMessagePacket();

                                        fromBloxGuardianMessagePacket.respondingToMessageToBloxGuardianExternalId =
                                            messageToBloxGuardianExternalId;
                                        fromBloxGuardianMessagePacket.responsePayloadType = (int)TequaCreek.BloxChannelDotNetAPI.ResponsePayloadType.RemovePairedAccount;

                                        // Format return message as JSON object and save to file on filesystem
                                        await File.WriteAllTextAsync(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                                     JsonConvert.SerializeObject(fromBloxGuardianMessagePacket));
                                        File.Move(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                  baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_FILE_EXTENSION);

                                        // 3.)  Update status to show that file with message was written
                                        sqlCommandUpdateMessageToBloxGuardian.Parameters["@ProcessingStatus"].Value = (int)TequaCreek.BloxGuardianDataModelLibrary.MessageProcessingStatus.QueuedForMessagingSubsystem;
                                        sqlCommandUpdateMessageToBloxGuardian.Parameters["@MessageToBloxGuardianInternalID"].Value =
                                            messageToBloxGuardianInternalId;
                                        await sqlCommandUpdateMessageToBloxGuardian.ExecuteNonQueryAsync();

                                        break;

                                    case TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType.GetPendingMessagesForAccount:

                                        // TEMP CODE
                                        logger.LogDebug("Get Pending Messages for Account");
                                        // END TEMP CODE

                                        /////////////////////////////////////////////////////////
                                        //////  EE to BG - Get Pending Messages for Account /////
                                        /////////////////////////////////////////////////////////

                                        sqlStatement = new System.Text.StringBuilder();
                                        sqlStatement.Append("SELECT MTBG.message_to_bloxguardian_external_id, MTMD.message_pushed_date_time, MTBG.payload ");
                                        sqlStatement.Append("  FROM message_to_mobile_device MTMD INNER JOIN message_to_bloxguardian MTBG ON  ");
                                        sqlStatement.Append("       MTMD.message_to_bloxguardian_internal_id = MTBG.message_to_bloxguardian_internal_id ");
                                        sqlStatement.Append("  WHERE MTBG.allowed_communication_path_internal_id = @AllowedCommunicationPathInternalID AND ");
                                        sqlStatement.Append("        MTBG.ingame_user_id = @InGameUserId AND MTMD.processing_status = @ProcessingStatus AND ");
                                        sqlStatement.Append("        MTBG.message_source = @MessageSource ");
                                        sqlStatement.Append("  ORDER BY MTMD.message_pushed_date_time ");

                                        sqlCommandGetPendingMessagesForPairedAccount = sqlConnection1.CreateCommand();
                                        sqlCommandGetPendingMessagesForPairedAccount.CommandText = sqlStatement.ToString();
                                        sqlCommandGetPendingMessagesForPairedAccount.CommandTimeout = 600;
                                        sqlCommandGetPendingMessagesForPairedAccount.Parameters.Add(new NpgsqlParameter("@AllowedCommunicationPathInternalID", NpgsqlTypes.NpgsqlDbType.Integer));
                                        sqlCommandGetPendingMessagesForPairedAccount.Parameters.Add(new NpgsqlParameter("@InGameUserId", NpgsqlTypes.NpgsqlDbType.Varchar, 20));
                                        sqlCommandGetPendingMessagesForPairedAccount.Parameters.Add(new NpgsqlParameter("@ProcessingStatus", NpgsqlTypes.NpgsqlDbType.Integer));
                                        sqlCommandGetPendingMessagesForPairedAccount.Parameters.Add(new NpgsqlParameter("@MessageSource", NpgsqlTypes.NpgsqlDbType.Integer));

                                        sqlCommandGetPendingMessagesForPairedAccount.Parameters["@AllowedCommunicationPathInternalID"].Value = 0;
                                        sqlCommandGetPendingMessagesForPairedAccount.Parameters["@InGameUserId"].Value = "";
                                        sqlCommandGetPendingMessagesForPairedAccount.Parameters["@ProcessingStatus"].Value = 0;
                                        sqlCommandGetPendingMessagesForPairedAccount.Parameters["@MessageSource"].Value = 0;
                                        await sqlCommandGetPendingMessagesForPairedAccount.PrepareAsync();

                                        List<TequaCreek.BloxChannelDotNetAPI.Models.BloxGuardian.PendingMessagePayloadItem> messageList =
                                            new List<BloxChannelDotNetAPI.Models.BloxGuardian.PendingMessagePayloadItem>();

                                        // 1.)  Get pending messages for Account from database table

                                        sqlCommandGetPendingMessagesForPairedAccount.Parameters["@AllowedCommunicationPathInternalID"].Value =
                                          allowedCommunicationPathInternalId;
                                        sqlCommandGetPendingMessagesForPairedAccount.Parameters["@InGameUserId"].Value =
                                          inGameUserId;
                                        sqlCommandGetPendingMessagesForPairedAccount.Parameters["@ProcessingStatus"].Value =
                                            (int)TequaCreek.BloxGuardianDataModelLibrary.MessageToDeviceProcessingStatus.PushedToDevice;
                                        sqlCommandGetPendingMessagesForPairedAccount.Parameters["@MessageSource"].Value =
                                            (int)TequaCreek.BloxGuardianDataModelLibrary.MessageSource.FromExternal;

                                        sqlDataReaderGetPendingMessagesForPairedAccount = await sqlCommandGetPendingMessagesForPairedAccount.ExecuteReaderAsync();
                                        while (await sqlDataReaderGetPendingMessagesForPairedAccount.ReadAsync())
                                        {
                                            TequaCreek.BloxChannelDotNetAPI.Models.BloxGuardian.PendingMessagePayloadItem pendingMessage =
                                                new BloxChannelDotNetAPI.Models.BloxGuardian.PendingMessagePayloadItem();

                                            pendingMessage.messageToBloxGuardianExternalId =
                                                sqlDataReaderGetPendingMessagesForPairedAccount.GetString(ApplicationValues.PENDING_MESSAGE_FOR_ACCOUNT_LIST_QUERY_RESULT_COLUMN_OFFSET_MESSAGE_TO_BLOXGUARDIAN_EXTERNAL_ID);
                                            pendingMessage.pushedToDeviceDateTime =
                                                sqlDataReaderGetPendingMessagesForPairedAccount.GetDateTime(ApplicationValues.PENDING_MESSAGE_FOR_ACCOUNT_LIST_QUERY_RESULT_COLUMN_OFFSET_MESSAGE_PUSHED_DATE_TIME);
                                            pendingMessage.payload = sqlDataReaderGetPendingMessagesForPairedAccount.GetString(ApplicationValues.PENDING_MESSAGE_FOR_ACCOUNT_LIST_QUERY_RESULT_COLUMN_OFFSET_PAYLOAD);

                                            messageList.Add(pendingMessage);

                                        }       // while (await sqlDataReaderGetPendingMessagesForPairedAccount.ReadAsync())
                                        await sqlDataReaderGetPendingMessagesForPairedAccount.CloseAsync();

                                        // 2.)  Write return message to file on file system
                                        baseMessageFileName = PadZeroLeft(messageToBloxGuardianInternalId, 10);
                                        baseFileDirectory = this.configuration["AppSettings:ToExternalFileStorePrefix"] + Path.DirectorySeparatorChar +
                                                            PadZeroLeft(externalEndpointInternalId, 6);
                                        Directory.CreateDirectory(baseFileDirectory);       // PROGRAMMER'S NOTE:  This function will return if directory already created

                                        fromBloxGuardianMessagePacket = new BloxChannelDotNetAPI.Models.BloxGuardian.FromBloxGuardianMessagePacket();

                                        fromBloxGuardianMessagePacket.respondingToMessageToBloxGuardianExternalId =
                                            messageToBloxGuardianExternalId;
                                        fromBloxGuardianMessagePacket.responsePayloadType = (int)TequaCreek.BloxChannelDotNetAPI.ResponsePayloadType.GetPendingMessagesForAccount;
                                        fromBloxGuardianMessagePacket.responsePayload = Base64Encode(JsonConvert.SerializeObject(messageList));

                                        // Format return message as JSON object and save to file on filesystem
                                        await File.WriteAllTextAsync(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                                     JsonConvert.SerializeObject(fromBloxGuardianMessagePacket));
                                        File.Move(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                  baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_FILE_EXTENSION);

                                        // 3.)  Update status to show that file with message was written
                                        sqlCommandUpdateMessageToBloxGuardian.Parameters["@ProcessingStatus"].Value = (int)TequaCreek.BloxGuardianDataModelLibrary.MessageProcessingStatus.QueuedForMessagingSubsystem;
                                        sqlCommandUpdateMessageToBloxGuardian.Parameters["@MessageToBloxGuardianInternalID"].Value =
                                            messageToBloxGuardianInternalId;
                                        await sqlCommandUpdateMessageToBloxGuardian.ExecuteNonQueryAsync();

                                        break;

                                    case TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType.RemovePendingMessagesForAccount:

                                        // TEMP CODE
                                        logger.LogDebug("Remove Pending Messages for Account");
                                        // END TEMP CODE

                                        ////////////////////////////////////////////////////////////
                                        //////  EE to BG - Remove Pending Messages for Account /////
                                        ////////////////////////////////////////////////////////////

                                        sqlStatement = new System.Text.StringBuilder();
                                        sqlStatement.Append("UPDATE message_to_mobile_device ");
                                        sqlStatement.Append("  SET processing_status = @UpdateToProcessingStatus ");
                                        sqlStatement.Append("  WHERE message_to_mobile_device_internal_id IN ");
                                        sqlStatement.Append("(SELECT MTMD.message_to_mobile_device_internal_id ");
                                        sqlStatement.Append("   FROM message_to_mobile_device MTMD INNER JOIN message_to_bloxguardian MTBG ON  ");
                                        sqlStatement.Append("        MTMD.message_to_bloxguardian_internal_id = MTBG.message_to_bloxguardian_internal_id ");
                                        sqlStatement.Append("   WHERE MTBG.allowed_communication_path_internal_id = @AllowedCommunicationPathInternalID AND ");
                                        sqlStatement.Append("         MTBG.ingame_user_id = @InGameUserId AND MTMD.processing_status = @ProcessingStatus AND ");
                                        sqlStatement.Append("         MTBG.message_source = @MessageSource) ");

                                        sqlCommandRemovePendingMessagesForPairedAccount = sqlConnection1.CreateCommand();
                                        sqlCommandRemovePendingMessagesForPairedAccount.CommandText = sqlStatement.ToString();
                                        sqlCommandRemovePendingMessagesForPairedAccount.CommandTimeout = 600;
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters.Add(new NpgsqlParameter("@UpdateToProcessingStatus", NpgsqlTypes.NpgsqlDbType.Integer));
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters.Add(new NpgsqlParameter("@AllowedCommunicationPathInternalID", NpgsqlTypes.NpgsqlDbType.Integer));
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters.Add(new NpgsqlParameter("@InGameUserId", NpgsqlTypes.NpgsqlDbType.Varchar, 20));
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters.Add(new NpgsqlParameter("@ProcessingStatus", NpgsqlTypes.NpgsqlDbType.Integer));
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters.Add(new NpgsqlParameter("@MessageSource", NpgsqlTypes.NpgsqlDbType.Integer));

                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@UpdateToProcessingStatus"].Value = 0;
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@AllowedCommunicationPathInternalID"].Value = 0;
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@InGameUserId"].Value = "";
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@ProcessingStatus"].Value = 0;
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@MessageSource"].Value = 0;
                                        await sqlCommandRemovePendingMessagesForPairedAccount.PrepareAsync();

                                        // 1.)  Set status for pending messages for Account to "removed"
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@UpdateToProcessingStatus"].Value =
                                            (int)TequaCreek.BloxGuardianDataModelLibrary.MessageToDeviceProcessingStatus.Removed;
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@AllowedCommunicationPathInternalID"].Value =
                                          allowedCommunicationPathInternalId;
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@InGameUserId"].Value =
                                          inGameUserId;
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@ProcessingStatus"].Value =
                                            (int)TequaCreek.BloxGuardianDataModelLibrary.MessageToDeviceProcessingStatus.Removed;
                                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@MessageSource"].Value =
                                            (int)TequaCreek.BloxGuardianDataModelLibrary.MessageSource.FromExternal;
                                        await sqlCommandRemovePendingMessagesForPairedAccount.ExecuteNonQueryAsync();

                                        // 2.)  Write return message to file on file system
                                        baseMessageFileName = PadZeroLeft(messageToBloxGuardianInternalId, 10);
                                        baseFileDirectory = this.configuration["AppSettings:ToExternalFileStorePrefix"] + Path.DirectorySeparatorChar +
                                                            PadZeroLeft(externalEndpointInternalId, 6);
                                        Directory.CreateDirectory(baseFileDirectory);       // PROGRAMMER'S NOTE:  This function will return if directory already created

                                        fromBloxGuardianMessagePacket = new BloxChannelDotNetAPI.Models.BloxGuardian.FromBloxGuardianMessagePacket();

                                        fromBloxGuardianMessagePacket.respondingToMessageToBloxGuardianExternalId =
                                            messageToBloxGuardianExternalId;
                                        fromBloxGuardianMessagePacket.responsePayloadType = (int)TequaCreek.BloxChannelDotNetAPI.ResponsePayloadType.RemovePendingMessagesForAccount;

                                        // Format return message as JSON object and save to file on filesystem
                                        await File.WriteAllTextAsync(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                                     JsonConvert.SerializeObject(fromBloxGuardianMessagePacket));
                                        File.Move(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                  baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_FILE_EXTENSION);

                                        // 3.)  Update status to show that file with message was written
                                        sqlCommandUpdateMessageToBloxGuardian.Parameters["@ProcessingStatus"].Value =
                                            (int)TequaCreek.BloxGuardianDataModelLibrary.MessageProcessingStatus.QueuedForMessagingSubsystem;
                                        sqlCommandUpdateMessageToBloxGuardian.Parameters["@MessageToBloxGuardianInternalID"].Value =
                                            messageToBloxGuardianInternalId;
                                        await sqlCommandUpdateMessageToBloxGuardian.ExecuteNonQueryAsync();

                                        break;

                                    case TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType.SendMessageToMobileDevice:

                                        // TEMP CODE
                                        logger.LogDebug("Send Message to Mobile Device");
                                        // END TEMP CODE

                                        //////////////////////////////////////////////////////
                                        //////  EE to BG - Send Message to Mobile Device /////
                                        //////////////////////////////////////////////////////

                                        using (sqlConnection2 = new NpgsqlConnection(configuration["ConnectionStrings:BloxChannel"]))
                                        {
                                            await sqlConnection2.OpenAsync();

                                            sqlStatement = new System.Text.StringBuilder();
                                            sqlStatement.Append("SELECT IGAP.ingame_user_bg_account_pairing_internal_id, BGA.bloxguardian_account_external_id, ");
                                            sqlStatement.Append("       BGA.account_holder_last_name, BGA.account_holder_first_name, IGAP.pairing_status ");
                                            sqlStatement.Append("  FROM ingame_user_bg_account_pairing IGAP ");
                                            sqlStatement.Append("    LEFT OUTER JOIN bloxguardian_account BGA ");
                                            sqlStatement.Append("                    ON IGAP.bloxguardian_account_internal_id = BGA.bloxguardian_account_internal_id ");
                                            sqlStatement.Append("  WHERE IGAP.allowed_communication_path_internal_id = @AllowedCommunicationPathInternalID AND ");
                                            sqlStatement.Append("        IGAP.ingame_user_id = @InGameUserId ");

                                            sqlCommandGetInGameToAccountPairing = sqlConnection1.CreateCommand();
                                            sqlCommandGetInGameToAccountPairing.CommandText = sqlStatement.ToString();
                                            sqlCommandGetInGameToAccountPairing.CommandTimeout = 600;
                                            sqlCommandGetInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@AllowedCommunicationPathInternalID", NpgsqlTypes.NpgsqlDbType.Integer));
                                            sqlCommandGetInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@InGameUserId", NpgsqlTypes.NpgsqlDbType.Varchar, 20));

                                            sqlCommandGetInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value = 0;
                                            sqlCommandGetInGameToAccountPairing.Parameters["@InGameUserId"].Value = "";
                                            await sqlCommandGetInGameToAccountPairing.PrepareAsync();

                                            sqlStatement = new System.Text.StringBuilder();
                                            sqlStatement.Append("INSERT INTO message_to_mobile_device ");
                                            sqlStatement.Append("   (ingame_user_bg_account_pairing_internal_id, message_to_bloxguardian_internal_id, ");
                                            sqlStatement.Append("    message_pushed_date_time, processing_status) ");
                                            sqlStatement.Append("   VALUES (@InGameUserBGAccountPairingInternalID, @MessageToBloxGuardianInternalID, ");
                                            sqlStatement.Append("           @MessagePushedDateTime, @ProcessingStatus) ");

                                            sqlCommandInsertPendingMessageForPairedAccount = sqlConnection2.CreateCommand();
                                            sqlCommandInsertPendingMessageForPairedAccount.CommandText = sqlStatement.ToString();
                                            sqlCommandInsertPendingMessageForPairedAccount.CommandTimeout = 600;
                                            sqlCommandInsertPendingMessageForPairedAccount.Parameters.Add(new NpgsqlParameter("@InGameUserBGAccountPairingInternalID", NpgsqlTypes.NpgsqlDbType.Integer));
                                            sqlCommandInsertPendingMessageForPairedAccount.Parameters.Add(new NpgsqlParameter("@MessageToBloxGuardianInternalID", NpgsqlTypes.NpgsqlDbType.Integer));
                                            sqlCommandInsertPendingMessageForPairedAccount.Parameters.Add(new NpgsqlParameter("@MessagePushedDateTime", NpgsqlTypes.NpgsqlDbType.Timestamp));
                                            sqlCommandInsertPendingMessageForPairedAccount.Parameters.Add(new NpgsqlParameter("@ProcessingStatus", NpgsqlTypes.NpgsqlDbType.Integer));

                                            sqlCommandInsertPendingMessageForPairedAccount.Parameters["@InGameUserBGAccountPairingInternalID"].Value = 0;
                                            sqlCommandInsertPendingMessageForPairedAccount.Parameters["@MessageToBloxGuardianInternalID"].Value = "";
                                            sqlCommandInsertPendingMessageForPairedAccount.Parameters["@MessagePushedDateTime"].Value = DateTime.MinValue;
                                            sqlCommandInsertPendingMessageForPairedAccount.Parameters["@ProcessingStatus"].Value = 0;
                                            await sqlCommandInsertPendingMessageForPairedAccount.PrepareAsync();

                                            // 1.)  Obtain paired Account for specified Allowed Communication Path and In-Game User ID
                                            sqlCommandGetInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value =
                                                allowedCommunicationPathInternalId;
                                            sqlCommandGetInGameToAccountPairing.Parameters["@InGameUserId"].Value =
                                                inGameUserId;

                                            sqlDataReaderGetInGameToAccountPairing = await sqlCommandGetInGameToAccountPairing.ExecuteReaderAsync();
                                            if (await sqlDataReaderGetInGameToAccountPairing.ReadAsync())
                                            {
                                                if (sqlDataReaderGetInGameToAccountPairing.GetInt32(ApplicationValues.INGAME_USER_ID_BG_ACCOUNT_PAIRING_QUERY_RESULT_COLUMN_OFFSET_PAIRING_STATUS) ==
                                                    (int)TequaCreek.BloxGuardianDataModelLibrary.PairingStatus.CompletedPairing)
                                                {
                                                    inGameUserBGAccountPairingInternalId =
                                                        sqlDataReaderGetInGameToAccountPairing.GetInt32(ApplicationValues.INGAME_USER_ID_BG_ACCOUNT_PAIRING_QUERY_RESULT_COLUMN_OFFSET_PAIRING_INTERNAL_ID);
                                                    continueProcessing = true;
                                                }
                                                else
                                                {
                                                    logger.LogWarning("Invalid account pairing condition to process message with ID = {0}", messageToBloxGuardianInternalId);
                                                    continueProcessing = false;
                                                }
                                            }
                                            else
                                            {
                                                logger.LogWarning("Invalid account pairing condition to process message with ID = {0}", messageToBloxGuardianInternalId);
                                                continueProcessing = false;
                                            }
                                            await sqlDataReaderGetInGameToAccountPairing.CloseAsync();

                                            if (continueProcessing)
                                            {
                                                // 2.)  Write new record to Pending Messages to Mobile Device table
                                                sqlCommandInsertPendingMessageForPairedAccount.Parameters["@InGameUserBGAccountPairingInternalID"].Value =
                                                    inGameUserBGAccountPairingInternalId;
                                                sqlCommandInsertPendingMessageForPairedAccount.Parameters["@MessageToBloxGuardianInternalID"].Value =
                                                    messageToBloxGuardianInternalId;
                                                sqlCommandInsertPendingMessageForPairedAccount.Parameters["@MessagePushedDateTime"].Value = processingRunDateTime;
                                                sqlCommandInsertPendingMessageForPairedAccount.Parameters["@ProcessingStatus"].Value =
                                                    (int)TequaCreek.BloxGuardianDataModelLibrary.MessageToDeviceProcessingStatus.New;
                                                await sqlCommandInsertPendingMessageForPairedAccount.ExecuteNonQueryAsync();


                                                // 3.)  Push message to Mobile Device for Account
                                                // TEMP CODE
                                                logger.LogDebug("Message pushed to mobile device with code to be inserted here");
                                                // END TEMP CODE


                                                // 4.)  Write return message to file on file system
                                                baseMessageFileName = PadZeroLeft(messageToBloxGuardianInternalId, 10);
                                                baseFileDirectory = this.configuration["AppSettings:ToExternalFileStorePrefix"] + Path.DirectorySeparatorChar +
                                                                    PadZeroLeft(externalEndpointInternalId, 6);
                                                Directory.CreateDirectory(baseFileDirectory);       // PROGRAMMER'S NOTE:  This function will return if directory already created

                                                fromBloxGuardianMessagePacket = new BloxChannelDotNetAPI.Models.BloxGuardian.FromBloxGuardianMessagePacket();

                                                fromBloxGuardianMessagePacket.respondingToMessageToBloxGuardianExternalId =
                                                    messageToBloxGuardianExternalId;
                                                fromBloxGuardianMessagePacket.responsePayloadType = (int)TequaCreek.BloxChannelDotNetAPI.ResponsePayloadType.SendMessageToBloxGuardian;

                                                // Format return message as JSON object and save to file on filesystem
                                                await File.WriteAllTextAsync(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                                             JsonConvert.SerializeObject(fromBloxGuardianMessagePacket));
                                                File.Move(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                          baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_FILE_EXTENSION);

                                                // 5.)  Update status to show that file with message was written
                                                sqlCommandUpdateMessageToBloxGuardian.Parameters["@ProcessingStatus"].Value = (int)TequaCreek.BloxGuardianDataModelLibrary.MessageProcessingStatus.QueuedForMessagingSubsystem;
                                                sqlCommandUpdateMessageToBloxGuardian.Parameters["@MessageToBloxGuardianInternalID"].Value =
                                                    messageToBloxGuardianInternalId;
                                                await sqlCommandUpdateMessageToBloxGuardian.ExecuteNonQueryAsync();

                                            }

                                            await sqlConnection2.CloseAsync();
                                        }       // using (sqlConnection2 = new NpgsqlConnection(configuration["ConnectionStrings:BloxChannel"]))

                                        break;
                                }

                            }       // (continueProcessing)



                            break;

                        case TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.MESSAGE_TYPE_ID_INTERSYSTEM_TO_BLOXGUARDIAN:

                            // TEMP CODE
                            logger.LogDebug("Message is Inter-System to BloxGuardian");
                            // END TEMP CODE

                            switch ((TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType)messageOriginationTypeCode)
                            {
                                case TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType.InterSystemSendEmailVerification:

                                    // TEMP CODE
                                    logger.LogDebug("Send Email Verification");
                                    // END TEMP CODE

                                    ///////////////////////////////////////////////////////////
                                    //////  Inter-System to BG - Send E-Mail Verification /////
                                    ///////////////////////////////////////////////////////////

                                    bloxGuardianAccountInternalId = int.Parse(messageBody.Substring(messageBody.IndexOf(",") + 1));

                                    sqlStatement = new System.Text.StringBuilder();
                                    sqlStatement.Append("SELECT BGA.bloxguardian_account_external_id, BGA.account_holder_last_name, ");
                                    sqlStatement.Append("       BGA.account_holder_first_name, BGA.email_address ");
                                    sqlStatement.Append("  FROM ingame_bloxguardian_account BGA ");
                                    sqlStatement.Append("  WHERE BGA.bloxguardian_account_internal_id = @BloxGuardianAccountInternalID ");

                                    sqlCommandGetBloxGuardianAccount = sqlConnection1.CreateCommand();
                                    sqlCommandGetBloxGuardianAccount.CommandText = sqlStatement.ToString();
                                    sqlCommandGetBloxGuardianAccount.CommandTimeout = 600;
                                    sqlCommandGetBloxGuardianAccount.Parameters.Add(new NpgsqlParameter("@BloxGuardianAccountInternalID", NpgsqlTypes.NpgsqlDbType.Integer));

                                    sqlCommandGetBloxGuardianAccount.Parameters["@BloxGuardianAccountInternalID"].Value = 0;
                                    await sqlCommandGetBloxGuardianAccount.PrepareAsync();


                                    sqlCommandGetBloxGuardianAccount.Parameters["@BloxGuardianAccountInternalID"].Value = bloxGuardianAccountInternalId;
                                    sqlDataReaderGetBloxGuardianAccount = await sqlCommandGetBloxGuardianAccount.ExecuteReaderAsync();
                                    if (await sqlDataReaderGetBloxGuardianAccount.ReadAsync())
                                    {

                                        string bloxGuardianAccountExternalId = 
                                            sqlDataReaderGetBloxGuardianAccount.GetString(ApplicationValues.BLOXGUARDIAN_ACCOUNT_QUERY_RESULT_COLUMN_OFFSET_ACCOUNT_EXTERNAL_ID);
                                        string accountHolderLastName =
                                            sqlDataReaderGetBloxGuardianAccount.GetString(ApplicationValues.BLOXGUARDIAN_ACCOUNT_QUERY_RESULT_COLUMN_OFFSET_ACCOUNT_HOLDER_LAST_NAME);
                                        string accountHolderFirstName =
                                            sqlDataReaderGetBloxGuardianAccount.GetString(ApplicationValues.BLOXGUARDIAN_ACCOUNT_QUERY_RESULT_COLUMN_OFFSET_ACCOUNT_HOLDER_FIRST_NAME);
                                        string eMailAddress = 
                                            sqlDataReaderGetBloxGuardianAccount.GetString(ApplicationValues.BLOXGUARDIAN_ACCOUNT_QUERY_RESULT_COLUMN_OFFSET_EMAIL_ADDRESS);

                                        // TEMP CODE
                                        logger.LogDebug("Sending verification email message to BloxGuardian Account External ID {0} at address {1} - name = {2}",
                                                        bloxGuardianAccountExternalId, eMailAddress, accountHolderFirstName + " " + accountHolderLastName);
                                        // END TEMP CODE


                                        string initializationVector = TequaCreek.BloxGuardianDataModelLibrary
                                                                                 .SharedFunctions.Base64Encode(TequaCreek.BloxGuardianDataModelLibrary
                                                                                                                         .SharedConstantValues
                                                                                                                         .EMAIL_VERIFICATION_CODE_INITIALIZATION_VECTOR);
                                        string encryptionKey = TequaCreek.BloxGuardianDataModelLibrary
                                                                         .SharedFunctions.Base64Encode(TequaCreek.BloxGuardianDataModelLibrary
                                                                                                                 .SharedConstantValues
                                                                                                                 .EMAIL_VERIFICATION_CODE_ENCRYPTION_KEY);
                                        string encryptedEmailVerificationCode = TequaCreek.BloxGuardianDataModelLibrary.SharedFunctions
                                                                                          .AES_IV_Encrypt(bloxGuardianAccountExternalId, initializationVector, 
                                                                                                          encryptionKey);

                                        TequaCreek.BloxGuardianDataModelLibrary.Models.EmailVerifyDynamicTemplateData dynamicTemplateData =
                                            new BloxGuardianDataModelLibrary.Models.EmailVerifyDynamicTemplateData();

                                        dynamicTemplateData.header = configuration["AppSettings:InitialValidateEMailHeader"];
                                        dynamicTemplateData.text = configuration["AppSettings:InitialValidateEMailText"];
                                        dynamicTemplateData.clickbackLink = configuration["AppSettings:InitialValidateClickbackLink"]
                                                                               .Replace(TequaCreek.BloxGuardianDataModelLibrary
                                                                                                  .SharedConstantValues.REPLACE_TOKEN_ENCRYPTED_EMAIL_VERIFICATION_CODE,
                                                                                        encryptedEmailVerificationCode);
                                        dynamicTemplateData.buttonText = configuration["AppSettings:InitialValidateButtonText"];

                                        SendGridMessage sendGridMessage = new SendGridMessage();

                                        sendGridMessage.SetSubject(configuration["AppSettings:InitialValidateEMailSubject"]);
                                        sendGridMessage.SetFrom(new EmailAddress(configuration["AppSettings:EMailVerifySenderAddress"],
                                                                     configuration["AppSettings:EMailVerifySenderName"]));
                                        sendGridMessage.AddTo(new EmailAddress(eMailAddress, accountHolderFirstName + " " + accountHolderLastName));
                                        sendGridMessage.SetTemplateId(configuration["AppSettings:SendGridEMailVerifyTemplateID"]);
                                        sendGridMessage.SetTemplateData(dynamicTemplateData);

                                        var response = await sendGridClient.SendEmailAsync(sendGridMessage).ConfigureAwait(false);

                                        if (!response.IsSuccessStatusCode)
                                        {
                                            logger.LogError("Error sending verification email message to BloxGuardian Account External ID {0} at address {1} - Status Code = {2}",
                                                            bloxGuardianAccountExternalId, eMailAddress, response.StatusCode);
                                        }

                                    }
                                    else
                                    {
                                        logger.LogWarning("System integrity issue - Cannot find BloxGuardian Account record in BloxGuardianAccountMessageProcessingWorker::MessageReceivedAsync()");
                                    }       // (await sqlDataReaderGetBloxGuardianAccount.ReadAsync())


                                    break;

                                case TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType.InterSystemCompletedEmailVerification:

                                    bloxGuardianAccountInternalId = int.Parse(messageBody.Substring(messageBody.IndexOf(",") + 1));

                                    // TEMP CODE
                                    logger.LogDebug("Completed Email Verification");
                                    // END TEMP CODE

                                    ///////////////////////////////////////////////////////////////
                                    //////  Intersystem to BG - Completed E-Mail Verification /////
                                    ///////////////////////////////////////////////////////////////


                                    // PROGRAMMER'S NOTE:  Push message to mobile device for BG Account


                                    break;



                                case TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType.InterSystemNotifyPairingComplete:

                                    inGameUserBGAccountPairingInternalId = int.Parse(messageBody.Substring(messageBody.IndexOf(",") + 1));

                                    //////////////////////////////////////////////////////////////////
                                    //////  Intersystem to BG - Send Pairing Completion Messages /////
                                    //////////////////////////////////////////////////////////////////

                                    // TEMP CODE
                                    logger.LogDebug("Send Pairing Completion Notification ");
                                    // END TEMP CODE

                                    sqlStatement = new System.Text.StringBuilder();
                                    sqlStatement.Append("SELECT ACP.ingame_endpoint_internal_id, ACP.external_endpoint_internal_id, ");
                                    sqlStatement.Append("       BGA.account_holder_last_name, BGA.account_holder_first_name, IGAP.ingame_user_id ");
                                    sqlStatement.Append("  FROM ingame_user_bg_account_pairing IGAP ");
                                    sqlStatement.Append("    INNER JOIN bloxguardian_account BGA ");
                                    sqlStatement.Append("               ON IGAP.bloxguardian_account_internal_id = BGA.bloxguardian_account_internal_id ");
                                    sqlStatement.Append("    INNER JOIN allowed_communication_path ACP ");
                                    sqlStatement.Append("               ON IGAP.allowed_communication_path_internal_id = ACP.allowed_communication_path_internal_id ");
                                    sqlStatement.Append("  WHERE IGAP.ingame_user_bg_account_pairing_internal_id = @InGameUserBGAccountPairingInternalID ");

                                    sqlCommandGetInGameToAccountPairing = sqlConnection1.CreateCommand();
                                    sqlCommandGetInGameToAccountPairing.CommandText = sqlStatement.ToString();
                                    sqlCommandGetInGameToAccountPairing.CommandTimeout = 600;
                                    sqlCommandGetInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@AllowedCommunicationPathInternalID", NpgsqlTypes.NpgsqlDbType.Integer));
                                    sqlCommandGetInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@InGameUserId", NpgsqlTypes.NpgsqlDbType.Varchar, 20));

                                    sqlCommandGetInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value = 0;
                                    sqlCommandGetInGameToAccountPairing.Parameters["@InGameUserId"].Value = "";
                                    await sqlCommandGetInGameToAccountPairing.PrepareAsync();
                                    sqlDataReaderGetInGameToAccountPairing = await sqlCommandGetInGameToAccountPairing.ExecuteReaderAsync();
                                    if (await sqlDataReaderGetInGameToAccountPairing.ReadAsync())
                                    {

                                        inGameEndpointInternalId = sqlDataReaderGetInGameToAccountPairing.GetInt32(ApplicationValues.ACCOUNT_PAIRING_ENDPOINT_NOTIFY_QUERY_RESULT_COLUMN_OFFSET_INGAME_ENDPOINT_INTERNAL_ID);
                                        externalEndpointInternalId = sqlDataReaderGetInGameToAccountPairing.GetInt32(ApplicationValues.ACCOUNT_PAIRING_ENDPOINT_NOTIFY_QUERY_RESULT_COLUMN_OFFSET_EXTERNAL_ENDPOINT_INTERNAL_ID);

                                        int generatedSystemMessageId = int.Parse(System.DateTime.Now.ToString("MMddhhmmss"));

                                        // 1.)  Write return message to file on file system for External Endpoint
                                        baseMessageFileName = PadZeroLeft(generatedSystemMessageId, 10);
                                        baseFileDirectory = this.configuration["AppSettings:ToInGameFileStorePrefix"] + Path.DirectorySeparatorChar +
                                                            PadZeroLeft(inGameEndpointInternalId, 6);
                                        Directory.CreateDirectory(baseFileDirectory);       // PROGRAMMER'S NOTE:  This function will return if directory already created

                                        TequaCreek.BloxChannelDotNetAPI.Models.BloxGuardian.PairedAccountInformation pairedAccountInformationForEE =
                                            new BloxChannelDotNetAPI.Models.BloxGuardian.PairedAccountInformation();

                                        pairedAccountInformationForEE.accountHolderLastName =
                                            sqlDataReaderGetInGameToAccountPairing.GetString(ApplicationValues.ACCOUNT_PAIRING_ENDPOINT_NOTIFY_QUERY_RESULT_COLUMN_OFFSET_ACCOUNT_HOLDER_LAST_NAME);
                                        pairedAccountInformationForEE.accountHolderFirstName =
                                            sqlDataReaderGetInGameToAccountPairing.GetString(ApplicationValues.ACCOUNT_PAIRING_ENDPOINT_NOTIFY_QUERY_RESULT_COLUMN_OFFSET_ACCOUNT_HOLDER_FIRST_NAME);
                                        pairedAccountInformationForEE.inGameUserId =
                                            sqlDataReaderGetInGameToAccountPairing.GetString(ApplicationValues.ACCOUNT_PAIRING_ENDPOINT_NOTIFY_QUERY_RESULT_COLUMN_OFFSET_INGAME_USER_ID);

                                        fromBloxGuardianMessagePacket = new BloxChannelDotNetAPI.Models.BloxGuardian.FromBloxGuardianMessagePacket();

                                        fromBloxGuardianMessagePacket.respondingToMessageToBloxGuardianExternalId =
                                            TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.RESPONDING_TO_MESSAGE_ID_SENTINEL_VALUE_PAIRING_COMPLETE;
                                        fromBloxGuardianMessagePacket.responsePayloadType = (int)TequaCreek.BloxChannelDotNetAPI.ResponsePayloadType.PairedAccountInformation;
                                        fromBloxGuardianMessagePacket.responsePayload = Base64Encode(JsonConvert.SerializeObject(pairedAccountInformationForEE));

                                        // Format return message as JSON object and save to file on filesystem
                                        await File.WriteAllTextAsync(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                                        JsonConvert.SerializeObject(fromBloxGuardianMessagePacket));
                                        File.Move(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                  baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_FILE_EXTENSION);

                                        baseMessageFileName = PadZeroLeft(generatedSystemMessageId, 10);
                                        baseFileDirectory = this.configuration["AppSettings:ToExternalFileStorePrefix"] + Path.DirectorySeparatorChar +
                                                            PadZeroLeft(externalEndpointInternalId, 6);
                                        Directory.CreateDirectory(baseFileDirectory);       // PROGRAMMER'S NOTE:  This function will return if directory already created


                                        // 1.)  Write return message to file on file system for In-Game Endpoint

                                        TequaCreek.BloxChannelIGInternalAPI.Models.BloxGuardian.PairedAccountInformation pairedAccountInformationForIG =
                                            new BloxChannelIGInternalAPI.Models.BloxGuardian.PairedAccountInformation();

                                        pairedAccountInformationForIG.accountHolderLastName =
                                            sqlDataReaderGetInGameToAccountPairing.GetString(ApplicationValues.ACCOUNT_PAIRING_ENDPOINT_NOTIFY_QUERY_RESULT_COLUMN_OFFSET_ACCOUNT_HOLDER_LAST_NAME);
                                        pairedAccountInformationForIG.accountHolderFirstName =
                                            sqlDataReaderGetInGameToAccountPairing.GetString(ApplicationValues.ACCOUNT_PAIRING_ENDPOINT_NOTIFY_QUERY_RESULT_COLUMN_OFFSET_ACCOUNT_HOLDER_FIRST_NAME);
                                        pairedAccountInformationForIG.inGameUserId =
                                            sqlDataReaderGetInGameToAccountPairing.GetString(ApplicationValues.ACCOUNT_PAIRING_ENDPOINT_NOTIFY_QUERY_RESULT_COLUMN_OFFSET_INGAME_USER_ID);

                                        var fromBloxGuardianMessagePacketForIG = new BloxChannelIGInternalAPI.Models.BloxGuardian.FromBloxGuardianMessagePacket();

                                        fromBloxGuardianMessagePacketForIG.respondingToMessageToBloxGuardianExternalId =
                                            TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.RESPONDING_TO_MESSAGE_ID_SENTINEL_VALUE_PAIRING_COMPLETE;
                                        fromBloxGuardianMessagePacketForIG.responsePayloadType = (int)TequaCreek.BloxChannelDotNetAPI.ResponsePayloadType.PairedAccountInformation;
                                        fromBloxGuardianMessagePacketForIG.responsePayload = Base64Encode(JsonConvert.SerializeObject(pairedAccountInformationForIG));

                                        // Format return message as JSON object and save to file on filesystem
                                        await File.WriteAllTextAsync(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                                        JsonConvert.SerializeObject(fromBloxGuardianMessagePacket));
                                        File.Move(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                  baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_FILE_EXTENSION);
                                    }
                                    else
                                    {
                                        logger.LogWarning("System integrity issue - cannot find account pairing record in BloxGuardianMessageProcessingService::Worker::MessageReceivedAsync");
                                        continueProcessing = false;
                                    }       // (await sqlDataReaderGetInGameToAccountPairing.ReadAsync())
                                    await sqlDataReaderGetInGameToAccountPairing.CloseAsync();

                                    break;
                            }

                            break;

                    }       // (continueProcessing)

                    await sqlConnection1.CloseAsync();
                }       // using (sqlConnection1 = new SqlConnection(configuration["ConnectionStrings:BloxChannel"].ToString()))

            }
            catch (Exception e)
            {

                logger.LogError("Unhandled exception occured in Worker::FromBloxChannelMessageReceivedAsync().  Message is {0}", e.Message);
                logger.LogError(e.StackTrace);

            }
            finally
            {

                logger.LogInformation("Acknowledging message queue message with delivery tag {0}", @event.DeliveryTag);
                rmqFromBloxChannelChannel.BasicAck(@event.DeliveryTag, false);

            }

        }       // FromBloxChannelMessageReceivedAsync()


        private string PadZeroLeft(int inValue, int stringLength)
        {

            string returnValue = new string('0', stringLength) + inValue.ToString();

            returnValue = returnValue.Substring(returnValue.Length - stringLength);

            return returnValue;
        }

        public static string Base64Encode(string plainText)
        {
            var plainTextBytes = System.Text.Encoding.UTF8.GetBytes(plainText);
            return System.Convert.ToBase64String(plainTextBytes);
        }

        public static string Base64Decode(string base64EncodedData)
        {
            var base64EncodedBytes = System.Convert.FromBase64String(base64EncodedData);
            return System.Text.Encoding.UTF8.GetString(base64EncodedBytes);
        }

    }
}
