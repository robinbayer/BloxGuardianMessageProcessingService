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

namespace TequaCreek.BloxGuardianMessageProcessingService
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> logger;
        private IConfiguration configuration;

        private Timer timer = null!;

        private List<RabbitMQ.Client.Events.AsyncEventingBasicConsumer> consumers;
        private List<RabbitMQ.Client.IModel> channels;
        private List<RabbitMQ.Client.IConnection> connections;

        private RabbitMQ.Client.ConnectionFactory rmqFromBloxChannelConnectionFactory;
        private RabbitMQ.Client.IConnection rmqFromBloxChannelConnection;
        private RabbitMQ.Client.IModel rmqFromBloxChannelChannel;
        private RabbitMQ.Client.Events.AsyncEventingBasicConsumer rmqFromBloxChannelConsumer;

        public string processingServerId { get; set; }
        public Worker(IConfiguration configuration, ILogger<Worker> logger)
        {
            this.logger = logger;
            this.configuration = configuration;

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
                                        configuration["AppSettings:BC_MBToBG_MPSMessageQueueHostName"],
                                        configuration["AppSettings:BC_MBToBG_MPSMessageQueuePort"],
                                        configuration["AppSettings:BC_MBToBG_MPSMessageQueueVirtualHostName"]);
                rmqFromBloxChannelConnectionFactory = new RabbitMQ.Client.ConnectionFactory()
                {
                    HostName = configuration["AppSettings:BC_MBToBG_MPSMessageQueueHostName"],
                    Port = int.Parse(configuration["AppSettings:BC_MBToBG_MPSMessageQueuePort"]),
                    VirtualHost = configuration["AppSettings:BC_MBToBG_MPSMessageQueueVirtualHostName"],
                    UserName = configuration["AppSettings:BC_MBToBG_MPSMessageQueueUserName"],
                    Password = configuration["AppSettings:BC_MBToBG_MPSMessageQueueUserPassword"],
                    DispatchConsumersAsync = true
                };

                rmqFromBloxChannelConnection = rmqFromBloxChannelConnectionFactory.CreateConnection();
                rmqFromBloxChannelChannel = rmqFromBloxChannelConnection.CreateModel();

                exchangeName = configuration["AppSettings:BC_MBToBG_MPSMessageQueueExchangeName"];

                // PROGRAMMER'S NOTE:  Best practice is to just declare definitions.  If Exchange and Queue already exist, the functions
                //                     simply return without action.
                rmqFromBloxChannelChannel.ExchangeDeclare(exchangeName, RabbitMQ.Client.ExchangeType.Direct, true, false, null);

                /////////////////////////////////////////
                /// From BloxChannel messages channel ///
                /////////////////////////////////////////

                queueName = string.Format(TequaCreek.BloxChannelDataModelLibrary.SharedConstantValues.MESSAGE_QUEUE_NAME_PATTERN_BLOXCHANNEL_TO_BLOXGUARDIAN,
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

            NpgsqlConnection sqlConnection1;
            NpgsqlConnection sqlConnection2;
            NpgsqlCommand sqlCommandGetMessageToBloxGuardian;
            NpgsqlDataReader sqlDataReaderGetMessageToBloxGuardian;
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

                    using (sqlConnection2 = new NpgsqlConnection(configuration["ConnectionStrings:BloxChannel"]))
                    {
                        await sqlConnection2.OpenAsync();

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
                        sqlStatement.Append("SELECT IGAP.ingame_user_bg_account_pairing_internal_id, IGAP.bloxguardian_account_external_id, ");
                        sqlStatement.Append("       IGAP.account_holder_last_name, IGAP.account_holder_first_name, IGAP.pairing_status ");
                        sqlStatement.Append("  FROM ingame_user_bg_account_pairing IGAP ");
                        sqlStatement.Append("    LEFT OUTER JOIN bloxguardian_account BGA ");
                        sqlStatement.Append("                    ON IGAP.bloxguardian_account_internal_id = BGA.bloxguardian_account_internal_id ");
                        sqlStatement.Append("  WHERE IGAP.allowed_communication_path_internal_id = @AllowedCommunicationPathInternalID AND ");
                        sqlStatement.Append("        IGAP.ingame_user_id = @InGameUserId ");

                        sqlCommandGetInGameToAccountPairing = sqlConnection1.CreateCommand();
                        sqlCommandGetInGameToAccountPairing.CommandText = sqlStatement.ToString();
                        sqlCommandGetInGameToAccountPairing.CommandTimeout = 600;
                        sqlCommandGetInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@AllowedCommunicationPathInternalID", NpgsqlTypes.NpgsqlDbType.Varchar, 32));
                        sqlCommandGetInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@InGameUserId", NpgsqlTypes.NpgsqlDbType.Varchar, 20));

                        sqlCommandGetInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value = "";
                        sqlCommandGetInGameToAccountPairing.Parameters["@InGameUserId"].Value = "";
                        await sqlCommandGetMessageToBloxGuardian.PrepareAsync();

                        sqlStatement = new System.Text.StringBuilder();
                        sqlStatement.Append("DELETE FROM ingame_user_bg_account_pairing ");
                        sqlStatement.Append("  WHERE allowed_communication_path_internal_id = @AllowedCommunicationPathInternalID AND ");
                        sqlStatement.Append("        ingame_user_id = @InGameUserId ");

                        sqlCommandDeleteInGameToAccountPairing = sqlConnection1.CreateCommand();
                        sqlCommandDeleteInGameToAccountPairing.CommandText = sqlStatement.ToString();
                        sqlCommandDeleteInGameToAccountPairing.CommandTimeout = 600;
                        sqlCommandDeleteInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@AllowedCommunicationPathInternalID", NpgsqlTypes.NpgsqlDbType.Varchar, 32));
                        sqlCommandDeleteInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@InGameUserId", NpgsqlTypes.NpgsqlDbType.Varchar, 20));

                        sqlCommandDeleteInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value = "";
                        sqlCommandDeleteInGameToAccountPairing.Parameters["@InGameUserId"].Value = "";
                        await sqlCommandDeleteInGameToAccountPairing.PrepareAsync();

                        sqlStatement = new System.Text.StringBuilder();
                        sqlStatement.Append("INSERT INTO ingame_user_bg_account_pairing ");
                        sqlStatement.Append("  (allowed_communication_path_internal_id, ingame_user_id, bloxguardian_account_internal_id, ");
                        sqlStatement.Append("   pairing_status, record_added_date_time, record_last_updated_date_time) ");
                        sqlStatement.Append("  VALUES (allowed_communication_path_internal_id, ingame_user_id, pairing_status, ");
                        sqlStatement.Append("   record_added_date_time, record_last_updated_date_time) ");

                        sqlCommandInsertInGameToAccountPairing = sqlConnection1.CreateCommand();
                        sqlCommandInsertInGameToAccountPairing.CommandText = sqlStatement.ToString();
                        sqlCommandInsertInGameToAccountPairing.CommandTimeout = 600;
                        sqlCommandInsertInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@AllowedCommunicationPathInternalID", NpgsqlTypes.NpgsqlDbType.Varchar, 32));
                        sqlCommandInsertInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@InGameUserId", NpgsqlTypes.NpgsqlDbType.Varchar, 20));
                        sqlCommandInsertInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@BloxGuardianAccountInternalID", NpgsqlTypes.NpgsqlDbType.Integer));
                        sqlCommandInsertInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@PairingStatus", NpgsqlTypes.NpgsqlDbType.Integer));
                        sqlCommandInsertInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@RecordAddedDateTime", NpgsqlTypes.NpgsqlDbType.Date));
                        sqlCommandInsertInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@RecordLastUpdatedDateTime", NpgsqlTypes.NpgsqlDbType.Date));

                        sqlCommandInsertInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value = "";
                        sqlCommandInsertInGameToAccountPairing.Parameters["@InGameUserId"].Value = "";
                        sqlCommandInsertInGameToAccountPairing.Parameters["@BloxGuardianAccountInternalID"].Value = 0;
                        sqlCommandInsertInGameToAccountPairing.Parameters["@PairingStatus"].Value = 0;
                        sqlCommandInsertInGameToAccountPairing.Parameters["@RecordAddedDateTime"].Value = DateTime.MinValue;
                        sqlCommandInsertInGameToAccountPairing.Parameters["@RecordLastUpdatedDateTime"].Value = DateTime.MinValue;
                        await sqlCommandInsertInGameToAccountPairing.PrepareAsync();

                        sqlStatement = new System.Text.StringBuilder();
                        sqlStatement.Append("UPDATE ingame_user_bg_account_pairing ");
                        sqlStatement.Append("  SET pairing_status = @PairingStatus ");
                        sqlStatement.Append("  WHERE allowed_communication_path_internal_id = @AllowedCommunicationPathInternalID AND ");
                        sqlStatement.Append("        ingame_user_id = @InGameUserId ");

                        sqlCommandUpdateInGameToAccountPairing = sqlConnection1.CreateCommand();
                        sqlCommandUpdateInGameToAccountPairing.CommandText = sqlStatement.ToString();
                        sqlCommandUpdateInGameToAccountPairing.CommandTimeout = 600;
                        sqlCommandUpdateInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@PairingStatus", NpgsqlTypes.NpgsqlDbType.Integer));
                        sqlCommandUpdateInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@AllowedCommunicationPathInternalID", NpgsqlTypes.NpgsqlDbType.Varchar, 32));
                        sqlCommandUpdateInGameToAccountPairing.Parameters.Add(new NpgsqlParameter("@InGameUserId", NpgsqlTypes.NpgsqlDbType.Varchar, 20));

                        sqlCommandUpdateInGameToAccountPairing.Parameters["@PairingStatus"].Value = 0;
                        sqlCommandUpdateInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value = "";
                        sqlCommandUpdateInGameToAccountPairing.Parameters["@InGameUserId"].Value = "";
                        await sqlCommandUpdateInGameToAccountPairing.PrepareAsync();

                        sqlStatement = new System.Text.StringBuilder();
                        sqlStatement.Append("SELECT MTBG.message_to_bloxguardian_external_id, MTD.message_pushed_date_time, MTBG.payload ");
                        sqlStatement.Append("  FROM message_to_mobile_device MTD INNER JOIN message_to_bloxguardian MTBG ON  ");
                        sqlStatement.Append("       MTD.message_to_bloxguardian_internal_id = MTBG.message_to_bloxguardian_internal_id ");
                        sqlStatement.Append("  WHERE MTBG.allowed_communication_path_internal_id = @AllowedCommunicationPathInternalID AND ");
                        sqlStatement.Append("        MTBG.ingame_user_id = @InGameUserId AND MTD.processing_status = @ProcessingStatus AND ");
                        sqlStatement.Append("        MTBG.message_source = @MessageSource ");
                        sqlStatement.Append("  ORDER BY MTD.message_pushed_date_time ");

                        sqlCommandGetPendingMessagesForPairedAccount = sqlConnection1.CreateCommand();
                        sqlCommandGetPendingMessagesForPairedAccount.CommandText = sqlStatement.ToString();
                        sqlCommandGetPendingMessagesForPairedAccount.CommandTimeout = 600;
                        sqlCommandGetPendingMessagesForPairedAccount.Parameters.Add(new NpgsqlParameter("@AllowedCommunicationPathInternalID", NpgsqlTypes.NpgsqlDbType.Integer));
                        sqlCommandGetPendingMessagesForPairedAccount.Parameters.Add(new NpgsqlParameter("@InGameUserId", NpgsqlTypes.NpgsqlDbType.Varchar, 20));
                        sqlCommandGetPendingMessagesForPairedAccount.Parameters.Add(new NpgsqlParameter("@ProcessingStatus", NpgsqlTypes.NpgsqlDbType.Integer));
                        sqlCommandGetPendingMessagesForPairedAccount.Parameters.Add(new NpgsqlParameter("@MessageSource", NpgsqlTypes.NpgsqlDbType.Integer));

                        sqlCommandGetPendingMessagesForPairedAccount.Parameters["@AllowedCommunicationPathInternalID"].Value = "";
                        sqlCommandGetPendingMessagesForPairedAccount.Parameters["@InGameUserId"].Value = "";
                        sqlCommandGetPendingMessagesForPairedAccount.Parameters["@ProcessingStatus"].Value = 0;
                        sqlCommandGetPendingMessagesForPairedAccount.Parameters["@MessageSource"].Value = 0;
                        await sqlCommandGetPendingMessagesForPairedAccount.PrepareAsync();

                        sqlStatement = new System.Text.StringBuilder();
                        sqlStatement.Append("UPDATE message_to_mobile_device ");
                        sqlStatement.Append("  SET processing_status = @UpdateToProcessingStatus ");
                        sqlStatement.Append("  WHERE message_to_mobile_device_internal_id IN ");
                        sqlStatement.Append("(SELECT MTG.message_to_mobile_device_internal_id ");
                        sqlStatement.Append("   FROM message_to_mobile_device MTD INNER JOIN message_to_bloxguardian MTBG ON  ");
                        sqlStatement.Append("        MTD.message_to_bloxguardian_internal_id = MTBG.message_to_bloxguardian_internal_id ");
                        sqlStatement.Append("   WHERE MTBG.allowed_communication_path_internal_id = @AllowedCommunicationPathInternalID AND ");
                        sqlStatement.Append("         MTBG.ingame_user_id = @InGameUserId AND MTD.processing_status = @ProcessingStatus AND ");
                        sqlStatement.Append("         MTBG.message_source = @MessageSource) ");

                        sqlCommandRemovePendingMessagesForPairedAccount = sqlConnection1.CreateCommand();
                        sqlCommandRemovePendingMessagesForPairedAccount.CommandText = sqlStatement.ToString();
                        sqlCommandRemovePendingMessagesForPairedAccount.CommandTimeout = 600;
                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters.Add(new NpgsqlParameter("@UpdateProcessingStatus", NpgsqlTypes.NpgsqlDbType.Integer));
                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters.Add(new NpgsqlParameter("@AllowedCommunicationPathInternalID", NpgsqlTypes.NpgsqlDbType.Integer));
                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters.Add(new NpgsqlParameter("@InGameUserId", NpgsqlTypes.NpgsqlDbType.Varchar, 20));
                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters.Add(new NpgsqlParameter("@ProcessingStatus", NpgsqlTypes.NpgsqlDbType.Integer));
                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters.Add(new NpgsqlParameter("@MessageSource", NpgsqlTypes.NpgsqlDbType.Integer));

                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@UpdateProcessingStatus"].Value = 0;
                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@AllowedCommunicationPathInternalID"].Value = "";
                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@InGameUserId"].Value = "";
                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@ProcessingStatus"].Value = 0;
                        sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@MessageSource"].Value = 0;
                        await sqlCommandGetPendingMessagesForPairedAccount.PrepareAsync();

                        sqlStatement = new System.Text.StringBuilder();
                        sqlStatement.Append("INSERT INTO message_to_mobile_device ");
                        sqlStatement.Append("   (ingame_user_bg_account_pairing_internal_id, message_to_bloxguardian_internal_id, ");
                        sqlStatement.Append("    message_pushed_date_time, processing_status) ");
                        sqlStatement.Append("   VALUES (@InGameUserBGAccountPairingInternalID, @MessageToBloxGuardianInternalID, ");
                        sqlStatement.Append("           @MessagePushedDateTime, @ProcessingStatus) ");

                        sqlCommandInsertPendingMessageForPairedAccount = sqlConnection1.CreateCommand();
                        sqlCommandInsertPendingMessageForPairedAccount.CommandText = sqlStatement.ToString();
                        sqlCommandInsertPendingMessageForPairedAccount.CommandTimeout = 600;
                        sqlCommandInsertPendingMessageForPairedAccount.Parameters.Add(new NpgsqlParameter("@InGameUserBGAccountPairingInternalID", NpgsqlTypes.NpgsqlDbType.Integer));
                        sqlCommandInsertPendingMessageForPairedAccount.Parameters.Add(new NpgsqlParameter("@MessageToBloxGuardianInternalID", NpgsqlTypes.NpgsqlDbType.Integer));
                        sqlCommandInsertPendingMessageForPairedAccount.Parameters.Add(new NpgsqlParameter("@MessagePushedDateTime", NpgsqlTypes.NpgsqlDbType.Integer));
                        sqlCommandInsertPendingMessageForPairedAccount.Parameters.Add(new NpgsqlParameter("@ProcessingStatus", NpgsqlTypes.NpgsqlDbType.Integer));

                        sqlCommandInsertPendingMessageForPairedAccount.Parameters["@InGameUserBGAccountPairingInternalID"].Value = 0;
                        sqlCommandInsertPendingMessageForPairedAccount.Parameters["@MessageToBloxGuardianInternalID"].Value = "";
                        sqlCommandInsertPendingMessageForPairedAccount.Parameters["@MessagePushedDateTime"].Value = DateTime.MinValue;
                        sqlCommandInsertPendingMessageForPairedAccount.Parameters["@ProcessingStatus"].Value = 0;
                        await sqlCommandInsertPendingMessageForPairedAccount.PrepareAsync();

                        sqlStatement = new System.Text.StringBuilder();
                        sqlStatement.Append("UDPATE message_to_bloxguardian ");
                        sqlStatement.Append("   SET processing_status = @ProcessingStatus ");
                        sqlStatement.Append("   WHERE message_to_bloxguardian_internal_id = @MessageToBloxGuardianInternalID ");

                        sqlCommandUpdateMessageToBloxGuardian = sqlConnection1.CreateCommand();
                        sqlCommandUpdateMessageToBloxGuardian.CommandText = sqlStatement.ToString();
                        sqlCommandUpdateMessageToBloxGuardian.CommandTimeout = 600;
                        sqlCommandUpdateMessageToBloxGuardian.Parameters.Add(new NpgsqlParameter("@ProcessingStatus", NpgsqlTypes.NpgsqlDbType.Integer));
                        sqlCommandUpdateMessageToBloxGuardian.Parameters.Add(new NpgsqlParameter("@MessageToBloxGuardianInternalID", NpgsqlTypes.NpgsqlDbType.Integer));

                        sqlCommandUpdateMessageToBloxGuardian.Parameters["@ProcessingStatus"].Value = 0;
                        sqlCommandUpdateMessageToBloxGuardian.Parameters["@MessageToBloxGuardianInternalID"].Value = "";
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
                        //                                                            MESSAGE_TYPE_ID_BG_WEB_SERVICE_TO_BLOXGUARDIAN
                        //
                        // defined in TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues

                        string messageBody = System.Text.Encoding.UTF8.GetString(@event.Body.ToArray());
                        string messageType = messageBody.Substring(0, messageBody.IndexOf(","));
                        int messageToBloxGuardianInternalId = int.Parse(messageBody.Substring(messageBody.IndexOf(",") + 1));

                        switch (messageType)
                        {
                            case TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.MESSAGE_TYPE_ID_INGAME_TO_BLOXGUARDIAN:

                                // Determine message origination source (i.e. function that called BloxGuardian API from In-Game endpoint)
                                sqlCommandGetMessageToBloxGuardian.Parameters["@MessageToBloxGuardianInternalID"].Value = messageToBloxGuardianInternalId;
                                sqlDataReaderGetMessageToBloxGuardian = await sqlCommandGetMessageToBloxGuardian.ExecuteReaderAsync();
                                if (await sqlDataReaderGetMessageToBloxGuardian.ReadAsync())
                                {

                                    switch ((TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType)
                                             sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_MESSAGE_ORIGINATION_TYPE_CODE))
                                    {
                                        case TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType.RequestToPairToAccount:

                                            TequaCreek.BloxChannelDotNetAPI.Models.BloxGuardian.RequestPairingPayload requestPairingPayload = new BloxChannelDotNetAPI.Models.BloxGuardian.RequestPairingPayload();

                                            sqlCommandGetInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value =
                                                sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_ALLOWED_COMMUNICATION_PATH_INTERNAL_ID);
                                            sqlCommandGetInGameToAccountPairing.Parameters["@InGameUserId"].Value =
                                                sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INGAME_USER_ID);
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
                                                        sqlCommandDeleteInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value =
                                                          sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_ALLOWED_COMMUNICATION_PATH_INTERNAL_ID);
                                                        sqlCommandDeleteInGameToAccountPairing.Parameters["@InGameUserId"].Value =
                                                          sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INGAME_USER_ID);
                                                        await sqlCommandDeleteInGameToAccountPairing.ExecuteNonQueryAsync();

                                                        // Add new half-open pairing
                                                        sqlCommandInsertInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value =
                                                          sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_ALLOWED_COMMUNICATION_PATH_INTERNAL_ID);
                                                        sqlCommandInsertInGameToAccountPairing.Parameters["@InGameUserId"].Value =
                                                          sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INGAME_USER_ID);
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
                                                        sqlCommandUpdateInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value =
                                                          sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_ALLOWED_COMMUNICATION_PATH_INTERNAL_ID);
                                                        sqlCommandUpdateInGameToAccountPairing.Parameters["@InGameUserId"].Value =
                                                          sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INGAME_USER_ID);
                                                        await sqlCommandUpdateInGameToAccountPairing.ExecuteNonQueryAsync();

                                                        requestPairingPayload.bloxGuardianAccountId = sqlDataReaderGetInGameToAccountPairing.GetString(ApplicationValues.INGAME_USER_ID_BG_ACCOUNT_PAIRING_QUERY_RESULT_COLUMN_OFFSET_BLOXGUARDIAN_ACCOUNT_EXTERNAL_ID);
                                                        requestPairingPayload.accountHolderLastName = sqlDataReaderGetInGameToAccountPairing.GetString(ApplicationValues.INGAME_USER_ID_BG_ACCOUNT_PAIRING_QUERY_RESULT_COLUMN_OFFSET_ACCOUNT_HOLDER_LAST_NAME);
                                                        requestPairingPayload.accountHolderFirstName = sqlDataReaderGetInGameToAccountPairing.GetString(ApplicationValues.INGAME_USER_ID_BG_ACCOUNT_PAIRING_QUERY_RESULT_COLUMN_OFFSET_ACCOUNT_HOLDER_FIRST_NAME);
                                                        requestPairingPayload.pairingStatus = (int)TequaCreek.BloxGuardianDataModelLibrary.PairingStatus.CompletedPairing;
                                                        break;

                                                }

                                            }
                                            else
                                            {

                                                // 4.)  If passed all prior validations and not a completed pairing (Step 3),
                                                //      write a half-open pairing record to database table

                                                // Add new half-open pairing
                                                sqlCommandInsertInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value =
                                                  sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_ALLOWED_COMMUNICATION_PATH_INTERNAL_ID);
                                                sqlCommandInsertInGameToAccountPairing.Parameters["@InGameUserId"].Value =
                                                  sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INGAME_USER_ID);
                                                sqlCommandInsertInGameToAccountPairing.Parameters["@BloxGuardianAccountInternalID"].Value = System.DBNull.Value;
                                                sqlCommandInsertInGameToAccountPairing.Parameters["@PairingStatus"].Value = (int)TequaCreek.BloxGuardianDataModelLibrary.PairingStatus.HalfOpenFromInGame;
                                                sqlCommandInsertInGameToAccountPairing.Parameters["@RecordAddedDateTime"].Value = processingRunDateTime;
                                                sqlCommandInsertInGameToAccountPairing.Parameters["@RecordLastUpdatedDateTime"].Value = processingRunDateTime;
                                                await sqlCommandInsertInGameToAccountPairing.ExecuteNonQueryAsync();

                                                requestPairingPayload.pairingStatus = (int)TequaCreek.BloxGuardianDataModelLibrary.PairingStatus.HalfOpenFromInGame;

                                            }       // (await sqlDataReaderGetInGameToAccountPairing.ReadAsync())

                                            // 5.)  Write return message to file on file system
                                            baseMessageFileName = PadZeroLeft(sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INTERNAL_ID), 10);
                                            baseFileDirectory = this.configuration["AppSettings:ToInGameFileStorePrefix"] + Path.DirectorySeparatorChar +
                                                                PadZeroLeft(sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INGAME_ENDPOINT_INTERNAL_ID), 6);
                                            Directory.CreateDirectory(baseFileDirectory);       // PROGRAMMER'S NOTE:  This function will return if directory already created

                                            fromBloxGuardianMessagePacket = new BloxChannelDotNetAPI.Models.BloxGuardian.FromBloxGuardianMessagePacket();

                                            fromBloxGuardianMessagePacket.respondingToMessageToBloxGuardianExternalId =
                                                sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_EXTERNAL_ID);
                                            fromBloxGuardianMessagePacket.responsePayloadType = (int)TequaCreek.BloxChannelDotNetAPI.ResponsePayloadType.RequestToPairToAccount;
                                            fromBloxGuardianMessagePacket.responsePayload = Base64Encode(JsonConvert.SerializeObject(requestPairingPayload));

                                            // Format return message as JSON object and save to file on filesystem
                                            await File.WriteAllTextAsync(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                                         JsonConvert.SerializeObject(fromBloxGuardianMessagePacket));
                                            File.Move(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                      baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_FILE_EXTENSION);

                                            // Update status to show that file with message was written
                                            sqlCommandUpdateMessageToBloxGuardian.Parameters["@ProcessingStatus"].Value = TequaCreek.BloxGuardianDataModelLibrary.MessageProcessingStatus.QueuedForMessagingSubsystem;
                                            sqlCommandUpdateMessageToBloxGuardian.Parameters["@MessageToBloxGuardianInternalID"].Value =
                                                sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INTERNAL_ID);
                                            await sqlCommandUpdateMessageToBloxGuardian.ExecuteNonQueryAsync();

                                            break;

                                        case TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType.GetPairedAccount:

                                            // 1.)  Check for existing completed pairing.  If found, return formatted message
                                            //      with BloxGuardian Account External ID and Account Holder Name

                                            TequaCreek.BloxChannelDotNetAPI.Models.BloxGuardian.GetPairingPayload getPairingPayload = new BloxChannelDotNetAPI.Models.BloxGuardian.GetPairingPayload();

                                            sqlCommandGetInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value =
                                                sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_ALLOWED_COMMUNICATION_PATH_INTERNAL_ID);
                                            sqlCommandGetInGameToAccountPairing.Parameters["@InGameUserId"].Value =
                                                sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INGAME_USER_ID);

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

                                            // 2.)  Write return message to file on file system
                                            baseMessageFileName = PadZeroLeft(sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INTERNAL_ID), 10);
                                            baseFileDirectory = this.configuration["AppSettings:ToInGameFileStorePrefix"] + Path.DirectorySeparatorChar +
                                                                PadZeroLeft(sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INGAME_ENDPOINT_INTERNAL_ID), 6);
                                            Directory.CreateDirectory(baseFileDirectory);       // PROGRAMMER'S NOTE:  This function will return if directory already created

                                            fromBloxGuardianMessagePacket = new BloxChannelDotNetAPI.Models.BloxGuardian.FromBloxGuardianMessagePacket();

                                            fromBloxGuardianMessagePacket.respondingToMessageToBloxGuardianExternalId =
                                                sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_EXTERNAL_ID);
                                            fromBloxGuardianMessagePacket.responsePayloadType = (int)TequaCreek.BloxChannelDotNetAPI.ResponsePayloadType.GetPairedAccount;
                                            fromBloxGuardianMessagePacket.responsePayload = Base64Encode(JsonConvert.SerializeObject(getPairingPayload));

                                            // Format return message as JSON object and save to file on filesystem
                                            await File.WriteAllTextAsync(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                                         JsonConvert.SerializeObject(fromBloxGuardianMessagePacket));
                                            File.Move(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                      baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_FILE_EXTENSION);

                                            // 3.)  Update status to show that file with message was written
                                            sqlCommandUpdateMessageToBloxGuardian.Parameters["@ProcessingStatus"].Value = TequaCreek.BloxGuardianDataModelLibrary.MessageProcessingStatus.QueuedForMessagingSubsystem;
                                            sqlCommandUpdateMessageToBloxGuardian.Parameters["@MessageToBloxGuardianInternalID"].Value =
                                                sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INTERNAL_ID);
                                            await sqlCommandUpdateMessageToBloxGuardian.ExecuteNonQueryAsync();

                                            break;

                                        case TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType.RemovePairedAccount:

                                            // PROGRAMMER'S NOTE:  Need to check for pending messages (in database) and remove them
                                            //                     Will not send push message to device requesting removal,
                                            //                     but instead will allow message to time out or notify device via
                                            //                     API return code if attempt to interact with a previously-removed
                                            //                     message (for security purposes)

                                            sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@UpdateProcessingStatus"].Value = 
                                                TequaCreek.BloxGuardianDataModelLibrary.MessageToDeviceProcessingStatus.Removed;
                                            sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@AllowedCommunicationPathInternalID"].Value =
                                              sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_ALLOWED_COMMUNICATION_PATH_INTERNAL_ID);
                                            sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@InGameUserId"].Value =
                                              sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INGAME_USER_ID);
                                            sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@ProcessingStatus"].Value = 
                                                TequaCreek.BloxGuardianDataModelLibrary.MessageToDeviceProcessingStatus.PushedToDevice;
                                            sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@MessageSource"].Value =
                                                TequaCreek.BloxGuardianDataModelLibrary.MessageSource.FromInGame;
                                            await sqlCommandRemovePendingMessagesForPairedAccount.ExecuteNonQueryAsync();

                                            // 1.)  Issue a delete command and return succes, regardless of whether a pairing record
                                            //      was found
                                            sqlCommandDeleteInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value =
                                              sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_ALLOWED_COMMUNICATION_PATH_INTERNAL_ID);
                                            sqlCommandDeleteInGameToAccountPairing.Parameters["@InGameUserId"].Value =
                                              sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INGAME_USER_ID);

                                            await sqlCommandDeleteInGameToAccountPairing.ExecuteNonQueryAsync();

                                            // 2.)  Write return message to file on file system
                                            baseMessageFileName = PadZeroLeft(sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INTERNAL_ID), 10);
                                            baseFileDirectory = this.configuration["AppSettings:ToInGameFileStorePrefix"] + Path.DirectorySeparatorChar +
                                                                PadZeroLeft(sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INGAME_ENDPOINT_INTERNAL_ID), 6);
                                            Directory.CreateDirectory(baseFileDirectory);       // PROGRAMMER'S NOTE:  This function will return if directory already created

                                            fromBloxGuardianMessagePacket = new BloxChannelDotNetAPI.Models.BloxGuardian.FromBloxGuardianMessagePacket();

                                            fromBloxGuardianMessagePacket.respondingToMessageToBloxGuardianExternalId =
                                                sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_EXTERNAL_ID);
                                            fromBloxGuardianMessagePacket.responsePayloadType = (int)TequaCreek.BloxChannelDotNetAPI.ResponsePayloadType.RemovePairedAccount;

                                            // Format return message as JSON object and save to file on filesystem
                                            await File.WriteAllTextAsync(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                                         JsonConvert.SerializeObject(fromBloxGuardianMessagePacket));
                                            File.Move(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                      baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_FILE_EXTENSION);

                                            // 3.)  Update status to show that file with message was written
                                            sqlCommandUpdateMessageToBloxGuardian.Parameters["@ProcessingStatus"].Value = TequaCreek.BloxGuardianDataModelLibrary.MessageProcessingStatus.QueuedForMessagingSubsystem;
                                            sqlCommandUpdateMessageToBloxGuardian.Parameters["@MessageToBloxGuardianInternalID"].Value =
                                                sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INTERNAL_ID);
                                            await sqlCommandUpdateMessageToBloxGuardian.ExecuteNonQueryAsync();

                                            break;

                                        case TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType.GetPendingMessagesForAccount:

                                            List<TequaCreek.BloxChannelDotNetAPI.Models.BloxGuardian.PendingMessagePayloadItem> messageList =
                                                new List<BloxChannelDotNetAPI.Models.BloxGuardian.PendingMessagePayloadItem>();

                                            // 1.)  Get pending messages from database table for Paired Account

                                            sqlCommandGetPendingMessagesForPairedAccount.Parameters["@AllowedCommunicationPathInternalID"].Value =
                                              sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_ALLOWED_COMMUNICATION_PATH_INTERNAL_ID);
                                            sqlCommandGetPendingMessagesForPairedAccount.Parameters["@InGameUserId"].Value =
                                              sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INGAME_USER_ID);
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
                                            baseMessageFileName = PadZeroLeft(sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INTERNAL_ID), 10);
                                            baseFileDirectory = this.configuration["AppSettings:ToInGameFileStorePrefix"] + Path.DirectorySeparatorChar +
                                                                PadZeroLeft(sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INGAME_ENDPOINT_INTERNAL_ID), 6);
                                            Directory.CreateDirectory(baseFileDirectory);       // PROGRAMMER'S NOTE:  This function will return if directory already created

                                            fromBloxGuardianMessagePacket = new BloxChannelDotNetAPI.Models.BloxGuardian.FromBloxGuardianMessagePacket();

                                            fromBloxGuardianMessagePacket.respondingToMessageToBloxGuardianExternalId =
                                                sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_EXTERNAL_ID);
                                            fromBloxGuardianMessagePacket.responsePayloadType = (int)TequaCreek.BloxChannelDotNetAPI.ResponsePayloadType.GetPendingMessagesForAccount;
                                            fromBloxGuardianMessagePacket.responsePayload = Base64Encode(JsonConvert.SerializeObject(messageList));

                                            // Format return message as JSON object and save to file on filesystem
                                            await File.WriteAllTextAsync(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                                         JsonConvert.SerializeObject(fromBloxGuardianMessagePacket));
                                            File.Move(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                      baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_FILE_EXTENSION);

                                            // 3.)  Update status to show that file with message was written
                                            sqlCommandUpdateMessageToBloxGuardian.Parameters["@ProcessingStatus"].Value = TequaCreek.BloxGuardianDataModelLibrary.MessageProcessingStatus.QueuedForMessagingSubsystem;
                                            sqlCommandUpdateMessageToBloxGuardian.Parameters["@MessageToBloxGuardianInternalID"].Value =
                                                sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INTERNAL_ID);
                                            await sqlCommandUpdateMessageToBloxGuardian.ExecuteNonQueryAsync();

                                            break;

                                        case TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType.RemovePendingMessagesForAccount:

                                            // 1.)  Set status for pending messages for Account to "removed"
                                            sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@UpdateProcessingStatus"].Value =
                                                TequaCreek.BloxGuardianDataModelLibrary.MessageToDeviceProcessingStatus.Removed;
                                            sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@AllowedCommunicationPathInternalID"].Value =
                                              sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_ALLOWED_COMMUNICATION_PATH_INTERNAL_ID);
                                            sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@InGameUserId"].Value =
                                              sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INGAME_USER_ID);
                                            sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@ProcessingStatus"].Value =
                                                TequaCreek.BloxGuardianDataModelLibrary.MessageToDeviceProcessingStatus.PushedToDevice;
                                            sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@MessageSource"].Value =
                                                TequaCreek.BloxGuardianDataModelLibrary.MessageSource.FromInGame;
                                            await sqlCommandRemovePendingMessagesForPairedAccount.ExecuteNonQueryAsync();

                                            // 2.)  Write return message to file on file system
                                            baseMessageFileName = PadZeroLeft(sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INTERNAL_ID), 10);
                                            baseFileDirectory = this.configuration["AppSettings:ToInGameFileStorePrefix"] + Path.DirectorySeparatorChar +
                                                                PadZeroLeft(sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INGAME_ENDPOINT_INTERNAL_ID), 6);
                                            Directory.CreateDirectory(baseFileDirectory);       // PROGRAMMER'S NOTE:  This function will return if directory already created

                                            fromBloxGuardianMessagePacket = new BloxChannelDotNetAPI.Models.BloxGuardian.FromBloxGuardianMessagePacket();

                                            fromBloxGuardianMessagePacket.respondingToMessageToBloxGuardianExternalId =
                                                sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_EXTERNAL_ID);
                                            fromBloxGuardianMessagePacket.responsePayloadType = (int)TequaCreek.BloxChannelDotNetAPI.ResponsePayloadType.RemovePendingMessagesForAccount;

                                            // Format return message as JSON object and save to file on filesystem
                                            await File.WriteAllTextAsync(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                                         JsonConvert.SerializeObject(fromBloxGuardianMessagePacket));
                                            File.Move(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                      baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_FILE_EXTENSION);

                                            // 3.)  Update status to show that file with message was written
                                            sqlCommandUpdateMessageToBloxGuardian.Parameters["@ProcessingStatus"].Value = TequaCreek.BloxGuardianDataModelLibrary.MessageProcessingStatus.QueuedForMessagingSubsystem;
                                            sqlCommandUpdateMessageToBloxGuardian.Parameters["@MessageToBloxGuardianInternalID"].Value =
                                                sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INTERNAL_ID);
                                            await sqlCommandUpdateMessageToBloxGuardian.ExecuteNonQueryAsync();

                                            break;

                                        case TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType.SendMessageToMobileDevice:

                                            // 1.)  Obtain paired Account for specified Allowed Communication Path and In-Game User ID
                                            sqlCommandGetInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value =
                                                sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_ALLOWED_COMMUNICATION_PATH_INTERNAL_ID);
                                            sqlCommandGetInGameToAccountPairing.Parameters["@InGameUserId"].Value =
                                                sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INGAME_USER_ID);

                                            sqlDataReaderGetInGameToAccountPairing = await sqlCommandGetInGameToAccountPairing.ExecuteReaderAsync();
                                            if (await sqlDataReaderGetInGameToAccountPairing.ReadAsync())
                                            {
                                                if (sqlDataReaderGetInGameToAccountPairing.GetInt32(ApplicationValues.INGAME_USER_ID_BG_ACCOUNT_PAIRING_QUERY_RESULT_COLUMN_OFFSET_PAIRING_STATUS) ==
                                                    (int)TequaCreek.BloxGuardianDataModelLibrary.PairingStatus.CompletedPairing)
                                                {
                                                    // 2.)  Write new record to Pending Messages to Mobile Device table
                                                    sqlCommandInsertPendingMessageForPairedAccount.Parameters["@InGameUserBGAccountPairingInternalID"].Value =
                                                        sqlDataReaderGetInGameToAccountPairing.GetInt32(ApplicationValues.INGAME_USER_ID_BG_ACCOUNT_PAIRING_QUERY_RESULT_COLUMN_OFFSET_PAIRING_INTERNAL_ID);
                                                    sqlCommandInsertPendingMessageForPairedAccount.Parameters["@MessageToBloxGuardianInternalID"].Value =
                                                        sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INTERNAL_ID);
                                                    sqlCommandInsertPendingMessageForPairedAccount.Parameters["@MessagePushedDateTime"].Value = processingRunDateTime;
                                                    sqlCommandInsertPendingMessageForPairedAccount.Parameters["@ProcessingStatus"].Value = 
                                                        TequaCreek.BloxGuardianDataModelLibrary.MessageToDeviceProcessingStatus.New;
                                                    await sqlCommandInsertPendingMessageForPairedAccount.ExecuteNonQueryAsync();



                                                    // 3.)  Push message to Mobile Device for Account




                                                }
                                                else
                                                {
                                                    // PROGRAMMER'S NOTE:  Need to handle invalid pairing condition
                                                }
                                            }
                                            else
                                            {
                                                // PROGRAMMER'S NOTE:  Need to handle invalid pairing condition
                                            }

                                            // 4.)  Write return message to file on file system
                                            baseMessageFileName = PadZeroLeft(sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INTERNAL_ID), 10);
                                            baseFileDirectory = this.configuration["AppSettings:ToInGameFileStorePrefix"] + Path.DirectorySeparatorChar +
                                                                PadZeroLeft(sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INGAME_ENDPOINT_INTERNAL_ID), 6);
                                            Directory.CreateDirectory(baseFileDirectory);       // PROGRAMMER'S NOTE:  This function will return if directory already created

                                            fromBloxGuardianMessagePacket = new BloxChannelDotNetAPI.Models.BloxGuardian.FromBloxGuardianMessagePacket();

                                            fromBloxGuardianMessagePacket.respondingToMessageToBloxGuardianExternalId =
                                                sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_EXTERNAL_ID);
                                            fromBloxGuardianMessagePacket.responsePayloadType = (int)TequaCreek.BloxChannelDotNetAPI.ResponsePayloadType.SendMessageToBloxGuardian;

                                            // Format return message as JSON object and save to file on filesystem
                                            await File.WriteAllTextAsync(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                                         JsonConvert.SerializeObject(fromBloxGuardianMessagePacket));
                                            File.Move(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                      baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_FILE_EXTENSION);

                                            // 3.)  Update status to show that file with message was written
                                            sqlCommandUpdateMessageToBloxGuardian.Parameters["@ProcessingStatus"].Value = TequaCreek.BloxGuardianDataModelLibrary.MessageProcessingStatus.QueuedForMessagingSubsystem;
                                            sqlCommandUpdateMessageToBloxGuardian.Parameters["@MessageToBloxGuardianInternalID"].Value =
                                                sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INTERNAL_ID);
                                            await sqlCommandUpdateMessageToBloxGuardian.ExecuteNonQueryAsync();

                                            break;

                                    }

                                }
                                else
                                {
                                    // PROGRAMMER'S NOTE:  Need to log database integrity error and fault message
                                }
                                await sqlDataReaderGetMessageToBloxGuardian.CloseAsync();

                                break;

                            case TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.MESSAGE_TYPE_ID_EXTERNAL_TO_BLOXGUARDIAN:

                                sqlCommandGetMessageToBloxGuardian.Parameters["@MessageToBloxGuardianInternalID"].Value = messageToBloxGuardianInternalId;
                                sqlDataReaderGetMessageToBloxGuardian = await sqlCommandGetMessageToBloxGuardian.ExecuteReaderAsync();
                                if (await sqlDataReaderGetMessageToBloxGuardian.ReadAsync())
                                {

                                    switch ((TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType)
                                             sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_MESSAGE_ORIGINATION_TYPE_CODE))
                                    {

                                        case TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType.GetPairedAccount:

                                            // 1.)  Check for existing completed pairing.  If found, return formatted message
                                            //      with BloxGuardian Account External ID and Account Holder Name

                                            TequaCreek.BloxChannelDotNetAPI.Models.BloxGuardian.GetPairingPayload getPairingPayload = new BloxChannelDotNetAPI.Models.BloxGuardian.GetPairingPayload();

                                            sqlCommandGetInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value =
                                                sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_ALLOWED_COMMUNICATION_PATH_INTERNAL_ID);
                                            sqlCommandGetInGameToAccountPairing.Parameters["@InGameUserId"].Value =
                                                sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INGAME_USER_ID);
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

                                            // 2.)  Write return message to file on file system
                                            baseMessageFileName = PadZeroLeft(sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INTERNAL_ID), 10);
                                            baseFileDirectory = this.configuration["AppSettings:ToExternalFileStorePrefix"] + Path.DirectorySeparatorChar +
                                                                PadZeroLeft(sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_EXTERNAL_ENDPOINT_INTERNAL_ID), 6);
                                            Directory.CreateDirectory(baseFileDirectory);       // PROGRAMMER'S NOTE:  This function will return if directory already created

                                            fromBloxGuardianMessagePacket = new BloxChannelDotNetAPI.Models.BloxGuardian.FromBloxGuardianMessagePacket();

                                            fromBloxGuardianMessagePacket.respondingToMessageToBloxGuardianExternalId =
                                                sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_EXTERNAL_ID);
                                            fromBloxGuardianMessagePacket.responsePayloadType = (int)TequaCreek.BloxChannelDotNetAPI.ResponsePayloadType.GetPairedAccount;
                                            fromBloxGuardianMessagePacket.responsePayload = Base64Encode(JsonConvert.SerializeObject(getPairingPayload));

                                            // Format return message as JSON object and save to file on filesystem
                                            await File.WriteAllTextAsync(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                                         JsonConvert.SerializeObject(fromBloxGuardianMessagePacket));
                                            File.Move(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                      baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_FILE_EXTENSION);

                                            // 3.)  Update status to show that file with message was written
                                            sqlCommandUpdateMessageToBloxGuardian.Parameters["@ProcessingStatus"].Value = TequaCreek.BloxGuardianDataModelLibrary.MessageProcessingStatus.QueuedForMessagingSubsystem;
                                            sqlCommandUpdateMessageToBloxGuardian.Parameters["@MessageToBloxGuardianInternalID"].Value =
                                                sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INTERNAL_ID);
                                            await sqlCommandUpdateMessageToBloxGuardian.ExecuteNonQueryAsync();

                                            break;

                                        case TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType.RemovePairedAccount:

                                            // PROGRAMMER'S NOTE:  Need to check for pending messages (in database) and remove them
                                            //                     Will not send push message to device requesting removal,
                                            //                     but instead will allow message to time out or notify device via
                                            //                     API return code if attempt to interact with a previously-removed
                                            //                     message (for security purposes)

                                            sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@UpdateProcessingStatus"].Value =
                                                TequaCreek.BloxGuardianDataModelLibrary.MessageToDeviceProcessingStatus.Removed;
                                            sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@AllowedCommunicationPathInternalID"].Value =
                                              sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_ALLOWED_COMMUNICATION_PATH_INTERNAL_ID);
                                            sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@InGameUserId"].Value =
                                              sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INGAME_USER_ID);
                                            sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@ProcessingStatus"].Value =
                                                TequaCreek.BloxGuardianDataModelLibrary.MessageToDeviceProcessingStatus.PushedToDevice;
                                            sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@MessageSource"].Value =
                                                TequaCreek.BloxGuardianDataModelLibrary.MessageSource.FromExternal;
                                            await sqlCommandRemovePendingMessagesForPairedAccount.ExecuteNonQueryAsync();

                                            // 1.)  Issue a delete command and return succes, regardless of whether a pairing record
                                            //      was found
                                            sqlCommandDeleteInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value =
                                              sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_ALLOWED_COMMUNICATION_PATH_INTERNAL_ID);
                                            sqlCommandDeleteInGameToAccountPairing.Parameters["@InGameUserId"].Value =
                                              sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INGAME_USER_ID);

                                            await sqlCommandDeleteInGameToAccountPairing.ExecuteNonQueryAsync();

                                            // 2.)  Write return message to file on file system
                                            baseMessageFileName = PadZeroLeft(sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INTERNAL_ID), 10);
                                            baseFileDirectory = this.configuration["AppSettings:ToExternalFileStorePrefix"] + Path.DirectorySeparatorChar +
                                                                PadZeroLeft(sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_EXTERNAL_ENDPOINT_INTERNAL_ID), 6);
                                            Directory.CreateDirectory(baseFileDirectory);       // PROGRAMMER'S NOTE:  This function will return if directory already created

                                            fromBloxGuardianMessagePacket = new BloxChannelDotNetAPI.Models.BloxGuardian.FromBloxGuardianMessagePacket();

                                            fromBloxGuardianMessagePacket.respondingToMessageToBloxGuardianExternalId =
                                                sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_EXTERNAL_ID);
                                            fromBloxGuardianMessagePacket.responsePayloadType = (int)TequaCreek.BloxChannelDotNetAPI.ResponsePayloadType.RemovePairedAccount;

                                            // Format return message as JSON object and save to file on filesystem
                                            await File.WriteAllTextAsync(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                                         JsonConvert.SerializeObject(fromBloxGuardianMessagePacket));
                                            File.Move(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                      baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_FILE_EXTENSION);

                                            // 3.)  Update status to show that file with message was written
                                            sqlCommandUpdateMessageToBloxGuardian.Parameters["@ProcessingStatus"].Value = TequaCreek.BloxGuardianDataModelLibrary.MessageProcessingStatus.QueuedForMessagingSubsystem;
                                            sqlCommandUpdateMessageToBloxGuardian.Parameters["@MessageToBloxGuardianInternalID"].Value =
                                                sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INTERNAL_ID);
                                            await sqlCommandUpdateMessageToBloxGuardian.ExecuteNonQueryAsync();

                                            break;

                                        case TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType.GetPendingMessagesForAccount:

                                            List<TequaCreek.BloxChannelDotNetAPI.Models.BloxGuardian.PendingMessagePayloadItem> messageList =
                                                new List<BloxChannelDotNetAPI.Models.BloxGuardian.PendingMessagePayloadItem>();

                                            // 1.)  Get pending messages for Account from database table

                                            sqlCommandGetPendingMessagesForPairedAccount.Parameters["@AllowedCommunicationPathInternalID"].Value =
                                              sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_ALLOWED_COMMUNICATION_PATH_INTERNAL_ID);
                                            sqlCommandGetPendingMessagesForPairedAccount.Parameters["@InGameUserId"].Value =
                                              sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INGAME_USER_ID);
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
                                            baseMessageFileName = PadZeroLeft(sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INTERNAL_ID), 10);
                                            baseFileDirectory = this.configuration["AppSettings:ToExternalFileStorePrefix"] + Path.DirectorySeparatorChar +
                                                                PadZeroLeft(sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_EXTERNAL_ENDPOINT_INTERNAL_ID), 6);
                                            Directory.CreateDirectory(baseFileDirectory);       // PROGRAMMER'S NOTE:  This function will return if directory already created

                                            fromBloxGuardianMessagePacket = new BloxChannelDotNetAPI.Models.BloxGuardian.FromBloxGuardianMessagePacket();

                                            fromBloxGuardianMessagePacket.respondingToMessageToBloxGuardianExternalId =
                                                sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_EXTERNAL_ID);
                                            fromBloxGuardianMessagePacket.responsePayloadType = (int)TequaCreek.BloxChannelDotNetAPI.ResponsePayloadType.GetPendingMessagesForAccount;
                                            fromBloxGuardianMessagePacket.responsePayload = Base64Encode(JsonConvert.SerializeObject(messageList));

                                            // Format return message as JSON object and save to file on filesystem
                                            await File.WriteAllTextAsync(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                                         JsonConvert.SerializeObject(fromBloxGuardianMessagePacket));
                                            File.Move(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                      baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_FILE_EXTENSION);

                                            // 3.)  Update status to show that file with message was written
                                            sqlCommandUpdateMessageToBloxGuardian.Parameters["@ProcessingStatus"].Value = TequaCreek.BloxGuardianDataModelLibrary.MessageProcessingStatus.QueuedForMessagingSubsystem;
                                            sqlCommandUpdateMessageToBloxGuardian.Parameters["@MessageToBloxGuardianInternalID"].Value =
                                                sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INTERNAL_ID);
                                            await sqlCommandUpdateMessageToBloxGuardian.ExecuteNonQueryAsync();

                                            break;

                                        case TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType.RemovePendingMessagesForAccount:

                                            // 1.)  Set status for pending messages for Account to "removed"
                                            sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@UpdateProcessingStatus"].Value =
                                                TequaCreek.BloxGuardianDataModelLibrary.MessageToDeviceProcessingStatus.Removed;
                                            sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@AllowedCommunicationPathInternalID"].Value =
                                              sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_ALLOWED_COMMUNICATION_PATH_INTERNAL_ID);
                                            sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@InGameUserId"].Value =
                                              sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INGAME_USER_ID);
                                            sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@ProcessingStatus"].Value =
                                                TequaCreek.BloxGuardianDataModelLibrary.MessageToDeviceProcessingStatus.PushedToDevice;
                                            sqlCommandRemovePendingMessagesForPairedAccount.Parameters["@MessageSource"].Value =
                                                TequaCreek.BloxGuardianDataModelLibrary.MessageSource.FromExternal;
                                            await sqlCommandRemovePendingMessagesForPairedAccount.ExecuteNonQueryAsync();

                                            // 2.)  Write return message to file on file system
                                            baseMessageFileName = PadZeroLeft(sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INTERNAL_ID), 10);
                                            baseFileDirectory = this.configuration["AppSettings:ToExternalFileStorePrefix"] + Path.DirectorySeparatorChar +
                                                                PadZeroLeft(sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_EXTERNAL_ENDPOINT_INTERNAL_ID), 6);
                                            Directory.CreateDirectory(baseFileDirectory);       // PROGRAMMER'S NOTE:  This function will return if directory already created

                                            fromBloxGuardianMessagePacket = new BloxChannelDotNetAPI.Models.BloxGuardian.FromBloxGuardianMessagePacket();

                                            fromBloxGuardianMessagePacket.respondingToMessageToBloxGuardianExternalId =
                                                sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_EXTERNAL_ID);
                                            fromBloxGuardianMessagePacket.responsePayloadType = (int)TequaCreek.BloxChannelDotNetAPI.ResponsePayloadType.RemovePendingMessagesForAccount;

                                            // Format return message as JSON object and save to file on filesystem
                                            await File.WriteAllTextAsync(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                                         JsonConvert.SerializeObject(fromBloxGuardianMessagePacket));
                                            File.Move(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                      baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_FILE_EXTENSION);

                                            // 3.)  Update status to show that file with message was written
                                            sqlCommandUpdateMessageToBloxGuardian.Parameters["@ProcessingStatus"].Value = TequaCreek.BloxGuardianDataModelLibrary.MessageProcessingStatus.QueuedForMessagingSubsystem;
                                            sqlCommandUpdateMessageToBloxGuardian.Parameters["@MessageToBloxGuardianInternalID"].Value =
                                                sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INTERNAL_ID);
                                            await sqlCommandUpdateMessageToBloxGuardian.ExecuteNonQueryAsync();

                                            break;

                                        case TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType.SendMessageToMobileDevice:

                                            // 1.)  Obtain paired Account for specified Allowed Communication Path and In-Game User ID
                                            sqlCommandGetInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value =
                                                sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_ALLOWED_COMMUNICATION_PATH_INTERNAL_ID);
                                            sqlCommandGetInGameToAccountPairing.Parameters["@InGameUserId"].Value =
                                                sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INGAME_USER_ID);

                                            sqlDataReaderGetInGameToAccountPairing = await sqlCommandGetInGameToAccountPairing.ExecuteReaderAsync();
                                            if (await sqlDataReaderGetInGameToAccountPairing.ReadAsync())
                                            {
                                                if (sqlDataReaderGetInGameToAccountPairing.GetInt32(ApplicationValues.INGAME_USER_ID_BG_ACCOUNT_PAIRING_QUERY_RESULT_COLUMN_OFFSET_PAIRING_STATUS) ==
                                                    (int)TequaCreek.BloxGuardianDataModelLibrary.PairingStatus.CompletedPairing)
                                                {
                                                    // 2.)  Write new record to Pending Messages to Mobile Device table
                                                    sqlCommandInsertPendingMessageForPairedAccount.Parameters["@InGameUserBGAccountPairingInternalID"].Value =
                                                        sqlDataReaderGetInGameToAccountPairing.GetInt32(ApplicationValues.INGAME_USER_ID_BG_ACCOUNT_PAIRING_QUERY_RESULT_COLUMN_OFFSET_PAIRING_INTERNAL_ID);
                                                    sqlCommandInsertPendingMessageForPairedAccount.Parameters["@MessageToBloxGuardianInternalID"].Value =
                                                        sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INTERNAL_ID);
                                                    sqlCommandInsertPendingMessageForPairedAccount.Parameters["@MessagePushedDateTime"].Value = processingRunDateTime;
                                                    sqlCommandInsertPendingMessageForPairedAccount.Parameters["@ProcessingStatus"].Value =
                                                        TequaCreek.BloxGuardianDataModelLibrary.MessageToDeviceProcessingStatus.New;
                                                    await sqlCommandInsertPendingMessageForPairedAccount.ExecuteNonQueryAsync();



                                                    // 3.)  Push message to Mobile Device for Account




                                                }
                                                else
                                                {
                                                    // PROGRAMMER'S NOTE:  Need to handle invalid pairing condition
                                                }
                                            }
                                            else
                                            {
                                                // PROGRAMMER'S NOTE:  Need to handle invalid pairing condition
                                            }

                                            // 4.)  Write return message to file on file system
                                            baseMessageFileName = PadZeroLeft(sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INTERNAL_ID), 10);
                                            baseFileDirectory = this.configuration["AppSettings:ToExternalFileStorePrefix"] + Path.DirectorySeparatorChar +
                                                                PadZeroLeft(sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_EXTERNAL_ENDPOINT_INTERNAL_ID), 6);
                                            Directory.CreateDirectory(baseFileDirectory);       // PROGRAMMER'S NOTE:  This function will return if directory already created

                                            fromBloxGuardianMessagePacket = new BloxChannelDotNetAPI.Models.BloxGuardian.FromBloxGuardianMessagePacket();

                                            fromBloxGuardianMessagePacket.respondingToMessageToBloxGuardianExternalId =
                                                sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_EXTERNAL_ID);
                                            fromBloxGuardianMessagePacket.responsePayloadType = (int)TequaCreek.BloxChannelDotNetAPI.ResponsePayloadType.SendMessageToBloxGuardian;

                                            // Format return message as JSON object and save to file on filesystem
                                            await File.WriteAllTextAsync(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                                         JsonConvert.SerializeObject(fromBloxGuardianMessagePacket));
                                            File.Move(baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_TEMPORARY_FILE_EXTENSION,
                                                      baseFileDirectory + Path.DirectorySeparatorChar + baseMessageFileName + "." + TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.BLOXGUARDIAN_MESSAGE_FILE_EXTENSION);

                                            // 3.)  Update status to show that file with message was written
                                            sqlCommandUpdateMessageToBloxGuardian.Parameters["@ProcessingStatus"].Value = TequaCreek.BloxGuardianDataModelLibrary.MessageProcessingStatus.QueuedForMessagingSubsystem;
                                            sqlCommandUpdateMessageToBloxGuardian.Parameters["@MessageToBloxGuardianInternalID"].Value =
                                                sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MSG_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INTERNAL_ID);
                                            await sqlCommandUpdateMessageToBloxGuardian.ExecuteNonQueryAsync();

                                            break;
                                    }

                                }
                                else
                                {
                                    // PROGRAMMER'S NOTE:  Need to log database integrity error and fault message
                                }
                                await sqlDataReaderGetMessageToBloxGuardian.CloseAsync();

                                break;

                            case TequaCreek.BloxGuardianDataModelLibrary.SharedConstantValues.MESSAGE_TYPE_ID_BG_WEB_SERVICE_TO_BLOXGUARDIAN:





                                break;

                        }



                        await sqlConnection2.CloseAsync();
                    }       // using (sqlConnection2 = new SqlConnection(configuration["ConnectionStrings:BloxChannel"].ToString()))

                    await sqlConnection1.CloseAsync();
                }       // using (sqlConnection2 = new NpgsqlConnection(configuration["ConnectionStrings:BloxChannel"]))

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
