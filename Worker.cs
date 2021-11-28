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
                timer = new Timer(async o => {await PerformTimedWorkActivities(null); }, null, TimeSpan.Zero, 
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


            NpgsqlConnection sqlConnection1;
            NpgsqlConnection sqlConnection2;
            NpgsqlCommand sqlCommandGetMessageToBloxGuardian;
            NpgsqlDataReader sqlDataReaderGetMessageToBloxGuardian;
            NpgsqlCommand sqlCommandGetInGameToAccountPairing;
            NpgsqlDataReader sqlDataReaderGetGetInGameToAccountPairing;
            NpgsqlCommand sqlCommandInsertInGameToAccountPairing;
            NpgsqlCommand sqlCommandDeleteInGameToAccountPairing;

            // TEMP CODE
            logger.LogDebug("Entering Worker:MessageReceivedAsync()");
            // END TEMP CODE

            try
            {

                using (sqlConnection1 = new NpgsqlConnection(configuration["ConnectionStrings:BloxChannel"]))
                {
                    await sqlConnection1.OpenAsync();

                    using (sqlConnection2 = new NpgsqlConnection(configuration["ConnectionStrings:BloxChannel"]))
                    {
                        await sqlConnection2.OpenAsync();

                        sqlStatement = new System.Text.StringBuilder();
                        sqlStatement.Append("SELECT MSG.message_origination_type_code, MSG.payload, MSG.allowed_communication_path_internal_id, ");
                        sqlStatement.Append("       MSG.ingame_user_id ");
                        sqlStatement.Append("  FROM message_to_bloxguardian MSG ");
                        sqlStatement.Append("  WHERE MSG.message_to_bloxguardian_internal_id = @MessageToBloxGuardianInternalID ");

                        sqlCommandGetMessageToBloxGuardian = sqlConnection1.CreateCommand();
                        sqlCommandGetMessageToBloxGuardian.CommandText = sqlStatement.ToString();
                        sqlCommandGetMessageToBloxGuardian.CommandTimeout = 600;
                        sqlCommandGetMessageToBloxGuardian.Parameters.Add(new NpgsqlParameter("@MessageToBloxGuardianInternalID", NpgsqlTypes.NpgsqlDbType.Integer));

                        sqlCommandGetMessageToBloxGuardian.Parameters["@MessageToBloxGuardianInternalID"].Value = 0;
                        await sqlCommandGetMessageToBloxGuardian.PrepareAsync();

                        sqlStatement = new System.Text.StringBuilder();
                        sqlStatement.Append("SELECT IGAP.bloxguardian_account_external_id, IGAP.account_holder_last_name, ");
                        sqlStatement.Append("       IGAP.account_holder_first_name, IGAP.pairing_status ");
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

                        /*
                         * CREATE TABLE IF NOT EXISTS public.ingame_user_bg_account_pairing
                        (
                            ingame_user_bg_account_pairing_internal_id integer NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
                            allowed_communication_path_internal_id integer NOT NULL,
                            ingame_user_id character varying(20) COLLATE pg_catalog."default",
                            bloxguardian_account_internal_id integer,
                            pairing_status integer NOT NULL,
                            record_added_date_time timestamp without time zone NOT NULL,
                            record_last_updated_date_time timestamp without time zone NOT NULL,
                            CONSTRAINT ingame_user_bg_account_pairing_pkey PRIMARY KEY (ingame_user_bg_account_pairing_internal_id)
                        )
                         * */


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
                                             sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MESSAGE_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_MESSAGE_ORIGINATION_TYPE_CODE))
                                    {
                                        case TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType.RequestToPairToAccount:

                                            TequaCreek.BloxChannelDotNetAPI.Models.BloxGuardian.RequestPairingPayload payload = new BloxChannelDotNetAPI.Models.BloxGuardian.RequestPairingPayload();

                                            sqlCommandGetInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value = 
                                                sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MESSAGE_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_ALLOWED_COMMUNICATION_PATH_INTERNAL_ID);
                                            sqlCommandGetInGameToAccountPairing.Parameters["@InGameUserId"].Value = 
                                                sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MESSAGE_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INGAME_USER_ID);
                                            sqlDataReaderGetGetInGameToAccountPairing = await sqlCommandGetInGameToAccountPairing.ExecuteReaderAsync();
                                            if (await sqlDataReaderGetGetInGameToAccountPairing.ReadAsync())
                                            {

                                                switch ((TequaCreek.BloxGuardianDataModelLibrary.PairingStatus)
                                                         sqlDataReaderGetGetInGameToAccountPairing.GetInt32(ApplicationValues.INGAME_USER_ID_BG_ACCOUNT_PAIRING_QUERY_RESULT_COLUMN_OFFSET_PAIRING_STATUS))
                                                {

                                                    // 1.)  Check for existing completed pairing.  If found, return error that
                                                    //      pairing already in place.  If found, return formatted message
                                                    //      with BloxGuardian Account External ID and Account Holder Name
                                                    case TequaCreek.BloxGuardianDataModelLibrary.PairingStatus.CompletedPairing:
                                                        payload.pairingStatus = (int)TequaCreek.BloxGuardianDataModelLibrary.PairingStatus.PairingAlreadyExists;
                                                        break;

                                                    // 2.)  If prior half-open pairing (from In-Game) exists, remove it and set
                                                    //      up new half-pairing
                                                    case TequaCreek.BloxGuardianDataModelLibrary.PairingStatus.HalfOpenFromInGame:

                                                        // Delete existing pairing
                                                        sqlCommandDeleteInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value =
                                                          sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MESSAGE_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_ALLOWED_COMMUNICATION_PATH_INTERNAL_ID);
                                                        sqlCommandDeleteInGameToAccountPairing.Parameters["@InGameUserId"].Value = 
                                                          sqlDataReaderGetMessageToBloxGuardian.GetString(ApplicationValues.MESSAGE_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_INGAME_USER_ID);

                                                        // Add new half-open pairing




                                                        sqlCommandInsertInGameToAccountPairing.Parameters["@AllowedCommunicationPathInternalID"].Value = "";
                                                        sqlCommandInsertInGameToAccountPairing.Parameters["@InGameUserId"].Value = "";
                                                        sqlCommandInsertInGameToAccountPairing.Parameters["@BloxGuardianAccountInternalID"].Value = 0;
                                                        sqlCommandInsertInGameToAccountPairing.Parameters["@PairingStatus"].Value = 0;
                                                        sqlCommandInsertInGameToAccountPairing.Parameters["@RecordAddedDateTime"].Value = DateTime.MinValue;
                                                        sqlCommandInsertInGameToAccountPairing.Parameters["@RecordLastUpdatedDateTime"].Value = DateTime.MinValue;




                                                        payload.pairingStatus = (int)TequaCreek.BloxGuardianDataModelLibrary.PairingStatus.HalfOpenFromInGame;



                                                        break;

                                                    // 3.)  Check for half-open pairing (from Mobile Device) that references Allowed Comm Path
                                                    //      plus In-Game User ID and complete pairing.  PROGRAMMER'S NOTE:  In theory this 
                                                    //      could happen if the in-game pairing request message arrived after an immediate
                                                    //      action on the mobile device to approve the pairing.  If found, return formatted
                                                    //      message with BloxGuardian Account External ID and Account Holder Name
                                                    case TequaCreek.BloxGuardianDataModelLibrary.PairingStatus.HalfOpenFromMobileDevice:

                                                        /*
                                                        public class RequestPairingPayload
                                                        {
                                                            public int pairingStatus { get; set; }
                                                            public string bloxGuardianAccountId { get; set; }
                                                            public string accountHolderLastName { get; set; }
                                                            public string accountHolderFirstName { get; set; }
                                                        }
                                                         * */

                                                        // Complete half-open pairing from device



                                                        payload.pairingStatus = (int)TequaCreek.BloxGuardianDataModelLibrary.PairingStatus.CompletedPairing;

                                                        break;

                                                }

                                            }
                                            else
                                            {

                                                // 4.)  If passed all prior validations and not a completed pairing (Step 3),
                                                //      write a half-open pairing record to database table

                                                /*
                                                public class RequestPairingPayload
                                                {
                                                    public int pairingStatus { get; set; }
                                                    public string bloxGuardianAccountId { get; set; }
                                                    public string accountHolderLastName { get; set; }
                                                    public string accountHolderFirstName { get; set; }
                                                }
                                                 * */
                                                // Add new half-open pairing

                                                payload.pairingStatus = (int)TequaCreek.BloxGuardianDataModelLibrary.PairingStatus.HalfOpenFromInGame;

                                            }       // (await sqlDataReaderGetGetInGameToAccountPairing.ReadAsync())


                                            // 5.)  Write formatted return message to associated Message Broker file system

                                            break;

                                        case TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType.GetPairedAccount:

                                            // 1.)  Check for existing completed pairing.  If found, return formatted message
                                            //      with BloxGuardian Account External ID and Account Holder Name

                                            // 2.)  If not found, return null value in fields

                                            /*
                                            public class GetPairingPayload
                                            {
                                                public int pairingStatus { get; set; }
                                                public string bloxGuardianAccountId { get; set; }
                                                public string accountHolderLastName { get; set; }
                                                public string accountHolderFirstName { get; set; }
                                            }
                                             * */

                                            break;

                                        case TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType.RemovePairedAccount:

                                            // 1.)  Issue a delete command and return succes, regardless of whether a pairing record
                                            //      was found

                                            // NO PAYLOAD IS RETURNED

                                            break;

                                        case TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType.GetPendingMessagesForAccount:

                                            /*  array of zero or more
                                             *     public class PendingMessagePayloadItem
                                                    {
                                                        public string bloxGuardianMessageId { get; set; }
                                                        public string encodedMessagePayload { get; set; }
                                                    }

                                             * */


                                            break;

                                        case TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType.RemovePendingMessagesForAccount:

                                            // Requires call to mobile device and hold processing

                                            // NO PAYLOAD IS RETURNED

                                            break;

                                        case TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType.SendMessageToMobileDevice:

                                            // Requires call to mobile device and hold processing

                                            // NO PAYLOAD IS RETURNED

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
                                             sqlDataReaderGetMessageToBloxGuardian.GetInt32(ApplicationValues.MESSAGE_TO_BLOXGUARDIAN_QUERY_RESULT_COLUMN_OFFSET_MESSAGE_ORIGINATION_TYPE_CODE))
                                    {

                                        case TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType.GetPairedAccount:
                                            break;

                                        case TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType.RemovePairedAccount:
                                            break;

                                        case TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType.GetPendingMessagesForAccount:

                                            /*
                                             *     public class PendingMessagePayloadItem
                                                    {
                                                        public string bloxGuardianMessageId { get; set; }
                                                        public string encodedMessagePayload { get; set; }
                                                    }

                                             * */

                                            break;

                                        case TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType.RemovePendingMessagesForAccount:
                                            break;

                                        case TequaCreek.BloxGuardianDataModelLibrary.MessageOriginationType.SendMessageToMobileDevice:
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

    }
}
