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

        private RabbitMQ.Client.ConnectionFactory rmqToInGameConnectionFactory;
        private RabbitMQ.Client.IConnection rmqToInGameConnection;
        private RabbitMQ.Client.IModel rmqToInGameChannel;
        private RabbitMQ.Client.Events.AsyncEventingBasicConsumer rmqToInGameConsumer;
        private RabbitMQ.Client.ConnectionFactory rmqToExternalConnectionFactory;
        private RabbitMQ.Client.IConnection rmqToExternalConnection;
        private RabbitMQ.Client.IModel rmqToExternalChannel;
        private RabbitMQ.Client.Events.AsyncEventingBasicConsumer rmqToExternalConsumer;

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
            bool handleBloxChannelToBloxGuardian;
            bool handleBloxGuardianToBloxChannel;

            try
            {

                logger.LogInformation("Starting BloxGuardian Message Processing Service (Assigned Processing Server ID = {0})", this.processingServerId);
                logger.LogInformation("Assembly version {0}", Assembly.GetEntryAssembly().GetName().Version);
                logger.LogInformation("File version {0}", Assembly.GetEntryAssembly().GetCustomAttribute<AssemblyFileVersionAttribute>().Version);
                logger.LogInformation("Product version {0}", Assembly.GetEntryAssembly().GetCustomAttribute<AssemblyInformationalVersionAttribute>().InformationalVersion);

                handleBloxChannelToBloxGuardian = (configuration["AppSettings:HandleBloxChannelToBloxGuardian"] == "Y");
                handleBloxGuardianToBloxChannel = (configuration["AppSettings:HandleBloxGuardianToBloxChannel"] == "Y");

                // 1.)  Need to bind all active direct queues to message handler for this processing server ID
                logger.LogInformation("Setting up Production messaging queue handling");

                if (handleBloxChannelToBloxGuardian)
                {
                    logger.LogInformation("Opening production connection to To-BloxGuardian message queue host");
                    logger.LogInformation("  -- Host Name = {0}, Port = {1}, Virtual Host = {0}",
                                          configuration["AppSettings:ToBloxGuardianMessageQueueHostName"],
                                          configuration["AppSettings:ToBloxGuardianMessageQueuePort"],
                                          configuration["AppSettings:ToBloxGuardianMessageQueueVirtualHostName"]);
                    rmqToExternalConnectionFactory = new RabbitMQ.Client.ConnectionFactory()
                    {
                        HostName = configuration["AppSettings:ToBloxGuardianMessageQueueHostName"],
                        Port = int.Parse(configuration["AppSettings:ToBloxGuardianMessageQueuePort"]),
                        VirtualHost = configuration["AppSettings:ToBloxGuardianMessageQueueVirtualHostName"],
                        UserName = configuration["AppSettings:ToBloxGuardianMessageQueueUserName"],
                        Password = configuration["AppSettings:ToBloxGuardianMessageQueueUserPassword"],
                        DispatchConsumersAsync = true
                    };

                    rmqToExternalConnection = rmqToExternalConnectionFactory.CreateConnection();
                    rmqToExternalChannel = rmqToExternalConnection.CreateModel();

                    exchangeName = configuration["AppSettings:ToBloxGuardianMessageQueueExchangeName"];

                    // PROGRAMMER'S NOTE:  Best practice is to just declare definitions.  If Exchange and Queue already exist, the functions
                    //                     simply return without action.
                    rmqToExternalChannel.ExchangeDeclare(exchangeName, RabbitMQ.Client.ExchangeType.Direct, true, false, null);

                    ////////////////////////////////////////
                    /// To BloxGuardian messages channel ///
                    ////////////////////////////////////////

                    queueName = string.Format(TequaCreek.BloxChannelDataModelLibrary.SharedConstantValues.MESSAGE_QUEUE_NAME_PATTERN_BLOXCHANNEL_TO_BLOXGUARDIAN,
                                              this.processingServerId);
                    logger.LogInformation("Setting up To-BloxGuardian queue {0} definition (if not already exists)", queueName);

                    Dictionary<string, object> args = new Dictionary<string, object>();

                    // PROGRAMMER'S NOTE:  Add any queue definition arguments here (NOTE:  this may cause a conflict if queue already defined
                    //                                                                     with different args)

                    rmqToExternalChannel.QueueDeclare(queueName, true, false, false, args);

                    logger.LogInformation("Attaching message handler Worker::ToBloxGuardianMessageReceivedAsync() to To-BloxGuardian queue {0}", queueName);

                    rmqToExternalConsumer = new RabbitMQ.Client.Events.AsyncEventingBasicConsumer(rmqToExternalChannel);
                    rmqToExternalConsumer.Received += ToBloxGuardianMessageReceivedAsync;

                    // PROGRAMMER'S NOTE:  May want to add other event handlers here for shutdown, etc.

                    rmqToExternalChannel.BasicConsume(queueName, false, "", false, false, null, rmqToExternalConsumer);

                }       // (handleInGameToExternal)


                if (handleBloxGuardianToBloxChannel)
                {
                    logger.LogInformation("Opening production connection to To-BloxChannel message queue host");
                    logger.LogInformation("  -- Host Name = {0}, Port = {1}, Virtual Host = {0}",
                                          configuration["AppSettings:ToBloxChannelMessageQueueHostName"],
                                          configuration["AppSettings:ToBloxChannelMessageQueuePort"],
                                          configuration["AppSettings:ToBloxChannelMessageQueueVirtualHostName"]);
                    rmqToInGameConnectionFactory = new RabbitMQ.Client.ConnectionFactory()
                    {
                        HostName = configuration["AppSettings:ToBloxChannelMessageQueueHostName"],
                        Port = int.Parse(configuration["AppSettings:ToBloxChannelMessageQueuePort"]),
                        VirtualHost = configuration["AppSettings:ToBloxChannelMessageQueueVirtualHostName"],
                        UserName = configuration["AppSettings:ToBloxChannelMessageQueueUserName"],
                        Password = configuration["AppSettings:ToBloxChannelMessageQueueUserPassword"],
                        DispatchConsumersAsync = true
                    };

                    rmqToInGameConnectionFactory.DispatchConsumersAsync = true;
                    rmqToInGameConnection = rmqToInGameConnectionFactory.CreateConnection();
                    rmqToInGameChannel = rmqToInGameConnection.CreateModel();

                    exchangeName = configuration["AppSettings:ToBloxChannelMessageQueueExchangeName"];

                    // PROGRAMMER'S NOTE:  Best practice is to just declare definitions.  If Exchange and Queue already exist, the functions
                    //                     simply return without action.
                    rmqToInGameChannel.ExchangeDeclare(exchangeName, RabbitMQ.Client.ExchangeType.Direct, true, false, null);

                    ///////////////////////////////////////
                    /// To BloxChannel messages channel ///
                    ///////////////////////////////////////

                    queueName = string.Format(TequaCreek.BloxChannelDataModelLibrary.SharedConstantValues.MESSAGE_QUEUE_NAME_PATTERN_BLOXGUARDIAN_TO_BLOXCHANNEL,
                                              this.processingServerId);
                    logger.LogInformation("Setting up To-BloxChannel queue {0} definition (if not already exists)", queueName);

                    rmqToInGameChannel.QueueDeclare(queueName, true, false, false, null);

                    logger.LogInformation("Attaching message handler Worker::ToBloxChannelMessageReceivedAsync() to To-BloxChannel queue {0}", queueName);

                    rmqToInGameConsumer = new RabbitMQ.Client.Events.AsyncEventingBasicConsumer(rmqToInGameChannel);
                    rmqToInGameConsumer.Received += ToBloxChannelMessageReceivedAsync;

                    // PROGRAMMER'S NOTE:  May want to add other event handlers here for shutdown, etc.

                    rmqToInGameChannel.BasicConsume(queueName, false, "", false, false, null, rmqToInGameConsumer);

                }       // (handleExternalToInGame)

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
            // BLOX-25
            if (configuration["AppSettings:ForceCloseExpiredUnclosedCommThreads"] == "Y")
            {
                await ForceCloseExpiredNonClosedCommunicationThreads();
            }

            // BLOX-26
            if (configuration["AppSettings:RemoveAgedUnprocessedMessageFiles"] == "Y")
            {
                RemoveAgedUnprocessedMessageFiles();
            }

        }       // PerformTimedWorkActivities()

        private void RemoveAgedUnprocessedMessageFiles()
        {

            string toInGameEndpointMessagesBaseFileDirectory;
            string toExternalEndpointMessagesBaseFileDirectory;
            DateTime agedDateTime = System.DateTime.Now.AddDays(-1 * int.Parse(configuration["AppSettings:AgedUnprocessedMessageFilesDaysCount"]));

            logger.LogInformation("Checking In-Game Endpoint Message Storage for aged, unprocessed message files");
            toExternalEndpointMessagesBaseFileDirectory = this.configuration["AppSettings:ToExternalFileStorePrefix"];

            var files = from file in Directory.EnumerateFiles(toExternalEndpointMessagesBaseFileDirectory, "*.*",
                                                              SearchOption.AllDirectories)
                        select new
                        {
                            FileName = file,
                        };

            foreach (var file in files)
            {
                if (System.IO.File.GetCreationTime(file.FileName) < agedDateTime)
                {
                    logger.LogInformation("Deleting In-Game Endpoint message file {0}", file.FileName);
                    System.IO.File.Delete(file.FileName);
                }
            }

            logger.LogInformation("Checking External Endpoint Message Storage for aged, unprocessed message files");
            toInGameEndpointMessagesBaseFileDirectory = this.configuration["AppSettings:ToInGameFileStorePrefix"];

            files = from file in Directory.EnumerateFiles(toExternalEndpointMessagesBaseFileDirectory, "*.*",
                                                          SearchOption.AllDirectories)
                        select new
                        {
                            FileName = file,
                        };

            foreach (var file in files)
            {
                if (System.IO.File.GetCreationTime(file.FileName) < agedDateTime)
                {
                    logger.LogInformation("Deleting External Endpoint message file {0}", file.FileName);
                    System.IO.File.Delete(file.FileName);
                }
            }

        }       // RemoveAgedUnprocessedMessageFiles()

        private async Task ForceCloseExpiredNonClosedCommunicationThreads()
        {

            System.Text.StringBuilder sqlStatement;
            System.DateTime processingRunDateTime;

            NpgsqlConnection sqlConnection1;
            NpgsqlConnection sqlConnection2;
            //NpgsqlCommand sqlCommandGetExpiredUnclosedCommThreads;
            //NpgsqlDataReader sqlDataReaderGetExpiredUnclosedCommThreads;

            try
            {
                processingRunDateTime = System.DateTime.Now;

                using (sqlConnection1 = new NpgsqlConnection(configuration["ConnectionStrings:BloxChannel"]))
                {
                    await sqlConnection1.OpenAsync();

                    using (sqlConnection2 = new NpgsqlConnection(configuration["ConnectionStrings:BloxChannel"]))
                    {
                        await sqlConnection2.OpenAsync();

                        //sqlStatement = new System.Text.StringBuilder();
                        //sqlStatement.Append("SELECT CT.communication_thread_internal_id, CT.bloxchannel_communication_thread_id, CT.push_message_to_endpoint, ");
                        //sqlStatement.Append("       CT.ingame_endpoint_internal_id, IG.ingame_endpoint_bloxchannel_id, EE.external_endpoint_internal_id, ");
                        //sqlStatement.Append("       EE.external_endpoint_bloxchannel_id, EE.allow_push_to_endpoint, EE.endpoint_basic_auth_user_id, ");
                        //sqlStatement.Append("       EE.endpoint_basic_auth_password, EE.close_communication_thread_url, EE.parameter_style_code ");
                        //sqlStatement.Append("  FROM communication_thread CT ");
                        //sqlStatement.Append("       INNER JOIN external_endpoint EE ON CT.external_endpoint_internal_id = EE.external_endpoint_internal_id ");
                        //sqlStatement.Append("       INNER JOIN ingame_endpoint IG ON CT.ingame_endpoint_internal_id = IG.ingame_endpoint_internal_id ");
                        //sqlStatement.Append("  WHERE CT.expiration_date_time < @ProcessingRunDateTime and CT.closed = false ");

                        //sqlCommandGetExpiredUnclosedCommThreads = sqlConnection1.CreateCommand();
                        //sqlCommandGetExpiredUnclosedCommThreads.CommandText = sqlStatement.ToString();
                        //sqlCommandGetExpiredUnclosedCommThreads.CommandTimeout = 600;
                        //sqlCommandGetExpiredUnclosedCommThreads.Parameters.Add(new NpgsqlParameter("@ProcessingRunDateTime", NpgsqlTypes.NpgsqlDbType.Date));

                        //sqlCommandGetExpiredUnclosedCommThreads.Parameters["@ProcessingRunDateTime"].Value = DateTime.MinValue;
                        //await sqlCommandGetExpiredUnclosedCommThreads.PrepareAsync();


                        await sqlConnection2.CloseAsync();
                    }       // using (sqlConnection2 = new SqlConnection(configuration["ConnectionStrings:BloxChannel"].ToString()))

                    await sqlConnection1.CloseAsync();
                }       // using (sqlConnection2 = new NpgsqlConnection(configuration["ConnectionStrings:BloxChannel"]))

            }
            catch (Exception e)
            {

                logger.LogError("Unhandled exception occured in Worker::ForceCloseExpiredNonClosedCommunicationThreads().  Message is {0}", e.Message);
                logger.LogError(e.StackTrace);

            }

        }       // ForceCloseExpiredNonClosedCommunicationThreads()

        public async Task ToBloxChannelMessageReceivedAsync(object sender, BasicDeliverEventArgs @event)
        {

            System.Text.StringBuilder sqlStatement;

            NpgsqlConnection sqlConnection1;
            NpgsqlConnection sqlConnection2;
            //NpgsqlCommand sqlCommandGetMessageToInGameEndpoint;
            //NpgsqlDataReader sqlDataReaderGetMessageToInGameEndpoint;

            // TEMP CODE
            logger.LogDebug("Entering Worker:ToBloxChannelMessageReceivedAsync()");
            // END TEMP CODE

            try
            {

                using (sqlConnection1 = new NpgsqlConnection(configuration["ConnectionStrings:BloxChannel"]))
                {
                    await sqlConnection1.OpenAsync();

                    using (sqlConnection2 = new NpgsqlConnection(configuration["ConnectionStrings:BloxChannel"]))
                    {
                        await sqlConnection2.OpenAsync();

                        //sqlStatement = new System.Text.StringBuilder();
                        //sqlStatement.Append("SELECT CT.communication_thread_internal_id, CT.bloxchannel_communication_thread_id, CT.ingame_endpoint_internal_id, ");

                        //// RJB 2021-11-13  BLOX-24
                        //sqlStatement.Append("       CT.push_message_to_endpoint, IG.ingame_endpoint_bloxchannel_id, EE.external_endpoint_internal_id, ");
                        //sqlStatement.Append("       EE.external_endpoint_bloxchannel_id, EE.allow_push_to_endpoint, EE.endpoint_basic_auth_user_id,");
                        //sqlStatement.Append("       EE.endpoint_basic_auth_password, EE.close_communication_thread_url, EE.parameter_style_code, ");

                        //sqlStatement.Append("       MSG.message_origination_type_code, MSG.payload_type_id, MSG.payload, ");
                        //sqlStatement.Append("       MSG.message_to_ingame_endpoint_external_id, MSG.response_to_message_id ");
                        //sqlStatement.Append("  FROM message_to_ingame_endpoint MSG ");
                        //sqlStatement.Append("        INNER JOIN communication_thread CT ON MSG.communication_thread_internal_id = CT.communication_thread_internal_id ");
                        //sqlStatement.Append("        INNER JOIN external_endpoint EE ON CT.external_endpoint_internal_id = EE.external_endpoint_internal_id ");

                        //// RJB 2021-11-13  BLOX-24
                        //sqlStatement.Append("        INNER JOIN in_game_endpoint IG ON CT.ingame_endpoint_internal_id = IG.ingame_endpoint_internal_id ");

                        //sqlStatement.Append("  WHERE MSG.message_to_ingame_endpoint_internal_id = @MessageToInGameEndpointInternalID ");

                        //sqlCommandGetMessageToInGameEndpoint = sqlConnection1.CreateCommand();
                        //sqlCommandGetMessageToInGameEndpoint.CommandText = sqlStatement.ToString();
                        //sqlCommandGetMessageToInGameEndpoint.CommandTimeout = 600;
                        //sqlCommandGetMessageToInGameEndpoint.Parameters.Add(new NpgsqlParameter("@MessageToInGameEndpointInternalID", NpgsqlTypes.NpgsqlDbType.Integer));

                        //sqlCommandGetMessageToInGameEndpoint.Parameters["@MessageToInGameEndpointInternalID"].Value = 0;
                        //await sqlCommandGetMessageToInGameEndpoint.PrepareAsync();



                        await sqlConnection2.CloseAsync();
                    }       // using (sqlConnection2 = new SqlConnection(configuration["ConnectionStrings:BloxChannel"].ToString()))

                    await sqlConnection1.CloseAsync();
                }       // using (sqlConnection2 = new NpgsqlConnection(configuration["ConnectionStrings:BloxChannel"]))

            }
            catch (Exception e)
            {

                logger.LogError("Unhandled exception occured in Worker::ToBloxChannelMessageReceivedAsync().  Message is {0}", e.Message);
                logger.LogError(e.StackTrace);

            }
            finally
            {

                logger.LogInformation("Acknowledging message queue message with delivery tag {0}", @event.DeliveryTag);
                rmqToInGameChannel.BasicAck(@event.DeliveryTag, false);

            }

        }       // ToBloxChannelMessageReceivedAsync()

        public async Task ToBloxGuardianMessageReceivedAsync(object sender, BasicDeliverEventArgs @event)
        {

            System.Text.StringBuilder sqlStatement;


            NpgsqlConnection sqlConnection1;
            NpgsqlConnection sqlConnection2;
            //NpgsqlCommand sqlCommandGetMessageToExternalEndpoint;
            //NpgsqlDataReader sqlDataReaderGetMessageToExternalEndpoint;

            // TEMP CODE
            logger.LogDebug("Entering Worker:ToBloxGuardianMessageReceivedAsync()");
            // END TEMP CODE

            try
            {

                using (sqlConnection1 = new NpgsqlConnection(configuration["ConnectionStrings:BloxChannel"]))
                {
                    await sqlConnection1.OpenAsync();

                    using (sqlConnection2 = new NpgsqlConnection(configuration["ConnectionStrings:BloxChannel"]))
                    {
                        await sqlConnection2.OpenAsync();

                        //sqlStatement = new System.Text.StringBuilder();
                        //sqlStatement.Append("SELECT CT.communication_thread_internal_id, CT.bloxchannel_communication_thread_id, CT.push_message_to_endpoint, ");
                        //sqlStatement.Append("       IGE.ingame_endpoint_bloxchannel_id, ");
                        //sqlStatement.Append("       EE.external_endpoint_internal_id, EE.external_endpoint_bloxchannel_id, EE.create_communication_thread_url, ");
                        //sqlStatement.Append("       EE.post_subsequent_message_url, EE.close_communication_thread_url, EE.parameter_style_code, ");
                        //sqlStatement.Append("       EE.endpoint_basic_auth_user_id, EE.endpoint_basic_auth_password, ");
                        //sqlStatement.Append("       MSG.message_origination_type_code, MSG.payload_type_id, MSG.payload, MSG.message_to_external_endpoint_external_id ");
                        //sqlStatement.Append("  FROM message_to_external_endpoint MSG ");
                        //sqlStatement.Append("       INNER JOIN communication_thread CT ON MSG.communication_thread_internal_id = CT.communication_thread_internal_id ");
                        //sqlStatement.Append("       INNER JOIN ingame_endpoint IGE ON CT.ingame_endpoint_internal_id = IGE.ingame_endpoint_internal_id ");
                        //sqlStatement.Append("       INNER JOIN external_endpoint EE ON CT.external_endpoint_internal_id = EE.external_endpoint_internal_id ");
                        //sqlStatement.Append("  WHERE MSG.message_to_external_endpoint_internal_id = @MessageToExternalEndpointInternalID ");

                        //sqlCommandGetMessageToExternalEndpoint = sqlConnection1.CreateCommand();
                        //sqlCommandGetMessageToExternalEndpoint.CommandText = sqlStatement.ToString();
                        //sqlCommandGetMessageToExternalEndpoint.CommandTimeout = 600;
                        //sqlCommandGetMessageToExternalEndpoint.Parameters.Add(new NpgsqlParameter("@MessageToExternalEndpointInternalID", NpgsqlTypes.NpgsqlDbType.Integer));

                        //sqlCommandGetMessageToExternalEndpoint.Parameters["@MessageToExternalEndpointInternalID"].Value = 0;
                        //await sqlCommandGetMessageToExternalEndpoint.PrepareAsync();



                        await sqlConnection2.CloseAsync();
                    }       // using (sqlConnection2 = new SqlConnection(configuration["ConnectionStrings:BloxChannel"].ToString()))

                    await sqlConnection1.CloseAsync();
                }       // using (sqlConnection2 = new NpgsqlConnection(configuration["ConnectionStrings:BloxChannel"]))

            }
            catch (Exception e)
            {

                logger.LogError("Unhandled exception occured in Worker::ToBloxGuardianMessageReceivedAsync().  Message is {0}", e.Message);
                logger.LogError(e.StackTrace);

            }
            finally
            {

                logger.LogInformation("Acknowledging message queue message with delivery tag {0}", @event.DeliveryTag);
                rmqToExternalChannel.BasicAck(@event.DeliveryTag, false);

            }

        }       // ToBloxGuardianMessageReceivedAsync()


        private string PadZeroLeft(int inValue, int stringLength)
        {

            string returnValue = new string('0', stringLength) + inValue.ToString();

            returnValue = returnValue.Substring(returnValue.Length - stringLength);

            return returnValue;
        }

    }
}
